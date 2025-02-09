use std::{num::NonZeroUsize, sync::Arc};

use dozer_log::{
    camino::{Utf8Path, Utf8PathBuf},
    dyn_clone,
    replication::create_data_storage,
    storage::{self, Object, Queue, Storage},
    tokio::{sync::mpsc::error::SendError, task::JoinHandle},
};
use dozer_types::{
    bincode,
    log::{error, info},
    models::app_config::DataStorage,
    node::{NodeHandle, OpIdentifier, SourceStates},
    parking_lot::Mutex,
    thiserror::{self, Error},
    types::Field,
};
use tempdir::TempDir;

use crate::{errors::ExecutionError, processor_record::ProcessorRecordStore};

#[derive(Debug)]
pub struct CheckpointFactory {
    queue: Queue,
    storage: Box<dyn Storage>, // only used in test now
    prefix: String,
    record_store: Arc<ProcessorRecordStore>,
    state: Mutex<CheckpointWriterFactoryState>,
}

#[derive(Debug, Clone)]
pub struct CheckpointFactoryOptions {
    pub storage_config: DataStorage,
    pub persist_queue_capacity: usize,
}

impl Default for CheckpointFactoryOptions {
    fn default() -> Self {
        Self {
            storage_config: DataStorage::Local(()),
            persist_queue_capacity: 100,
        }
    }
}

#[derive(Debug, Error)]
pub enum ReadCheckpointError {
    #[error("not enough data, expected {expected}, remaining {remaining}")]
    NotEnoughData { expected: usize, remaining: usize },
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

#[derive(Debug, Clone)]
struct Checkpoint {
    /// The number of slices that the record store is split into.
    num_slices: NonZeroUsize,
    processor_prefix: String,
    epoch_id: u64,
    source_states: SourceStates,
}

#[derive(Debug, Clone, Default)]
pub struct OptionCheckpoint {
    checkpoint: Option<Checkpoint>,
}

impl OptionCheckpoint {
    pub fn num_slices(&self) -> usize {
        self.checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.num_slices.get())
    }

    pub fn next_epoch_id(&self) -> u64 {
        self.checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.epoch_id + 1)
    }

    pub fn get_source_state(&self, node_handle: &NodeHandle) -> Option<OpIdentifier> {
        self.checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.source_states.get(node_handle))
            .copied()
    }

    pub async fn load_processor_data(
        &self,
        factory: &CheckpointFactory,
        node_handle: &NodeHandle,
    ) -> Result<Option<Vec<u8>>, storage::Error> {
        if let Some(checkpoint) = &self.checkpoint {
            let key = processor_key(&checkpoint.processor_prefix, node_handle);
            info!("Restoring processor {node_handle} from {key}");
            factory.storage.download_object(key).await.map(Some)
        } else {
            Ok(None)
        }
    }
}

impl CheckpointFactory {
    // We need tokio runtime so mark the function as async.
    pub async fn new(
        checkpoint_dir: String,
        options: CheckpointFactoryOptions,
    ) -> Result<(Self, OptionCheckpoint, JoinHandle<()>), ExecutionError> {
        let (storage, prefix) =
            create_data_storage(options.storage_config, checkpoint_dir.to_string()).await?;
        let (record_store, checkpoint) = read_record_store_slices(&*storage, &prefix).await?;
        if let Some(checkpoint) = &checkpoint.checkpoint {
            info!(
                "Restored record store from {}th checkpoint, last epoch id is {}, processor states are stored in {}",
                checkpoint.num_slices, checkpoint.epoch_id, checkpoint.processor_prefix
            );
        }

        let (queue, worker) = Queue::new(
            dyn_clone::clone_box(&*storage),
            options.persist_queue_capacity,
        );

        let state = Mutex::new(CheckpointWriterFactoryState {
            next_record_index: record_store.num_records(),
        });

        Ok((
            Self {
                queue,
                storage,
                prefix,
                record_store: Arc::new(record_store),
                state,
            },
            checkpoint,
            worker,
        ))
    }

    pub fn storage(&self) -> &dyn Storage {
        &*self.storage
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn record_store(&self) -> &Arc<ProcessorRecordStore> {
        &self.record_store
    }

    fn write_record_store_slice(
        &self,
        key: String,
        source_states: &SourceStates,
    ) -> Result<(), ExecutionError> {
        let mut state = self.state.lock();
        let (data, num_records_serialized) =
            self.record_store.serialize_slice(state.next_record_index)?;
        state.next_record_index += num_records_serialized;
        drop(state);

        self.write_record_store_slice_data(key, source_states, data)
            .map_err(|_| ExecutionError::CheckpointWriterThreadPanicked)
    }

    fn write_record_store_slice_data(
        &self,
        key: String,
        source_states: &SourceStates,
        data: Vec<u8>,
    ) -> Result<(), SendError<String>> {
        let source_states =
            bincode::serialize(source_states).expect("Source states should be serializable");

        self.queue.create_upload(key.clone())?;
        self.queue.upload_chunk(
            key.clone(),
            (source_states.len() as u64).to_le_bytes().to_vec(),
        )?;
        self.queue.upload_chunk(key.clone(), source_states)?;
        self.queue.upload_chunk(key.clone(), data)?;
        self.queue.complete_upload(key)?;
        Ok(())
    }

    fn read_record_store_slice_data(
        mut data: &[u8],
    ) -> Result<(SourceStates, &[u8]), ReadCheckpointError> {
        if data.len() < 8 {
            return Err(ReadCheckpointError::NotEnoughData {
                expected: 8,
                remaining: data.len(),
            });
        }
        let source_states_len = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
        data = &data[8..];

        if data.len() < source_states_len {
            return Err(ReadCheckpointError::NotEnoughData {
                expected: source_states_len,
                remaining: data.len(),
            });
        }
        let source_states = bincode::deserialize(&data[..source_states_len])?;
        data = &data[source_states_len..];

        Ok((source_states, data))
    }
}

#[derive(Debug)]
struct CheckpointWriterFactoryState {
    next_record_index: usize,
}

#[derive(Debug)]
pub struct CheckpointWriter {
    factory: Arc<CheckpointFactory>,
    record_store_key: String,
    source_states: Arc<SourceStates>,
    processor_prefix: String,
}

fn record_store_prefix(factory_prefix: &str) -> Utf8PathBuf {
    AsRef::<Utf8Path>::as_ref(factory_prefix).join("record_store")
}

fn processor_prefix(factory_prefix: &str, epoch_id: &str) -> String {
    AsRef::<Utf8Path>::as_ref(factory_prefix)
        .join(epoch_id)
        .into_string()
}

fn processor_key(processor_prefix: &str, node_handle: &NodeHandle) -> String {
    AsRef::<Utf8Path>::as_ref(processor_prefix)
        .join(node_handle.to_string())
        .into_string()
}

impl CheckpointWriter {
    pub fn new(
        factory: Arc<CheckpointFactory>,
        epoch_id: u64,
        source_states: Arc<SourceStates>,
    ) -> Self {
        // Format with `u64` max number of digits.
        let epoch_id = format!("{:020}", epoch_id);
        let record_store_key = record_store_prefix(&factory.prefix)
            .join(&epoch_id)
            .into_string();
        let processor_prefix = processor_prefix(&factory.prefix, &epoch_id);
        Self {
            factory,
            record_store_key,
            source_states,
            processor_prefix,
        }
    }

    pub fn queue(&self) -> &Queue {
        &self.factory.queue
    }

    pub fn create_processor_object(
        &self,
        node_handle: &NodeHandle,
    ) -> Result<Object, ExecutionError> {
        let key = processor_key(&self.processor_prefix, node_handle);
        Object::new(self.factory.queue.clone(), key)
            .map_err(|_| ExecutionError::CheckpointWriterThreadPanicked)
    }

    fn drop(&mut self) -> Result<(), ExecutionError> {
        self.factory.write_record_store_slice(
            std::mem::take(&mut self.record_store_key),
            &self.source_states,
        )
    }
}

impl Drop for CheckpointWriter {
    fn drop(&mut self) {
        if let Err(e) = self.drop() {
            error!("Failed to write record store slice: {:?}", e);
        }
    }
}

async fn read_record_store_slices(
    storage: &dyn Storage,
    factory_prefix: &str,
) -> Result<(ProcessorRecordStore, OptionCheckpoint), ExecutionError> {
    let record_store = ProcessorRecordStore::new()?;
    let record_store_prefix = record_store_prefix(factory_prefix);

    let mut last_checkpoint: Option<Checkpoint> = None;
    let mut continuation_token = None;
    loop {
        let objects = storage
            .list_objects(record_store_prefix.to_string(), continuation_token)
            .await?;

        if let Some(object) = objects.objects.last() {
            let object_name = AsRef::<Utf8Path>::as_ref(&object.key)
                .strip_prefix(&record_store_prefix)
                .map_err(|_| ExecutionError::UnrecognizedCheckpoint(object.key.clone()))?;
            let epoch_id = object_name
                .as_str()
                .parse()
                .map_err(|_| ExecutionError::UnrecognizedCheckpoint(object.key.clone()))?;
            info!("Downloading {}", object.key);
            let data = storage.download_object(object.key.clone()).await?;
            let (source_states, _) = CheckpointFactory::read_record_store_slice_data(&data)?;
            let processor_prefix = processor_prefix(factory_prefix, object_name.as_str());

            if let Some(last_checkpoint) = last_checkpoint.as_mut() {
                last_checkpoint.num_slices = last_checkpoint
                    .num_slices
                    .checked_add(objects.objects.len())
                    .expect("shouldn't overflow");
                last_checkpoint.epoch_id = epoch_id;
                last_checkpoint.source_states = source_states;
                last_checkpoint.processor_prefix = processor_prefix;
            } else {
                info!("Current source states are {source_states:?}");
                last_checkpoint = Some(Checkpoint {
                    num_slices: NonZeroUsize::new(objects.objects.len())
                        .expect("have at least one element"),
                    epoch_id,
                    source_states,
                    processor_prefix,
                });
            }
        }

        for object in objects.objects {
            info!("Downloading {}", object.key);
            let data = storage.download_object(object.key).await?;
            let (_, data) = CheckpointFactory::read_record_store_slice_data(&data)?;
            record_store.deserialize_and_extend(data)?;
        }

        continuation_token = objects.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    Ok((
        record_store,
        OptionCheckpoint {
            checkpoint: last_checkpoint,
        },
    ))
}

/// This is only meant to be used in tests.
pub async fn create_checkpoint_factory_for_test(
    records: &[Vec<Field>],
) -> (TempDir, Arc<CheckpointFactory>, JoinHandle<()>) {
    // Create empty checkpoint storage.
    let temp_dir = TempDir::new("create_checkpoint_factory_for_test").unwrap();
    let checkpoint_dir = temp_dir.path().to_str().unwrap().to_string();
    let (checkpoint_factory, _, handle) =
        CheckpointFactory::new(checkpoint_dir.clone(), Default::default())
            .await
            .unwrap();
    let factory = Arc::new(checkpoint_factory);

    // Write data to checkpoint.
    for record in records {
        factory.record_store().create_ref(record).unwrap();
    }
    // Writer must be dropped outside tokio context.
    let epoch_id = 42;
    let source_states: SourceStates = [(
        NodeHandle::new(Some(1), "id".to_string()),
        OpIdentifier::new(1, 1),
    )]
    .into_iter()
    .collect();
    let source_states_clone = Arc::new(source_states.clone());
    std::thread::spawn(move || {
        drop(CheckpointWriter::new(
            factory,
            epoch_id,
            source_states_clone,
        ))
    })
    .join()
    .unwrap();
    handle.await.unwrap();

    // Create a new factory that loads from the checkpoint.
    let (factory, last_checkpoint, handle) =
        CheckpointFactory::new(checkpoint_dir, Default::default())
            .await
            .unwrap();
    let last_checkpoint = last_checkpoint.checkpoint.unwrap();
    assert_eq!(last_checkpoint.num_slices.get(), 1);
    assert_eq!(last_checkpoint.epoch_id, epoch_id);
    assert_eq!(last_checkpoint.source_states, source_states);
    assert_eq!(factory.record_store().num_records(), records.len());

    (temp_dir, Arc::new(factory), handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    use dozer_log::tokio;

    #[tokio::test]
    async fn checkpoint_writer_should_write_records() {
        create_checkpoint_factory_for_test(&[vec![Field::Int(0)]]).await;
    }
}
