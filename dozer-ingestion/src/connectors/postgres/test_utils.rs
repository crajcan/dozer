use crate::connectors::postgres::tests::client::TestPostgresClient;
use postgres_types::PgLsn;
use std::error::Error;

use crate::connectors::postgres::replication_slot_helper::ReplicationSlotHelper;
use dozer_types::models::config::Config;
use std::str::FromStr;
use tokio_postgres::{error::DbError, Error as PostgresError, SimpleQueryMessage};

use super::connection::client::Client;

pub async fn get_client(app_config: Config) -> TestPostgresClient {
    let config = app_config
        .connections
        .get(0)
        .unwrap()
        .config
        .as_ref()
        .unwrap();

    TestPostgresClient::new(config).await
}

pub async fn create_slot(client_mut: &mut Client, slot_name: &str) -> PgLsn {
    client_mut
        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        .await
        .unwrap();

    let created_lsn = ReplicationSlotHelper::create_replication_slot(client_mut, slot_name)
        .await
        .unwrap()
        .unwrap();
    client_mut.simple_query("COMMIT;").await.unwrap();

    PgLsn::from_str(&created_lsn).unwrap()
}

pub async fn retry_drop_active_slot(
    e: PostgresError,
    client_mut: &mut Client,
    slot_name: &str,
) -> Result<Vec<SimpleQueryMessage>, PostgresError> {
    match e.source() {
        None => Err(e),
        Some(err) => match err.downcast_ref::<DbError>() {
            Some(db_error) if db_error.code().code().eq("55006") => {
                let err = db_error.to_string();
                let parts = err.rsplit_once(' ').unwrap();

                client_mut
                    .simple_query(format!("select pg_terminate_backend('{}');", parts.1).as_ref())
                    .await
                    .unwrap();

                ReplicationSlotHelper::drop_replication_slot(client_mut, slot_name).await
            }
            _ => Err(e),
        },
    }
}
