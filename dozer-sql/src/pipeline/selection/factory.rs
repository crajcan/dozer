use std::collections::HashMap;

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::builder::ExpressionBuilder;
use dozer_core::processor_record::ProcessorRecordStore;
use dozer_core::{
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::models::udf_config::UdfConfig;
use dozer_types::{errors::internal::BoxedError, types::Schema};
use sqlparser::ast::Expr as SqlExpr;

use super::processor::SelectionProcessor;

#[derive(Debug)]
pub struct SelectionProcessorFactory {
    statement: SqlExpr,
    id: String,
    udfs: Vec<UdfConfig>,
}

impl SelectionProcessorFactory {
    /// Creates a new [`SelectionProcessorFactory`].
    pub fn new(id: String, statement: SqlExpr, udf_config: Vec<UdfConfig>) -> Self {
        Self {
            statement,
            id,
            udfs: udf_config,
        }
    }
}

impl ProcessorFactory for SelectionProcessorFactory {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn type_name(&self) -> String {
        "Selection".to_string()
    }
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }

    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;
        Ok(schema.clone())
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _record_store: &ProcessorRecordStore,
        checkpoint_data: Option<Vec<u8>>,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        match ExpressionBuilder::new(schema.fields.len()).build(
            false,
            &self.statement,
            schema,
            &self.udfs,
        ) {
            Ok(expression) => Ok(Box::new(SelectionProcessor::new(
                schema.clone(),
                expression,
                checkpoint_data,
            ))),
            Err(e) => Err(e.into()),
        }
    }
}
