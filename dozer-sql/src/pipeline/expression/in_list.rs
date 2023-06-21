use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{
    Expression, ExpressionExecutor, ExpressionType,
};
use dozer_types::types::{Field, FieldType, Record, Schema};

pub(crate) fn get_in_list_operator_type(
    arg: &Expression,
    list: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    let return_type = arg.get_type(schema)?.return_type;
    for val in list {
        let val_type = val.get_type(schema)?.return_type;
        if val_type != return_type {
            return Err(PipelineError::InvalidExpression(format!(
                "Expected list member to have type {return_type:?} but found {val_type:?} \
                 Expected because left side of IN expression has type {return_type:?}"
            )));
        }
    }

    Ok(ExpressionType::new(
        FieldType::Boolean,
        false,
        dozer_types::types::SourceDefinition::Dynamic,
        false,
    ))
}

pub(crate) fn evaluate_in_list(
    schema: &Schema,
    arg: &Expression,
    list: &[Expression],
    record: &Record,
) -> Result<Field, PipelineError> {
    let arg_field = arg.evaluate(record, schema)?;
    let arg_type = arg.get_type(schema)?.return_type;

    for val in list {
        let val_field = val.evaluate(record, schema)?;
        let val_type = val.get_type(schema)?.return_type;
        if val_type != arg_type {
            return Err(PipelineError::InvalidExpression(format!(
                "Expected list member to have type {arg_type:?} but found {val_type:?} \
                 Expected because left side of IN expression has type {arg_type:?}"
            )));
        }
        if arg_field == val_field {
            return Ok(Field::Boolean(true));
        }
    }
    Ok(Field::Boolean(false))
}
