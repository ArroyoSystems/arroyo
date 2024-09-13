use std::sync::Arc;

use arrow_schema::{DataType, Schema};
use arroyo_rpc::{
    df::{ArroyoSchema, ArroyoSchemaRef},
    IS_RETRACT_FIELD, TIMESTAMP_FIELD,
};
use datafusion::common::{internal_err, plan_err, DFSchema, DFSchemaRef, Result, TableReference};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::DisplayAs;

use super::{ArroyoExtension, NodeWithIncomingEdges};
use crate::builder::{NamedNode, Planner};

pub(crate) const DEBEZIUM_UNROLLING_EXTENSION_NAME: &str = "DebeziumUnrollingExtension";
pub(crate) const TO_DEBEZIUM_EXTENSION_NAME: &str = "ToDebeziumExtension";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DebeziumUnrollingExtension {
    input: LogicalPlan,
    schema: DFSchemaRef,
}

impl DebeziumUnrollingExtension {
    pub(crate) fn as_debezium_schema(
        input_schema: &DFSchemaRef,
        qualifier: Option<TableReference>,
    ) -> Result<DFSchemaRef> {
        let timestamp_field = if input_schema.has_column_with_unqualified_name(TIMESTAMP_FIELD) {
            Some(
                input_schema
                    .field_with_unqualified_name(TIMESTAMP_FIELD)?
                    .clone(),
            )
        } else {
            None
        };
        let struct_schema: Vec<_> = input_schema
            .fields()
            .iter()
            .filter(|field| field.name() != TIMESTAMP_FIELD && field.name() != IS_RETRACT_FIELD)
            .cloned()
            .collect();

        let struct_type = DataType::Struct(struct_schema.into());

        let before = Arc::new(arrow::datatypes::Field::new(
            "before",
            struct_type.clone(),
            true,
        ));
        let after = Arc::new(arrow::datatypes::Field::new(
            "after",
            struct_type.clone(),
            true,
        ));

        let op = Arc::new(arrow::datatypes::Field::new("op", DataType::Utf8, true));
        let mut fields = vec![before, after, op];

        if let Some(timestamp_field) = timestamp_field {
            fields.push(Arc::new(timestamp_field));
        }

        let schema = match qualifier {
            Some(qualifier) => {
                DFSchema::try_from_qualified_schema(qualifier, &Schema::new(fields))?
            }
            None => DFSchema::try_from(Schema::new(fields))?,
        };
        Ok(Arc::new(schema))
    }

    pub fn try_new(input: LogicalPlan) -> Result<Self> {
        let input_schema = input.schema();
        // confirm that the input schema has before, after and op columns, and before and after match
        let Some(before_index) = input_schema.index_of_column_by_name(None, "before") else {
            return plan_err!("DebeziumUnrollingExtension requires a before column");
        };
        let Some(after_index) = input_schema.index_of_column_by_name(None, "after") else {
            return plan_err!("DebeziumUnrollingExtension requires an after column");
        };
        let Some(op_index) = input_schema.index_of_column_by_name(None, "op") else {
            return plan_err!("DebeziumUnrollingExtension requires an op column");
        };
        let before_type = input_schema.field(before_index).data_type();
        let after_type = input_schema.field(after_index).data_type();
        if before_type != after_type {
            return plan_err!(
                "before and after columns must have the same type, not {} and {}",
                before_type,
                after_type
            );
        }
        // check that op is a string
        let op_type = input_schema.field(op_index).data_type();
        if *op_type != DataType::Utf8 {
            return plan_err!("op column must be a string, not {}", op_type);
        }
        // create the output schema
        let DataType::Struct(fields) = before_type else {
            return plan_err!(
                "before and after columns must be structs, not {}",
                before_type
            );
        };
        // determine the qualifier from the before and after columns
        let qualifier = match (
            input_schema.qualified_field(before_index).0,
            input_schema.qualified_field(after_index).0,
        ) {
            (Some(before_qualifier), Some(after_qualifier)) => {
                if before_qualifier != after_qualifier {
                    return plan_err!("before and after columns must have the same alias");
                }
                Some(before_qualifier.clone())
            }
            (None, None) => None,
            _ => return plan_err!("before and after columns must both have an alias or neither"),
        };
        let mut fields = fields.to_vec();
        fields.push(Arc::new(arrow::datatypes::Field::new(
            IS_RETRACT_FIELD,
            DataType::Boolean,
            false,
        )));

        let Some(input_timestamp_field) =
            input_schema.index_of_column_by_name(None, TIMESTAMP_FIELD)
        else {
            return plan_err!("DebeziumUnrollingExtension requires a timestamp field");
        };
        fields.push(Arc::new(input_schema.field(input_timestamp_field).clone()));
        let arrow_schema = Schema::new(fields);

        let schema = match qualifier {
            Some(qualifier) => DFSchema::try_from_qualified_schema(qualifier, &arrow_schema)?,
            None => DFSchema::try_from(arrow_schema)?,
        };
        Ok(Self {
            input,
            schema: Arc::new(schema),
        })
    }
}

impl UserDefinedLogicalNodeCore for DebeziumUnrollingExtension {
    fn name(&self) -> &str {
        DEBEZIUM_UNROLLING_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DebeziumUnrollingExtension")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Self::try_new(inputs[0].clone())
    }
}

impl ArroyoExtension for DebeziumUnrollingExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        _planner: &Planner,
        _index: usize,
        _input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        plan_err!("DebeziumUnrollingExtension should not be planned")
    }

    fn output_schema(&self) -> ArroyoSchema {
        ArroyoSchema::from_schema_unkeyed(Arc::new(self.schema.as_ref().into())).unwrap()
    }

    fn transparent(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ToDebeziumExtension {
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
}

impl ToDebeziumExtension {
    pub(crate) fn try_new(input: LogicalPlan) -> Result<Self> {
        let input_schema = input.schema();
        let schema = DebeziumUnrollingExtension::as_debezium_schema(input_schema, None)
            .expect("should be able to create ToDebeziumExtenison");
        Ok(Self {
            input: Arc::new(input),
            schema,
        })
    }
}

impl DisplayAs for ToDebeziumExtension {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ToDebeziumExtension")
    }
}

impl UserDefinedLogicalNodeCore for ToDebeziumExtension {
    fn name(&self) -> &str {
        TO_DEBEZIUM_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ToDebeziumExtension")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Self::try_new(inputs[0].clone())
    }
}

impl ArroyoExtension for ToDebeziumExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        _planner: &Planner,
        _index: usize,
        _input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        internal_err!("ToDebeziumExtension should not be planned")
    }

    fn output_schema(&self) -> ArroyoSchema {
        ArroyoSchema::from_schema_unkeyed(Arc::new(self.schema.as_ref().into())).unwrap()
    }

    fn transparent(&self) -> bool {
        true
    }
}
