use std::sync::Arc;

use anyhow::Result;
use arroyo_datastream::logical::{LogicalEdge, LogicalNode};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use datafusion_common::{DFSchemaRef, DataFusionError, OwnedTableReference, Result as DFResult};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore};
use watermark_node::WatermarkNode;

use crate::builder::{NamedNode, Planner};
use crate::schemas::{add_timestamp_field, has_timestamp_field};
use join::JoinExtension;

use self::debezium::{
    DebeziumUnrollingExtension, ToDebeziumExtension, DEBEZIUM_UNROLLING_EXTENSION_NAME,
    TO_DEBEZIUM_EXTENSION_NAME,
};
use self::{
    aggregate::{AggregateExtension, AGGREGATE_EXTENSION_NAME},
    join::JOIN_NODE_NAME,
    key_calculation::{KeyCalculationExtension, KEY_CALCULATION_NAME},
    remote_table::{RemoteTableExtension, REMOTE_TABLE_NAME},
    sink::{SinkExtension, SINK_NODE_NAME},
    table_source::{TableSourceExtension, TABLE_SOURCE_NAME},
    watermark_node::WATERMARK_NODE_NAME,
    window_fn::{WindowFunctionExtension, WINDOW_FUNCTION_EXTENSION_NAME},
};

pub(crate) mod aggregate;
pub(crate) mod debezium;
pub(crate) mod join;
pub(crate) mod key_calculation;
pub(crate) mod remote_table;
pub(crate) mod sink;
pub(crate) mod table_source;
pub(crate) mod watermark_node;
pub(crate) mod window_fn;
pub(crate) trait ArroyoExtension {
    // if the extension has a name, return it so that we can memoize.
    fn node_name(&self) -> Option<NamedNode>;
    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges>;
    fn output_schema(&self) -> ArroyoSchema;
    fn transparent(&self) -> bool {
        false
    }
}

pub(crate) struct NodeWithIncomingEdges {
    pub node: LogicalNode,
    pub edges: Vec<LogicalEdge>,
}

impl<'a> TryFrom<&'a dyn UserDefinedLogicalNode> for &'a dyn ArroyoExtension {
    type Error = DataFusionError;

    fn try_from(node: &'a dyn UserDefinedLogicalNode) -> DFResult<Self, Self::Error> {
        match node.name() {
            TABLE_SOURCE_NAME => {
                let table_source_extension = node
                    .as_any()
                    .downcast_ref::<TableSourceExtension>()
                    .unwrap();
                Ok(table_source_extension as &dyn ArroyoExtension)
            }
            WATERMARK_NODE_NAME => {
                let watermark_node = node.as_any().downcast_ref::<WatermarkNode>().unwrap();
                Ok(watermark_node as &dyn ArroyoExtension)
            }
            SINK_NODE_NAME => {
                let sink_extension = node.as_any().downcast_ref::<SinkExtension>().unwrap();
                Ok(sink_extension as &dyn ArroyoExtension)
            }
            KEY_CALCULATION_NAME => {
                let key_calculation_extension = node
                    .as_any()
                    .downcast_ref::<KeyCalculationExtension>()
                    .unwrap();
                Ok(key_calculation_extension as &dyn ArroyoExtension)
            }
            AGGREGATE_EXTENSION_NAME => {
                let aggregate_extension =
                    node.as_any().downcast_ref::<AggregateExtension>().unwrap();
                Ok(aggregate_extension as &dyn ArroyoExtension)
            }
            REMOTE_TABLE_NAME => {
                let remote_table_extension = node
                    .as_any()
                    .downcast_ref::<RemoteTableExtension>()
                    .unwrap();
                Ok(remote_table_extension as &dyn ArroyoExtension)
            }
            JOIN_NODE_NAME => {
                let join_extension = node.as_any().downcast_ref::<JoinExtension>().unwrap();
                Ok(join_extension as &dyn ArroyoExtension)
            }
            WINDOW_FUNCTION_EXTENSION_NAME => {
                let window_function_extension = node
                    .as_any()
                    .downcast_ref::<WindowFunctionExtension>()
                    .unwrap();
                Ok(window_function_extension as &dyn ArroyoExtension)
            }
            DEBEZIUM_UNROLLING_EXTENSION_NAME => {
                let debezium_unrolling_extension = node
                    .as_any()
                    .downcast_ref::<DebeziumUnrollingExtension>()
                    .unwrap();
                Ok(debezium_unrolling_extension as &dyn ArroyoExtension)
            }
            TO_DEBEZIUM_EXTENSION_NAME => {
                let to_debezium_extension =
                    node.as_any().downcast_ref::<ToDebeziumExtension>().unwrap();
                Ok(to_debezium_extension as &dyn ArroyoExtension)
            }
            other => Err(DataFusionError::Plan(format!("unexpected node: {}", other))),
        }
    }
}

impl<'a> TryFrom<&'a Arc<dyn UserDefinedLogicalNode>> for &'a dyn ArroyoExtension {
    type Error = DataFusionError;

    fn try_from(node: &'a Arc<dyn UserDefinedLogicalNode>) -> DFResult<Self, Self::Error> {
        TryFrom::try_from(&**node)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TimestampAppendExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) qualifier: Option<OwnedTableReference>,
    pub(crate) schema: DFSchemaRef,
}

impl TimestampAppendExtension {
    fn new(input: LogicalPlan, qualifier: Option<OwnedTableReference>) -> Self {
        if has_timestamp_field(input.schema().clone()) {
            unreachable!("shouldn't be adding timestamp to a plan that already has it: plan :\n {:?}\n schema: {:?}", input, input.schema());
        }
        let schema = add_timestamp_field(input.schema().clone(), qualifier.clone()).unwrap();
        Self {
            input,
            qualifier,
            schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for TimestampAppendExtension {
    fn name(&self) -> &str {
        "TimestampAppendExtension"
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
        write!(
            f,
            "TimestampAppendExtension({:?}): {}",
            self.qualifier,
            self.schema
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self::new(inputs[0].clone(), self.qualifier.clone())
    }
}
