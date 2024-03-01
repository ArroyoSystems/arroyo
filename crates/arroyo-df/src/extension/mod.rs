use std::sync::Arc;

use anyhow::Result;
use arroyo_datastream::logical::{LogicalEdge, LogicalNode};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_expr::UserDefinedLogicalNode;
use watermark_node::WatermarkNode;

use crate::builder::{NamedNode, Planner};
use join::JoinExtension;

use self::{
    aggregate::{AggregateExtension, AGGREGATE_EXTENSION_NAME},
    join::JOIN_NODE_NAME,
    key_calculation::{KeyCalculationExtension, KEY_CALCULATION_NAME},
    remote_table::{RemoteTableExtension, REMOTE_TABLE_NAME},
    sink::{SinkExtension, SINK_NODE_NAME},
    table_source::{TableSourceExtension, TABLE_SOURCE_NAME},
    watermark_node::WATERMARK_NODE_NAME,
};

pub(crate) mod aggregate;
pub(crate) mod join;
pub(crate) mod key_calculation;
pub(crate) mod remote_table;
pub(crate) mod sink;
pub(crate) mod table_source;
pub(crate) mod watermark_node;
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
}

pub(crate) struct NodeWithIncomingEdges {
    pub node: LogicalNode,
    pub edges: Vec<LogicalEdge>,
}

impl<'a> TryFrom<&'a Arc<dyn UserDefinedLogicalNode>> for &'a dyn ArroyoExtension {
    type Error = DataFusionError;

    fn try_from(node: &'a Arc<dyn UserDefinedLogicalNode>) -> DFResult<Self, Self::Error> {
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
            other => Err(DataFusionError::Plan(format!("unexpected node: {}", other))),
        }
    }
}
