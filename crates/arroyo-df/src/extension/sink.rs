use std::sync::Arc;

use anyhow::{bail, Result};

use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use datafusion_common::{DFSchemaRef, DataFusionError, OwnedTableReference};

use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};

use prost::Message;

use crate::{
    builder::{NamedNode, Planner},
    tables::Table,
};

use super::{remote_table::RemoteTableExtension, ArroyoExtension, NodeWithIncomingEdges};

pub(crate) const SINK_NODE_NAME: &'static str = "SinkExtension";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SinkExtension {
    pub(crate) name: OwnedTableReference,
    pub(crate) table: Table,
    pub(crate) schema: DFSchemaRef,
    input: Arc<LogicalPlan>,
}

impl SinkExtension {
    pub fn new(
        name: OwnedTableReference,
        table: Table,
        schema: DFSchemaRef,
        input: Arc<LogicalPlan>,
    ) -> Self {
        let remote_input = match input.as_ref() {
            // we only segment operators at extension points,
            // so this is necessary to make sure everything between the sink and the prior extension is calculated.
            LogicalPlan::Extension(_) => input.clone(),
            _ => {
                let remote_table_extension = RemoteTableExtension {
                    input: input.as_ref().clone(),
                    name: name.clone(),
                    schema: schema.clone(),
                    materialize: false,
                };
                LogicalPlan::Extension(Extension {
                    node: Arc::new(remote_table_extension),
                })
                .into()
            }
        };

        Self {
            name,
            table,
            schema,
            input: remote_input,
        }
    }
}

impl UserDefinedLogicalNodeCore for SinkExtension {
    fn name(&self) -> &str {
        SINK_NODE_NAME
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
            "SinkExtension({:?}): {}",
            self.name,
            self.schema
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Self {
            name: self.name.clone(),
            table: self.table.clone(),
            schema: self.schema.clone(),
            input: Arc::new(inputs[0].clone()),
        }
    }
}

impl ArroyoExtension for SinkExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        _planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        if input_schemas.len() != 1 {
            bail!("sink should have exactly one input");
        }
        // should have exactly one input
        let input_schema = input_schemas[0].clone();

        let operator_config = (self.table.connector_op().map_err(|e| {
            DataFusionError::Plan(format!("failed to calculate connector op error: {}", e))
        })?)
        .encode_to_vec();
        let node = LogicalNode {
            operator_id: format!("sink_{}_{}", self.name, index),
            description: self.table.connector_op().unwrap().description.clone(),
            operator_name: OperatorName::ConnectorSink,
            parallelism: 1,
            operator_config,
        };
        let edge = LogicalEdge::project_all(LogicalEdgeType::Forward, (*input_schema).clone());
        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![edge],
        })
    }

    fn output_schema(&self) -> ArroyoSchema {
        // this is kinda fake?
        ArroyoSchema::from_schema_keys(Arc::new(self.input.schema().as_ref().into()), vec![])
            .unwrap()
    }
}
