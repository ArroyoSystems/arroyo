use std::{fmt::Formatter, sync::Arc};

use anyhow::{bail, Result};

use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::{
    df::{ArroyoSchema, ArroyoSchemaRef},
    grpc::api::ValuePlanOperator,
};
use datafusion::common::{DFSchemaRef, OwnedTableReference};

use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use prost::Message;

use crate::{
    builder::{NamedNode, Planner},
    physical::ArroyoPhysicalExtensionCodec,
};

use super::{ArroyoExtension, NodeWithIncomingEdges};

pub(crate) const REMOTE_TABLE_NAME: &str = "RemoteTableExtension";

/* Lightweight extension that allows us to segment the graph and merge nodes with the same name.
  An Extension Planner will be used to isolate computation to individual nodes.
*/
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RemoteTableExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) name: OwnedTableReference,
    pub(crate) schema: DFSchemaRef,
    pub(crate) materialize: bool,
}

impl ArroyoExtension for RemoteTableExtension {
    fn node_name(&self) -> Option<NamedNode> {
        if self.materialize {
            Some(NamedNode::RemoteTable(self.name.to_owned()))
        } else {
            None
        }
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        match input_schemas.len() {
            0 => bail!("RemoteTableExtension should have exactly one input"),
            1 => {}
            _multiple_inputs => {
                // check they are all the same
                let first = input_schemas[0].clone();
                for schema in input_schemas.iter().skip(1) {
                    if *schema != first {
                        bail!("If a node has multiple inputs, they must all have the same schema");
                    }
                }
            }
        }
        let physical_plan = planner.sync_plan(&self.input)?;
        let physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            physical_plan,
            &ArroyoPhysicalExtensionCodec::default(),
        )?;
        let config = ValuePlanOperator {
            name: format!("value_calculation({})", self.name),
            physical_plan: physical_plan_node.encode_to_vec(),
        };
        let node = LogicalNode {
            operator_id: format!("value_{}", index),
            description: self.name.to_string(),
            operator_name: OperatorName::ArrowValue,
            parallelism: 1,
            operator_config: config.encode_to_vec(),
        };
        let edges = input_schemas
            .into_iter()
            .map(|schema| LogicalEdge::project_all(LogicalEdgeType::Forward, (*schema).clone()))
            .collect();
        Ok(NodeWithIncomingEdges { node, edges })
    }

    fn output_schema(&self) -> ArroyoSchema {
        ArroyoSchema::from_schema_keys(Arc::new(self.schema.as_ref().into()), vec![]).unwrap()
    }
}

impl UserDefinedLogicalNodeCore for RemoteTableExtension {
    fn name(&self) -> &str {
        REMOTE_TABLE_NAME
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

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RemoteTableExtension: {}",
            self.schema
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self {
            input: inputs[0].clone(),
            name: self.name.clone(),
            schema: self.schema.clone(),
            materialize: self.materialize,
        }
    }
}
