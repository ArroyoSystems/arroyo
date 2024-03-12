use std::{fmt::Formatter, sync::Arc};

use anyhow::{bail, Result};

use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::{
    df::{ArroyoSchema, ArroyoSchemaRef},
    grpc::api::ValuePlanOperator,
};
use datafusion_common::{DFSchemaRef, OwnedTableReference};

use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use prost::Message;

use crate::{
    builder::{NamedNode, Planner},
    physical::ArroyoPhysicalExtensionCodec,
};

use super::{ArroyoExtension, NodeWithIncomingEdges};

pub(crate) const REMOTE_TABLE_NAME: &'static str = "RemoteTableExtension";

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
        if input_schemas.len() != 1 {
            bail!("RemoteTableExtension should have exactly one input");
        }
        let input_schema = input_schemas[0].clone();
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
        let edge = LogicalEdge::project_all(LogicalEdgeType::Forward, (*input_schema).clone());
        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![edge],
        })
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
