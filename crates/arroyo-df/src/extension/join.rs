use crate::builder::{NamedNode, Planner};
use crate::extension::{ArroyoExtension, NodeWithIncomingEdges};
use crate::physical::ArroyoPhysicalExtensionCodec;
use anyhow::bail;
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_rpc::grpc::api::JoinOperator;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::expr::Expr;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::generated::datafusion::PhysicalPlanNode;
use datafusion_proto::physical_plan::AsExecutionPlan;
use prost::Message;
use std::sync::Arc;

pub(crate) const JOIN_NODE_NAME: &str = "JoinNode";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinExtension {
    pub(crate) rewritten_join: LogicalPlan,
    pub(crate) is_instant: bool,
}

impl ArroyoExtension for JoinExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> anyhow::Result<NodeWithIncomingEdges> {
        if input_schemas.len() != 2 {
            bail!("join should have exactly two inputs");
        }
        let left_schema = input_schemas[0].clone();
        let right_schema = input_schemas[1].clone();

        let join_plan = planner.sync_plan(&self.rewritten_join)?;
        let physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            join_plan.clone(),
            &ArroyoPhysicalExtensionCodec::default(),
        )?;
        let operator_name = if self.is_instant {
            OperatorName::InstantJoin
        } else {
            OperatorName::Join
        };
        let config = JoinOperator {
            name: format!("join_{}", index),
            left_schema: Some(left_schema.as_ref().clone().try_into()?),
            right_schema: Some(right_schema.as_ref().clone().try_into()?),
            output_schema: Some(self.output_schema().try_into()?),
            join_plan: physical_plan_node.encode_to_vec(),
        };
        let logical_node = LogicalNode {
            operator_id: format!("join_{}", index),
            description: "join".to_string(),
            operator_name,
            operator_config: config.encode_to_vec(),
            parallelism: 1,
        };
        let left_edge =
            LogicalEdge::project_all(LogicalEdgeType::LeftJoin, left_schema.as_ref().clone());
        let right_edge =
            LogicalEdge::project_all(LogicalEdgeType::RightJoin, right_schema.as_ref().clone());
        Ok(NodeWithIncomingEdges {
            node: logical_node,
            edges: vec![left_edge, right_edge],
        })
    }

    fn output_schema(&self) -> ArroyoSchema {
        ArroyoSchema::from_schema_unkeyed(Arc::new(self.schema().as_ref().clone().into())).unwrap()
    }
}

impl UserDefinedLogicalNodeCore for JoinExtension {
    fn name(&self) -> &str {
        JOIN_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.rewritten_join]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.rewritten_join.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "JoinExtension: {}",
            self.schema()
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self {
            rewritten_join: inputs[0].clone(),
            is_instant: self.is_instant,
        }
    }
}
