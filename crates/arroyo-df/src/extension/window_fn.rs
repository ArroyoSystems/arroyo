use std::sync::Arc;

use arroyo_datastream::logical::{LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::{df::ArroyoSchema, grpc::api::WindowFunctionOperator, TIMESTAMP_FIELD};
use datafusion::common::{Column, DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use prost::Message;

use crate::physical::ArroyoPhysicalExtensionCodec;

use super::{ArroyoExtension, NodeWithIncomingEdges};

pub(crate) const WINDOW_FUNCTION_EXTENSION_NAME: &str = "WindowFunctionExtension";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct WindowFunctionExtension {
    window_plan: LogicalPlan,
    key_fields: Vec<usize>,
}

impl WindowFunctionExtension {
    pub fn new(window_plan: LogicalPlan, key_fields: Vec<usize>) -> Self {
        Self {
            window_plan,
            key_fields,
        }
    }
}

impl UserDefinedLogicalNodeCore for WindowFunctionExtension {
    fn name(&self) -> &str {
        WINDOW_FUNCTION_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.window_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.window_plan.schema()
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "WindowFunction: {}",
            self.schema()
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[datafusion::prelude::Expr], inputs: &[LogicalPlan]) -> Self {
        Self::new(inputs[0].clone(), self.key_fields.clone())
    }
}

impl ArroyoExtension for WindowFunctionExtension {
    fn node_name(&self) -> Option<crate::builder::NamedNode> {
        None
    }

    fn plan_node(
        &self,
        planner: &crate::builder::Planner,
        index: usize,
        input_schemas: Vec<arroyo_rpc::df::ArroyoSchemaRef>,
    ) -> anyhow::Result<super::NodeWithIncomingEdges> {
        if input_schemas.len() != 1 {
            return Err(anyhow::anyhow!(
                "WindowFunctionExtension requires exactly one input"
            ));
        }
        let input_schema = input_schemas[0].clone();
        let input_df_schema =
            Arc::new(DFSchema::try_from(input_schema.schema.as_ref().clone()).unwrap());

        let binning_function = planner.create_physical_expr(
            &Expr::Column(Column::new_unqualified(TIMESTAMP_FIELD.to_string())),
            &input_df_schema,
        )?;
        let binning_function_proto =
            serialize_physical_expr(binning_function, &DefaultPhysicalExtensionCodec {})?;

        let window_plan = planner.sync_plan(&self.window_plan)?;
        let codec = ArroyoPhysicalExtensionCodec::default();
        let window_plan_proto = PhysicalPlanNode::try_from_physical_plan(window_plan, &codec)?;

        let config = WindowFunctionOperator {
            name: "WindowFunction".to_string(),
            input_schema: Some(input_schema.as_ref().clone().try_into()?),
            binning_function: binning_function_proto.encode_to_vec(),
            window_function_plan: window_plan_proto.encode_to_vec(),
        };

        let logical_node = LogicalNode {
            operator_id: format!("window_function_{}", index),
            description: "window function".to_string(),
            operator_name: OperatorName::WindowFunction,
            operator_config: config.encode_to_vec(),
            parallelism: 1,
        };

        let edge = arroyo_datastream::logical::LogicalEdge::project_all(
            // TODO: detect when this shuffle is unnecessary
            LogicalEdgeType::Shuffle,
            input_schema.as_ref().clone(),
        );

        Ok(NodeWithIncomingEdges {
            node: logical_node,
            edges: vec![edge],
        })
    }

    fn output_schema(&self) -> arroyo_rpc::df::ArroyoSchema {
        ArroyoSchema::from_schema_unkeyed(Arc::new(self.schema().as_ref().clone().into())).unwrap()
    }
}
