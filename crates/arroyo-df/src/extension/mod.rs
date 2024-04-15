use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arroyo_datastream::logical::{
    DylibUdfConfig, LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName,
};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_rpc::grpc::api::{AsyncUdfOperator, AsyncUdfOrdering};
use datafusion_common::{
    DFField, DFSchema, DFSchemaRef, DataFusionError, OwnedTableReference, Result as DFResult,
};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore};
use datafusion_proto::protobuf::{PhysicalExprNode, ProjectionNode};
use prost::Message;
use watermark_node::WatermarkNode;

use crate::builder::{NamedNode, Planner};
use crate::schemas::{add_timestamp_field, has_timestamp_field};
use crate::ASYNC_RESULT_FIELD;
use join::JoinExtension;

use self::{
    aggregate::AggregateExtension, key_calculation::KeyCalculationExtension,
    remote_table::RemoteTableExtension, sink::SinkExtension, table_source::TableSourceExtension,
    window_fn::WindowFunctionExtension,
};

pub(crate) mod aggregate;
pub(crate) mod join;
pub(crate) mod key_calculation;
pub(crate) mod remote_table;
pub(crate) mod sink;
pub(crate) mod table_source;
pub(crate) mod watermark_node;
pub(crate) mod window_fn;
pub(crate) trait ArroyoExtension: Debug {
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

fn try_from_t<'a, T: ArroyoExtension + 'static>(
    node: &'a Arc<dyn UserDefinedLogicalNode>,
) -> Result<&'a dyn ArroyoExtension, ()> {
    node.as_any()
        .downcast_ref::<T>()
        .map(|t| t as &dyn ArroyoExtension)
        .ok_or_else(|| ())
}

impl<'a> TryFrom<&'a Arc<dyn UserDefinedLogicalNode>> for &'a dyn ArroyoExtension {
    type Error = DataFusionError;

    fn try_from(node: &'a Arc<dyn UserDefinedLogicalNode>) -> DFResult<Self, Self::Error> {
        try_from_t::<TableSourceExtension>(node)
            .or_else(|_| try_from_t::<WatermarkNode>(node))
            .or_else(|_| try_from_t::<SinkExtension>(node))
            .or_else(|_| try_from_t::<KeyCalculationExtension>(node))
            .or_else(|_| try_from_t::<AggregateExtension>(node))
            .or_else(|_| try_from_t::<RemoteTableExtension>(node))
            .or_else(|_| try_from_t::<JoinExtension>(node))
            .or_else(|_| try_from_t::<WindowFunctionExtension>(node))
            .or_else(|_| try_from_t::<AsyncUDFExtension>(node))
            .map_err(|_| DataFusionError::Plan(format!("unexpected node: {}", node.name())))
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AsyncUDFExtension {
    pub(crate) input: Arc<LogicalPlan>,
    pub(crate) name: String,
    pub(crate) udf: DylibUdfConfig,
    pub(crate) arg_exprs: Vec<Expr>,
    pub(crate) final_exprs: Vec<Expr>,
    pub(crate) ordered: bool,
    pub(crate) max_concurrency: usize,
    pub(crate) final_schema: DFSchemaRef,
}

impl ArroyoExtension for AsyncUDFExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        let arg_exprs = self
            .arg_exprs
            .iter()
            .map(|e| {
                let p = planner.create_physical_expr(&e, self.input.schema())?;
                Ok(PhysicalExprNode::try_from(p)?.encode_to_vec())
            })
            .collect::<DFResult<Vec<_>>>()
            .map_err(|e| anyhow!("failed to build async udf extension: {:?}", e))?;

        let mut final_fields = self.input.schema().fields().clone();
        final_fields.push(DFField::new_unqualified(
            ASYNC_RESULT_FIELD,
            self.udf.return_type.clone(),
            true,
        ));
        let post_udf_schema = DFSchema::new_with_metadata(final_fields, HashMap::new())?;

        let final_exprs = self
            .final_exprs
            .iter()
            .map(|e| {
                let p = planner.create_physical_expr(e, &post_udf_schema)?;
                Ok(PhysicalExprNode::try_from(p)?.encode_to_vec())
            })
            .collect::<DFResult<Vec<_>>>()
            .map_err(|e| anyhow!("failed to build async udf extension: {:?}", e))?;

        ProjectionNode {
            input: None,
            expr: vec![],
            optional_alias: None,
        };

        let config = AsyncUdfOperator {
            name: self.name.clone(),
            udf: Some(self.udf.clone().into()),
            arg_exprs,
            final_exprs,
            ordering: if self.ordered {
                AsyncUdfOrdering::Ordered as i32
            } else {
                AsyncUdfOrdering::Unordered as i32
            },
            max_concurrency: 0,
        };

        let node = LogicalNode {
            operator_id: format!("async_udf_{}", index),
            description: format!("async_udf<{}>", self.name),
            operator_name: OperatorName::AsyncUdf,
            operator_config: config.encode_to_vec(),
            parallelism: 1,
        };

        let incoming_edge =
            LogicalEdge::project_all(LogicalEdgeType::Forward, input_schemas[0].as_ref().clone());
        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![incoming_edge],
        })
    }

    fn output_schema(&self) -> ArroyoSchema {
        ArroyoSchema::from_fields(
            self.final_schema
                .fields()
                .into_iter()
                .map(|f| (**f.field()).clone())
                .collect(),
        )
    }
}

impl UserDefinedLogicalNodeCore for AsyncUDFExtension {
    fn name(&self) -> &str {
        "AsyncUDFNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.final_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.arg_exprs
            .iter()
            .chain(self.final_exprs.iter())
            .map(|e| e.to_owned())
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "AsyncUdfExtension<{}>: {}",
            self.name,
            self.final_schema
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        eprintln!(
            "can't recreate with new exprs {:?} {:?} {:?}",
            self.arg_exprs, self.final_exprs, exprs
        );
        self.clone()
    }
}
