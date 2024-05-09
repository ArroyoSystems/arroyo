use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use arrow_schema::{DataType, TimeUnit};
use arroyo_datastream::logical::{
    DylibUdfConfig, LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName,
};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_rpc::grpc::api::{AsyncUdfOperator, AsyncUdfOrdering};
use arroyo_rpc::{IS_RETRACT_FIELD, TIMESTAMP_FIELD};
use datafusion::common::{
    DFField, DFSchema, DFSchemaRef, DataFusionError, OwnedTableReference, Result as DFResult,
};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::protobuf::ProjectionNode;
use prost::Message;
use watermark_node::WatermarkNode;

use crate::builder::{NamedNode, Planner};
use crate::schemas::{add_timestamp_field, has_timestamp_field};
use crate::ASYNC_RESULT_FIELD;
use join::JoinExtension;

use self::debezium::{DebeziumUnrollingExtension, ToDebeziumExtension};
use self::updating_aggregate::UpdatingAggregateExtension;
use self::{
    aggregate::AggregateExtension, key_calculation::KeyCalculationExtension,
    remote_table::RemoteTableExtension, sink::SinkExtension, table_source::TableSourceExtension,
    window_fn::WindowFunctionExtension,
};

pub(crate) mod aggregate;
pub(crate) mod debezium;
pub(crate) mod join;
pub(crate) mod key_calculation;
pub(crate) mod remote_table;
pub(crate) mod sink;
pub(crate) mod table_source;
pub(crate) mod updating_aggregate;
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
    fn transparent(&self) -> bool {
        false
    }
}

pub(crate) struct NodeWithIncomingEdges {
    pub node: LogicalNode,
    pub edges: Vec<LogicalEdge>,
}

fn try_from_t<T: ArroyoExtension + 'static>(
    node: &dyn UserDefinedLogicalNode,
) -> Result<&dyn ArroyoExtension, ()> {
    node.as_any()
        .downcast_ref::<T>()
        .map(|t| t as &dyn ArroyoExtension)
        .ok_or(())
}

impl<'a> TryFrom<&'a dyn UserDefinedLogicalNode> for &'a dyn ArroyoExtension {
    type Error = DataFusionError;

    fn try_from(node: &'a dyn UserDefinedLogicalNode) -> DFResult<Self, Self::Error> {
        try_from_t::<TableSourceExtension>(node)
            .or_else(|_| try_from_t::<WatermarkNode>(node))
            .or_else(|_| try_from_t::<SinkExtension>(node))
            .or_else(|_| try_from_t::<KeyCalculationExtension>(node))
            .or_else(|_| try_from_t::<AggregateExtension>(node))
            .or_else(|_| try_from_t::<RemoteTableExtension>(node))
            .or_else(|_| try_from_t::<JoinExtension>(node))
            .or_else(|_| try_from_t::<WindowFunctionExtension>(node))
            .or_else(|_| try_from_t::<AsyncUDFExtension>(node))
            .or_else(|_| try_from_t::<ToDebeziumExtension>(node))
            .or_else(|_| try_from_t::<DebeziumUnrollingExtension>(node))
            .or_else(|_| try_from_t::<UpdatingAggregateExtension>(node))
            .map_err(|_| DataFusionError::Plan(format!("unexpected node: {}", node.name())))
    }
}

impl<'a> TryFrom<&'a Arc<dyn UserDefinedLogicalNode>> for &'a dyn ArroyoExtension {
    type Error = DataFusionError;

    fn try_from(node: &'a Arc<dyn UserDefinedLogicalNode>) -> DFResult<Self, Self::Error> {
        TryFrom::try_from(node.as_ref())
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
        if has_timestamp_field(input.schema()) {
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
    pub(crate) timeout: Duration,
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
                let p = planner.create_physical_expr(e, self.input.schema())?;
                Ok(serialize_physical_expr(p, &DefaultPhysicalExtensionCodec {})?.encode_to_vec())
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
                Ok(serialize_physical_expr(p, &DefaultPhysicalExtensionCodec {})?.encode_to_vec())
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
            max_concurrency: self.max_concurrency as u32,
            timeout_micros: self.timeout.as_micros() as u64,
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
                .iter()
                .map(|f| (**f.field()).clone())
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct IsRetractExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) schema: DFSchemaRef,
    pub(crate) timestamp_qualifier: Option<OwnedTableReference>,
}

impl IsRetractExtension {
    pub(crate) fn new(
        input: LogicalPlan,
        timestamp_qualifier: Option<OwnedTableReference>,
    ) -> Self {
        let mut output_fields = input.schema().fields().clone();
        let timestamp_index = output_fields.len() - 1;
        output_fields[timestamp_index] = DFField::new(
            timestamp_qualifier.clone(),
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        );
        output_fields.push(DFField::new_unqualified(
            IS_RETRACT_FIELD,
            DataType::Boolean,
            false,
        ));
        let schema = Arc::new(
            DFSchema::new_with_metadata(output_fields, input.schema().metadata().clone()).unwrap(),
        );
        Self {
            input,
            schema,
            timestamp_qualifier,
        }
    }
}

impl UserDefinedLogicalNodeCore for IsRetractExtension {
    fn name(&self) -> &str {
        "IsRetractExtension"
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
        write!(f, "IsRetractExtension")
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self::new(inputs[0].clone(), self.timestamp_qualifier.clone())
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
        assert_eq!(
            &UserDefinedLogicalNode::expressions(self),
            exprs,
            "Tried to recreate async UDF node with different expressions"
        );

        Self {
            input: Arc::new(inputs[0].clone()),
            name: self.name.clone(),
            udf: self.udf.clone(),
            arg_exprs: self.arg_exprs.clone(),
            final_exprs: self.final_exprs.clone(),
            ordered: self.ordered,
            max_concurrency: self.max_concurrency,
            timeout: self.timeout,
            final_schema: self.final_schema.clone(),
        }
    }
}
