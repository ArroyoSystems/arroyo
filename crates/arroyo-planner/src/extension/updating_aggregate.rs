use super::{ArroyoExtension, IsRetractExtension, NodeWithIncomingEdges};
use crate::builder::{NamedNode, Planner};
use crate::functions::multi_hash;
use crate::physical::ArroyoPhysicalExtensionCodec;
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::config::config;
use arroyo_rpc::{
    df::ArroyoSchema, grpc::api::UpdatingAggregateOperator,
};
use datafusion::common::{
    internal_err, plan_err, DFSchemaRef, Result, TableReference, ToDFSchema,
};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    col, lit, Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore,
};
use datafusion::prelude::named_struct;
use datafusion::scalar::ScalarValue;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use std::sync::Arc;
use std::time::Duration;

pub(crate) const UPDATING_AGGREGATE_EXTENSION_NAME: &str = "UpdatingAggregateExtension";

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) struct UpdatingAggregateExtension {
    pub(crate) aggregate: LogicalPlan,
    pub(crate) key_fields: Vec<usize>,
    pub(crate) final_calculation: LogicalPlan,
    pub(crate) timestamp_qualifier: Option<TableReference>,
    pub(crate) ttl: Duration,
}

impl UpdatingAggregateExtension {
    pub fn new(
        aggregate: LogicalPlan,
        key_fields: Vec<usize>,
        timestamp_qualifier: Option<TableReference>,
        ttl: Duration,
    ) -> Result<Self> {
        let final_calculation = LogicalPlan::Extension(Extension {
            node: Arc::new(IsRetractExtension::new(
                aggregate.clone(),
                timestamp_qualifier.clone(),
            )),
        });

        Ok(Self {
            aggregate,
            key_fields,
            final_calculation,
            timestamp_qualifier,
            ttl,
        })
    }
}

impl UserDefinedLogicalNodeCore for UpdatingAggregateExtension {
    fn name(&self) -> &str {
        UPDATING_AGGREGATE_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.aggregate]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.final_calculation.schema()
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "UpdatingAggregateExtension")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Self::new(
            inputs[0].clone(),
            self.key_fields.clone(),
            self.timestamp_qualifier.clone(),
            self.ttl,
        )
    }
}

impl ArroyoExtension for UpdatingAggregateExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<arroyo_rpc::df::ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        if input_schemas.len() != 1 {
            return plan_err!(
                "UpdatingAggregateExtension requires exactly one input schema, found {}",
                input_schemas.len()
            );
        }

        let input_schema = input_schemas[0].clone();
        let input_dfschema = input_schema.schema.clone().to_dfschema()?;

        let aggregate_exec = PhysicalPlanNode::try_from_physical_plan(
            planner.sync_plan(&self.aggregate)?,
            &ArroyoPhysicalExtensionCodec::default(),
        )?;
        
        let key_exprs: Vec<Expr> = self
            .key_fields
            .iter()
            .map(|&i| col(input_schema.schema.field(i).name()))
            .collect();
        let hash_expr = if key_exprs.is_empty() {
            Expr::Literal(ScalarValue::FixedSizeBinary(16, Some(vec![0; 16])))
        } else {
            Expr::ScalarFunction(ScalarFunction {
                func: multi_hash(),
                args: key_exprs,
            })
        };

        let updating_meta_expr =
            named_struct(vec![lit("is_retract"), lit(false), lit("id"), hash_expr]);

        let config = UpdatingAggregateOperator {
            name: "UpdatingAggregate".to_string(),
            input_schema: Some((*input_schema).clone().into()),
            final_schema: Some(self.output_schema().into()),
            aggregate_exec: aggregate_exec.encode_to_vec(),
            metadata_expr: planner
                .serialize_as_physical_expr(&updating_meta_expr, &input_dfschema)?,
            flush_interval_micros: config()
                .pipeline
                .update_aggregate_flush_interval
                .as_micros() as u64,
            ttl_micros: self.ttl.as_micros() as u64,
        };

        let node = LogicalNode::single(
            index as u32,
            format!("updating_aggregate_{}", index),
            OperatorName::UpdatingAggregate,
            config.encode_to_vec(),
            "UpdatingAggregate".to_string(),
            1,
        );

        let edge = LogicalEdge::project_all(LogicalEdgeType::Shuffle, (*input_schema).clone());

        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![edge],
        })
    }

    fn output_schema(&self) -> arroyo_rpc::df::ArroyoSchema {
        ArroyoSchema::from_schema_unkeyed(Arc::new(self.schema().as_ref().into())).unwrap()
    }
}
