use crate::operator::{ArrowContext, ArrowOperator, ArrowOperatorConstructor};
use arrow::datatypes::{Schema, SchemaRef};
use arrow_array::RecordBatch;
use arroyo_types::ArroyoRecordBatch;
use async_trait::async_trait;
use datafusion_common::DataFusionError;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::{AggregateUDF, Expr, ScalarUDF, WindowUDF};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::{PhysicalExprNode, ProjectionExecNode};
use prost::bytes::Bytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use arroyo_rpc::grpc::api;
use crate::operators::exprs_from_proto;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectionOperatorConfig {
    name: String,
    exprs: Vec<u8>,
}

pub struct ProjectionOperator {
    name: String,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}


impl ArrowOperatorConstructor<api::ProjectionOperator> for ProjectionOperator {
    fn from_config(config: api::ProjectionOperator) -> Box<dyn ArrowOperator> {
        let exprs = exprs_from_proto(config.expressions);
        Box::new(Self {
            name: config.name,
            exprs,
        })
    }
}

#[async_trait]
impl ArrowOperator for ProjectionOperator {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_batch(&mut self, batch: ArroyoRecordBatch, ctx: &mut ArrowContext) {
        let record_batch =
            RecordBatch::try_new(ctx.in_schemas[0].schema.clone(), batch.columns).unwrap();

        let arrays = self
            .exprs
            .iter()
            .map(|expr| expr.evaluate(&record_batch))
            .map(|r| r.map(|v| v.into_array(record_batch.num_rows())))
            .collect::<datafusion_common::Result<Vec<_>>>()
            .unwrap();

        let record = ArroyoRecordBatch::new(arrays);

        ctx.collector.collect(record).await;
    }
}
