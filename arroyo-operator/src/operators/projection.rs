use crate::operator::{ArrowContext, ArrowOperator, ArrowOperatorConstructor};
use arrow::datatypes::{Schema, SchemaRef};
use arrow_array::RecordBatch;
use arroyo_types::ArrowRecord;
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

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectionOperatorConfig {
    name: String,
    exprs: Vec<u8>,
}

pub struct ProjectionOperator {
    name: String,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

pub struct Registry {}

impl FunctionRegistry for Registry {
    fn udfs(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn udf(&self, name: &str) -> datafusion_common::Result<Arc<ScalarUDF>> {
        todo!()
    }

    fn udaf(&self, name: &str) -> datafusion_common::Result<Arc<AggregateUDF>> {
        todo!()
    }

    fn udwf(&self, name: &str) -> datafusion_common::Result<Arc<WindowUDF>> {
        todo!()
    }
}

impl ArrowOperatorConstructor for ProjectionOperator {
    fn from_config(config: Value) -> Box<dyn ArrowOperator> {
        let mut config: ProjectionOperatorConfig = serde_json::from_value(config).unwrap();
        let mut buf: Bytes = config.exprs.into();

        let node = ProjectionExecNode::decode(&mut buf).unwrap();

        let registry = Registry {};

        let schema = Schema::empty();

        let exprs = node
            .expr
            .iter()
            .map(|expr| parse_physical_expr(expr, &registry, &schema).unwrap())
            .collect();

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

    async fn process_batch(&mut self, batch: ArrowRecord, ctx: &mut ArrowContext) {
        let record_batch =
            RecordBatch::try_new(ctx.out_schema.schema.clone(), batch.columns).unwrap();

        let arrays = self
            .exprs
            .iter()
            .map(|expr| expr.evaluate(&record_batch))
            .map(|r| r.map(|v| v.into_array(record_batch.num_rows())))
            .collect::<datafusion_common::Result<Vec<_>>>()
            .unwrap();

        let record = ArrowRecord::new(arrays);

        ctx.collector.collect(record).await;
    }
}
