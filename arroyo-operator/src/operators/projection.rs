use crate::operator::{ArrowContext, ArrowOperator};
use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use arroyo_rpc::grpc::TableDescriptor;
use arroyo_types::ArrowRecord;
use async_trait::async_trait;
use datafusion_physical_expr::PhysicalExpr;
use std::sync::Arc;

pub struct ProjectionOperator {
    name: String,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    output_schema: SchemaRef,
}

#[async_trait]
impl ArrowOperator for ProjectionOperator {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_batch(&mut self, batch: ArrowRecord, ctx: &mut ArrowContext) {
        let record_batch = RecordBatch::try_new(self.output_schema.clone(), batch.columns).unwrap();

        let arrays = self
            .expr
            .iter()
            .map(|expr| expr.evaluate(&record_batch))
            .map(|r| r.map(|v| v.into_array(record_batch.num_rows())))
            .collect::<datafusion_common::Result<Vec<_>>>()
            .unwrap();

        let record = ArrowRecord::new(arrays);

        ctx.collector.collect(record).await;
    }
}
