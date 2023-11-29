use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arroyo_types::{Key, Record, RecordBatchData};
use datafusion_physical_expr::PhysicalExpr;
use std::sync::Arc;

use crate::{engine::Context, stream_node::ProcessFuncTrait};

pub struct ProjectionOperator {
    name: String,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    output_schema: SchemaRef,
}

#[async_trait::async_trait]
impl ProcessFuncTrait for ProjectionOperator {
    type InKey = ();
    type InT = ();
    type OutKey = ();
    type OutT = ();

    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_element(&mut self, record: &Record<(), ()>, ctx: &mut Context<(), ()>) {
        unimplemented!("only record batches supported");
    }

    async fn process_record_batch(
        &mut self,
        record_batch: &RecordBatchData,
        ctx: &mut Context<(), ()>,
    ) {
        let batch = &record_batch.0;
        let arrays = self
            .exprs
            .iter()
            .map(|expr| expr.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        let projected_batch = if arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(self.output_schema.clone(), arrays, &options)
                .map_err(Into::into)
        } else {
            RecordBatch::try_new(self.output_schema.clone(), arrays).map_err(Into::into)
        };
        ctx.collect_record_batch(projected_batch.unwrap()).await;
    }
}

pub struct StructsToRecordBatch< {
    name: String,
    schema: SchemaRef,
}
