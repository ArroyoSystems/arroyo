use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arroyo_formats::SchemaData;
use arroyo_types::Data;
use arroyo_types::RecordBatchBuilder;
use arroyo_types::{Key, Record, RecordBatchData};
use datafusion_physical_expr::PhysicalExpr;
use tracing::info;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::SystemTime;

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
        let arrays : Vec<_> = self
            .exprs
            .iter()
            .map(|expr| expr.evaluate(batch))
            .map(|r| r.unwrap().into_array(batch.num_rows()))
            .collect();

        let projected_batch = if arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(self.output_schema.clone(), arrays, &options)
                .unwrap()
        } else {
            RecordBatch::try_new(self.output_schema.clone(), arrays).unwrap()
        };
        ctx.collect_record_batch(projected_batch).await;
    }
}

pub struct StructToRecordBatch<K: Key, D: Data, R: RecordBatchBuilder<Data = D>> {
    name: String,
    batch_builder: Option<R>,
    items: usize,
    _phantom: PhantomData<(K, D)>,
}

impl <K: Key, D: Data, R: RecordBatchBuilder<Data = D>> StructToRecordBatch<K, D, R> {
    pub fn new(name: String) -> Self {
        Self {
            name,
            batch_builder: None,
            items: 0,
            _phantom: PhantomData,
        }
    }
}

struct KeyValueRecordBatchBuilder<K: Key, D: Data> {
    key_builder: R::KeyBuilder,
    value_builder: R::ValueBuilder,
    _phantom: PhantomData<(K, D)>,
}

#[async_trait::async_trait]
impl<K: Key, D: Data, R: RecordBatchBuilder<Data = D>> ProcessFuncTrait for StructToRecordBatch<K, D, R> {
    type InKey = K;
    type InT = D;
    type OutKey = ();
    type OutT = ();

    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_element(&mut self, record: &Record<K, D>, ctx: &mut Context<(), ()>) {
        info!("in {} received record: {:?}",self.name, record);
        let batch_builder = self.batch_builder.get_or_insert_with(|| R::default());
        batch_builder.add_data(Some(record.value.clone()));
        self.items += 1;
        if self.items >= 10 {
            let batch = batch_builder.flush();
            info!("flushing {:?}", batch);
            ctx.collect_record_batch(batch).await;
            self.items = 0;
        }
    }

    async fn process_record_batch(
        &mut self,
        record_batch: &RecordBatchData,
        ctx: &mut Context<(), ()>,
    ) {
        unimplemented!("expect to read record batches");
    }

    async fn handle_checkpoint(
        &mut self,
        _checkpoint_barrier: &arroyo_types::CheckpointBarrier,
        ctx: &mut Context<Self::OutKey, Self::OutT>,
    ) {
        if let Some(mut batch_builder) = self.batch_builder.take() {
            let batch = batch_builder.flush();
            ctx.collect_record_batch(batch).await;
            self.items = 0;
        }
    }

    async fn handle_watermark(
        &mut self,
        watermark: arroyo_types::Watermark,
        ctx: &mut Context<Self::OutKey, Self::OutT>,
    ) {
        // flush any buffered records
        if let Some(mut batch_builder) = self.batch_builder.take() {
            let batch = batch_builder.flush();
            ctx.collect_record_batch(batch).await;
            self.items = 0;
        }
        // by default, just pass watermarks on down
        ctx.broadcast(arroyo_types::Message::Watermark(watermark))
            .await;
    }
}


pub struct RecordBatchToStruct<D: SchemaData> {
    name: String,
    _phantom: PhantomData<D>,
}

impl <D: SchemaData> RecordBatchToStruct<D> {
    pub fn new(name: String) -> Self {
        Self {
            name,
            _phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl <D: SchemaData> ProcessFuncTrait for RecordBatchToStruct<D> {
    type InKey = ();
    type InT = ();
    type OutKey = ();
    type OutT = D;

    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_element(&mut self, record: &Record<(), ()>, ctx: &mut Context<(), D>) {
        unimplemented!("expect to read record batches");
    }

    async fn process_record_batch(
        &mut self,
        record_batch: &RecordBatchData,
        ctx: &mut Context<(), D>,
    ) {
        info!("received record batch: {:?}", record_batch);
        for value in D::iterator_from_record_batch(record_batch.0.clone()).unwrap() {
            info!("collecting value: {:?}", value);
            ctx.collect(Record{
                timestamp: SystemTime::now(),
                key: None,
                value,
            }).await;
        }
    }
}