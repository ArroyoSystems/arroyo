use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow_array::builder::PrimitiveBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_array::StructArray;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Fields;
use arrow_schema::Schema;
use arrow_schema::TimeUnit;
use arroyo_formats::SchemaData;
use arroyo_types::to_nanos;
use arroyo_types::Data;
use arroyo_types::RecordBatchBuilder;
use arroyo_types::{Key, Record, RecordBatchData};
use datafusion_physical_expr::PhysicalExpr;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::info;

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

    fn from_config(name: String, config: Vec<u8>) -> Result<Self> {
        let config = String::from_utf8(config)?;
        let exprs = serde_json::from_str(&config)?;
        let output_schema = Schema::new(
            exprs
                .iter()
                .enumerate()
                .map(|(i, expr)| Field::new(&format!("col{}", i), expr.data_type(&[]), true))
                .collect(),
        );
        Ok(Self {
            name,
            exprs,
            output_schema: Arc::new(output_schema),
        })
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
        let arrays: Vec<_> = self
            .exprs
            .iter()
            .map(|expr| expr.evaluate(batch))
            .map(|r| r.unwrap().into_array(batch.num_rows()))
            .collect();

        let projected_batch = if arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(self.output_schema.clone(), arrays, &options).unwrap()
        } else {
            RecordBatch::try_new(self.output_schema.clone(), arrays).unwrap()
        };
        ctx.collect_record_batch(projected_batch).await;
    }
}

pub struct StructToRecordBatch<K: RecordBatchBuilder, T: RecordBatchBuilder>
where
    K::Data: Key,
{
    name: String,
    batch_builder: Option<KeyValueTimestampRecordBatchBuilder<K, T>>,
    items: usize,
}

impl<K: RecordBatchBuilder, T: RecordBatchBuilder> StructToRecordBatch<K, T>
where
    K::Data: Key,
{
    pub fn new(name: String) -> Self {
        Self {
            name,
            batch_builder: None,
            items: 0,
        }
    }

    async fn flush_batch(&mut self, ctx: &mut Context<(), ()>) {
        if self.items > 0 {
            let batch = self.batch_builder.as_mut().unwrap().flush();
            ctx.collect_record_batch(batch).await;
            self.items = 0;
        }
    }
}

#[derive(Default, Debug)]
pub struct EmptyRecordBatchBuilder {
    items: usize,
}

impl RecordBatchBuilder for EmptyRecordBatchBuilder {
    type Data = ();
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
    fn as_struct_array(&mut self) -> StructArray {
        let items = self.items;
        self.items = 0;
        StructArray::new_null(Fields::empty(), items)
    }

    fn add_data(&mut self, _data: Option<Self::Data>) {
        self.items += 1;
    }

    fn nullable() -> Self {
        Self::default()
    }

    fn flush(&mut self) -> RecordBatch {
        let items = self.items;
        self.items = 0;
        RecordBatch::try_new_with_options(
            self.schema(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(items)),
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
impl<K: RecordBatchBuilder, T: RecordBatchBuilder> ProcessFuncTrait for StructToRecordBatch<K, T>
where
    K::Data: Key,
{
    type InKey = K::Data;
    type InT = T::Data;
    type OutKey = ();
    type OutT = ();

    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_element(
        &mut self,
        record: &Record<K::Data, T::Data>,
        ctx: &mut Context<(), ()>,
    ) {
        let batch_builder = self
            .batch_builder
            .get_or_insert_with(|| KeyValueTimestampRecordBatchBuilder::<K, T>::new());
        batch_builder.add_record(record);
        self.items += 1;
        if self.items >= 10 {
            self.flush_batch(ctx).await;
        }
    }

    async fn process_record_batch(
        &mut self,
        _record_batch: &RecordBatchData,
        _ctx: &mut Context<(), ()>,
    ) {
        unimplemented!("expect to read elements");
    }

    async fn handle_checkpoint(
        &mut self,
        _checkpoint_barrier: &arroyo_types::CheckpointBarrier,
        ctx: &mut Context<Self::OutKey, Self::OutT>,
    ) {
        self.flush_batch(ctx).await;
    }

    async fn handle_watermark(
        &mut self,
        watermark: arroyo_types::Watermark,
        ctx: &mut Context<Self::OutKey, Self::OutT>,
    ) {
        // flush any buffered records
        self.flush_batch(ctx).await;
        // by default, just pass watermarks on down
        ctx.broadcast(arroyo_types::Message::Watermark(watermark))
            .await;
    }
}

struct KeyValueTimestampRecordBatchBuilder<K: RecordBatchBuilder, T: RecordBatchBuilder> {
    key_builder: K,
    value_builder: T,
    timestamp_builder: PrimitiveBuilder<TimestampNanosecondType>,
    schema: SchemaRef,
}

impl<K: RecordBatchBuilder, T: RecordBatchBuilder> KeyValueTimestampRecordBatchBuilder<K, T>
where
    K::Data: Key,
{
    fn new() -> Self {
        let key_builder = K::default();
        let value_builder = T::default();
        let key_field = Field::new(
            "key",
            DataType::Struct(key_builder.schema().fields.clone()),
            true,
        );
        let value_field = Field::new(
            "value",
            DataType::Struct(value_builder.schema().fields.clone()),
            false,
        );
        let timestamp_field = Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        );
        let schema = Arc::new(Schema::new(vec![key_field, value_field, timestamp_field]));
        Self {
            key_builder: K::default(),
            value_builder: T::default(),
            timestamp_builder: PrimitiveBuilder::<TimestampNanosecondType>::new(),
            schema,
        }
    }

    fn add_record(&mut self, record: &Record<K::Data, T::Data>) {
        self.key_builder.add_data(record.key.clone());
        self.value_builder.add_data(Some(record.value.clone()));
        self.timestamp_builder
            .append_value(to_nanos(record.timestamp) as i64);
    }

    fn flush(&mut self) -> RecordBatch {
        let key_array = self.key_builder.as_struct_array();
        let value_array = self.value_builder.as_struct_array();
        let timestamp_array = self.timestamp_builder.finish();
        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(key_array),
                Arc::new(value_array),
                Arc::new(timestamp_array),
            ],
        )
        .unwrap()
    }
}

pub struct RecordBatchToStruct<K: SchemaData + Key, T: SchemaData> {
    name: String,
    _phantom: PhantomData<(K, T)>,
}

impl<K: SchemaData + Key, T: SchemaData> RecordBatchToStruct<K, T> {
    pub fn new(name: String) -> Self {
        Self {
            name,
            _phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<K: SchemaData + Key, T: SchemaData> ProcessFuncTrait for RecordBatchToStruct<K, T> {
    type InKey = ();
    type InT = ();
    type OutKey = K;
    type OutT = T;

    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_element(&mut self, record: &Record<(), ()>, ctx: &mut Context<K, T>) {
        unimplemented!("expect to read record batches");
    }

    async fn process_record_batch(
        &mut self,
        record_batch: &RecordBatchData,
        ctx: &mut Context<K, T>,
    ) {
        let rows = record_batch.0.num_rows();
        if rows == 0 {
            return;
        }
        let key_array = record_batch
            .0
            .column_by_name("key")
            .expect("should have column key");
        let key_struct_array = key_array.as_struct();
        let mut key_iterator = K::nullable_iterator_from_struct_array(key_struct_array).unwrap();
        let mut value_iterator = T::iterator_from_record_batch(
            record_batch
                .0
                .column_by_name("value")
                .expect("should have column value")
                .as_struct()
                .into(),
        )
        .unwrap();
        let timestamp_array = record_batch
            .0
            .column_by_name("timestamp")
            .expect("should have column timestamp")
            .as_any()
            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
            .unwrap();
        let mut timestamp_iterator = timestamp_array.iter().map(|v| {
            SystemTime::UNIX_EPOCH
                + std::time::Duration::from_nanos(v.expect("must have timestamp value") as u64)
        });
        for i in 0..rows {
            let key = key_iterator
                .next()
                .expect("iterator should be as long as record batch");
            let value = value_iterator
                .next()
                .expect("iterator should be as long as record batch");
            let timestamp = timestamp_iterator
                .next()
                .expect("iterator should be as long as record batch");
            ctx.collect(Record {
                timestamp,
                key,
                value,
            })
            .await;
        }
    }
}
