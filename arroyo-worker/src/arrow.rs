use anyhow::Result;
use arrow::datatypes::Fields;
use arrow::datatypes::SchemaRef;
use arrow_array::builder::PrimitiveBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_array::StructArray;
use arroyo_formats::SchemaData;
use arroyo_types::to_nanos;
use arroyo_types::KeyValueTimestampRecordBatch;
use arroyo_types::KeyValueTimestampRecordBatchBuilder;
use arroyo_types::RecordBatchBuilder;
use arroyo_types::{Key, Record, RecordBatchData};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_execution::FunctionRegistry;
use datafusion_execution::TaskContext;
use datafusion_execution::runtime_env::RuntimeConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_expr::AggregateUDF;
use datafusion_expr::ScalarUDF;
use datafusion_expr::WindowUDF;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message;
use std::collections::HashSet;
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

impl ProjectionOperator {
    pub fn from_config(name: String, config: Vec<u8>) -> Result<Self> {
        let proto_config: arroyo_rpc::grpc::api::ProjectionOperator =
            arroyo_rpc::grpc::api::ProjectionOperator::decode(&mut config.as_slice()).unwrap();

        let registry = Registry {};
        let input_schema = serde_json::from_slice(&proto_config.input_schema)?;
        let output_schema = serde_json::from_slice(&proto_config.output_schema)?;

        let exprs: Vec<_> = proto_config
            .expressions
            .into_iter()
            .map(|expr| PhysicalExprNode::decode(&mut expr.as_slice()).unwrap())
            .map(|expr| parse_physical_expr(&expr, &registry, &input_schema).unwrap())
            .collect();
        Ok(Self {
            name,
            exprs,
            output_schema: Arc::new(output_schema),
        })
    }
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
        info!("incoming record batch {:?}", record_batch);
        info!("expressions: {:#?}", self.exprs);
        info!("output schema {:#?}", self.output_schema);
        let batch = &record_batch.0;
        let mut data: KeyValueTimestampRecordBatch = batch.try_into().unwrap();
        let arrays: Vec<_> = self
            .exprs
            .iter()
            .map(|expr| expr.evaluate(&data.value_batch))
            .map(|r| r.unwrap().into_array(batch.num_rows()))
            .collect();

        data.value_batch = if arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(self.output_schema.clone(), arrays, &options).unwrap()
        } else {
            RecordBatch::try_new(self.output_schema.clone(), arrays).unwrap()
        };
        ctx.collect_record_batch((&data).into()).await;
    }
}

pub struct ValueExecutionOperator {
    name: String,
    execution_plan: Arc<dyn ExecutionPlan>
}

impl ValueExecutionOperator {
    pub fn from_config(name: String, config: Vec<u8>) -> Result<Self> {
        let proto_config: arroyo_rpc::grpc::api::ValuePlanOperator =
            arroyo_rpc::grpc::api::ValuePlanOperator::decode(&mut config.as_slice()).unwrap();

        let registry = Registry {};

        let plan = PhysicalPlanNode::decode(&mut proto_config.physical_plan.as_slice()).unwrap();
        let execution_plan = plan.try_into_physical_plan(&registry, 
            &RuntimeEnv::new(RuntimeConfig::new())?, &DefaultPhysicalExtensionCodec{})?;

        Ok(Self {
            name,
            execution_plan
        })
    }
}

#[async_trait::async_trait]
impl ProcessFuncTrait for ValueExecutionOperator {
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
        info!("incoming record batch {:?}", record_batch);
        let batch = &record_batch.0;
        let mut data: KeyValueTimestampRecordBatch = batch.try_into().unwrap();
        let session_context = SessionContext::new();
        session_context.register_batch("memory",  data.value_batch.clone());
        let records = self.execution_plan.execute(0, session_context.task_ctx()).unwrap();
        while let Some(batch) = records.next().await {
            batch.unwrap();
        }
        let batch = &record_batch.0;
        let mut data: KeyValueTimestampRecordBatch = batch.try_into().unwrap();
        let arrays: Vec<_> = self
            .exprs
            .iter()
            .map(|expr| expr.evaluate(&data.value_batch))
            .map(|r| r.unwrap().into_array(batch.num_rows()))
            .collect();

        data.value_batch = if arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(self.output_schema.clone(), arrays, &options).unwrap()
        } else {
            RecordBatch::try_new(self.output_schema.clone(), arrays).unwrap()
        };
        ctx.collect_record_batch((&data).into()).await;
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
