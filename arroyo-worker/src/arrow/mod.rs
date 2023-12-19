use anyhow::Result;
use arrow::compute::kernels;
use arrow::datatypes::Fields;
use arrow::datatypes::SchemaRef;
use arrow_array::builder::PrimitiveBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::PrimitiveArray;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_array::StructArray;
use arrow_json::writer::record_batches_to_json_rows;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arroyo_df::EmptyPartitionStream;
use arroyo_formats::SchemaData;
use arroyo_rpc::grpc::api::MemTableScan;
use arroyo_rpc::grpc::controller_grpc_client::ControllerGrpcClient;
use arroyo_rpc::grpc::SinkDataReq;
use arroyo_types::from_millis;
use arroyo_types::from_nanos;
use arroyo_types::to_micros;
use arroyo_types::to_millis;
use arroyo_types::to_nanos;
use arroyo_types::KeyValueTimestampRecordBatch;
use arroyo_types::KeyValueTimestampRecordBatchBuilder;
use arroyo_types::Message;
use arroyo_types::RecordBatchBuilder;
use arroyo_types::Watermark;
use arroyo_types::{Key, Record, RecordBatchData};
use datafusion::datasource::ViewTable;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::streaming::StreamingTableExec;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_common::DataFusionError;
use datafusion_common::ScalarValue;
use datafusion_execution::runtime_env::RuntimeConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::FunctionRegistry;
use datafusion_execution::TaskContext;
use datafusion_expr::AggregateUDF;
use datafusion_expr::ScalarUDF;
use datafusion_expr::WindowUDF;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::PhysicalExprNode;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::lock;
use futures::StreamExt;
use prost::Message as ProstMessage;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::collections::HashMap;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::SystemTime;
use tonic::transport::Channel;
use tracing::info;

use crate::connectors::nexmark::GeneratorConfig;
use crate::connectors::nexmark::NexmarkConfig;
use crate::connectors::nexmark::NexmarkGenerator;
use crate::{engine::Context, stream_node::ProcessFuncTrait};

pub struct ProjectionOperator {
    name: String,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    output_schema: SchemaRef,
}

pub mod tumbling_aggregating_window;

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
#[derive(Debug)]
pub struct MemTablePhysicalExtensionCodec {}

impl PhysicalExtensionCodec for MemTablePhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let mem_table_scan = MemTableScan::decode(buf).map_err(|err| {
            DataFusionError::Internal(format!("failed to decode mem table {}", err))
        })?;
        todo!()
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        todo!()
    }
}

pub struct ValueExecutionOperator {
    name: String,
    locked_batch: Arc<RwLock<Option<RecordBatch>>>,
    execution_plan: Arc<dyn ExecutionPlan>,
}

#[derive(Debug, Default)]
struct ArrowPhysicalExtensionCodec {
    locked_batch: Arc<RwLock<Option<RecordBatch>>>,
}

impl PhysicalExtensionCodec for ArrowPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let empty_partition_scheme: EmptyPartitionStream = serde_json::from_slice(buf)
            .map_err(|err| DataFusionError::Internal(format!("couldn't deserialize: {}", err)))?;
        let reader = RwLockRecordBatchReader {
            schema: empty_partition_scheme.schema(),
            locked_batch: self.locked_batch.clone(),
        };
        Ok(Arc::new(reader))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        todo!()
    }
}

impl ValueExecutionOperator {
    pub fn from_config(name: String, config: Vec<u8>) -> Result<Self> {
        let proto_config: arroyo_rpc::grpc::api::ValuePlanOperator =
            arroyo_rpc::grpc::api::ValuePlanOperator::decode(&mut config.as_slice()).unwrap();
        let locked_batch = Arc::new(RwLock::default());
        let registry = Registry {};

        let plan = PhysicalPlanNode::decode(&mut proto_config.physical_plan.as_slice()).unwrap();
        //info!("physical plan is {:#?}", plan);
        let codec = ArrowPhysicalExtensionCodec {
            locked_batch: locked_batch.clone(),
        };

        let execution_plan = plan.try_into_physical_plan(
            &registry,
            &RuntimeEnv::new(RuntimeConfig::new())?,
            &codec,
        )?;

        Ok(Self {
            name,
            locked_batch,
            execution_plan,
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
        //info!("incoming record batch {:?}", record_batch);
        let batch = &record_batch.0;
        {
            let mut writer = self.locked_batch.write().unwrap();
            *writer = Some(batch.clone());
        }
        let session_context = SessionContext::new();
        let mut records = self
            .execution_plan
            .execute(0, session_context.task_ctx())
            .expect("successfully computed?");
        while let Some(batch) = records.next().await {
            let batch = batch.expect("should be able to compute batch");
            //info!("batch {:?}", batch);
            ctx.collect_record_batch(batch).await;
        }
    }
}

#[derive(Debug)]
struct RwLockRecordBatchReader {
    schema: SchemaRef,
    locked_batch: Arc<RwLock<Option<RecordBatch>>>,
}

impl DisplayAs for RwLockRecordBatchReader {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RW Lock RecordBatchReader")
    }
}

impl ExecutionPlan for RwLockRecordBatchReader {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion_physical_expr::Partitioning {
        datafusion_physical_expr::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion_physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<datafusion_execution::SendableRecordBatchStream> {
        let result = self.locked_batch.read().unwrap().clone().unwrap();
        Ok(Box::pin(MemoryStream::try_new(
            vec![result],
            self.schema.clone(),
            None,
        )?))
    }

    fn statistics(&self) -> datafusion_common::Statistics {
        todo!()
    }
}

pub struct KeyExecutionOperator {
    name: String,
    locked_batch: Arc<RwLock<Option<RecordBatch>>>,
    execution_plan: Arc<dyn ExecutionPlan>,
    key_fields: Vec<usize>,
}

impl KeyExecutionOperator {
    pub fn from_config(name: String, config: Vec<u8>) -> Result<Self> {
        let proto_config =
            arroyo_rpc::grpc::api::KeyPlanOperator::decode(&mut config.as_slice()).unwrap();
        let locked_batch = Arc::new(RwLock::default());
        let registry = Registry {};

        let plan = PhysicalPlanNode::decode(&mut proto_config.physical_plan.as_slice()).unwrap();
        //info!("physical plan is {:#?}", plan);
        let codec = ArrowPhysicalExtensionCodec {
            locked_batch: locked_batch.clone(),
        };

        let execution_plan = plan.try_into_physical_plan(
            &registry,
            &RuntimeEnv::new(RuntimeConfig::new())?,
            &codec,
        )?;

        Ok(Self {
            name,
            locked_batch,
            execution_plan,
            key_fields: proto_config
                .key_fields
                .into_iter()
                .map(|field| field as usize)
                .collect(),
        })
    }
}

#[async_trait::async_trait]
impl ProcessFuncTrait for KeyExecutionOperator {
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
        //info!("incoming record batch {:?}", record_batch);
        let batch = &record_batch.0;
        {
            let mut writer = self.locked_batch.write().unwrap();
            *writer = Some(batch.clone());
        }
        let session_context = SessionContext::new();
        let mut records = self
            .execution_plan
            .execute(0, session_context.task_ctx())
            .expect("successfully computed?");
        while let Some(batch) = records.next().await {
            let batch = batch.expect("should be able to compute batch");
            //TODO: sort by the key
            //info!("batch {:?}", batch);
            ctx.collect_record_batch(batch).await;
        }
    }
}

pub struct StructToRecordBatch<T: RecordBatchBuilder> {
    name: String,
    batch_builder: Option<KeyValueTimestampRecordBatchBuilder<T>>,
    items: usize,
}

impl<T: RecordBatchBuilder> StructToRecordBatch<T> {
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
impl<T: RecordBatchBuilder> ProcessFuncTrait for StructToRecordBatch<T> {
    type InKey = ();
    type InT = T::Data;
    type OutKey = ();
    type OutT = ();

    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_element(&mut self, record: &Record<(), T::Data>, ctx: &mut Context<(), ()>) {
        let batch_builder = self
            .batch_builder
            .get_or_insert_with(|| KeyValueTimestampRecordBatchBuilder::<T>::new());
        batch_builder.add_record(record);
        self.items += 1;
        if self.items >= 1000 {
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

pub struct DefaultTimestampWatermark {}

#[async_trait::async_trait]
impl ProcessFuncTrait for DefaultTimestampWatermark {
    type InKey = ();
    type InT = ();
    type OutKey = ();
    type OutT = ();

    fn name(&self) -> String {
        "Watermark".to_string()
    }

    async fn process_element(&mut self, record: &Record<(), ()>, ctx: &mut Context<(), ()>) {
        unimplemented!("expect to read record batches");
    }

    async fn process_record_batch(
        &mut self,
        record_batch: &RecordBatchData,
        ctx: &mut Context<(), ()>,
    ) {
        let record_batch = &record_batch.0;
        let schema = record_batch.schema();
        let output_batch = if schema
            .fields()
            .iter()
            .any(|field| field.name() == "_timestamp")
        {
            record_batch.clone()
        } else {
            let current_time = to_nanos(SystemTime::now());
            let current_time_scalar =
                ScalarValue::TimestampNanosecond(Some(current_time as i64), None);
            let time_column = current_time_scalar.to_array_of_size(record_batch.num_rows());
            let mut record_batch_columns = record_batch.columns().to_vec();
            record_batch_columns.push(time_column);
            let mut schema_columns = schema.fields().to_vec();
            schema_columns.push(Arc::new(Field::new(
                "_timestamp",
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                false,
            )));
            let schema = Schema::new_with_metadata(schema_columns, HashMap::new());
            RecordBatch::try_new(Arc::new(schema), record_batch_columns).unwrap()
        };
        let timestamp_column = output_batch
            .column_by_name("_timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
            .unwrap();

        let min_timestamp = kernels::aggregate::min(timestamp_column).unwrap();
        let watermark = from_nanos(min_timestamp as u128) - Duration::from_secs(3600);
        ctx.collect_record_batch(output_batch).await;

        ctx.collector
            .broadcast(Message::Watermark(Watermark::EventTime(watermark)))
            .await;
    }

    async fn on_close(&mut self, ctx: &mut Context<Self::OutKey, Self::OutT>) {
        // send final watermark on close
        ctx.collector
            .broadcast(Message::Watermark(Watermark::EventTime(from_millis(
                u64::MAX,
            ))))
            .await;
    }
}

#[derive(Default)]
pub struct GrpcRecordBatchSink {
    client: Option<ControllerGrpcClient<Channel>>,
}

#[async_trait::async_trait]
impl ProcessFuncTrait for GrpcRecordBatchSink {
    type InKey = ();
    type InT = ();
    type OutKey = ();
    type OutT = ();

    fn name(&self) -> String {
        "GRPC".to_string()
    }

    async fn process_element(&mut self, record: &Record<(), ()>, ctx: &mut Context<(), ()>) {
        unimplemented!("expect to read record batches");
    }

    async fn on_start(&mut self, _: &mut Context<(), ()>) {
        let controller_addr = std::env::var(arroyo_types::CONTROLLER_ADDR_ENV)
            .unwrap_or_else(|_| crate::LOCAL_CONTROLLER_ADDR.to_string());

        self.client = Some(
            ControllerGrpcClient::connect(controller_addr)
                .await
                .unwrap(),
        );
    }

    async fn process_record_batch(
        &mut self,
        record_batch: &RecordBatchData,
        ctx: &mut Context<(), ()>,
    ) {
        // info!("record batch grpc received batch {:?}", record_batch);
        let json_rows = record_batches_to_json_rows(&[&record_batch.0]).unwrap();
        for map in json_rows {
            let value = serde_json::to_string(&map).unwrap();
            //info!("sending map {:?}", value);
            self.client
                .as_mut()
                .unwrap()
                .send_sink_data(SinkDataReq {
                    job_id: ctx.task_info.job_id.clone(),
                    operator_id: ctx.task_info.operator_id.clone(),
                    subtask_index: ctx.task_info.task_index as u32,
                    timestamp: to_micros(SystemTime::now()),
                    key: value.clone(),
                    value,
                    done: false,
                })
                .await
                .unwrap();
        }
    }
}
