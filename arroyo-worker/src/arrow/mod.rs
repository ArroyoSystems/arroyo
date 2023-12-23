use anyhow::Result;
use apache_avro::GenericSingleObjectReader;
use arrow::compute::kernels;
use arrow::datatypes::SchemaRef;
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::PrimitiveArray;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_json::writer::record_batches_to_json_rows;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arroyo_df::physical::SingleLockedBatch;
use arroyo_formats::SchemaData;
use arroyo_rpc::grpc::api::{ConnectorOp, MemTableScan, PeriodicWatermark};
use arroyo_rpc::grpc::controller_grpc_client::ControllerGrpcClient;
use arroyo_rpc::grpc::{api, SinkDataReq};
use arroyo_types::{ArrowMessage, from_millis, Record};
use arroyo_types::from_nanos;
use arroyo_types::to_micros;
use arroyo_types::to_nanos;
use arroyo_types::KeyValueTimestampRecordBatch;
use arroyo_types::KeyValueTimestampRecordBatchBuilder;
use arroyo_types::Message;
use arroyo_types::RecordBatchBuilder;
use arroyo_types::Watermark;
use arroyo_types::{Key, RecordBatchData};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DFSchemaRef;
use datafusion_common::DataFusionError;
use datafusion_common::ScalarValue;
use datafusion_common::{DFSchema, Result as DFResult};
use datafusion_execution::runtime_env::RuntimeConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::FunctionRegistry;
use datafusion_execution::TaskContext;
use datafusion_expr::AggregateUDF;
use datafusion_expr::ScalarUDF;
use datafusion_expr::WindowUDF;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::PhysicalExprNode;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message as ProstMessage;
use rand::SeedableRng;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::SystemTime;
use tonic::transport::Channel;
use tracing::info;
use crate::engine::ArrowContext;

use crate::operator::{ArrowOperator, ArrowOperatorConstructor};

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
        let empty_partition_scheme: SingleLockedBatch = serde_json::from_slice(buf)
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

impl ArrowOperatorConstructor<api::ValuePlanOperator, Self> for ValueExecutionOperator {
    fn from_config(config: api::ValuePlanOperator) -> Result<Self> {
        let locked_batch = Arc::new(RwLock::default());
        let registry = Registry {};

        let plan = PhysicalPlanNode::decode(&mut config.physical_plan.as_slice()).unwrap();
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
            name: config.name,
            locked_batch,
            execution_plan,
        })
    }
}

#[async_trait::async_trait]
impl ArrowOperator for ValueExecutionOperator {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_batch(
        &mut self,
        record_batch: RecordBatch,
        ctx: &mut ArrowContext,
    ) {
        //info!("incoming record batch {:?}", record_batch);
        let batch = record_batch.clone();
        {
            let mut writer = self.locked_batch.write().unwrap();
            *writer = Some(batch);
        }
        let session_context = SessionContext::new();
        let mut records = self
            .execution_plan
            .execute(0, session_context.task_ctx())
            .expect("successfully computed?");
        while let Some(batch) = records.next().await {
            let batch = batch.expect("should be able to compute batch");
            //info!("batch {:?}", batch);
            ctx.collect(batch).await;
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

    fn statistics(&self) -> DFResult<datafusion_common::Statistics> {
        todo!()
    }
}

pub struct KeyExecutionOperator {
    name: String,
    locked_batch: Arc<RwLock<Option<RecordBatch>>>,
    execution_plan: Arc<dyn ExecutionPlan>,
    key_fields: Vec<usize>,
}

impl ArrowOperatorConstructor<api::KeyPlanOperator, Self> for KeyExecutionOperator {
    fn from_config(config: api::KeyPlanOperator) -> Result<Self> {
        let locked_batch = Arc::new(RwLock::default());
        let registry = Registry {};

        let plan = PhysicalPlanNode::decode(&mut config.physical_plan.as_slice()).unwrap();
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
            name: config.name,
            locked_batch,
            execution_plan,
            key_fields: config
                .key_fields
                .into_iter()
                .map(|field| field as usize)
                .collect(),
        })
    }
}

#[async_trait::async_trait]
impl ArrowOperator for KeyExecutionOperator {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut ArrowContext,
    ) {
        //info!("incoming record batch {:?}", record_batch);
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
            ctx.collect(batch).await;
        }
    }
}


pub struct DefaultTimestampWatermark {
}

impl ArrowOperatorConstructor<api::PeriodicWatermark, Self> for DefaultTimestampWatermark {
    fn from_config(config: PeriodicWatermark) -> Result<Self> {
        Ok(DefaultTimestampWatermark { })
    }
}

#[async_trait::async_trait]
impl ArrowOperator for DefaultTimestampWatermark {
    fn name(&self) -> String {
        "Watermark".to_string()
    }

    async fn process_batch(
        &mut self,
        record_batch: RecordBatch,
        ctx: &mut ArrowContext,
    ) {
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
            
            let time_column = current_time_scalar
                .to_array_of_size(record_batch.num_rows())
                .unwrap();
            
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
        ctx.collect(output_batch).await;

        ctx.collector
            .broadcast(ArrowMessage::Watermark(Watermark::EventTime(watermark)))
            .await;
    }

    async fn on_close(&mut self, ctx: &mut ArrowContext) {
        // send final watermark on close
        ctx.collector
            .broadcast(ArrowMessage::Watermark(Watermark::EventTime(from_millis(
                u64::MAX,
            ))))
            .await;
    }
}

#[derive(Default)]
pub struct GrpcRecordBatchSink {
    client: Option<ControllerGrpcClient<Channel>>,
}

impl ArrowOperatorConstructor<api::ConnectorOp, Self> for GrpcRecordBatchSink {
    fn from_config(_: ConnectorOp) -> Result<Self> {
        Ok(Self {
            client: None,
        })
    }
}

#[async_trait::async_trait]
impl ArrowOperator for GrpcRecordBatchSink {
    fn name(&self) -> String {
        "GRPC".to_string()
    }

    async fn on_start(&mut self, _: &mut ArrowContext) {
        let controller_addr = std::env::var(arroyo_types::CONTROLLER_ADDR_ENV)
            .unwrap_or_else(|_| crate::LOCAL_CONTROLLER_ADDR.to_string());

        self.client = Some(
            ControllerGrpcClient::connect(controller_addr)
                .await
                .unwrap(),
        );
    }

    async fn process_batch(
        &mut self,
        record_batch: RecordBatch,
        ctx: &mut ArrowContext,
    ) {
        // info!("record batch grpc received batch {:?}", record_batch);
        let json_rows = record_batches_to_json_rows(&[&record_batch]).unwrap();
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
