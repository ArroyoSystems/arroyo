use anyhow::Result;
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
use arroyo_df::physical::ArroyoPhysicalExtensionCodec;
use arroyo_df::physical::DecodingContext;
use arroyo_formats::SchemaData;
use arroyo_rpc::grpc::api::{ConnectorOp, MemTableScan, PeriodicWatermark};
use arroyo_rpc::grpc::controller_grpc_client::ControllerGrpcClient;
use arroyo_rpc::grpc::{api, SinkDataReq};
use arroyo_types::{ArrowMessage, from_millis};
use arroyo_types::from_nanos;
use arroyo_types::to_micros;
use arroyo_types::to_nanos;
use arroyo_types::KeyValueTimestampRecordBatchBuilder;
use arroyo_types::Message;
use arroyo_types::RecordBatchBuilder;
use arroyo_types::Watermark;
use arroyo_types::{Key, Record, RecordBatchData};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_execution::runtime_env::RuntimeConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::FunctionRegistry;
use datafusion_execution::TaskContext;
use datafusion_expr::AggregateUDF;
use datafusion_expr::ScalarUDF;
use datafusion_expr::WindowUDF;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
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

    fn udf(&self, _name: &str) -> datafusion_common::Result<Arc<ScalarUDF>> {
        todo!()
    }

    fn udaf(&self, _name: &str) -> datafusion_common::Result<Arc<AggregateUDF>> {
        todo!()
    }

    fn udwf(&self, _name: &str) -> datafusion_common::Result<Arc<WindowUDF>> {
        todo!()
    }
}



pub struct ValueExecutionOperator {
    name: String,
    locked_batch: Arc<RwLock<Option<RecordBatch>>>,
    execution_plan: Arc<dyn ExecutionPlan>,
}


impl ArrowOperatorConstructor<api::ValuePlanOperator, Self> for ValueExecutionOperator {
    fn from_config(config: api::ValuePlanOperator) -> Result<Self> {
        let locked_batch = Arc::new(RwLock::default());
        let registry = Registry {};

        let plan = PhysicalPlanNode::decode(&mut config.physical_plan.as_slice()).unwrap();
        //info!("physical plan is {:#?}", plan);
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::SingleLockedBatch(locked_batch.clone()),
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
        _t: datafusion::physical_plan::DisplayFormatType,
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
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
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::SingleLockedBatch(locked_batch.clone()),
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
