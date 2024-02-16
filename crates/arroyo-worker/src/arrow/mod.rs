use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use arroyo_df::physical::ArroyoPhysicalExtensionCodec;
use arroyo_df::physical::DecodingContext;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::{ArrowOperator, OperatorConstructor, OperatorNode};
use arroyo_rpc::grpc::api;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use datafusion_execution::runtime_env::RuntimeConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::{FunctionRegistry, TaskContext};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message as ProstMessage;
use std::sync::Arc;
use std::sync::RwLock;

pub mod join_with_expiration;
pub mod session_aggregating_window;
pub mod sliding_aggregating_window;
pub(crate) mod sync;
pub mod tumbling_aggregating_window;

pub struct ValueExecutionOperator {
    name: String,
    locked_batch: Arc<RwLock<Option<RecordBatch>>>,
    execution_plan: Arc<dyn ExecutionPlan>,
}

pub struct ValueExecutionConstructor;
impl OperatorConstructor for ValueExecutionConstructor {
    type ConfigT = api::ValuePlanOperator;
    fn with_config(
        &self,
        config: api::ValuePlanOperator,
        registry: Arc<dyn FunctionRegistry>,
    ) -> Result<OperatorNode> {
        let locked_batch = Arc::new(RwLock::default());

        let plan = PhysicalPlanNode::decode(&mut config.physical_plan.as_slice()).unwrap();

        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::SingleLockedBatch(locked_batch.clone()),
        };

        let execution_plan = plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::new(RuntimeConfig::new())?,
            &codec,
        )?;

        Ok(OperatorNode::from_operator(Box::new(
            ValueExecutionOperator {
                name: config.name,
                locked_batch,
                execution_plan,
            },
        )))
    }
}

#[async_trait::async_trait]
impl ArrowOperator for ValueExecutionOperator {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_batch(&mut self, record_batch: RecordBatch, ctx: &mut ArrowContext) {
        {
            let mut writer = self.locked_batch.write().unwrap();
            *writer = Some(record_batch);
        }

        let session_context = SessionContext::new();
        let mut records = self
            .execution_plan
            .execute(0, session_context.task_ctx())
            .expect("successfully computed?");
        while let Some(batch) = records.next().await {
            let batch = batch.expect("should be able to compute batch");
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
    #[allow(unused)]
    key_fields: Vec<usize>,
}

pub struct KeyExecutionConstructor;

impl OperatorConstructor for KeyExecutionConstructor {
    type ConfigT = api::KeyPlanOperator;

    fn with_config(
        &self,
        config: api::KeyPlanOperator,
        registry: Arc<dyn FunctionRegistry>,
    ) -> Result<OperatorNode> {
        let locked_batch = Arc::new(RwLock::default());

        let plan = PhysicalPlanNode::decode(&mut config.physical_plan.as_slice()).unwrap();
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::SingleLockedBatch(locked_batch.clone()),
        };

        let execution_plan = plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::new(RuntimeConfig::new())?,
            &codec,
        )?;

        Ok(OperatorNode::from_operator(Box::new(
            KeyExecutionOperator {
                name: config.name,
                locked_batch,
                execution_plan,
                key_fields: config
                    .key_fields
                    .into_iter()
                    .map(|field| field as usize)
                    .collect(),
            },
        )))
    }
}

#[async_trait::async_trait]
impl ArrowOperator for KeyExecutionOperator {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext) {
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
