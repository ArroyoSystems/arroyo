use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use arroyo_df::physical::ArroyoPhysicalExtensionCodec;
use arroyo_df::physical::DecodingContext;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::{ArrowOperator, OperatorConstructor, OperatorNode, Registry};
use arroyo_rpc::grpc::api;
use datafusion::common::DataFusionError;
use datafusion::common::Result as DFResult;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{DisplayAs, PlanProperties};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message as ProstMessage;
use std::sync::Arc;
use std::sync::RwLock;

pub mod async_udf;
pub mod instant_join;
pub mod join_with_expiration;
pub mod session_aggregating_window;
pub mod sliding_aggregating_window;
pub(crate) mod sync;
pub mod tumbling_aggregating_window;
pub mod updating_aggregator;
pub mod watermark_generator;
pub mod window_fn;

pub struct ValueExecutionOperator {
    name: String,
    executor: StatelessPhysicalExecutor,
}

pub struct ValueExecutionConstructor;
impl OperatorConstructor for ValueExecutionConstructor {
    type ConfigT = api::ValuePlanOperator;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<OperatorNode> {
        let executor = StatelessPhysicalExecutor::new(&config.physical_plan, &registry)?;
        Ok(OperatorNode::from_operator(Box::new(
            ValueExecutionOperator {
                name: config.name,
                executor,
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
        let mut records = self.executor.process_batch(record_batch).await;
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
    plan_properties: PlanProperties,
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

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("not supported".into()))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<datafusion::execution::SendableRecordBatchStream> {
        let result = self.locked_batch.read().unwrap().clone().unwrap();
        Ok(Box::pin(MemoryStream::try_new(
            vec![result],
            self.schema.clone(),
            None,
        )?))
    }

    fn statistics(&self) -> DFResult<datafusion::common::Statistics> {
        todo!()
    }

    fn reset(&self) -> DFResult<()> {
        Ok(())
    }
}

pub struct KeyExecutionOperator {
    name: String,
    executor: StatelessPhysicalExecutor,
    #[allow(unused)]
    key_fields: Vec<usize>,
}

pub struct KeyExecutionConstructor;

impl OperatorConstructor for KeyExecutionConstructor {
    type ConfigT = api::KeyPlanOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<OperatorNode> {
        let executor = StatelessPhysicalExecutor::new(&config.physical_plan, &registry)?;

        Ok(OperatorNode::from_operator(Box::new(
            KeyExecutionOperator {
                name: config.name,
                executor,
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
        let mut records = self.executor.process_batch(batch).await;
        while let Some(batch) = records.next().await {
            let batch = batch.expect("should be able to compute batch");
            //TODO: sort by the key
            //info!("batch {:?}", batch);
            ctx.collect(batch).await;
        }
    }
}

pub struct StatelessPhysicalExecutor {
    batch: Arc<RwLock<Option<RecordBatch>>>,
    plan: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
}

impl StatelessPhysicalExecutor {
    pub fn new(mut proto: &[u8], registry: &Registry) -> anyhow::Result<Self> {
        let batch = Arc::new(RwLock::default());

        let plan = PhysicalPlanNode::decode(&mut proto).unwrap();
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::SingleLockedBatch(batch.clone()),
        };

        let plan =
            plan.try_into_physical_plan(registry, &RuntimeEnv::new(RuntimeConfig::new())?, &codec)?;

        Ok(Self {
            batch,
            plan,
            task_context: SessionContext::new().task_ctx(),
        })
    }

    pub async fn process_batch(&mut self, batch: RecordBatch) -> SendableRecordBatchStream {
        {
            let mut writer = self.batch.write().unwrap();
            *writer = Some(batch);
        }
        self.plan.reset().expect("reset execution plan");
        self.plan
            .execute(0, self.task_context.clone())
            .expect("successfully computed?")
    }

    pub async fn process_single(&mut self, batch: RecordBatch) -> RecordBatch {
        let mut stream = self.process_batch(batch).await;
        let result = stream.next().await.unwrap().unwrap();
        assert!(
            stream.next().await.is_none(),
            "Should only produce one output batch"
        );
        result
    }
}
