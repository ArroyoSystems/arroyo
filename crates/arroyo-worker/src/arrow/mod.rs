use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{
    ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator, OperatorConstructor,
    Registry,
};
use arroyo_planner::physical::ArroyoPhysicalExtensionCodec;
use arroyo_planner::physical::DecodingContext;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::api;
use datafusion::common::Result as DFResult;
use datafusion::common::{internal_err, DataFusionError};
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{FunctionRegistry, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::physical_plan::{DisplayAs, PlanProperties};
use datafusion_proto::physical_plan::from_proto::{parse_physical_expr, parse_physical_sort_expr};
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
};
use datafusion_proto::protobuf::physical_aggregate_expr_node::AggregateFunction;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use datafusion_proto::protobuf::{proto_error, PhysicalExprNode, PhysicalPlanNode};
use futures::StreamExt;
use itertools::Itertools;
use prost::Message as ProstMessage;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::RwLock;

pub mod async_udf;
pub mod incremental_aggregator;
pub mod instant_join;
pub mod join_with_expiration;
pub mod lookup_join;
pub mod session_aggregating_window;
pub mod sliding_aggregating_window;
pub(crate) mod sync;
pub mod tumbling_aggregating_window;
mod updating_cache;
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
    ) -> anyhow::Result<ConstructedOperator> {
        let executor = StatelessPhysicalExecutor::new(&config.physical_plan, &registry)?;
        Ok(ConstructedOperator::from_operator(Box::new(
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

    fn display(&self) -> DisplayableOperator {
        DisplayableOperator {
            name: (&self.name).into(),
            fields: vec![("plan", (&*self.executor.plan).into())],
        }
    }

    async fn process_batch(
        &mut self,
        record_batch: RecordBatch,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        let mut records = self.executor.process_batch(record_batch).await;
        while let Some(batch) = records.next().await {
            let batch = batch.expect("should be able to compute batch");
            collector.collect(batch).await;
        }
    }
}

pub struct ProjectionOperator {
    name: String,
    output_schema: ArroyoSchema,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

pub struct ProjectionConstructor;
impl OperatorConstructor for ProjectionConstructor {
    type ConfigT = api::ProjectionOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let input_schema: ArroyoSchema = config.input_schema.unwrap().try_into()?;
        let output_schema: ArroyoSchema = config.output_schema.unwrap().try_into()?;

        let exprs: anyhow::Result<_> = config
            .exprs
            .iter()
            .map(|expr| {
                Ok(parse_physical_expr(
                    &PhysicalExprNode::decode(&mut expr.as_slice())?,
                    registry.as_ref(),
                    &input_schema.schema,
                    &DefaultPhysicalExtensionCodec {},
                )?)
            })
            .collect();

        Ok(ConstructedOperator::from_operator(Box::new(
            ProjectionOperator {
                name: config.name,
                output_schema,
                exprs: exprs?,
            },
        )))
    }
}

#[async_trait::async_trait]
impl ArrowOperator for ProjectionOperator {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn display(&self) -> DisplayableOperator {
        DisplayableOperator {
            name: (&self.name).into(),
            fields: vec![(
                "exprs",
                AsDisplayable::List(self.exprs.iter().map(|e| e.to_string()).collect()),
            )],
        }
    }

    async fn process_batch(
        &mut self,
        record_batch: RecordBatch,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        let outputs = self
            .exprs
            .iter()
            .map(|e| {
                e.evaluate(&record_batch)
                    .and_then(|f| f.into_array(record_batch.num_rows()))
                    .unwrap()
            })
            .collect_vec();

        collector
            .collect(RecordBatch::try_new(self.output_schema.schema.clone(), outputs).unwrap())
            .await;
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
    fn name(&self) -> &str {
        "rw_lock_reader"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
    ) -> anyhow::Result<ConstructedOperator> {
        let executor = StatelessPhysicalExecutor::new(&config.physical_plan, &registry)?;

        Ok(ConstructedOperator::from_operator(Box::new(
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

    fn display(&self) -> DisplayableOperator {
        DisplayableOperator {
            name: Cow::Borrowed(&self.name),
            fields: vec![
                ("key_fields", AsDisplayable::Debug(&self.key_fields)),
                ("plan", AsDisplayable::Plan(self.executor.plan.as_ref())),
            ],
        }
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        let mut records = self.executor.process_batch(batch).await;
        while let Some(batch) = records.next().await {
            let batch = batch.expect("should be able to compute batch");
            //TODO: sort by the key
            //info!("batch {:?}", batch);
            collector.collect(batch).await;
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
            plan.try_into_physical_plan(registry, &RuntimeEnvBuilder::new().build()?, &codec)?;

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
            .unwrap_or_else(|e| {
                panic!(
                    "failed to compute plan: {}\n{}",
                    e,
                    displayable(&*self.plan).indent(false)
                )
            })
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

pub fn decode_aggregate(
    schema: &SchemaRef,
    name: &str,
    expr: &PhysicalExprNode,
    registry: &dyn FunctionRegistry,
) -> DFResult<Arc<AggregateFunctionExpr>> {
    let codec = &DefaultPhysicalExtensionCodec {};
    let expr_type = expr
        .expr_type
        .as_ref()
        .ok_or_else(|| proto_error("Unexpected empty aggregate physical expression"))?;

    match expr_type {
        ExprType::AggregateExpr(agg_node) => {
            let input_phy_expr: Vec<Arc<dyn PhysicalExpr>> = agg_node
                .expr
                .iter()
                .map(|e| parse_physical_expr(e, registry, schema, codec))
                .collect::<DFResult<Vec<_>>>()?;
            let ordering_req: LexOrdering = agg_node
                .ordering_req
                .iter()
                .map(|e| parse_physical_sort_expr(e, registry, schema, codec))
                .collect::<DFResult<LexOrdering>>()?;
            agg_node
                .aggregate_function
                .as_ref()
                .map(|func| match func {
                    AggregateFunction::UserDefinedAggrFunction(udaf_name) => {
                        let agg_udf = match &agg_node.fun_definition {
                            Some(buf) => codec.try_decode_udaf(udaf_name, buf)?,
                            None => registry.udaf(udaf_name)?,
                        };

                        AggregateExprBuilder::new(agg_udf, input_phy_expr)
                            .schema(Arc::clone(schema))
                            .alias(name)
                            .with_ignore_nulls(agg_node.ignore_nulls)
                            .with_distinct(agg_node.distinct)
                            .order_by(ordering_req)
                            .build()
                            .map(Arc::new)
                    }
                })
                .transpose()?
                .ok_or_else(|| proto_error("Invalid AggregateExpr, missing aggregate_function"))
        }
        _ => internal_err!("Invalid aggregate expression for AggregateExec"),
    }
}
