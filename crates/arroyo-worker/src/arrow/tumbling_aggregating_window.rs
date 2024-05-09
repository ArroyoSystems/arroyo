use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::{
    collections::{BTreeMap, HashMap},
    mem,
    sync::{Arc, RwLock},
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use arrow::compute::{partition, sort_to_indices, take};
use arrow_array::{types::TimestampNanosecondType, Array, PrimitiveArray, RecordBatch};
use arrow_schema::SchemaRef;
use arroyo_df::schemas::add_timestamp_field_arrow;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::{ArrowOperator, OperatorConstructor, OperatorNode, Registry};
use arroyo_rpc::grpc::{api, TableConfig};
use arroyo_state::timestamp_table_config;
use arroyo_types::{from_nanos, print_time, to_nanos, CheckpointBarrier, Watermark};
use datafusion::common::ScalarValue;
use datafusion::{execution::context::SessionContext, physical_plan::ExecutionPlan};
use futures::{stream::FuturesUnordered, StreamExt};

use arroyo_df::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use arroyo_rpc::df::ArroyoSchema;
use datafusion::execution::{
    runtime_env::{RuntimeConfig, RuntimeEnv},
    SendableRecordBatchStream,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::{
    physical_plan::{from_proto::parse_physical_expr, AsExecutionPlan},
    protobuf::{PhysicalExprNode, PhysicalPlanNode},
};
use prost::Message;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use super::sync::streams::KeyedCloneableStreamFuture;
type NextBatchFuture<K> = KeyedCloneableStreamFuture<K, SendableRecordBatchStream>;

pub struct TumblingAggregatingWindowFunc<K: Copy> {
    width: Duration,
    binning_function: Arc<dyn PhysicalExpr>,
    partial_aggregation_plan: Arc<dyn ExecutionPlan>,
    partial_schema: ArroyoSchema,
    finish_execution_plan: Arc<dyn ExecutionPlan>,
    aggregate_with_timestamp_schema: SchemaRef,
    final_projection: Option<Arc<dyn ExecutionPlan>>,
    // the partial aggregation plan shares a reference to it,
    // which is only used on the exec()
    receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    final_batches_passer: Arc<RwLock<Vec<RecordBatch>>>,
    futures: Arc<Mutex<FuturesUnordered<NextBatchFuture<K>>>>,
    execs: BTreeMap<K, BinComputingHolder<K>>,
}

impl<K: Copy> TumblingAggregatingWindowFunc<K> {
    fn bin_start(&self, timestamp: SystemTime) -> SystemTime {
        if self.width == Duration::ZERO {
            return timestamp;
        }
        let mut nanos = to_nanos(timestamp);
        nanos -= nanos % self.width.as_nanos();

        from_nanos(nanos)
    }
}

struct BinComputingHolder<K: Copy> {
    active_exec: Option<NextBatchFuture<K>>,
    finished_batches: Vec<RecordBatch>,
    sender: Option<UnboundedSender<RecordBatch>>,
}

impl<K: Copy> Default for BinComputingHolder<K> {
    fn default() -> Self {
        Self {
            active_exec: None,
            finished_batches: Vec::new(),
            sender: None,
        }
    }
}

type PolledFutureT = <NextBatchFuture<SystemTime> as Future>::Output;

impl TumblingAggregatingWindowFunc<SystemTime> {
    fn add_bin_start_as_timestamp(
        batch: &RecordBatch,
        bin_start: SystemTime,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let bin_start = ScalarValue::TimestampNanosecond(Some(to_nanos(bin_start) as i64), None);
        let timestamp_array = bin_start.to_array_of_size(batch.num_rows()).unwrap();
        let mut columns = batch.columns().to_vec();
        columns.push(timestamp_array);
        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|err| anyhow::anyhow!("schema: {:?}\nbatch:{:?}\nerr:{}", schema, batch, err))
    }
}

pub struct TumblingAggregateWindowConstructor;

impl OperatorConstructor for TumblingAggregateWindowConstructor {
    type ConfigT = api::TumblingWindowAggregateOperator;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<OperatorNode> {
        let width = Duration::from_micros(config.width_micros);
        let input_schema: ArroyoSchema = config
            .input_schema
            .ok_or_else(|| anyhow!("requires input schema"))?
            .try_into()?;
        let binning_function = PhysicalExprNode::decode(&mut config.binning_function.as_slice())?;
        let binning_function = parse_physical_expr(
            &binning_function,
            registry.as_ref(),
            &input_schema.schema,
            &DefaultPhysicalExtensionCodec {},
        )?;

        let receiver = Arc::new(RwLock::new(None));
        let final_batches_passer = Arc::new(RwLock::new(Vec::new()));

        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::UnboundedBatchStream(receiver.clone()),
        };

        let partial_aggregation_plan =
            PhysicalPlanNode::decode(&mut config.partial_aggregation_plan.as_slice())?;

        // deserialize partial aggregation into execution plan with an UnboundedBatchStream source.
        // this is behind a RwLock and will have a new channel swapped in before computation is initialized
        // for each bin.
        let partial_aggregation_plan = partial_aggregation_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;

        let partial_schema = config
            .partial_schema
            .ok_or_else(|| anyhow!("requires partial schema"))?
            .try_into()?;

        let finish_plan = PhysicalPlanNode::decode(&mut config.final_aggregation_plan.as_slice())?;

        let final_codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::LockedBatchVec(final_batches_passer.clone()),
        };

        // deserialize the finish plan to read directly from a Vec<RecordBatch> behind a RWLock.
        let finish_execution_plan = finish_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::new(RuntimeConfig::new()).unwrap(),
            &final_codec,
        )?;
        let finish_projection = config
            .final_projection
            .map(|proto| PhysicalPlanNode::decode(&mut proto.as_slice()))
            .transpose()?;

        let final_projection_plan = finish_projection
            .map(|finish_projection| {
                finish_projection.try_into_physical_plan(
                    registry.as_ref(),
                    &RuntimeEnv::new(RuntimeConfig::new()).unwrap(),
                    &final_codec,
                )
            })
            .transpose()?;

        let aggregate_with_timestamp_schema =
            add_timestamp_field_arrow(finish_execution_plan.schema());

        Ok(OperatorNode::from_operator(Box::new(
            TumblingAggregatingWindowFunc {
                width,
                binning_function,
                partial_aggregation_plan,
                partial_schema,
                finish_execution_plan,
                aggregate_with_timestamp_schema,
                final_projection: final_projection_plan,
                receiver,
                final_batches_passer,
                futures: Arc::new(Mutex::new(FuturesUnordered::new())),
                execs: BTreeMap::new(),
            },
        )))
    }
}

#[async_trait::async_trait]
impl ArrowOperator for TumblingAggregatingWindowFunc<SystemTime> {
    fn name(&self) -> String {
        "tumbling_window".to_string()
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        let watermark = ctx.last_present_watermark();
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("t", watermark)
            .await
            .expect("should be able to load table");
        for (timestamp, batch) in table.all_batches_for_watermark(watermark) {
            let bin = self.bin_start(*timestamp);
            let holder = self.execs.entry(bin).or_default();
            batch
                .iter()
                .for_each(|batch| holder.finished_batches.push(batch.clone()));
        }
    }

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext) {
        let bin = self
            .binning_function
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let indices = sort_to_indices(bin.as_ref(), None, None).unwrap();
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).unwrap())
            .collect();
        let sorted = RecordBatch::try_new(batch.schema(), columns).unwrap();
        let sorted_bins = take(&*bin, &indices, None).unwrap();

        let partition = partition(vec![sorted_bins.clone()].as_slice()).unwrap();
        let typed_bin = sorted_bins
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
            .unwrap();

        for range in partition.ranges() {
            // the binning function already rounded down to the bin start.
            let bin_start = from_nanos(typed_bin.value(range.start) as u128);
            let watermark = ctx.last_present_watermark();

            if watermark.is_some() && bin_start < self.bin_start(watermark.unwrap()) {
                warn!(
                    "bin start {} is before watermark {}, skipping",
                    print_time(bin_start),
                    print_time(watermark.unwrap())
                );
                continue;
            }

            let bin_batch = sorted.slice(range.start, range.end - range.start);
            let bin_exec = self.execs.entry(bin_start).or_default();
            if bin_exec.active_exec.is_none() {
                let (unbounded_sender, unbounded_receiver) = unbounded_channel();
                bin_exec.sender = Some(unbounded_sender);
                {
                    let mut internal_receiver = self.receiver.write().unwrap();
                    *internal_receiver = Some(unbounded_receiver);
                }
                self.partial_aggregation_plan.reset().unwrap();
                let new_exec = self
                    .partial_aggregation_plan
                    .execute(0, SessionContext::new().task_ctx())
                    .unwrap();
                let next_batch_future = NextBatchFuture::new(bin_start, new_exec);
                self.futures.lock().await.push(next_batch_future.clone());
                bin_exec.active_exec = Some(next_batch_future);
            }
            bin_exec
                .sender
                .as_ref()
                .expect("just set this")
                .send(bin_batch)
                .unwrap();
        }
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut ArrowContext,
    ) -> Option<Watermark> {
        if let Some(watermark) = ctx.last_present_watermark() {
            let bin = self.bin_start(watermark);
            while !self.execs.is_empty() {
                let should_pop = {
                    let Some((first_bin, _exec)) = self.execs.first_key_value() else {
                        unreachable!("isn't empty")
                    };
                    *first_bin < bin
                };
                if should_pop {
                    let Some((popped_bin, mut exec)) = self.execs.pop_first() else {
                        unreachable!("should have an entry")
                    };
                    if let Some(mut active_exec) = exec.active_exec.take() {
                        exec.sender.take();
                        while let (_bin, Some((batch, new_exec))) = active_exec.await {
                            active_exec = new_exec;
                            let batch = batch.expect("should be able to compute batch");
                            exec.finished_batches.push(batch);
                        }
                    }
                    {
                        let mut batches = self.final_batches_passer.write().unwrap();
                        let finished_batches = mem::take(&mut exec.finished_batches);
                        *batches = finished_batches;
                    }
                    self.finish_execution_plan
                        .reset()
                        .expect("reset execution plan");
                    let mut final_exec = self
                        .finish_execution_plan
                        .execute(0, SessionContext::new().task_ctx())
                        .unwrap();
                    let mut aggregate_results = vec![];
                    while let Some(batch) = final_exec.next().await {
                        let batch = batch.expect("should be able to compute batch");
                        let with_timestamp = Self::add_bin_start_as_timestamp(
                            &batch,
                            popped_bin,
                            self.aggregate_with_timestamp_schema.clone(),
                        )
                        .expect("should be able to add timestamp");
                        if self.final_projection.is_some() {
                            aggregate_results.push(with_timestamp);
                        } else {
                            ctx.collect(with_timestamp).await;
                        }
                    }
                    if let Some(final_projection) = self.final_projection.as_ref() {
                        {
                            let mut batches = self.final_batches_passer.write().unwrap();
                            *batches = aggregate_results;
                        }
                        final_projection.reset().expect("reset execution plan");
                        let mut final_projection_exec = final_projection
                            .execute(0, SessionContext::new().task_ctx())
                            .unwrap();
                        while let Some(batch) = final_projection_exec.next().await {
                            let batch = batch.expect("should be able to compute batch");
                            ctx.collect(batch).await;
                        }
                    }
                } else {
                    break;
                }
            }
        }
        Some(watermark)
    }

    fn future_to_poll(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>> {
        let future = self.futures.clone();
        Some(Box::pin(async move {
            let mut future = future.lock().await;
            let result: Option<PolledFutureT> = if future.is_empty() {
                futures::future::pending().await
            } else {
                future.next().await
            };
            Box::new(result) as Box<dyn Any + Send>
        }))
    }

    async fn handle_future_result(&mut self, result: Box<dyn Any + Send>, _: &mut ArrowContext) {
        let data: Box<Option<PolledFutureT>> = result.downcast().expect("invalid data in future");
        if let Some((bin, batch_option)) = *data {
            match batch_option {
                None => {
                    debug!("future for {} was finished elsewhere", print_time(bin));
                }
                Some((batch, future)) => match self.execs.get_mut(&bin) {
                    Some(exec) => {
                        exec.finished_batches
                            .push(batch.expect("should've been able to compute a batch"));
                        self.futures.lock().await.push(future);
                    }
                    None => unreachable!(
                        "FuturesUnordered returned a batch, but we can't find the exec"
                    ),
                },
            }
        }
    }

    async fn handle_checkpoint(&mut self, _b: CheckpointBarrier, ctx: &mut ArrowContext) {
        let watermark = ctx
            .watermark()
            .and_then(|watermark: Watermark| match watermark {
                Watermark::EventTime(watermark) => Some(watermark),
                Watermark::Idle => None,
            });
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("t", watermark)
            .await
            .expect("should get table");

        // This was a separate map just to the active execs, which could, in corner cases, be much smaller.
        for (bin, exec) in self.execs.iter_mut() {
            exec.sender.take();
            let Some(mut active_exec) = exec.active_exec.take() else {
                continue;
            };
            while let (_bin_, Some((batch, next_exec))) = active_exec.await {
                active_exec = next_exec;
                let batch = batch.expect("should be able to compute batch");
                let state_batch = Self::add_bin_start_as_timestamp(
                    &batch,
                    *bin,
                    self.partial_schema.schema.clone(),
                )
                .expect("should be able to add timestamp");
                table.insert(*bin, state_batch);
                exec.finished_batches.push(batch);
            }
        }
        table.flush(watermark).await.unwrap();
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        vec![(
            "t".to_string(),
            timestamp_table_config(
                "t",
                "tumbling_intermediate",
                self.width,
                false,
                self.partial_schema.clone(),
            ),
        )]
        .into_iter()
        .collect()
    }
}
