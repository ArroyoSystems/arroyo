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
use arrow_schema::{DataType, Field, FieldRef, Schema, TimeUnit};
use arroyo_df::schemas::window_arrow_struct;
use arroyo_rpc::{
    grpc::{api, TableConfig},
    ArroyoSchema,
};
use arroyo_state::timestamp_table_config;
use arroyo_types::{from_nanos, print_time, to_nanos, CheckpointBarrier, Watermark};
use datafusion::{execution::context::SessionContext, physical_plan::ExecutionPlan};
use datafusion_common::ScalarValue;
use futures::{stream::FuturesUnordered, StreamExt};

use crate::engine::ArrowContext;
use crate::operator::{ArrowOperator, ArrowOperatorConstructor, OperatorNode};

use arroyo_df::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use datafusion_execution::{
    runtime_env::{RuntimeConfig, RuntimeEnv},
    SendableRecordBatchStream,
};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_proto::{
    physical_plan::{from_proto::parse_physical_expr, AsExecutionPlan},
    protobuf::{PhysicalExprNode, PhysicalPlanNode},
};
use prost::Message;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tracing::info;

use super::{sync::streams::KeyedCloneableStreamFuture, EmptyRegistry};
type NextBatchFuture<K> = KeyedCloneableStreamFuture<K, SendableRecordBatchStream>;

pub struct TumblingAggregatingWindowFunc<K: Copy> {
    width: Duration,
    binning_function: Arc<dyn PhysicalExpr>,
    partial_aggregation_plan: Arc<dyn ExecutionPlan>,
    partial_schema: ArroyoSchema,
    finish_execution_plan: Arc<dyn ExecutionPlan>,
    // the partial aggregation plan shares a reference to it,
    // which is only used on the exec()
    receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    final_batches_passer: Arc<RwLock<Vec<RecordBatch>>>,
    futures: Arc<Mutex<FuturesUnordered<NextBatchFuture<K>>>>,
    execs: BTreeMap<K, BinComputingHolder<K>>,
    window_field: FieldRef,
    window_index: usize,
}

impl<K: Copy> TumblingAggregatingWindowFunc<K> {
    fn bin_start(&self, timestamp: SystemTime) -> SystemTime {
        if self.width == Duration::ZERO {
            return timestamp;
        }
        let mut nanos = to_nanos(timestamp);
        nanos -= nanos % self.width.as_nanos();
        let result = from_nanos(nanos);
        result
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

impl TumblingAggregatingWindowFunc<SystemTime> {}

impl ArrowOperatorConstructor<api::TumblingWindowAggregateOperator>
    for TumblingAggregatingWindowFunc<SystemTime>
{
    fn from_config(config: api::TumblingWindowAggregateOperator) -> Result<OperatorNode> {
        let width = Duration::from_micros(config.width_micros);
        let input_schema: ArroyoSchema = config
            .input_schema
            .ok_or_else(|| anyhow!("requires input schema"))?
            .try_into()?;
        let binning_function = PhysicalExprNode::decode(&mut config.binning_function.as_slice())?;
        let binning_function =
            parse_physical_expr(&binning_function, &EmptyRegistry {}, &input_schema.schema)?;

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
            &EmptyRegistry {},
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
            &EmptyRegistry {},
            &RuntimeEnv::new(RuntimeConfig::new()).unwrap(),
            &final_codec,
        )?;
        let window_field = Arc::new(Field::new(
            config.window_field_name,
            window_arrow_struct(),
            true,
        ));

        Ok(OperatorNode::from_operator(Box::new(Self {
            width,
            binning_function,
            partial_aggregation_plan,
            partial_schema,
            finish_execution_plan,
            receiver,
            final_batches_passer,
            futures: Arc::new(Mutex::new(FuturesUnordered::new())),
            execs: BTreeMap::new(),
            window_field,
            window_index: config.window_index as usize,
        })))
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
                return;
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

    async fn handle_watermark(&mut self, _watermark: Watermark, ctx: &mut ArrowContext) {
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
                    let start = SystemTime::now();
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
                    let mut final_exec = self
                        .finish_execution_plan
                        .execute(0, SessionContext::new().task_ctx())
                        .unwrap();
                    while let Some(batch) = final_exec.next().await {
                        let batch = batch.expect("should be able to compute batch");
                        let bin_start = to_nanos(popped_bin) as i64;
                        let bin_end = to_nanos(popped_bin + self.width) as i64;
                        let timestamp = bin_end - 1;

                        let timestamp_array =
                            ScalarValue::TimestampNanosecond(Some(timestamp), None)
                                .to_array_of_size(batch.num_rows())
                                .unwrap();
                        let mut fields = batch.schema().fields().as_ref().to_vec();
                        fields.push(Arc::new(Field::new(
                            "_timestamp",
                            DataType::Timestamp(TimeUnit::Nanosecond, None),
                            false,
                        )));

                        fields.insert(self.window_index, self.window_field.clone());

                        let mut columns = batch.columns().to_vec();
                        columns.push(timestamp_array);
                        let DataType::Struct(struct_fields) = self.window_field.data_type() else {
                            unreachable!("should have struct for window field type")
                        };
                        let window_scalar = ScalarValue::Struct(
                            Some(vec![
                                ScalarValue::TimestampNanosecond(Some(bin_start), None),
                                ScalarValue::TimestampNanosecond(Some(bin_end), None),
                            ]),
                            struct_fields.clone(),
                        );
                        columns.insert(
                            self.window_index,
                            window_scalar.to_array_of_size(batch.num_rows()).unwrap(),
                        );

                        let batch_with_timestamp = RecordBatch::try_new(
                            Arc::new(Schema::new_with_metadata(fields, HashMap::new())),
                            columns,
                        )
                        .unwrap();
                        ctx.collect(batch_with_timestamp).await;
                    }
                    info!(
                        "flushing bin {:?} took {:?}",
                        popped_bin,
                        start.elapsed().unwrap()
                    );
                } else {
                    break;
                }
            }
        }
    }

    fn future_to_poll(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>> {
        let future = self.futures.clone();
        Some(Box::pin(async move {
            let result: Option<PolledFutureT> = future.lock().await.next().await;
            Box::new(result) as Box<dyn Any + Send>
        }))
    }

    async fn handle_future_result(&mut self, result: Box<dyn Any + Send>, _: &mut ArrowContext) {
        let data: Box<Option<PolledFutureT>> = result.downcast().expect("invalid data in future");
        match *data {
            Some((bin, batch_option)) => match batch_option {
                None => {
                    info!("future for {} was finished elsewhere", print_time(bin));
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
            },
            None => {}
        }
    }

    async fn handle_checkpoint(&mut self, _b: CheckpointBarrier, ctx: &mut ArrowContext) {
        let watermark = ctx
            .watermark()
            .map(|watermark: Watermark| match watermark {
                Watermark::EventTime(watermark) => Some(watermark),
                Watermark::Idle => None,
            })
            .flatten();
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("t", watermark)
            .await
            .expect("should get table");

        // TODO: this was a separate map just to the active execs, which could, in corner cases, be much smaller.
        for (bin, exec) in self.execs.iter_mut() {
            exec.sender.take();
            let bucket_nanos = to_nanos(*bin) as i64;
            let mut active_exec = exec.active_exec.take().expect("this should be active");
            while let (_bin_, Some((batch, next_exec))) = active_exec.await {
                active_exec = next_exec;
                let batch = batch.expect("should be able to compute batch");
                let bin_start = ScalarValue::TimestampNanosecond(Some(bucket_nanos), None);
                let timestamp_array = bin_start.to_array_of_size(batch.num_rows()).unwrap();
                let mut columns = batch.columns().to_vec();
                columns.push(timestamp_array);
                let state_batch =
                    RecordBatch::try_new(self.partial_schema.schema.clone(), columns).unwrap();
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
                self.partial_schema.clone(),
            ),
        )]
        .into_iter()
        .collect()
    }
}
