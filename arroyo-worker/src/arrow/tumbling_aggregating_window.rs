use std::{
    collections::{BTreeMap, HashMap},
    mem,
    pin::Pin,
    sync::{Arc, RwLock},
    task::Poll,
    time::SystemTime,
};

use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use arrow::compute::{partition, sort_to_indices, take};
use arrow_array::{
    types::{Int64Type, TimestampNanosecondType},
    Array, PrimitiveArray, RecordBatch,
};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use arroyo_df::schemas::{add_timestamp_field_arrow, window_arrow_struct};
use arroyo_rpc::{
    grpc::{api, api::window::Window, TableConfig},
    ArroyoSchema,
};
use arroyo_state::timestamp_table_config;
use arroyo_types::{
    from_nanos, to_nanos, ArrowMessage, CheckpointBarrier, SignalMessage, Watermark,
};
use datafusion::{execution::context::SessionContext, physical_plan::{ExecutionPlan, aggregates::AggregateExec}};
use datafusion_common::ScalarValue;
use futures::stream::FuturesUnordered;

use crate::operator::{ArrowOperator, ArrowOperatorConstructor};
use crate::{engine::ArrowContext, operator::RunContext};
use arroyo_df::physical::{ArroyoMemExec, ArroyoPhysicalExtensionCodec, DecodingContext};
use datafusion_execution::{
    runtime_env::{RuntimeConfig, RuntimeEnv},
    FunctionRegistry, SendableRecordBatchStream,
};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_proto::{
    physical_plan::{from_proto::parse_physical_expr, AsExecutionPlan},
    protobuf::{
        physical_plan_node::PhysicalPlanType, AggregateMode, PhysicalExprNode, PhysicalPlanNode,
    },
};
use prost::Message;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_stream::{Stream, StreamExt};
use tracing::info;

use super::{sync::streams::KeyedCloneableStreamFuture, EmptyRegistry};

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
    futures: FuturesUnordered<NextBatchFuture<K>>,
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

type NextBatchFuture<K> = KeyedCloneableStreamFuture<K, SendableRecordBatchStream>;

impl ArrowOperatorConstructor<api::WindowAggregateOperator, Self>
    for TumblingAggregatingWindowFunc<SystemTime>
{
    fn from_config(proto_config: api::WindowAggregateOperator) -> Result<Self> {
        let binning_function =
            PhysicalExprNode::decode(&mut proto_config.binning_function.as_slice()).unwrap();
        let input_schema: Schema = serde_json::from_slice(proto_config.input_schema.as_slice())
            .context(format!(
                "failed to deserialize schema of length {}",
                proto_config.input_schema.len()
            ))?;

        let binning_function = parse_physical_expr(&binning_function, &EmptyRegistry {}, &input_schema)?;

        let mut physical_plan =
            PhysicalPlanNode::decode(&mut proto_config.physical_plan.as_slice()).unwrap();

        let Some(arroyo_rpc::grpc::api::Window {
            window: Some(Window::TumblingWindow(window)),
        }) = proto_config.window
        else {
            bail!("expected tumbling window")
        };
        let window_field = Arc::new(Field::new(
            proto_config.window_field_name,
            window_arrow_struct(),
            true,
        ));

        let receiver = Arc::new(RwLock::new(None));
        let final_batches_passer = Arc::new(RwLock::new(Vec::new()));

        let PhysicalPlanType::Aggregate(mut aggregate) = physical_plan
            .physical_plan_type
            .take()
            .ok_or_else(|| anyhow!("missing physical plan"))?
        else {
            bail!("expected aggregate physical plan, not {:?}", physical_plan);
        };

        let AggregateMode::Final = aggregate.mode() else {
            bail!("expect AggregateMode to be Final so we can decompose it for checkpointing.")
        };

        // pull the input out to be computed separately for each bin.
        let partial_aggregation_plan = aggregate.input.as_ref().unwrap();

        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::UnboundedBatchStream(receiver.clone()),
        };

        // deserialize partial aggregation into execution plan with an UnboundedBatchStream source.
        // this is behind a RwLock and will have a new channel swapped in before computation is initialized
        // for each bin.
        let partial_aggregation_plan = partial_aggregation_plan.try_into_physical_plan(
            &EmptyRegistry {},
            &RuntimeEnv::new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;

        let partial_schema = partial_aggregation_plan.schema();
        let table_provider = ArroyoMemExec {
            table_name: "partial".into(),
            schema: partial_schema,
        };

        // swap in an ArroyoMemExec as the source.
        // This is a flexible table source that can be decoded in various ways depending on what is needed.
        aggregate.input = Some(Box::new(PhysicalPlanNode::try_from_physical_plan(
            Arc::new(table_provider),
            &ArroyoPhysicalExtensionCodec::default(),
        )?));

        let finish_plan = PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Aggregate(aggregate)),
        };

        let final_codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::LockedBatchVec(final_batches_passer.clone()),
        };

        // deserialize the finish plan to read directly from a Vec<RecordBatch> behind a RWLock.
        let finish_execution_plan = finish_plan.try_into_physical_plan(
            &EmptyRegistry {},
            &RuntimeEnv::new(RuntimeConfig::new()).unwrap(),
            &final_codec,
        )?;

        // Calculate the partial schema so that it can be saved to state.
        let key_indices: Vec<_> = proto_config
            .key_fields
            .into_iter()
            .map(|x| x as usize)
            .collect();

        let schema_ref = partial_aggregation_plan.schema();
        // timestamp is stored in the bin, will be appended prior to writing to state.
        let partial_schema = add_timestamp_field_arrow(schema_ref);
        let timestamp_index = partial_schema.fields().len() - 1;
        let partial_schema = ArroyoSchema {
            schema: partial_schema,
            timestamp_index: timestamp_index,
            key_indices,
        };

        Ok(Self {
            width: Duration::from_micros(window.size_micros),
            binning_function,
            partial_aggregation_plan,
            partial_schema,
            finish_execution_plan,
            receiver,
            final_batches_passer,
            futures: FuturesUnordered::new(),
            execs: BTreeMap::new(),
            window_field,
            window_index: proto_config.window_index as usize,
        })
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
            let bin = from_nanos(typed_bin.value(range.start) as u128);
            let bin_batch = sorted.slice(range.start, range.end - range.start);
            let bin_exec = self.execs.entry(bin).or_default();
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
                let next_batch_future = NextBatchFuture::new(bin, new_exec);
                info!("created future for {:?}", bin);
                self.futures.push(next_batch_future.clone());
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

    async fn handle_watermark(&mut self, watermark: Watermark, ctx: &mut ArrowContext) {
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
                        let bin_start = to_nanos(bin) as i64;
                        let bin_end = to_nanos(bin + self.width) as i64;
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
        // by default, just pass watermarks on down
        ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(watermark)))
            .await;
    }

    async fn handle_checkpoint(&mut self, b: CheckpointBarrier, ctx: &mut ArrowContext) {
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

    async fn select_run<St: Stream<Item = (usize, ArrowMessage)> + Send + Sync + Unpin>(
        &mut self,
        ctx: &mut ArrowContext,
        run_context: &mut RunContext<St>,
    ) -> bool {
        tokio::select! {
            Some(control_message) = ctx.control_rx.recv() => {
                self.handle_controller_message(control_message, ctx).await;
            }

            p = run_context.sel.next() => {
                match p {
                    Some(((idx, message), s)) => {
                        return self.handle_message(idx, message, s, ctx, run_context).await;
                    }
                    None => {
                        info!("[{}] Stream completed",ctx.task_info.operator_name);
                        return true;
                    }
                }
            }
            Some((bin, batch_option)) = self.futures.next() => {
                match batch_option {
                    None => {
                            info!("future for {:?} was finished elsewhere", bin);
                    }
                    Some((batch, future)) => {
                        match self.execs.get_mut(&bin) {
                            Some(exec) => {
                                exec.finished_batches.push(batch.expect("should've been able to compute a batch"));
                                self.futures.push(future);
                            },
                            None => unreachable!("FuturesUnordered returned a batch, but we can't find the exec"),
                        }

                    }
                }
            }
            // TODO:
            /*_ = interval.tick() => {
                self.handle_tick(ticks, ctx).await;
                ticks += 1;
            }*/
        }
        false
    }
}
