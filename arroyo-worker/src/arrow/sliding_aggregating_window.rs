use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::{Display, Formatter},
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
    from_nanos, print_time, to_nanos, ArrowMessage, CheckpointBarrier, SignalMessage, Watermark,
};
use datafusion::{
    execution::context::SessionContext,
    physical_plan::{aggregates::AggregateExec, ExecutionPlan},
};
use datafusion_common::ScalarValue;
use datafusion_physical_plan::DisplayAs;
use futures::stream::FuturesUnordered;

use crate::operator::{ArrowOperator, ArrowOperatorConstructor, RunContext};
use crate::{engine::ArrowContext, operator::OperatorNode};
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

pub struct SlidingAggregatingWindowFunc<K: Copy> {
    slide: Duration,
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
    tiered_record_batches: TieredRecordBatchHolder,
    window_field: FieldRef,
    window_index: usize,
    state: SlidingWindowState,
}
#[derive(Debug)]
enum SlidingWindowState {
    // We haven't received any data.
    NoData,
    // We've received data, but don't have any data in the memory_view.
    OnlyBufferedData { earliest_bin_time: SystemTime },
    // There is data in memory_view waiting to be emitted.
    // will trigger on a watermark after next_window_start + self.slide
    InMemoryData { next_window_start: SystemTime },
}

impl Display for SlidingWindowState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SlidingWindowState::NoData => write!(f, "NoData"),
            SlidingWindowState::OnlyBufferedData { earliest_bin_time } => {
                write!(f, "OnlyBufferedData({})", print_time(*earliest_bin_time))
            }
            SlidingWindowState::InMemoryData { next_window_start } => {
                write!(f, "InMemoryData({})", print_time(*next_window_start))
            }
        }
    }
}

impl<K: Copy> SlidingAggregatingWindowFunc<K> {
    fn bin_start(&self, timestamp: SystemTime) -> SystemTime {
        if self.slide == Duration::ZERO {
            return timestamp;
        }
        let mut nanos = to_nanos(timestamp);
        nanos -= nanos % self.slide.as_nanos();
        let result = from_nanos(nanos);
        result
    }
}

impl SlidingAggregatingWindowFunc<SystemTime> {
    fn should_advance(&self, watermark: SystemTime) -> bool {
        let watermark_bin = self.bin_start(watermark);
        match self.state {
            SlidingWindowState::NoData => false,
            SlidingWindowState::OnlyBufferedData { earliest_bin_time } => {
                earliest_bin_time + self.slide <= watermark_bin
            }
            SlidingWindowState::InMemoryData { next_window_start } => {
                next_window_start + self.slide <= watermark_bin
            }
        }
    }

    async fn advance(&mut self, ctx: &mut ArrowContext) -> Result<()> {
        let bin_start = match self.state {
            SlidingWindowState::NoData => unreachable!(),
            SlidingWindowState::OnlyBufferedData { earliest_bin_time } => earliest_bin_time,
            SlidingWindowState::InMemoryData { next_window_start } => next_window_start,
        };
        let partial_table = ctx
            .table_manager
            .get_expiring_time_key_table("t", ctx.last_present_watermark())
            .await?;

        let bin_end = bin_start + self.slide;
        partial_table.flush(Some(bin_end)).await?;

        if let Some(mut bin_exec) = self.execs.remove(&bin_start) {
            // If there are any active computations, finish them and write them to state.
            if let Some(mut active_exec) = bin_exec.active_exec.take() {
                bin_exec.sender.take();
                let bucket_nanos = to_nanos(bin_start) as i64;
                while let (_bin, Some((batch, new_exec))) = active_exec.await {
                    active_exec = new_exec;
                    let batch = batch.expect("should be able to compute batch");

                    let bin_start_scalar =
                        ScalarValue::TimestampNanosecond(Some(bucket_nanos), None);
                    let timestamp_array =
                        bin_start_scalar.to_array_of_size(batch.num_rows()).unwrap();
                    let mut columns = batch.columns().to_vec();
                    columns.push(timestamp_array);
                    let state_batch =
                        RecordBatch::try_new(self.partial_schema.schema.clone(), columns).unwrap();
                    partial_table.insert(bin_start, state_batch);
                    bin_exec.finished_batches.push(batch);
                }
            }
            for batch in bin_exec.finished_batches {
                self.tiered_record_batches.insert(batch, bin_start)?;
            }
        }
        partial_table.flush_timestamp(bin_end).await?;
        self.tiered_record_batches
            .delete_before(bin_start - self.width)?;
        partial_table.expire_timestamp(bin_end - self.width + self.slide);
        let interval_start = bin_end - self.width;
        let interval_end = bin_end;
        {
            let mut batches = self.final_batches_passer.write().unwrap();
            *batches = self
                .tiered_record_batches
                .batches_for_interval(interval_start, interval_end)?;
        }
        let mut final_exec = self
            .finish_execution_plan
            .execute(0, SessionContext::new().task_ctx())
            .unwrap();

        self.state = if self.tiered_record_batches.is_empty() {
            match partial_table.get_min_time() {
                Some(min_time) => SlidingWindowState::OnlyBufferedData {
                    earliest_bin_time: self.bin_start(min_time),
                },
                None => SlidingWindowState::NoData,
            }
        } else {
            SlidingWindowState::InMemoryData {
                next_window_start: bin_end,
            }
        };
        while let Some(batch) = final_exec.next().await {
            let batch = batch.expect("should be able to compute batch");
            let timestamp_array =
                ScalarValue::TimestampNanosecond(Some(to_nanos(interval_end) as i64 - 1), None)
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
                    ScalarValue::TimestampNanosecond(Some(to_nanos(interval_start) as i64), None),
                    ScalarValue::TimestampNanosecond(Some(to_nanos(interval_end) as i64), None),
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

        Ok(())
    }
}

#[derive(Debug)]
struct TieredRecordBatchHolder {
    tier_widths: Vec<Duration>,
    tiers: Vec<RecordBatchTier>,
}

#[derive(Default, Debug)]
struct RecordBatchPane {
    batches: Vec<RecordBatch>,
}

#[derive(Debug)]
struct RecordBatchTier {
    width: Duration,
    start_time: Option<SystemTime>,
    panes: VecDeque<RecordBatchPane>,
}

impl RecordBatchTier {
    fn new(width: Duration) -> Self {
        Self {
            width,
            start_time: None,
            panes: VecDeque::new(),
        }
    }
    fn insert(&mut self, batch: RecordBatch, timestamp: SystemTime) -> Result<()> {
        // calculate the bin start for timestamp, based on width
        let bin_start = self.bin_start(timestamp);
        // if we don't have a start time, set it to the bin start
        if self.start_time.is_none() {
            self.start_time = Some(bin_start);
            let pane = RecordBatchPane {
                batches: vec![batch],
            };
            self.panes.push_back(pane);
            return Ok(());
        }
        // if the bin_start is before start_time, error out
        let start_time = self.start_time.unwrap();
        let bin_index =
            (bin_start.duration_since(start_time)?.as_nanos() / self.width.as_nanos()) as usize;
        while self.panes.len() <= bin_index {
            self.panes.push_back(RecordBatchPane::default());
        }
        let pane = self.panes.get_mut(bin_index).unwrap();
        pane.batches.push(batch);
        Ok(())
    }

    fn bin_start(&self, timestamp: SystemTime) -> SystemTime {
        if self.width == Duration::ZERO {
            return timestamp;
        }
        let mut nanos = to_nanos(timestamp);
        nanos -= nanos % self.width.as_nanos();
        let result = from_nanos(nanos);
        result
    }

    fn batches_for_timestamp(&self, bin_start: SystemTime) -> Result<Vec<RecordBatch>> {
        if self
            .start_time
            .map(|start_time| start_time > bin_start)
            .unwrap_or(true)
        {
            return Ok(Vec::new());
        }
        let start_time = self.start_time.unwrap();
        let bin_index =
            (bin_start.duration_since(start_time)?.as_nanos() / self.width.as_nanos()) as usize;
        if self.panes.len() <= bin_index {
            return Ok(Vec::new());
        }
        Ok(self.panes[bin_index].batches.clone())
    }

    fn delete_before(&mut self, cutoff: SystemTime) -> Result<()> {
        let bin_start = self.bin_start(cutoff);
        if self
            .start_time
            .map(|start_time| start_time >= bin_start)
            .unwrap_or(true)
        {
            return Ok(());
        }

        let bin_index = (bin_start
            .duration_since(self.start_time.unwrap())
            .unwrap()
            .as_nanos()
            / self.width.as_nanos()) as usize;
        if bin_index >= self.panes.len() {
            self.panes.clear();
            return Ok(());
        }
        self.panes.drain(0..bin_index);
        self.start_time = Some(bin_start);
        Ok(())
    }
}

impl TieredRecordBatchHolder {
    fn print_tier_contents(&self) {
        for (i, tier) in self.tiers.iter().enumerate() {
            info!(
                "tier {}, start {}:",
                i,
                tier.start_time.map(print_time).unwrap_or_default()
            );
            for (j, pane) in tier.panes.iter().enumerate() {
                info!("pane {} has {} batches", j, pane.batches.len());
            }
        }
    }

    fn new(tier_widths: Vec<Duration>) -> Result<Self> {
        // check that each width evenly divides the next one:
        for i in 0..tier_widths.len() - 1 {
            let width = tier_widths[i];
            let next_width = tier_widths[i + 1];
            if next_width.as_nanos() % width.as_nanos() != 0 {
                bail!(
                    "tier width {} does not evenly divide next tier width {}",
                    width.as_nanos(),
                    next_width.as_nanos()
                );
            }
        }
        let tiers = tier_widths
            .iter()
            .map(|width| RecordBatchTier::new(*width))
            .collect::<Vec<_>>();
        Ok(Self { tier_widths, tiers })
    }

    fn insert(&mut self, batch: RecordBatch, timestamp: SystemTime) -> Result<()> {
        for tier in self.tiers.iter_mut() {
            tier.insert(batch.clone(), timestamp)?;
        }
        Ok(())
    }

    fn batches_for_interval(
        &self,
        interval_start: SystemTime,
        interval_end: SystemTime,
    ) -> Result<Vec<RecordBatch>> {
        // we want to divide the interval into a minimum number of bins, i.e. use the largest bins where possible
        // so we start with the largest bin and work our way down.
        let mut batches = Vec::new();
        let mut current_tier = 0;
        let mut current_start = interval_start;
        while current_start < interval_end {
            // check the current tier
            let tier_end = current_start + self.tier_widths[current_tier];
            if tier_end > interval_end {
                // if the tier end is past the interval end, we need to drop down
                current_tier -= 1;
                continue;
            }
            // check the next tier
            if current_tier < self.tier_widths.len() - 1 {
                let next_tier = &self.tiers[current_tier + 1];
                let next_tier_end = current_start + next_tier.width;
                if next_tier.bin_start(current_start) == current_start
                    && next_tier_end <= interval_end
                {
                    // if the next tier starts at the current start, and ends before the interval end, we can skip this tier
                    current_tier += 1;
                    continue;
                }
            }
            // if we get here, we need to add the current tier
            let tier = &self.tiers[current_tier];
            batches.extend(tier.batches_for_timestamp(current_start)?);
            current_start += tier.width;
        }
        if current_start != interval_end {
            bail!(
                "interval end {:?} does not match current start {:?}",
                interval_end,
                current_start
            );
        }
        Ok(batches)
    }

    fn delete_before(&mut self, cutoff: SystemTime) -> Result<()> {
        for tier in self.tiers.iter_mut() {
            tier.delete_before(cutoff)?;
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.tiers[0]
            .panes
            .iter()
            .all(|entry| entry.batches.is_empty())
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

impl ArrowOperatorConstructor<api::WindowAggregateOperator>
    for SlidingAggregatingWindowFunc<SystemTime>
{
    fn from_config(proto_config: api::WindowAggregateOperator) -> Result<OperatorNode> {
        let binning_function =
            PhysicalExprNode::decode(&mut proto_config.binning_function.as_slice()).unwrap();
        let input_schema: Schema = serde_json::from_slice(proto_config.input_schema.as_slice())
            .context(format!(
                "failed to deserialize schema of length {}",
                proto_config.input_schema.len()
            ))?;

        let binning_function =
            parse_physical_expr(&binning_function, &EmptyRegistry {}, &input_schema)?;

        let mut physical_plan =
            PhysicalPlanNode::decode(&mut proto_config.physical_plan.as_slice()).unwrap();

        let Some(arroyo_rpc::grpc::api::Window {
            window: Some(Window::SlidingWindow(window)),
        }) = proto_config.window
        else {
            bail!("expected Sliding window")
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

        Ok(OperatorNode::from_operator(Box::new(Self {
            slide: Duration::from_micros(window.slide_micros),
            width: Duration::from_micros(window.size_micros),
            binning_function,
            partial_aggregation_plan,
            partial_schema,
            finish_execution_plan,
            receiver,
            final_batches_passer,
            futures: FuturesUnordered::new(),
            execs: BTreeMap::new(),
            tiered_record_batches: TieredRecordBatchHolder::new(vec![Duration::from_micros(
                window.slide_micros,
            )])?,
            window_field,
            window_index: proto_config.window_index as usize,
            state: SlidingWindowState::NoData,
        })))
    }
}

#[async_trait::async_trait]

impl ArrowOperator for SlidingAggregatingWindowFunc<SystemTime> {
    fn name(&self) -> String {
        "sliding_window".to_string()
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        let watermark = ctx.last_present_watermark();
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("t", watermark)
            .await
            .expect("should be able to load table");
        // bins before the watermark should be put into the TieredRecordBatchHolder, those after in the exec.
        let watermark_bin = self.bin_start(watermark.unwrap_or_else(|| SystemTime::UNIX_EPOCH));
        for (timestamp, batches) in table.all_batches_for_watermark(watermark) {
            let bin = self.bin_start(*timestamp);
            if bin < watermark_bin {
                for batch in batches {
                    self.tiered_record_batches
                        .insert(batch.clone(), bin)
                        .unwrap();
                }
                continue;
            }
            let holder = self.execs.entry(bin).or_default();
            batches
                .iter()
                .for_each(|batch| holder.finished_batches.push(batch.clone()));
        }

        if self.tiered_record_batches.is_empty() {
            match table.get_min_time() {
                Some(min_time) => {
                    self.state = SlidingWindowState::OnlyBufferedData {
                        earliest_bin_time: self.bin_start(min_time),
                    }
                }
                None => self.state = SlidingWindowState::NoData,
            }
        } else {
            self.state = SlidingWindowState::InMemoryData {
                next_window_start: watermark_bin,
            };
        }
    }

    // TODO: filter out late data
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

            self.state = match self.state {
                SlidingWindowState::NoData => SlidingWindowState::OnlyBufferedData {
                    earliest_bin_time: bin_start,
                },
                SlidingWindowState::OnlyBufferedData { earliest_bin_time } => {
                    SlidingWindowState::OnlyBufferedData {
                        earliest_bin_time: earliest_bin_time.min(bin_start),
                    }
                }
                SlidingWindowState::InMemoryData { next_window_start } => {
                    SlidingWindowState::InMemoryData { next_window_start }
                }
            };
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
        let Some(last_watermark) = ctx.last_present_watermark() else {
            return;
        };

        while self.should_advance(last_watermark) {
            self.advance(ctx).await.unwrap();
        }

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
            let bucket_nanos: i64 = to_nanos(*bin) as i64;
            let Some(mut active_exec) = exec.active_exec.take() else {
                continue;
            };
            while let (_bin_, Some((batch, next_exec))) = active_exec.await {
                if _bin_ != *bin {
                    unreachable!("should only get batches for the bin we're working on");
                }
                active_exec = next_exec;
                let batch = batch.expect("should be able to compute batch");
                let bin_start = ScalarValue::TimestampNanosecond(Some(bucket_nanos), None);
                let timestamp_array = bin_start.to_array_of_size(batch.num_rows()).unwrap();
                let mut columns: Vec<Arc<dyn Array>> = batch.columns().to_vec();
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
                "Sliding_intermediate",
                self.width,
                self.partial_schema.clone(),
            ),
        )]
        .into_iter()
        .collect()
    }
}
