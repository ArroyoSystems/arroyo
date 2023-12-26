use std::hash::Hash;
use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use crate::inq_reader::InQReader;
use arrow::row::{Row, RowConverter};
use arrow_array::{Array, ArrayRef, PrimitiveArray, RecordBatch};
use arrow_array::types::TimestampNanosecondType;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use datafusion_common::ScalarValue;
use arroyo_datastream::ArroyoSchema;
use arroyo_metrics::gauge_for_task;
use arroyo_rpc::{
    grpc::{
        CheckpointMetadata, TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior,
        TaskCheckpointEventType,
    },
    ControlMessage, ControlResp,
};
use arroyo_state::{BackingStore, StateBackend, StateStore};
use arroyo_types::{from_micros, ArrowMessage, CheckpointBarrier, TaskInfo, Watermark, BYTES_RECV, BYTES_SENT, MESSAGES_RECV, MESSAGES_SENT, Key, Data, Window, to_millis, from_millis};
use futures::{FutureExt, StreamExt};
use prometheus::{labels, IntCounter, IntGauge};
use rand::{random};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error, Instrument, warn};
use arroyo_state::tables::time_key_map::TimeKeyMap;
use crate::{ControlOutcome, SourceFinishType};
use crate::engine::{ArrowContext, CheckpointCounter, WatermarkHolder};
use crate::metrics::TaskCounters;

pub trait TimerT: Data + PartialEq + Eq + 'static {}

impl<T: Data + PartialEq + Eq + 'static> TimerT for T {}

pub fn server_for_hash(x: u64, n: usize) -> usize {
    let range_size = u64::MAX / (n as u64);
    (n - 1).min((x / range_size) as usize)
}

pub static TIMER_TABLE: char = '[';

pub trait TimeWindowAssigner: Send + 'static {
    fn windows(&self, ts: SystemTime) -> Vec<Window>;

    fn next(&self, window: Window) -> Window;

    fn safe_retention_duration(&self) -> Option<Duration>;
}

pub trait WindowAssigner: Send + 'static {}

#[derive(Clone, Copy)]
pub struct TumblingWindowAssigner {
    pub size: Duration,
}

impl TimeWindowAssigner for TumblingWindowAssigner {
    fn windows(&self, ts: SystemTime) -> Vec<Window> {
        let key = to_millis(ts) / (self.size.as_millis() as u64);
        vec![Window {
            start: from_millis(key * self.size.as_millis() as u64),
            end: from_millis((key + 1) * (self.size.as_millis() as u64)),
        }]
    }

    fn next(&self, window: Window) -> Window {
        Window {
            start: window.end,
            end: window.end + self.size,
        }
    }

    fn safe_retention_duration(&self) -> Option<Duration> {
        Some(self.size)
    }
}

#[derive(Clone, Copy)]
pub struct InstantWindowAssigner {}

impl TimeWindowAssigner for InstantWindowAssigner {
    fn windows(&self, ts: SystemTime) -> Vec<Window> {
        vec![Window {
            start: ts,
            end: ts + Duration::from_nanos(1),
        }]
    }

    fn next(&self, window: Window) -> Window {
        Window {
            start: window.start + Duration::from_micros(1),
            end: window.end + Duration::from_micros(1),
        }
    }

    fn safe_retention_duration(&self) -> Option<Duration> {
        Some(Duration::ZERO)
    }
}

#[derive(Copy, Clone)]
pub struct SlidingWindowAssigner {
    pub size: Duration,
    pub slide: Duration,
}
//  012345678
//  --x------
// [--x]
//  [-x-]
//   [x--]
//    [---]

impl SlidingWindowAssigner {
    fn start(&self, ts: SystemTime) -> SystemTime {
        let ts_millis = to_millis(ts);
        let earliest_window_start = ts_millis - self.size.as_millis() as u64;

        let remainder = earliest_window_start % (self.slide.as_millis() as u64);

        from_millis(earliest_window_start - remainder + self.slide.as_millis() as u64)
    }
}

impl TimeWindowAssigner for SlidingWindowAssigner {
    fn windows(&self, ts: SystemTime) -> Vec<Window> {
        let mut windows =
            Vec::with_capacity(self.size.as_millis() as usize / self.slide.as_millis() as usize);

        let mut start = self.start(ts);

        while start <= ts {
            windows.push(Window {
                start,
                end: start + self.size,
            });
            start += self.slide;
        }

        windows
    }

    fn next(&self, window: Window) -> Window {
        let start_time = window.start + self.slide;
        Window {
            start: start_time,
            end: start_time + self.size,
        }
    }

    fn safe_retention_duration(&self) -> Option<Duration> {
        Some(self.size)
    }
}

#[derive(Encode, Decode, Clone, Debug, PartialEq, Eq)]
pub struct ArrowTimerValue {
    pub time: SystemTime,
    pub key: Vec<u8>,
    pub data: Vec<u8>,
}


#[derive(Encode, Decode, Clone, Debug, PartialEq, Eq)]
pub struct TimerValue<K: Key, T: Decode + Encode + Clone + PartialEq + Eq> {
    pub time: SystemTime,
    pub key: K,
    pub data: T,
}

pub struct ProcessFnUtils {}

impl ProcessFnUtils {
    pub async fn finished_timers<OutK: Key, OutT: Data, Timer: Data + Eq + PartialEq>(
        watermark: SystemTime,
        ctx: &mut ArrowContext,
    ) -> Vec<(OutK, TimerValue<OutK, Timer>)> {
        let mut state = ctx
            .state
            .get_time_key_map(TIMER_TABLE, ctx.last_present_watermark())
            .await;
        state.evict_all_before_watermark(watermark)
    }

    pub async fn send_checkpoint_event<OutK: Key, OutT: Data>(
        barrier: arroyo_types::CheckpointBarrier,
        ctx: &mut ArrowContext,
        event_type: TaskCheckpointEventType,
    ) {
        // These messages are received by the engine control thread,
        // which then sends a TaskCheckpointEventReq to the controller.
        ctx.control_tx
            .send(arroyo_rpc::ControlResp::CheckpointEvent(
                arroyo_rpc::CheckpointEvent {
                    checkpoint_epoch: barrier.epoch,
                    operator_id: ctx.task_info.operator_id.clone(),
                    subtask_index: ctx.task_info.task_index as u32,
                    time: std::time::SystemTime::now(),
                    event_type,
                },
            ))
            .await
            .unwrap();
    }
}

pub trait ArrowOperatorConstructor<C: prost::Message, T: BaseOperator>: BaseOperator {
    fn from_config(config: C) -> anyhow::Result<T>;
}

#[async_trait]
pub trait BaseOperator: Send + 'static {
    fn start(
        mut self: Box<Self>,
        task_info: TaskInfo,
        restore_from: Option<CheckpointMetadata>,
        control_rx: Receiver<ControlMessage>,
        control_tx: Sender<ControlResp>,
        in_schemas: Vec<ArroyoSchema>,
        out_schema: Option<ArroyoSchema>,
        projection: Option<Vec<usize>>,
        in_qs: Vec<Vec<Receiver<ArrowMessage>>>,
        out_qs: Vec<Vec<Sender<ArrowMessage>>>,
    ) -> tokio::task::JoinHandle<()> {
        // if in_qs.len() != 1 {
        //     panic!(
        //         "Wrong number of logical inputs for node {} (expected {}, found {})",
        //         task_info.operator_name,
        //         1,
        //         in_qs.len()
        //     );
        // }

        let mut in_qs: Vec<_> = in_qs.into_iter().flatten().collect();
        let tables = self.tables();

        tokio::spawn(async move {
            let mut ctx = ArrowContext::new(
                task_info,
                restore_from,
                control_rx,
                control_tx,
                in_qs.len(),
                in_schemas,
                out_schema,
                projection,
                out_qs,
                tables,
            )
                .await;

            self.on_start(&mut ctx).await;


            let final_message = self.run_behavior(&mut ctx, in_qs).await;

            if let Some(final_message) = final_message {
                ctx.broadcast(final_message).await;
            }
            tracing::info!("Task finished {}-{}", ctx.task_info.operator_name, ctx.task_info.task_index);

            ctx.control_tx
                .send(ControlResp::TaskFinished {
                    operator_id: ctx.task_info.operator_id.clone(),
                    task_index: ctx.task_info.task_index,
                })
                .await
                .expect("control response unwrap");
        })
    }

    async fn run_behavior(mut self: Box<Self>, ctx: &mut ArrowContext, in_qs: Vec<Receiver<ArrowMessage>>) -> Option<ArrowMessage>;

    async fn checkpoint(
        &mut self,
        checkpoint_barrier: CheckpointBarrier,
        ctx: &mut ArrowContext,
    ) -> bool {
        ctx.send_checkpoint_event(
            checkpoint_barrier,
            TaskCheckpointEventType::StartedCheckpointing,
        ).await;

        self.handle_checkpoint(checkpoint_barrier, ctx).await;

        ctx.send_checkpoint_event(
            checkpoint_barrier,
            TaskCheckpointEventType::FinishedOperatorSetup,
        )
            .await;

        let watermark = ctx.watermarks.last_present_watermark();

        ctx.state.checkpoint(checkpoint_barrier, watermark).await;

        ctx.send_checkpoint_event(checkpoint_barrier, TaskCheckpointEventType::FinishedSync)
            .await;

        ctx.broadcast(ArrowMessage::Barrier(checkpoint_barrier))
            .await;

        checkpoint_barrier.then_stop
    }


    fn name(&self) -> String;

    fn tables(&self) -> Vec<TableDescriptor>;

    async fn on_start(&mut self, ctx: &mut ArrowContext);

    async fn on_close(&mut self, ctx: &mut ArrowContext);

    #[allow(unused)]
    async fn handle_checkpoint(&mut self, b: CheckpointBarrier, ctx: &mut ArrowContext) {}
}


#[async_trait]
impl <T: ArrowOperator> BaseOperator for T {
    async fn run_behavior(mut self: Box<Self>, ctx: &mut ArrowContext, mut in_qs: Vec<Receiver<ArrowMessage>>) -> Option<ArrowMessage> {
        let task_info = ctx.task_info.clone();
        let name = self.name();
        let mut counter = CheckpointCounter::new(in_qs.len());
        let mut closed: HashSet<usize> = HashSet::new();
        let mut sel = InQReader::new();
        let in_partitions = in_qs.len();

        for (i, mut q) in in_qs.into_iter().enumerate() {
            let stream = async_stream::stream! {
                  while let Some(item) = q.recv().await {
                    yield(i,item);
                  }
                };
            sel.push(Box::pin(stream));
        }
        let mut blocked = vec![];
        let mut final_message = None;

        let mut ticks = 0u64;
        let mut interval = tokio::time::interval(self.tick_interval().unwrap_or(Duration::from_secs(60)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                    Some(control_message) = ctx.control_rx.recv() => {
                        self.handle_controller_message(control_message, ctx).await;
                    }

                    p = sel.next() => {
                        match p {
                            Some(((idx, message), s)) => {
                                let local_idx = idx;

                                tracing::debug!("[{}] Handling message {}-{}, {:?}",
                                    ctx.task_info.operator_name, 0, local_idx, message);

                                if let ArrowMessage::Record(record) = message {
                                    TaskCounters::MessagesReceived.for_task(&ctx.task_info).inc();
                                    Self::process_batch(&mut(*self),record, ctx)
                                        .instrument(tracing::trace_span!("handle_fn",
                                            name,
                                            operator_id = task_info.operator_id,
                                            subtask_idx = task_info.task_index)
                                    ).await;
                                } else {
                                    match self.handle_control_message(idx, &message, &mut counter, &mut closed, in_partitions, ctx).await {
                                        ControlOutcome::Continue => {}
                                        ControlOutcome::Stop => {
                                            final_message = Some(ArrowMessage::Stop);
                                            break;
                                        }
                                        ControlOutcome::Finish => {
                                            final_message = Some(ArrowMessage::EndOfData);
                                            break;
                                        }
                                    }
                                }

                                if counter.is_blocked(idx){
                                    blocked.push(s);
                                } else {
                                    if counter.all_clear() && !blocked.is_empty(){
                                        for q in blocked.drain(..){
                                            sel.push(q);
                                        }
                                    }
                                    sel.push(s);
                                }
                            }
                            None => {
                                tracing::info!("[{}] Stream completed",ctx.task_info.operator_name);
                                break;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        self.handle_tick(ticks, ctx).await;
                        ticks += 1;
                    }
                }
        }
        final_message
    }

    fn name(&self) -> String {
        self.name()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        self.tables()
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        self.on_start(ctx).await;
    }

    async fn on_close(&mut self, ctx: &mut ArrowContext) {
        self.on_close(ctx).await;
    }

    async fn handle_checkpoint(&mut self, b: CheckpointBarrier, ctx: &mut ArrowContext) {
        self.handle_checkpoint(b, ctx).await;
    }
}

#[async_trait::async_trait]
pub trait ArrowOperator: Send + 'static + Sized {

    async fn handle_watermark_int(
        &mut self,
        watermark: Watermark,
        ctx: &mut ArrowContext,
    ) {
        // process timers
        tracing::trace!(
            "handling watermark {:?} for {}-{}",
            watermark,
            ctx.task_info.operator_name,
            ctx.task_info.task_index
        );

        if let Watermark::EventTime(t) = watermark {
            // let finished = ProcessFnUtils::finished_timers(t, ctx).await;
            //
            // for (k, tv) in finished {
            //     self.handle_timer(k, tv.data, ctx).await;
            // }
        }

        self.handle_watermark(watermark, ctx).await;
    }


    async fn handle_controller_message(
        &mut self,
        control_message: ControlMessage,
        ctx: &mut ArrowContext,
    ) {
        match control_message {
            ControlMessage::Checkpoint(_) => {
                error!("shouldn't receive checkpoint")
            }
            ControlMessage::Stop { .. } => {
                error!("shouldn't receive stop")
            }
            ControlMessage::Commit { epoch, .. } => {
                self.handle_commit(epoch, ctx).await;
            }
            ControlMessage::LoadCompacted { compacted } => {
                ctx.load_compacted(compacted).await;
            }
            ControlMessage::NoOp => {}
        }
    }

    async fn handle_control_message(
        &mut self,
        idx: usize,
        message: &ArrowMessage,
        counter: &mut CheckpointCounter,
        closed: &mut HashSet<usize>,
        in_partitions: usize,
        ctx: &mut ArrowContext,
    ) -> ControlOutcome {
        match message {
            ArrowMessage::Record(record) => {
                unreachable!();
            }
            ArrowMessage::Barrier(t) => {
                tracing::debug!(
                    "received barrier in {}-{}-{}-{}",
                    self.name(),
                    ctx.task_info.operator_id,
                    ctx.task_info.task_index,
                    idx
                );

                if counter.all_clear() {
                    ctx.control_tx
                        .send(ControlResp::CheckpointEvent(
                            arroyo_rpc::CheckpointEvent {
                                checkpoint_epoch: t.epoch,
                                operator_id: ctx.task_info.operator_id.clone(),
                                subtask_index: ctx.task_info.task_index as u32,
                                time: SystemTime::now(),
                                event_type: TaskCheckpointEventType::StartedAlignment,
                            },
                        ))
                        .await
                        .unwrap();
                }

                if counter.mark(idx, &t) {
                    debug!(
                        "Checkpointing {}-{}-{}",
                        self.name(),
                        ctx.task_info.operator_id,
                        ctx.task_info.task_index
                    );

                    if self.checkpoint(*t, ctx).await {
                        return crate::ControlOutcome::Stop;
                    }
                }
            }
            ArrowMessage::Watermark(watermark) => {
                debug!(
                    "received watermark {:?} in {}-{}",
                    watermark,
                    self.name(),
                    ctx.task_info.task_index
                );

                let watermark = ctx
                    .watermarks
                    .set(idx, *watermark)
                    .expect("watermark index is too big");

                if let Some(watermark) = watermark {
                    if let Watermark::EventTime(t) = watermark {
                        ctx.state.handle_watermark(t);
                    }

                    self.handle_watermark_int(watermark, ctx).await;
                }
            }
            ArrowMessage::Stop => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return ControlOutcome::Stop;
                }
            }
            ArrowMessage::EndOfData => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return ControlOutcome::Finish;
                }
            }
        }
        ControlOutcome::Continue
    }


    fn name(&self) -> String;

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![]
    }

    fn tick_interval(&self) -> Option<Duration> {
        None
    }

    #[allow(unused_variables)]
    async fn on_start(&mut self, ctx: &mut ArrowContext) {}

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext);

    #[allow(unused_variables)]
    async fn handle_timer(&mut self, key: Vec<u8>, value: Vec<u8>, ctx: &mut ArrowContext) {}

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut ArrowContext,
    ) {
        ctx.broadcast(ArrowMessage::Watermark(watermark)).await;
    }

    #[allow(unused_variables)]
    async fn handle_checkpoint(&mut self, b: CheckpointBarrier, ctx: &mut ArrowContext) {}

    #[allow(unused_variables)]
    async fn handle_commit(&mut self, epoch: u32, ctx: &mut ArrowContext) {
        warn!("default handling of commit with epoch {:?}", epoch);
    }

    #[allow(unused_variables)]
    async fn handle_tick(&mut self, tick: u64, ctx: &mut ArrowContext) {}

    #[allow(unused_variables)]
    async fn on_close(&mut self, ctx: &mut ArrowContext) {}
}

pub fn get_timestamp_col<'a, 'b>(batch: &'a RecordBatch, ctx: &'b mut ArrowContext) -> &'a PrimitiveArray<TimestampNanosecondType> {
    batch
        .column(ctx.out_schema.as_ref().unwrap().timestamp_col)
        .as_any()
        .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
        .unwrap()
}