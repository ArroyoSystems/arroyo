use std::fmt::Debug;
use std::hash::Hash;
use std::pin::Pin;
use std::time::Duration;
use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};

use crate::engine::{ArrowContext, CheckpointCounter};
use crate::inq_reader::InQReader;
use crate::metrics::TaskCounters;
use crate::ControlOutcome;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::{Array, PrimitiveArray, RecordBatch};
use arroyo_rpc::grpc::TableConfig;
use arroyo_rpc::ArroyoSchema;
use arroyo_rpc::{
    grpc::{CheckpointMetadata, TableDescriptor, TaskCheckpointEventType},
    ControlMessage, ControlResp,
};
use arroyo_state::BackingStore;
use arroyo_types::{
    from_millis, to_millis, ArrowMessage, CheckpointBarrier, Data, Key, SignalMessage, TaskInfo,
    TaskInfoRef, Watermark, Window,
};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use futures::{Future, StreamExt};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::Stream;
use tracing::{debug, error, info, warn, Instrument};

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
        /* TODO: decide how we want to handle timers in Arrow world.
        let mut state = ctx
           .state
           .get_time_key_map(TIMER_TABLE, ctx.last_present_watermark())
           .await;
        //state.evict_all_before_watermark(watermark)*/
        vec![]
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

            let final_message = self.run_behavior(&mut ctx, in_qs).await;

            if let Some(final_message) = final_message {
                ctx.broadcast(final_message).await;
            }
            tracing::info!(
                "Task finished {}-{}",
                ctx.task_info.operator_name,
                ctx.task_info.task_index
            );

            ctx.control_tx
                .send(ControlResp::TaskFinished {
                    operator_id: ctx.task_info.operator_id.clone(),
                    task_index: ctx.task_info.task_index,
                })
                .await
                .expect("control response unwrap");
        })
    }

    async fn run_behavior(
        mut self: Box<Self>,
        ctx: &mut ArrowContext,
        in_qs: Vec<Receiver<ArrowMessage>>,
    ) -> Option<ArrowMessage>;

    async fn checkpoint(
        &mut self,
        checkpoint_barrier: CheckpointBarrier,
        ctx: &mut ArrowContext,
    ) -> bool {
        ctx.send_checkpoint_event(
            checkpoint_barrier,
            TaskCheckpointEventType::StartedCheckpointing,
        )
        .await;

        self.handle_checkpoint(checkpoint_barrier, ctx).await;

        ctx.send_checkpoint_event(
            checkpoint_barrier,
            TaskCheckpointEventType::FinishedOperatorSetup,
        )
        .await;

        let watermark = ctx.watermarks.last_present_watermark();

        ctx.table_manager
            .checkpoint(checkpoint_barrier, watermark)
            .await;

        ctx.send_checkpoint_event(checkpoint_barrier, TaskCheckpointEventType::FinishedSync)
            .await;

        ctx.broadcast(ArrowMessage::Signal(SignalMessage::Barrier(
            checkpoint_barrier,
        )))
        .await;

        checkpoint_barrier.then_stop
    }

    fn name(&self) -> String;

    fn tables(&self) -> HashMap<String, TableConfig>;

    #[allow(unused)]
    async fn handle_checkpoint(&mut self, b: CheckpointBarrier, ctx: &mut ArrowContext) {}
}

pub struct RunContext<St: Stream<Item = (usize, ArrowMessage)> + Send + Sync> {
    task_info: TaskInfoRef,
    name: String,
    counter: CheckpointCounter,
    closed: HashSet<usize>,
    pub sel: InQReader<St>,
    in_partitions: usize,
    blocked: Vec<St>,
    final_message: Option<ArrowMessage>,
    // TODO: ticks
}

#[async_trait]
impl<T: ArrowOperator> BaseOperator for T {
    async fn run_behavior(
        mut self: Box<Self>,
        ctx: &mut ArrowContext,
        mut in_qs: Vec<Receiver<ArrowMessage>>,
    ) -> Option<ArrowMessage> {
        self.on_start(ctx).await;

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
        let mut interval =
            tokio::time::interval(self.tick_interval().unwrap_or(Duration::from_secs(60)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut run_context = RunContext {
            task_info,
            name,
            counter,
            closed,
            sel,
            in_partitions,
            blocked,
            final_message,
        };

        loop {
            if self.select_run(ctx, &mut run_context).await {
                break;
            }
        }
        self.on_close(ctx).await;
        run_context.final_message
    }

    fn name(&self) -> String {
        self.name()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        self.tables()
    }

    async fn handle_checkpoint(&mut self, b: CheckpointBarrier, ctx: &mut ArrowContext) {
        self.handle_checkpoint(b, ctx).await;
    }
}

#[async_trait::async_trait]
pub trait ArrowOperator: Send + 'static + Sized {
    async fn handle_watermark_int(&mut self, watermark: Watermark, ctx: &mut ArrowContext) {
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
        message: &SignalMessage,
        counter: &mut CheckpointCounter,
        closed: &mut HashSet<usize>,
        in_partitions: usize,
        ctx: &mut ArrowContext,
    ) -> ControlOutcome {
        match message {
            SignalMessage::Barrier(t) => {
                info!(
                    "received barrier in {}-{}-{}-{}",
                    self.name(),
                    ctx.task_info.operator_id,
                    ctx.task_info.task_index,
                    idx
                );

                if counter.all_clear() {
                    ctx.control_tx
                        .send(ControlResp::CheckpointEvent(arroyo_rpc::CheckpointEvent {
                            checkpoint_epoch: t.epoch,
                            operator_id: ctx.task_info.operator_id.clone(),
                            subtask_index: ctx.task_info.task_index as u32,
                            time: SystemTime::now(),
                            event_type: TaskCheckpointEventType::StartedAlignment,
                        }))
                        .await
                        .unwrap();
                }

                if counter.mark(idx, &t) {
                    info!(
                        "Checkpointing {}-{}-{}",
                        self.name(),
                        ctx.task_info.operator_id,
                        ctx.task_info.task_index
                    );

                    if self.checkpoint(*t, ctx).await {
                        return ControlOutcome::Stop;
                    }
                }
            }
            SignalMessage::Watermark(watermark) => {
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
                        // TOOD: pass to table_manager
                    }

                    self.handle_watermark_int(watermark, ctx).await;
                }
            }
            SignalMessage::Stop => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return ControlOutcome::StopAndSendStop;
                }
            }
            SignalMessage::EndOfData => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return ControlOutcome::Finish;
                }
            }
        }
        ControlOutcome::Continue
    }

    fn name(&self) -> String;

    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }

    fn tick_interval(&self) -> Option<Duration> {
        None
    }

    #[allow(unused_variables)]
    async fn on_start(&mut self, ctx: &mut ArrowContext) {}

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext);

    #[allow(unused_variables)]
    async fn handle_timer(&mut self, key: Vec<u8>, value: Vec<u8>, ctx: &mut ArrowContext) {}

    async fn handle_watermark(&mut self, watermark: Watermark, ctx: &mut ArrowContext) {
        ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(watermark)))
            .await;
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
            // TODO:
            /*_ = interval.tick() => {
                self.handle_tick(ticks, ctx).await;
                ticks += 1;
            }*/
        }
        false
    }

    async fn handle_message<St: Stream<Item = (usize, ArrowMessage)> + Send + Sync + Unpin>(
        &mut self,
        idx: usize,
        message: ArrowMessage,
        s: St,
        ctx: &mut ArrowContext,
        run_context: &mut RunContext<St>,
    ) -> bool {
        let local_idx = idx;

        tracing::debug!(
            "[{}] Handling message {}-{}, {:?}",
            ctx.task_info.operator_name,
            0,
            local_idx,
            message
        );

        match message {
            ArrowMessage::Data(record) => {
                TaskCounters::MessagesReceived
                    .for_task(&ctx.task_info)
                    .inc();
                Self::process_batch(&mut (*self), record, ctx)
                    .instrument(tracing::trace_span!(
                        "handle_fn",
                        run_context.name,
                        operator_id = run_context.task_info.operator_id,
                        subtask_idx = run_context.task_info.task_index
                    ))
                    .await;
            }
            ArrowMessage::Signal(signal) => {
                match self
                    .handle_control_message(
                        idx,
                        &signal,
                        &mut run_context.counter,
                        &mut run_context.closed,
                        run_context.in_partitions,
                        ctx,
                    )
                    .await
                {
                    ControlOutcome::Continue => {}
                    ControlOutcome::Stop => {
                        // just stop; the stop will have already been broadcasted for example by
                        // a final checkpoint
                        return true;
                    }
                    ControlOutcome::Finish => {
                        run_context.final_message =
                            Some(ArrowMessage::Signal(SignalMessage::EndOfData));
                        return true;
                    }
                    ControlOutcome::StopAndSendStop => {
                        run_context.final_message = Some(ArrowMessage::Signal(SignalMessage::Stop));
                        return true;
                    }
                }
            }
        }

        if run_context.counter.is_blocked(idx) {
            run_context.blocked.push(s);
        } else {
            if run_context.counter.all_clear() && !run_context.blocked.is_empty() {
                for q in run_context.blocked.drain(..) {
                    run_context.sel.push(q);
                }
            }
            run_context.sel.push(s);
        }
        false
    }
}

pub fn get_timestamp_col<'a, 'b>(
    batch: &'a RecordBatch,
    ctx: &'b mut ArrowContext,
) -> &'a PrimitiveArray<TimestampNanosecondType> {
    batch
        .column(ctx.out_schema.as_ref().unwrap().timestamp_index)
        .as_any()
        .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
        .unwrap()
}

#[async_trait::async_trait]
pub trait ArrowOperatorWithFuture: ArrowOperator {
    type Output;

    fn select_term(&mut self) -> Pin<Box<dyn Future<Output = Self::Output> + Send>>;
    async fn handle_output(&mut self, output: Self::Output);
}
