use crate::context::{ArrowContext, BatchReceiver};
use crate::inq_reader::InQReader;
use crate::{CheckpointCounter, ControlOutcome, SourceFinishType};
use arrow::array::RecordBatch;
use arroyo_metrics::TaskCounters;
use arroyo_rpc::grpc::{StopMode, TableConfig, TaskCheckpointEventType};
use arroyo_rpc::{ControlMessage, ControlResp};
use arroyo_types::{ArrowMessage, CheckpointBarrier, SignalMessage, Watermark};
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use futures::future::OptionFuture;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Barrier;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn, Instrument};

pub trait OperatorConstructor: Send {
    type ConfigT: prost::Message + Default;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<OperatorNode>;
}

pub enum OperatorNode {
    Source(Box<dyn SourceOperator + Send>),
    Operator(Box<dyn ArrowOperator + Send>),
}

// TODO: this is required currently because the FileSystemSink code isn't sync
unsafe impl Sync for OperatorNode {}

impl OperatorNode {
    pub fn from_source(source: Box<dyn SourceOperator + Send>) -> Self {
        OperatorNode::Source(source)
    }

    pub fn from_operator(operator: Box<dyn ArrowOperator + Send>) -> Self {
        OperatorNode::Operator(operator)
    }

    pub fn name(&self) -> String {
        match self {
            OperatorNode::Source(s) => s.name(),
            OperatorNode::Operator(s) => s.name(),
        }
    }

    pub fn tables(&self) -> HashMap<String, TableConfig> {
        match self {
            OperatorNode::Source(s) => s.tables(),
            OperatorNode::Operator(s) => s.tables(),
        }
    }

    async fn run_behavior(
        &mut self,
        ctx: &mut ArrowContext,
        in_qs: &mut Vec<BatchReceiver>,
        ready: Arc<Barrier>,
    ) {
        match self {
            OperatorNode::Source(s) => {
                s.on_start(ctx).await;

                ready.wait().await;
                info!(
                    "Running source {}-{}",
                    ctx.task_info.operator_name, ctx.task_info.task_index
                );
                let result = s.run(ctx).await;

                ctx.control_tx
                    .send(ControlResp::TaskDataFinished {
                        operator_id: ctx.task_info.operator_id.clone(),
                        task_index: ctx.task_info.task_index,
                    })
                    .await
                    .expect("control response unwrap");

                match result {
                    SourceFinishType::Graceful => {
                        ctx.broadcast(ArrowMessage::Signal(SignalMessage::Stop))
                            .await;
                    }
                    SourceFinishType::Immediate => {
                        ctx.broadcast(ArrowMessage::Signal(SignalMessage::Shutdown))
                            .await;
                        return;
                    }
                    SourceFinishType::Final => {
                        ctx.broadcast(ArrowMessage::Signal(SignalMessage::EndOfData))
                            .await;
                    }
                }
                // keep handling control messages until we get a shutdown
                loop {
                    match ctx.control_rx.recv().await {
                        Some(control_message) => {
                            if let Some(SourceFinishType::Immediate) =
                                s.handle_control_message(ctx, control_message).await
                            {
                                ctx.broadcast(ArrowMessage::Signal(SignalMessage::Shutdown))
                                    .await;
                                return;
                            }
                        }
                        None => {
                            warn!("source {}-{} received None from control channel, indicating sender has been dropped", 
                                ctx.task_info.operator_name, ctx.task_info.task_index);
                            return;
                        }
                    }
                }
            }
            OperatorNode::Operator(o) => operator_run_behavior(o, ctx, in_qs, ready).await,
        }
    }

    pub async fn start(
        mut self: Box<Self>,
        mut ctx: ArrowContext,
        mut in_qs: Vec<BatchReceiver>,
        ready: Arc<Barrier>,
    ) {
        info!(
            "Starting task {}-{}",
            ctx.task_info.operator_name, ctx.task_info.task_index
        );

        self.run_behavior(&mut ctx, &mut in_qs, ready).await;

        info!(
            "Task finished {}-{}",
            ctx.task_info.operator_name, ctx.task_info.task_index
        );

        ctx.control_tx
            .send(ControlResp::TaskFinished {
                operator_id: ctx.task_info.operator_id.clone(),
                task_index: ctx.task_info.task_index,
            })
            .await
            .expect("control response unwrap");
    }
}

async fn run_checkpoint(checkpoint_barrier: CheckpointBarrier, ctx: &mut ArrowContext) -> bool {
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

#[async_trait]
pub trait SourceOperator: Send + 'static {
    fn name(&self) -> String;

    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }

    #[allow(unused_variables)]
    async fn on_start(&mut self, ctx: &mut ArrowContext) {}

    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType;

    async fn flush_before_checkpoint(&mut self, cp: CheckpointBarrier, ctx: &mut ArrowContext);

    async fn start_checkpoint(
        &mut self,
        checkpoint_barrier: CheckpointBarrier,
        ctx: &mut ArrowContext,
    ) -> bool {
        ctx.send_checkpoint_event(
            checkpoint_barrier,
            TaskCheckpointEventType::StartedCheckpointing,
        )
        .await;

        run_checkpoint(checkpoint_barrier, ctx).await
    }

    async fn handle_control_message(
        &mut self,
        ctx: &mut ArrowContext,
        control_message: ControlMessage,
    ) -> Option<SourceFinishType> {
        match control_message {
            ControlMessage::Checkpoint(cp) => {
                self.flush_before_checkpoint(cp, ctx).await;
                if self.start_checkpoint(cp, ctx).await {
                    Some(SourceFinishType::Graceful)
                } else {
                    None
                }
            }
            ControlMessage::Stop { mode } => match mode {
                StopMode::Graceful => Some(SourceFinishType::Graceful),
                StopMode::Immediate => Some(SourceFinishType::Immediate),
            },
            ControlMessage::Commit { .. } => {
                unreachable!("sources don't commit");
            }
            ControlMessage::LoadCompacted { compacted } => {
                ctx.load_compacted(compacted).await;
                None
            }
            ControlMessage::NoOp => None,
        }
    }
}

async fn operator_run_behavior(
    this: &mut Box<dyn ArrowOperator + Send>,
    ctx: &mut ArrowContext,
    in_qs: &mut Vec<BatchReceiver>,
    ready: Arc<Barrier>,
) {
    this.on_start(ctx).await;

    ready.wait().await;
    info!(
        "Running operator {}-{}",
        ctx.task_info.operator_name, ctx.task_info.task_index
    );

    let task_info = ctx.task_info.clone();
    let name = this.name();
    let mut counter = CheckpointCounter::new(in_qs.len());
    let mut closed: HashSet<usize> = HashSet::new();
    let mut finished: HashSet<usize> = HashSet::new();
    let mut sel = InQReader::new();
    let in_partitions = in_qs.len();

    for (i, q) in in_qs.into_iter().enumerate() {
        let stream = async_stream::stream! {
          while let Some(item) = q.recv().await {
            yield(i,item);
          }
        };
        sel.push(Box::pin(stream));
    }
    let mut blocked = vec![];

    let mut ticks = 0u64;
    let mut interval =
        tokio::time::interval(this.tick_interval().unwrap_or(Duration::from_secs(60)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut operator_state = OperatorState::Running;

    loop {
        let operator_future: OptionFuture<_> = this.future_to_poll().into();
        tokio::select! {
            Some(control_message) = ctx.control_rx.recv() => {
                this.handle_controller_message(control_message, ctx).await;
            }

            p = sel.next() => {
                match p {
                    Some(((idx, message), s)) => {
                        let local_idx = idx;

                        debug!("[{}] Handling message {}-{}, {:?}",
                            ctx.task_info.operator_name, 0, local_idx, message);

                        match message {
                            ArrowMessage::Data(record) => {
                                if operator_state != OperatorState::Running {
                                    unreachable!("operator {}-{} received data while in state {:?}. data is {:?}",
                                        ctx.task_info.operator_name, ctx.task_info.task_index, operator_state, record);
                                }
                                TaskCounters::BatchesReceived.for_task(&ctx.task_info, |c| c.inc());
                                TaskCounters::MessagesReceived.for_task(&ctx.task_info, |c| c.inc_by(record.num_rows() as u64));
                                TaskCounters::BytesReceived.for_task(&ctx.task_info, |c| c.inc_by(record.get_array_memory_size() as u64));
                                this.process_batch_index(idx, in_partitions, record, ctx)
                                    .instrument(tracing::trace_span!("handle_fn",
                                        name,
                                        operator_id = task_info.operator_id,
                                        subtask_idx = task_info.task_index)
                                ).await;
                            }
                            ArrowMessage::Signal(signal) => {
                                match this.handle_control_message(idx, &signal, &mut counter, &mut closed,&mut finished, in_partitions, ctx).await {
                                    ControlOutcome::Continue => {}
                                    ControlOutcome::FinishedStoppingCheckpoint => {
                                        info!("finished stopping checkpoint in operator {}-{}, waiting for terminate",
                                            ctx.task_info.operator_name, ctx.task_info.task_index);
                                        operator_state = OperatorState::WaitingForTerminate;
                                    }
                                    ControlOutcome::NoMoreData { end_of_data } => {
                                        if operator_state != OperatorState::Running {
                                            warn!("received no more data update in operator {}-{} while in state {:?}",
                                                ctx.task_info.operator_name, ctx.task_info.task_index, operator_state);
                                        }
                                        if end_of_data {
                                            this.on_end_of_data(ctx).await;
                                            ctx.broadcast(ArrowMessage::Signal(SignalMessage::EndOfData)).await;
                                        } else {
                                            ctx.broadcast(ArrowMessage::Signal(SignalMessage::Stop)).await;
                                        }

                                        ctx.control_tx.send(ControlResp::TaskDataFinished {
                                            operator_id: ctx.task_info.operator_id.clone(),
                                            task_index: ctx.task_info.task_index }).await.unwrap();
                                            info!("received NoMoreData, stopping operator {}-{}", ctx.task_info.operator_name, ctx.task_info.task_index);
                                        operator_state = OperatorState::WaitingForStoppingCheckpoint;
                                    }
                                    ControlOutcome::Shutdown => {
                                        if operator_state != OperatorState::WaitingForTerminate {
                                            warn!("shutting down operator {}-{} while in state {:?}",
                                                ctx.task_info.operator_name, ctx.task_info.task_index, operator_state);
                                        }
                                        ctx.broadcast(ArrowMessage::Signal(SignalMessage::Shutdown)).await;
                                        return;
                                    }
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
                        info!("[{}] Stream completed",ctx.task_info.operator_name);
                        break;
                    }
                }
            }
            Some(val) = operator_future => {
                this.handle_future_result(val, ctx).await;
            }
            _ = interval.tick() => {
                this.handle_tick(ticks, ctx).await;
                ticks += 1;
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum OperatorState {
    Running,
    WaitingForStoppingCheckpoint,
    WaitingForTerminate,
}

#[async_trait::async_trait]
pub trait ArrowOperator: Send + 'static {
    async fn handle_watermark_int(&mut self, watermark: Watermark, ctx: &mut ArrowContext) {
        // process timers
        tracing::trace!(
            "handling watermark {:?} for {}-{}",
            watermark,
            ctx.task_info.operator_name,
            ctx.task_info.task_index
        );

        if let Watermark::EventTime(_t) = watermark {
            // let finished = ProcessFnUtils::finished_timers(t, ctx).await;
            //
            // for (k, tv) in finished {
            //     self.handle_timer(k, tv.data, ctx).await;
            // }
        }

        if let Some(watermark) = self.handle_watermark(watermark, ctx).await {
            ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(watermark)))
                .await;
        }
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
            ControlMessage::Commit { epoch, commit_data } => {
                self.handle_commit(epoch, &commit_data, ctx).await;
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
        finished: &mut HashSet<usize>,
        in_partitions: usize,
        ctx: &mut ArrowContext,
    ) -> ControlOutcome {
        match message {
            SignalMessage::Barrier(t) => {
                debug!(
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
                    debug!(
                        "Checkpointing {}-{}-{}",
                        self.name(),
                        ctx.task_info.operator_id,
                        ctx.task_info.task_index
                    );

                    ctx.send_checkpoint_event(*t, TaskCheckpointEventType::StartedCheckpointing)
                        .await;

                    self.handle_checkpoint(*t, ctx).await;

                    ctx.send_checkpoint_event(*t, TaskCheckpointEventType::FinishedOperatorSetup)
                        .await;

                    if run_checkpoint(*t, ctx).await {
                        return ControlOutcome::FinishedStoppingCheckpoint;
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
                    if let Watermark::EventTime(_t) = watermark {
                        // TOOD: pass to table_manager
                    }

                    self.handle_watermark_int(watermark, ctx).await;
                }
            }
            SignalMessage::Stop => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return ControlOutcome::NoMoreData { end_of_data: false };
                }
            }
            SignalMessage::EndOfData => {
                closed.insert(idx);
                finished.insert(idx);
                if closed.len() == in_partitions {
                    if finished.len() == in_partitions {
                        return ControlOutcome::NoMoreData { end_of_data: true };
                    }
                    return ControlOutcome::NoMoreData { end_of_data: false };
                }
            }
            SignalMessage::Shutdown => {
                return ControlOutcome::Shutdown;
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

    async fn process_batch_index(
        &mut self,
        _index: usize,
        _in_partitions: usize,
        batch: RecordBatch,
        ctx: &mut ArrowContext,
    ) {
        self.process_batch(batch, ctx).await
    }

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext);

    fn future_to_poll(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>> {
        None
    }

    #[allow(unused_variables)]
    async fn handle_future_result(&mut self, result: Box<dyn Any + Send>, ctx: &mut ArrowContext) {}

    #[allow(unused_variables)]
    async fn handle_timer(&mut self, key: Vec<u8>, value: Vec<u8>, ctx: &mut ArrowContext) {}

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut ArrowContext,
    ) -> Option<Watermark> {
        Some(watermark)
    }

    #[allow(unused_variables)]
    async fn handle_checkpoint(&mut self, b: CheckpointBarrier, ctx: &mut ArrowContext) {}

    #[allow(unused_variables)]
    async fn handle_commit(
        &mut self,
        epoch: u32,
        commit_data: &HashMap<String, HashMap<u32, Vec<u8>>>,
        ctx: &mut ArrowContext,
    ) {
        warn!("default handling of commit with epoch {:?}", epoch);
    }

    #[allow(unused_variables)]
    async fn handle_tick(&mut self, tick: u64, ctx: &mut ArrowContext) {}

    #[allow(unused_variables)]
    async fn on_end_of_data(&mut self, ctx: &mut ArrowContext) {}

    #[allow(unused_variables)]
    async fn on_close(&mut self, ctx: &mut ArrowContext) {}
}

#[derive(Default)]
pub struct Registry {
    udfs: HashMap<String, Arc<ScalarUDF>>,
}

impl Registry {
    pub fn add_udf(&mut self, udf: Arc<ScalarUDF>) {
        self.udfs.insert(udf.name().to_string(), udf);
    }
}

impl FunctionRegistry for Registry {
    fn udfs(&self) -> HashSet<String> {
        self.udfs.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> DFResult<Arc<ScalarUDF>> {
        self.udfs
            .get(name)
            .cloned()
            .ok_or_else(|| DataFusionError::Execution(format!("Udf {} not found", name)))
    }

    fn udaf(&self, name: &str) -> DFResult<Arc<AggregateUDF>> {
        Err(DataFusionError::NotImplemented(format!(
            "udaf {} not implemented",
            name
        )))
    }

    fn udwf(&self, name: &str) -> DFResult<Arc<WindowUDF>> {
        Err(DataFusionError::NotImplemented(format!(
            "udwf {} not implemented",
            name
        )))
    }
}
