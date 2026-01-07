use crate::context::{
    ArrowCollector, BatchReceiver, BatchSender, Collector, OperatorContext, SourceCollector,
    SourceContext, send_checkpoint_event,
};
use crate::inq_reader::InQReader;
use crate::udfs::{ArroyoUdaf, UdafArg};
use crate::{CheckpointCounter, ControlOutcome, SourceFinishType};
use anyhow::{anyhow, bail};
use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use arroyo_datastream::logical::{DylibUdfConfig, PythonUdfConfig};
use arroyo_metrics::TaskCounters;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::rpc::{TableConfig, TaskCheckpointEventType};
use arroyo_rpc::{ControlMessage, ControlResp};
use arroyo_state::tables::table_manager::TableManager;
use arroyo_storage::StorageProvider;
use arroyo_types::{
    ArrowMessage, ChainInfo, CheckpointBarrier, SignalMessage, TaskInfo, Watermark,
};
use arroyo_udf_host::parse::inner_type;
use arroyo_udf_host::{ContainerOrLocal, LocalUdf, SyncUdfDylib, UdfDylib, UdfInterface};
use arroyo_udf_python::PythonUDF;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{
    AggregateUDF, ScalarUDF, Signature, TypeSignature, Volatility, WindowUDF, create_udaf,
};
use datafusion::physical_plan::{ExecutionPlan, displayable};
use dlopen2::wrapper::Container;
use futures::future::OptionFuture;
use futures::stream::FuturesUnordered;
use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::io::ErrorKind;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Barrier;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;
use tracing::{Instrument, debug, error, info, trace, warn};

pub trait OperatorConstructor: Send {
    type ConfigT: prost::Message + Default;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator>;
}

pub struct SourceNode {
    pub operator: Box<dyn SourceOperator + Send>,
    pub context: OperatorContext,
}

pub enum ConstructedOperator {
    Source(Box<dyn SourceOperator + Send>),
    Operator(Box<dyn ArrowOperator + Send>),
}

impl ConstructedOperator {
    pub fn from_source(source: Box<dyn SourceOperator + Send>) -> Self {
        Self::Source(source)
    }

    pub fn from_operator(operator: Box<dyn ArrowOperator + Send>) -> Self {
        Self::Operator(operator)
    }

    pub fn name(&self) -> String {
        match self {
            Self::Source(s) => s.name(),
            Self::Operator(s) => s.name(),
        }
    }

    pub fn display(&self) -> DisplayableOperator<'_> {
        match self {
            Self::Source(_) => DisplayableOperator {
                name: self.name().into(),
                fields: vec![],
            },
            Self::Operator(op) => op.display(),
        }
    }
}

pub enum OperatorNode {
    Source(SourceNode),
    Chained(ChainedOperator),
}

// TODO: this is required currently because the FileSystemSink code isn't sync
unsafe impl Sync for OperatorNode {}

impl OperatorNode {
    pub fn name(&self) -> String {
        match self {
            OperatorNode::Source(s) => s.operator.name(),
            OperatorNode::Chained(s) => {
                format!(
                    "[{}]",
                    s.iter()
                        .map(|(o, _)| o.name())
                        .collect::<Vec<_>>()
                        .join(" -> ")
                )
            }
        }
    }

    pub fn task_info(&self) -> &Arc<TaskInfo> {
        match self {
            OperatorNode::Source(s) => &s.context.task_info,
            OperatorNode::Chained(s) => &s.context.task_info,
        }
    }

    pub fn operator_ids(&self) -> Vec<String> {
        match self {
            OperatorNode::Source(s) => vec![s.context.task_info.operator_id.clone()],
            OperatorNode::Chained(s) => s
                .iter()
                .map(|(_, ctx)| ctx.task_info.operator_id.clone())
                .collect(),
        }
    }

    async fn run_behavior(
        self,
        chain_info: &Arc<ChainInfo>,
        control_tx: Sender<ControlResp>,
        control_rx: Receiver<ControlMessage>,
        in_qs: &mut [BatchReceiver],
        ready: Arc<Barrier>,
        mut collector: ArrowCollector,
    ) -> DataflowResult<()> {
        match self {
            OperatorNode::Source(mut s) => {
                let mut source_context =
                    SourceContext::from_operator(s.context, chain_info.clone(), control_rx);

                let mut collector = SourceCollector::new(
                    source_context.out_schema.clone(),
                    collector,
                    control_tx.clone(),
                    &source_context.task_info,
                );

                s.operator.on_start(&mut source_context).await?;

                ready.wait().await;
                info!(
                    "Running source {}-{}",
                    source_context.task_info.operator_name, source_context.task_info.operator_name
                );

                source_context
                    .control_tx
                    .send(ControlResp::TaskStarted {
                        node_id: source_context.task_info.node_id,
                        task_index: source_context.task_info.task_index as usize,
                        start_time: SystemTime::now(),
                    })
                    .await
                    .unwrap();

                let result = s.operator.run(&mut source_context, &mut collector).await?;

                s.operator
                    .on_close(&mut source_context, &mut collector)
                    .await?;

                if let Some(final_message) = result.into() {
                    collector.broadcast(final_message).await;
                }
            }
            OperatorNode::Chained(mut o) => {
                let result = operator_run_behavior(
                    &mut o,
                    in_qs,
                    control_tx,
                    control_rx,
                    &mut collector,
                    ready,
                )
                .await?;
                if let Some(final_message) = result {
                    collector.broadcast(final_message).await;
                }
            }
        }

        Ok(())
    }

    fn node_id(&self) -> u32 {
        match self {
            OperatorNode::Source(s) => s.context.task_info.node_id,
            OperatorNode::Chained(s) => s.context.task_info.node_id,
        }
    }

    fn task_index(&self) -> u32 {
        match self {
            OperatorNode::Source(s) => s.context.task_info.task_index,
            OperatorNode::Chained(s) => s.context.task_info.task_index,
        }
    }

    pub async fn start(
        self: Box<Self>,
        control_tx: Sender<ControlResp>,
        control_rx: Receiver<ControlMessage>,
        mut in_qs: Vec<BatchReceiver>,
        out_qs: Vec<Vec<BatchSender>>,
        out_schema: Option<Arc<ArroyoSchema>>,
        ready: Arc<Barrier>,
    ) {
        info!(
            "Starting node {}-{} ({})",
            self.node_id(),
            self.task_index(),
            self.name()
        );

        let chain_info = Arc::new(ChainInfo {
            job_id: self.task_info().job_id.clone(),
            node_id: self.node_id(),
            description: self.name(),
            task_index: self.task_index(),
        });

        let collector = ArrowCollector::new(chain_info.clone(), out_schema, out_qs);

        match self
            .run_behavior(
                &chain_info,
                control_tx.clone(),
                control_rx,
                &mut in_qs,
                ready,
                collector,
            )
            .await
        {
            Ok(()) => {
                error!(
                    node_id = chain_info.node_id,
                    subtask_index = chain_info.task_index,
                    description = chain_info.description,
                    "Task finished"
                );

                control_tx
                    .send(ControlResp::TaskFinished {
                        node_id: chain_info.node_id,
                        task_index: chain_info.task_index as usize,
                    })
                    .await
                    .expect("control response unwrap");
            }
            Err(e) => {
                error!(node_id = chain_info.node_id,
                    subtask_index = chain_info.task_index,
                    description = chain_info.description,
                    error = ?e,
                    "Task failed");

                control_tx
                    .send(ControlResp::TaskFailed {
                        node_id: chain_info.node_id,
                        task_index: chain_info.task_index as usize,
                        error: e.into(),
                    })
                    .await
                    .expect("control response unwrap");
            }
        }
    }
}

async fn run_checkpoint(
    checkpoint_barrier: CheckpointBarrier,
    task_info: &TaskInfo,
    watermark: Option<SystemTime>,
    table_manager: &mut TableManager,
    control_tx: &Sender<ControlResp>,
) {
    table_manager
        .checkpoint(checkpoint_barrier, watermark)
        .await;

    send_checkpoint_event(
        control_tx,
        task_info,
        checkpoint_barrier,
        TaskCheckpointEventType::FinishedSync,
    )
    .await;
}

#[async_trait]
pub trait SourceOperator: Send + 'static {
    fn name(&self) -> String;

    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }

    #[allow(unused_variables)]
    async fn on_start(&mut self, ctx: &mut SourceContext) -> DataflowResult<()> {
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType>;

    #[allow(unused_variables)]
    async fn on_close(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<()> {
        Ok(())
    }

    async fn start_checkpoint(
        &mut self,
        checkpoint_barrier: CheckpointBarrier,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> bool {
        send_checkpoint_event(
            &ctx.control_tx,
            &ctx.task_info,
            checkpoint_barrier,
            TaskCheckpointEventType::StartedCheckpointing,
        )
        .await;

        run_checkpoint(
            checkpoint_barrier,
            &ctx.task_info,
            ctx.watermarks.last_present_watermark(),
            &mut ctx.table_manager,
            &ctx.control_tx,
        )
        .await;

        collector
            .broadcast(SignalMessage::Barrier(checkpoint_barrier))
            .await;

        checkpoint_barrier.then_stop
    }
}

macro_rules! call_with_collector {
    ($self:expr, $final_collector:expr, $name:ident, $arg:expr) => {
        match &mut $self.next {
            Some(next) => {
                let mut collector = ChainedCollector {
                    cur: next,
                    index: 0,
                    in_partitions: 1,
                    final_collector: $final_collector,
                };

                $self
                    .operator
                    .$name($arg, &mut $self.context, &mut collector)
                    .await
                    .map_err(|e| e.with_operator($self.context.task_info.operator_id.clone()))?;
            }
            None => {
                $self
                    .operator
                    .$name($arg, &mut $self.context, $final_collector)
                    .await
                    .map_err(|e| e.with_operator($self.context.task_info.operator_id.clone()))?;
            }
        }
    };
}

pub struct ChainedCollector<'a, 'b> {
    cur: &'a mut ChainedOperator,
    final_collector: &'b mut ArrowCollector,
    index: usize,
    in_partitions: usize,
}

#[async_trait]
impl<'a, 'b> Collector for ChainedCollector<'a, 'b>
where
    'b: 'a,
    'a: 'b,
{
    async fn collect(&mut self, batch: RecordBatch) -> DataflowResult<()> {
        if let Some(next) = &mut self.cur.next {
            let mut collector = ChainedCollector {
                cur: next,
                final_collector: self.final_collector,
                // all chained operators (other than the first one) must have a single input
                index: 0,
                in_partitions: 1,
            };

            self.cur
                .operator
                .process_batch_index(
                    self.index,
                    self.in_partitions,
                    batch,
                    &mut self.cur.context,
                    &mut collector,
                )
                .await?;
        } else {
            self.cur
                .operator
                .process_batch_index(
                    self.index,
                    self.in_partitions,
                    batch,
                    &mut self.cur.context,
                    self.final_collector,
                )
                .await?;
        };

        Ok(())
    }

    async fn broadcast_watermark(&mut self, watermark: Watermark) -> DataflowResult<()> {
        self.cur
            .handle_watermark(watermark, self.index, self.final_collector)
            .await?;

        Ok(())
    }
}

pub struct ChainedOperator {
    pub operator: Box<dyn ArrowOperator>,
    pub context: OperatorContext,
    pub next: Option<Box<ChainedOperator>>,
}

impl ChainedOperator {
    pub fn new(operator: Box<dyn ArrowOperator>, context: OperatorContext) -> Self {
        Self {
            operator,
            context,
            next: None,
        }
    }
}

pub struct ChainIteratorMut<'a> {
    current: Option<&'a mut ChainedOperator>,
}

impl<'a> Iterator for ChainIteratorMut<'a> {
    type Item = (&'a mut dyn ArrowOperator, &'a mut OperatorContext);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(current) = self.current.take() {
            let next = current.next.as_deref_mut();
            self.current = next;
            Some((current.operator.as_mut(), &mut current.context))
        } else {
            None
        }
    }
}

pub struct ChainIterator<'a> {
    current: Option<&'a ChainedOperator>,
}

impl<'a> Iterator for ChainIterator<'a> {
    type Item = (&'a dyn ArrowOperator, &'a OperatorContext);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(current) = self.current.take() {
            let next = current.next.as_deref();
            self.current = next;
            Some((current.operator.as_ref(), &current.context))
        } else {
            None
        }
    }
}

struct IndexedFuture {
    f: Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>,
    i: usize,
}

impl Future for IndexedFuture {
    type Output = (usize, Box<dyn Any + Send>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.f.as_mut().poll(cx) {
            Poll::Ready(r) => Poll::Ready((self.i, r)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl ChainedOperator {
    async fn handle_controller_message(
        &mut self,
        control_message: &ControlMessage,
        shutdown_after_commit: bool,
    ) -> DataflowResult<bool> {
        match control_message {
            ControlMessage::Checkpoint(_) => {
                error!("shouldn't receive checkpoint")
            }
            ControlMessage::Stop { .. } => {
                error!("shouldn't receive stop")
            }
            ControlMessage::Commit { epoch, commit_data } => {
                assert!(
                    self.next.is_none(),
                    "can only commit sinks, which cannot be chained"
                );
                self.operator
                    .handle_commit(*epoch, commit_data, &mut self.context)
                    .await
                    .map_err(|e| e.with_operator(self.context.task_info.operator_id.clone()))?;
                return Ok(shutdown_after_commit);
            }
            ControlMessage::LoadCompacted { compacted } => {
                self.iter_mut()
                    .find(|(_, ctx)| ctx.task_info.operator_id == compacted.operator_id)
                    .unwrap_or_else(|| {
                        panic!(
                            "could not load compacted data for unknown operator '{}'",
                            compacted.operator_id
                        )
                    })
                    .1
                    .load_compacted(compacted)
                    .await;
            }
            ControlMessage::NoOp => {}
        }

        Ok(false)
    }

    pub fn iter(&self) -> ChainIterator<'_> {
        ChainIterator {
            current: Some(self),
        }
    }

    pub fn iter_mut(&mut self) -> ChainIteratorMut<'_> {
        ChainIteratorMut {
            current: Some(self),
        }
    }

    fn name(&self) -> String {
        self.iter()
            .map(|(op, _)| op.name())
            .collect::<Vec<String>>()
            .join(" -> ")
    }

    fn tick_interval(&self) -> Option<Duration> {
        self.iter().filter_map(|(op, _)| op.tick_interval()).min()
    }

    async fn on_start(&mut self) -> DataflowResult<()> {
        for (op, ctx) in self.iter_mut() {
            op.on_start(ctx)
                .await
                .map_err(|e| e.with_operator(ctx.task_info.operator_id.clone()))?;
        }

        Ok(())
    }

    async fn process_batch_index<'a, 'b>(
        &'a mut self,
        index: usize,
        in_partitions: usize,
        batch: RecordBatch,
        final_collector: &'b mut ArrowCollector,
    ) -> DataflowResult<()>
    where
        'a: 'b,
    {
        let mut collector = ChainedCollector {
            cur: self,
            index,
            in_partitions,
            final_collector,
        };

        collector
            .collect(batch)
            .await
            .map_err(|e| e.with_operator(self.context.task_info.operator_id.clone()))
    }

    #[allow(clippy::type_complexity)]
    fn future_to_poll(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = (usize, Box<dyn Any + Send + Send>)> + Send>>> {
        let futures = self
            .iter_mut()
            .enumerate()
            .filter_map(|(i, (op, _))| {
                Some(IndexedFuture {
                    f: op.future_to_poll()?,
                    i,
                })
            })
            .collect::<Vec<_>>();

        match futures.len() {
            0 => None,
            1 => Some(Box::pin(futures.into_iter().next().unwrap())),
            _ => {
                Some(Box::pin(async move {
                    let mut futures = FuturesUnordered::from_iter(futures);
                    // we've guaranteed that the future unordered has at least one element so this
                    // will never unwrap
                    futures.next().await.unwrap()
                }))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_control_message(
        &mut self,
        idx: usize,
        message: &SignalMessage,
        counter: &mut CheckpointCounter,
        closed: &mut HashSet<usize>,
        in_partitions: usize,
        control_tx: &Sender<ControlResp>,
        chain_info: &ChainInfo,
        collector: &mut ArrowCollector,
    ) -> DataflowResult<ControlOutcome> {
        match message {
            SignalMessage::Barrier(t) => {
                debug!("received barrier in {}[{}]", chain_info, idx);

                if counter.all_clear() {
                    control_tx
                        .send(ControlResp::CheckpointEvent(arroyo_rpc::CheckpointEvent {
                            checkpoint_epoch: t.epoch,
                            node_id: chain_info.node_id,
                            operator_id: self.context.task_info.operator_id.clone(),
                            subtask_index: chain_info.task_index,
                            time: SystemTime::now(),
                            event_type: TaskCheckpointEventType::StartedAlignment,
                        }))
                        .await
                        .unwrap();
                }

                if counter.mark(idx, t) {
                    debug!("Checkpointing {chain_info}");

                    self.run_checkpoint(t, control_tx, collector)
                        .await
                        .map_err(|e| e.with_operator(self.context.task_info.operator_id.clone()))?;

                    collector.broadcast(SignalMessage::Barrier(*t)).await;

                    if t.then_stop {
                        // if this is a committing operator, we need to wait for the commit message
                        // before shutting down; otherwise we just stop
                        return if self.operator.is_committing() {
                            Ok(ControlOutcome::StopAfterCommit)
                        } else {
                            Ok(ControlOutcome::Stop)
                        };
                    }
                }
            }
            SignalMessage::Watermark(watermark) => {
                debug!("received watermark {:?} in {}", watermark, chain_info,);

                self.handle_watermark(*watermark, idx, collector)
                    .await
                    .map_err(|e| e.with_operator(self.context.task_info.operator_id.clone()))?;
            }
            SignalMessage::Stop => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return Ok(ControlOutcome::StopAndSendStop);
                }
            }
            SignalMessage::EndOfData => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return Ok(ControlOutcome::Finish);
                }
            }
        }
        Ok(ControlOutcome::Continue)
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        index: usize,
        final_collector: &mut ArrowCollector,
    ) -> DataflowResult<()> {
        trace!(
            "handling watermark {:?} for {}",
            watermark, self.context.task_info,
        );

        let watermark = self
            .context
            .watermarks
            .set(index, watermark)
            .expect("watermark index is too big");

        let Some(watermark) = watermark else {
            return Ok(());
        };

        if let Watermark::EventTime(_t) = watermark {
            // TOOD: pass to table_manager
        }

        match &mut self.next {
            Some(next) => {
                let mut collector = ChainedCollector {
                    cur: next,
                    index: 0,
                    in_partitions: 1,
                    final_collector,
                };

                let watermark = self
                    .operator
                    .handle_watermark(watermark, &mut self.context, &mut collector)
                    .await
                    .map_err(|e| e.with_operator(self.context.task_info.operator_id.clone()))?;

                if let Some(watermark) = watermark {
                    Box::pin(next.handle_watermark(watermark, 0, final_collector)).await?;
                }
            }
            None => {
                let watermark = self
                    .operator
                    .handle_watermark(watermark, &mut self.context, final_collector)
                    .await
                    .map_err(|e| e.with_operator(self.context.task_info.operator_id.clone()))?;
                if let Some(watermark) = watermark {
                    final_collector
                        .broadcast(SignalMessage::Watermark(watermark))
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn handle_future_result(
        &mut self,
        op_index: usize,
        result: Box<dyn Any + Send>,
        final_collector: &mut ArrowCollector,
    ) -> DataflowResult<()> {
        let mut op = self;
        for _ in 0..op_index {
            op = op
                .next
                .as_mut()
                .expect("Future produced from operator index larger than chain size");
        }

        match &mut op.next {
            None => {
                op.operator
                    .handle_future_result(result, &mut op.context, final_collector)
                    .await
                    .map_err(|e| e.with_operator(op.context.task_info.operator_id.clone()))?;
            }
            Some(next) => {
                let mut collector = ChainedCollector {
                    cur: next,
                    final_collector,
                    index: 0,
                    in_partitions: 1,
                };
                op.operator
                    .handle_future_result(result, &mut op.context, &mut collector)
                    .await
                    .map_err(|e| e.with_operator(op.context.task_info.operator_id.clone()))?;
            }
        }

        Ok(())
    }

    async fn run_checkpoint(
        &mut self,
        t: &CheckpointBarrier,
        control_tx: &Sender<ControlResp>,
        final_collector: &mut ArrowCollector,
    ) -> DataflowResult<()> {
        send_checkpoint_event(
            control_tx,
            &self.context.task_info,
            *t,
            TaskCheckpointEventType::StartedCheckpointing,
        )
        .await;

        call_with_collector!(self, final_collector, handle_checkpoint, *t);

        send_checkpoint_event(
            control_tx,
            &self.context.task_info,
            *t,
            TaskCheckpointEventType::FinishedOperatorSetup,
        )
        .await;

        let last_watermark = self.context.watermarks.last_present_watermark();

        run_checkpoint(
            *t,
            &self.context.task_info,
            last_watermark,
            &mut self.context.table_manager,
            control_tx,
        )
        .await;

        if let Some(next) = &mut self.next {
            Box::pin(next.run_checkpoint(t, control_tx, final_collector)).await?;
        }

        Ok(())
    }

    async fn handle_tick(
        &mut self,
        tick: u64,
        final_collector: &mut ArrowCollector,
    ) -> DataflowResult<()> {
        match &mut self.next {
            Some(next) => {
                let mut collector = ChainedCollector {
                    cur: next,
                    index: 0,
                    in_partitions: 1,
                    final_collector,
                };
                self.operator
                    .handle_tick(tick, &mut self.context, &mut collector)
                    .await?;
                Box::pin(next.handle_tick(tick, final_collector)).await?;
            }
            None => {
                self.operator
                    .handle_tick(tick, &mut self.context, final_collector)
                    .await?;
            }
        }

        Ok(())
    }

    async fn on_close(
        &mut self,
        final_message: &Option<SignalMessage>,
        final_collector: &mut ArrowCollector,
    ) -> DataflowResult<()> {
        match &mut self.next {
            Some(next) => {
                let mut collector = ChainedCollector {
                    cur: next,
                    index: 0,
                    in_partitions: 1,
                    final_collector,
                };

                self.operator
                    .on_close(final_message, &mut self.context, &mut collector)
                    .await?;

                Box::pin(next.on_close(final_message, final_collector)).await?;
            }
            None => {
                self.operator
                    .on_close(final_message, &mut self.context, final_collector)
                    .await?;
            }
        }

        Ok(())
    }
}

async fn operator_run_behavior(
    this: &mut ChainedOperator,
    in_qs: &mut [BatchReceiver],
    control_tx: Sender<ControlResp>,
    mut control_rx: Receiver<ControlMessage>,
    collector: &mut ArrowCollector,
    ready: Arc<Barrier>,
) -> DataflowResult<Option<SignalMessage>> {
    this.on_start().await?;

    let chain_info = &mut collector.chain_info.clone();

    ready.wait().await;

    info!("Running node {}", chain_info);

    control_tx
        .send(ControlResp::TaskStarted {
            node_id: chain_info.node_id,
            task_index: chain_info.task_index as usize,
            start_time: SystemTime::now(),
        })
        .await
        .unwrap();

    let name = this.name();
    let mut counter = CheckpointCounter::new(in_qs.len());
    let mut closed: HashSet<usize> = HashSet::new();
    let mut sel = InQReader::new();
    let in_partitions = in_qs.len();

    for (i, q) in in_qs.iter_mut().enumerate() {
        let stream = async_stream::stream! {
          while let Some(item) = q.recv().await {
            yield(i, item);
          }
        };
        sel.push(Box::pin(stream));
    }
    let mut blocked = vec![];
    let mut final_message = None;

    let mut ticks = 0u64;
    let mut interval =
        tokio::time::interval(this.tick_interval().unwrap_or(Duration::from_secs(60)));

    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut shutdown_after_commit = false;

    loop {
        let operator_future: OptionFuture<_> = this.future_to_poll().into();
        tokio::select! {
            Some(control_message) = control_rx.recv() => {
                if this.handle_controller_message(&control_message, shutdown_after_commit).await? {
                    break;
                }
            }

            p = sel.next(), if !shutdown_after_commit => {
                match p {
                    Some(((idx, message), s)) => {
                        let local_idx = idx;

                        trace!("[{}] Handling message {}-{}, {:?}",
                            chain_info.node_id, 0, local_idx, message);

                        match message {
                            ArrowMessage::Data(record) => {
                                TaskCounters::BatchesReceived.for_task(chain_info, |c| c.inc());
                                TaskCounters::MessagesReceived.for_task(chain_info, |c| c.inc_by(record.num_rows() as u64));
                                TaskCounters::BytesReceived.for_task(chain_info, |c| c.inc_by(record.get_array_memory_size() as u64));
                                this.process_batch_index(idx, in_partitions, record, collector)
                                    .instrument(tracing::trace_span!("handle_fn",
                                        name,
                                        node_id = chain_info.node_id,
                                        subtask_idx = chain_info.task_index)
                                ).await?;
                            }
                            ArrowMessage::Signal(signal) => {
                                match this.handle_control_message(idx, &signal, &mut counter, &mut closed, in_partitions,
                                    &control_tx, chain_info, collector).await? {
                                    ControlOutcome::Continue => {}
                                    ControlOutcome::Stop => {
                                        // just stop; the stop will have already been broadcast for example by
                                        // a final checkpoint
                                        break;
                                    }
                                    ControlOutcome::StopAfterCommit => {
                                        shutdown_after_commit = true;
                                    }
                                    ControlOutcome::Finish => {
                                        final_message = Some(SignalMessage::EndOfData);
                                        break;
                                    }
                                    ControlOutcome::StopAndSendStop => {
                                        final_message = Some(SignalMessage::Stop);
                                        break;
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
                        info!("[{}] Stream completed", chain_info);
                        if !shutdown_after_commit {
                            break;
                        }
                    }
                }
            }
            Some(val) = operator_future => {
                this.handle_future_result(val.0, val.1, collector).await?;
            }
            _ = interval.tick() => {
                this.handle_tick(ticks, collector).await?;
                ticks += 1;
            }
        }
    }

    this.on_close(&final_message, collector).await?;
    Ok(final_message)
}

pub enum AsDisplayable<'a> {
    Str(&'a str),
    String(String),
    Display(&'a dyn Display),
    Debug(&'a dyn Debug),
    Plan(&'a dyn ExecutionPlan),
    Schema(&'a Schema),
    List(Vec<String>),
}

impl<'a> From<&'a str> for AsDisplayable<'a> {
    fn from(s: &'a str) -> Self {
        AsDisplayable::Str(s)
    }
}

impl From<String> for AsDisplayable<'_> {
    fn from(s: String) -> Self {
        AsDisplayable::String(s)
    }
}

impl<'a> From<&'a dyn ExecutionPlan> for AsDisplayable<'a> {
    fn from(p: &'a dyn ExecutionPlan) -> Self {
        AsDisplayable::Plan(p)
    }
}

impl<'a> From<&'a Schema> for AsDisplayable<'a> {
    fn from(s: &'a Schema) -> Self {
        AsDisplayable::Schema(s)
    }
}

impl Display for AsDisplayable<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AsDisplayable::Str(s) => {
                write!(f, "{s}")
            }
            AsDisplayable::String(s) => {
                write!(f, "{s}")
            }
            AsDisplayable::Display(d) => {
                write!(f, "{d}")
            }
            AsDisplayable::Debug(d) => {
                write!(f, "{d:?}")
            }
            AsDisplayable::Plan(p) => {
                write!(f, "```\n{}\n```", displayable(*p).indent(false))
            }
            AsDisplayable::Schema(s) => {
                for field in s.fields() {
                    write!(f, "\n  * {}: {:?}, ", field.name(), field.data_type())?;
                }

                Ok(())
            }
            AsDisplayable::List(list) => {
                for s in list {
                    write!(f, "\n * {s}")?;
                }

                Ok(())
            }
        }
    }
}

pub struct DisplayableOperator<'a> {
    pub name: Cow<'a, str>,
    pub fields: Vec<(&'static str, AsDisplayable<'a>)>,
}

#[async_trait::async_trait]
pub trait ArrowOperator: Send + 'static {
    fn name(&self) -> String;

    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }

    fn is_committing(&self) -> bool {
        false
    }

    fn tick_interval(&self) -> Option<Duration> {
        None
    }

    fn display(&self) -> DisplayableOperator<'_> {
        DisplayableOperator {
            name: self.name().into(),
            fields: vec![],
        }
    }

    #[allow(unused_variables)]
    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    async fn process_batch_index(
        &mut self,
        index: usize,
        in_partitions: usize,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.process_batch(batch, ctx, collector).await
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()>;

    #[allow(clippy::type_complexity)]
    fn future_to_poll(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>> {
        None
    }

    #[allow(unused_variables)]
    async fn handle_future_result(
        &mut self,
        result: Box<dyn Any + Send>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<Option<Watermark>> {
        Ok(Some(watermark))
    }

    #[allow(unused_variables)]
    async fn handle_checkpoint(
        &mut self,
        b: CheckpointBarrier,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    async fn handle_commit(
        &mut self,
        epoch: u32,
        commit_data: &HashMap<String, HashMap<u32, Vec<u8>>>,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        warn!("default handling of commit with epoch {:?}", epoch);
        Ok(())
    }

    #[allow(unused_variables)]
    async fn handle_tick(
        &mut self,
        tick: u64,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    async fn on_close(
        &mut self,
        final_message: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct Registry {
    dylibs: Arc<std::sync::Mutex<HashMap<String, Arc<UdfDylib>>>>,
    udfs: HashMap<String, Arc<ScalarUDF>>,
    udafs: HashMap<String, Arc<AggregateUDF>>,
    udwfs: HashMap<String, Arc<WindowUDF>>,
}

impl Registry {
    pub async fn load_dylib(
        &mut self,
        name: &str,
        config: &DylibUdfConfig,
    ) -> anyhow::Result<Arc<UdfDylib>> {
        {
            let dylibs = self.dylibs.lock().unwrap();
            if let Some(dylib) = dylibs.get(&config.dylib_path) {
                return Ok(Arc::clone(dylib));
            }
        }

        // Download a UDF dylib from the object store
        let signature = Signature::exact(config.arg_types.clone(), Volatility::Volatile);

        let udf = StorageProvider::get_url(&config.dylib_path)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Unable to fetch UDF dylib from '{}': {:?}",
                    config.dylib_path, e
                )
            });

        let local_udfs_dir = "/tmp/arroyo/local_udfs";
        tokio::fs::create_dir_all(local_udfs_dir)
            .await
            .map_err(|e| anyhow!("unable to create local udfs dir: {:?}", e))?;

        let dylib_file_name = Path::new(&config.dylib_path)
            .file_name()
            .ok_or_else(|| anyhow!("Invalid dylib path: {}", config.dylib_path))?;
        let local_dylib_path = Path::new(local_udfs_dir).join(dylib_file_name);

        // write the dylib to a local file if it's not already present
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&local_dylib_path)
            .await
        {
            Ok(mut file) => {
                file.write_all(&udf).await?;
                file.sync_all().await?;
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                // nothing to do, UDF already written
            }
            Err(e) => {
                bail!(
                    "Failed to write UDF dylib to {}: {}",
                    local_dylib_path.to_string_lossy(),
                    e
                );
            }
        };

        let interface = if config.is_async {
            UdfInterface::Async(Arc::new(ContainerOrLocal::Container(unsafe {
                Container::load(local_dylib_path).unwrap()
            })))
        } else {
            UdfInterface::Sync(Arc::new(ContainerOrLocal::Container(unsafe {
                Container::load(local_dylib_path).unwrap()
            })))
        };

        let udf = Arc::new(UdfDylib::new(
            name.to_string(),
            signature,
            config.return_type.clone(),
            interface,
        ));

        self.dylibs
            .lock()
            .unwrap()
            .insert(config.dylib_path.clone(), udf.clone());

        if !config.is_async {
            self.add_udfs(&udf, config);
        }

        Ok(udf)
    }

    pub fn add_local_udf(&mut self, local_udf: &LocalUdf) {
        let udf = Arc::new(UdfDylib::new(
            (*local_udf.config.name).to_string(),
            (*local_udf.config.signature).clone(),
            (*local_udf.config.return_type).clone(),
            local_udf.config.udf.clone(),
        ));

        self.dylibs
            .lock()
            .unwrap()
            .insert(local_udf.config.name.to_string(), udf.clone());

        if !local_udf.is_async {
            self.add_udfs(
                &udf,
                &DylibUdfConfig {
                    dylib_path: local_udf.config.name.to_string(),
                    arg_types: match &local_udf.config.signature.type_signature {
                        TypeSignature::Exact(exact) => exact.clone(),
                        _ => {
                            panic!("only exact type signatures are supported for udfs")
                        }
                    },
                    return_type: (*local_udf.config.return_type).clone(),
                    aggregate: local_udf.is_aggregate,
                    is_async: local_udf.is_async,
                },
            );
        }
    }

    fn add_udfs(&mut self, dylib: &UdfDylib, config: &DylibUdfConfig) {
        let dylib: SyncUdfDylib = dylib.try_into().unwrap();
        if config.aggregate {
            let output_type = Arc::new(config.return_type.clone());

            let args: Vec<_> = config
                .arg_types
                .iter()
                .map(|arg| {
                    UdafArg::new(match arg {
                        DataType::List(f) => Arc::clone(f),
                        _ => {
                            panic!("arg type {:?} for UDAF {} is not a list", arg, dylib.name())
                        }
                    })
                })
                .collect();

            let name = dylib.name().to_string();
            let udaf = Arc::new(create_udaf(
                &name,
                config
                    .arg_types
                    .iter()
                    .map(|t| inner_type(t).expect("UDAF arg is not a vec"))
                    .collect(),
                Arc::new(config.return_type.clone()),
                Volatility::Volatile,
                Arc::new(move |_| {
                    Ok(Box::new(ArroyoUdaf::new(
                        args.clone(),
                        output_type.clone(),
                        Arc::new(dylib.clone()),
                    )))
                }),
                Arc::new(config.arg_types.clone()),
            ));
            self.udafs.insert(name, udaf);
        } else {
            self.udfs
                .insert(dylib.name().to_string(), Arc::new(ScalarUDF::from(dylib)));
        }
    }

    pub async fn add_python_udf(&mut self, udf: &PythonUdfConfig) -> anyhow::Result<()> {
        let udf = PythonUDF::parse(&*udf.definition).await?;

        self.udfs.insert((*udf.name).clone(), Arc::new(udf.into()));

        Ok(())
    }

    pub fn get_dylib(&self, path: &str) -> Option<Arc<UdfDylib>> {
        self.dylibs.lock().unwrap().get(path).cloned()
    }

    pub fn add_udf(&mut self, udf: Arc<ScalarUDF>) {
        self.udfs.insert(udf.name().to_string(), udf);
    }

    pub fn add_udaf(&mut self, udaf: Arc<AggregateUDF>) {
        self.udafs.insert(udaf.name().to_string(), udaf);
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
            .ok_or_else(|| DataFusionError::Execution(format!("UDF {name} not found")))
    }

    fn udaf(&self, name: &str) -> DFResult<Arc<AggregateUDF>> {
        self.udafs
            .get(name)
            .cloned()
            .ok_or_else(|| DataFusionError::Execution(format!("UDAF {name} not found")))
    }

    fn udwf(&self, name: &str) -> DFResult<Arc<WindowUDF>> {
        self.udwfs
            .get(name)
            .cloned()
            .ok_or_else(|| DataFusionError::Execution(format!("UDWF {name} not found")))
    }

    fn register_function_rewrite(
        &mut self,
        _: Arc<dyn FunctionRewrite + Send + Sync>,
    ) -> DFResult<()> {
        // no-op
        Ok(())
    }

    fn register_expr_planner(&mut self, _expr_planner: Arc<dyn ExprPlanner>) -> DFResult<()> {
        // no-op
        Ok(())
    }

    fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> DFResult<Option<Arc<ScalarUDF>>> {
        Ok(self.udfs.insert(udf.name().to_string(), udf))
    }

    fn register_udaf(&mut self, udaf: Arc<AggregateUDF>) -> DFResult<Option<Arc<AggregateUDF>>> {
        Ok(self.udafs.insert(udaf.name().to_string(), udaf))
    }

    fn register_udwf(&mut self, udwf: Arc<WindowUDF>) -> DFResult<Option<Arc<WindowUDF>>> {
        Ok(self.udwfs.insert(udwf.name().to_string(), udwf))
    }

    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        vec![]
    }
}
