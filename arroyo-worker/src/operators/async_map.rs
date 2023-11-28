use std::collections::VecDeque;
use std::fs;
use std::str::FromStr;
use std::future::Future;
use std::{fmt::Debug, path::PathBuf};

use std::marker::PhantomData;
use std::ops::Add;

use crate::engine::{Collector, Context, StreamNode};
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::TableDescriptor;
use arroyo_types::{
    from_millis, to_millis, CheckpointBarrier, Data, GlobalKey, Key, Message, Record, TaskInfo,
    UpdatingData, Watermark, Window,
};
use bincode::{config, Decode, Encode};
use futures::stream::FuturesUnordered;
use std::time::{Duration, SystemTime};
use tracing::{debug, info};

#[derive(StreamNode)]
pub struct AsyncMapOperator<InKey: Key, InT: Data, OutKey: Key, OutT: Data> {
    pub name: String,
    pub deque: VecDeque<Option<Message<OutKey, OutT>>>,
    pub map_fn: Box<dyn AsyncRecordMap<InKey, InT, OutKey, OutT> + Send>,
}

#[async_trait::async_trait]
trait AsyncRecordMap<InKey: Key, InT: Data, OutKey: Key, OutT: Data> {
    async fn map(&self, record: &Record<InKey, InT>, task_info: &TaskInfo) -> Record<OutKey, OutT>;
}

// Recursive expansion of process_fn macro
// ========================================

impl<InKey: Key, InT: Data, OutKey: Key, OutT: Data> AsyncMapOperator<InKey, InT, OutKey, OutT> {
    fn name(&self) -> String {
        self.name.clone()
    }
    async fn process_element(
        &mut self,
        record: &Record<InKey, InT>,
        ctx: &mut Context<OutKey, OutT>,
    ) -> Record<OutKey, OutT> {
        let fut = self.map_fn.map(record, &ctx.task_info);
        fut.await
    }


    fn start_fn(
        mut self: Box<Self>,
        task_info: arroyo_types::TaskInfo,
        restore_from: Option<arroyo_rpc::grpc::CheckpointMetadata>,
        control_rx: tokio::sync::mpsc::Receiver<arroyo_rpc::ControlMessage>,
        control_tx: tokio::sync::mpsc::Sender<arroyo_rpc::ControlResp>,
        mut in_qs: Vec<Vec<tokio::sync::mpsc::Receiver<crate::engine::QueueItem>>>,
        out_qs: Vec<Vec<crate::engine::OutQueue>>,
    ) -> tokio::task::JoinHandle<()> {
        use arroyo_types::*;
        use bincode;
        use bincode::config;
        use futures::stream::FuturesUnordered;
        use futures::{FutureExt, StreamExt};
        use std::collections::HashMap;
        use tokio;
        use tracing::Instrument;
        if in_qs.len() < 1usize {
            panic!(
                "Wrong number of logical inputs for node {} (expected {}, found {})",
                task_info.operator_name,
                1usize,
                in_qs.len()
            );
        }
        let mut in_qs: Vec<_> = in_qs.into_iter().flatten().collect();
        let tables = self.tables();
        tokio::spawn(async move {
            let mut ctx = crate::engine::Context::<OutKey, OutT>::new(
                task_info,
                restore_from,
                control_rx,
                control_tx,
                in_qs.len(),
                out_qs,
                tables,
            )
            .await;
            Self::on_start(&mut (*self), &mut ctx).await;
            let task_info = ctx.task_info.clone();
            let name = self.name();
            let mut counter = crate::engine::CheckpointCounter::new(in_qs.len());
            let mut closed: std::collections::HashSet<usize> = std::collections::HashSet::new();
            let mut sel = crate::inq_reader::InQReader::new();
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
            let mut final_message: Option<Message<OutKey, OutT>> = None;
            let mut futures = FuturesUnordered::new();
            let mut deque: VecDeque<Option<Record<OutKey, OutT>>> = VecDeque::new();
            let mut records_consumed = 0;
            let mut records_flushed = 0;
            loop {
                tokio::select! {
                  Some(control_message) = ctx.control_rx.recv() => {
                    match control_message {
                      arroyo_rpc::ControlMessage::Checkpoint(_) => tracing::warn!("shouldn't receive checkpoint"),arroyo_rpc::ControlMessage::Stop {
                        mode:_
                      } => tracing::warn!("shouldn't receive stop"),arroyo_rpc::ControlMessage::Commit {
                        epoch,commit_data
                      } => {
                        self.handle_commit(epoch,commit_data, &mut ctx).await;
                      },arroyo_rpc::ControlMessage::LoadCompacted {
                        compacted
                      } => {
                        ctx.load_compacted(compacted).await;
                      }arroyo_rpc::ControlMessage::NoOp => {}

                    }
                  }p = sel.next() => {
                    match p {
                      Some(((idx,item),s)) => {
                        match idx/(in_partitions/1usize){
                          0usize => {
                            let message = match item {
                              crate::engine::QueueItem::Data(datum) => {
                                *datum.downcast().expect(&format!("failed to downcast data in {}",self.name()))
                              }crate::engine::QueueItem::Bytes(bs) => {
                                crate::metrics::TaskCounters::BytesReceived.for_task(&ctx.task_info).inc_by(bs.len()as u64);
                                bincode::decode_from_slice(&bs,config::standard()).expect("Failed to deserialize message (expected <InKey, InT>)").0
                              }
                            };
                            let local_idx = idx-(in_partitions/1usize)*0usize;
                            tracing::debug!("[{}] Received message {}-{}, {:?} [{:?}]",ctx.task_info.operator_name,0usize,local_idx,message,stacker::remaining_stack());
                            if let arroyo_types::Message::Record(record) =  &message {
                              crate::metrics::TaskCounters::MessagesReceived.for_task(&ctx.task_info).inc();
                              let fut = self.map_fn.map(record, &ctx.task_info);
                              futures.push(with_index(fut, records_consumed));
                              records_consumed += 1;
                              deque.push_back(None);
                              //Self::process_element(&mut(*self),record, &mut ctx).instrument(tracing::trace_span!("handle_fn",name,operator_id = task_info.operator_id,subtask_idx = task_info.task_index)).await;
                            }else {
                              match Self::handle_control_message(&mut(*self),idx, &message, &mut counter, &mut closed,in_partitions, &mut ctx).await {
                                crate::ControlOutcome::Continue => {}
                                crate::ControlOutcome::Stop => {
                                  final_message = Some(arroyo_types::Message::Stop);
                                  break;
                                }crate::ControlOutcome::Finish => {
                                  final_message = Some(arroyo_types::Message::EndOfData);
                                  break;
                                }
                              }
                            }tracing::debug!("[{}] Handled message {}-{}, {:?} [{:?}]",ctx.task_info.operator_name,0usize,local_idx,message,stacker::remaining_stack());
                            if counter.is_blocked(idx){
                              blocked.push(s);
                            }else {
                              if counter.all_clear()&& !blocked.is_empty(){
                                for q in blocked.drain(..){
                                  sel.push(q);
                                }
                              }sel.push(s);
                            }
                          }_ => unreachable!()
                        }
                      }None => {
                        tracing::info!("[{}] Stream completed",ctx.task_info.operator_name);
                        break;
                      }
                    }
                  }
                    Some((index, record)) = futures.next() => {
                        let deque_index = index - records_flushed;
                        self.deque[deque_index] = Some(Message::Record(record));
                        while self.deque.front().map(|r| r.is_some()).unwrap_or(false) {
                            let message = self.deque.pop_front().flatten().unwrap();
                            records_flushed += 1;
                            match message {
                                Message::Record(record) => ctx.collect(
                                    record,
                                ).await,
                                Message::Barrier(barrier) => ctx.broadcast(
                                    Message::Barrier(barrier),
                                ).await,
                                Message::Watermark(watermark) => ctx.broadcast(
                                    Message::Watermark(watermark),
                                ).await,
                                _ => panic!()
                            }
                        }
                    }
                }
            }
            while futures.len() > 0 {
                let (index, record) = futures.next().await.unwrap();
                let deque_index = index - records_flushed;
                self.deque[deque_index] = Some(Message::Record(record));
                while self.deque.front().map(|r| r.is_some()).unwrap_or(false) {
                    let message = self.deque.pop_front().flatten().unwrap();
                    records_flushed += 1;
                    match message {
                        Message::Record(record) => ctx.collect(
                            record,
                        ).await,
                        Message::Barrier(barrier) => ctx.broadcast(
                            Message::Barrier(barrier),
                        ).await,
                        Message::Watermark(watermark) => ctx.broadcast(
                            Message::Watermark(watermark),
                        ).await,
                        _ => panic!()
                    }
                }
            }
            Self::on_close(&mut (*self), &mut ctx).await;
            if let Some(final_message) = final_message {
                ctx.broadcast(final_message).await;
            }
            tracing::info!(
                "Task finished {}-{}",
                ctx.task_info.operator_name,
                ctx.task_info.task_index
            );
            ctx.control_tx
                .send(arroyo_rpc::ControlResp::TaskFinished {
                    operator_id: ctx.task_info.operator_id.clone(),
                    task_index: ctx.task_info.task_index,
                })
                .await
                .expect("control response unwrap");
        })
    }
    async fn handle_control_message<CONTROL_K: arroyo_types::Key, CONTROL_T: arroyo_types::Data>(
        &mut self,
        idx: usize,
        message: &arroyo_types::Message<CONTROL_K, CONTROL_T>,
        counter: &mut crate::engine::CheckpointCounter,
        closed: &mut std::collections::HashSet<usize>,
        in_partitions: usize,
        ctx: &mut crate::engine::Context<OutKey, OutT>,
    ) -> crate::ControlOutcome {
        use arroyo_types::*;
        use tracing::info;
        use tracing::trace;
        match message {
            Message::Record(record) => {
                unreachable!();
            }
            Message::Barrier(t) => {
                tracing::debug!(
                    "received barrier in {}-{}-{}-{}",
                    self.name(),
                    ctx.task_info.operator_id,
                    ctx.task_info.task_index,
                    idx
                );
                if counter.all_clear() {
                    ctx.control_tx
                        .send(arroyo_rpc::ControlResp::CheckpointEvent(
                            arroyo_rpc::CheckpointEvent {
                                checkpoint_epoch: t.epoch,
                                operator_id: ctx.task_info.operator_id.clone(),
                                subtask_index: ctx.task_info.task_index as u32,
                                time: std::time::SystemTime::now(),
                                event_type:
                                    arroyo_rpc::grpc::TaskCheckpointEventType::StartedAlignment,
                            },
                        ))
                        .await
                        .unwrap();
                }
                if counter.mark(idx, &t) {
                    tracing::debug!(
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
            Message::Watermark(watermark) => {
                tracing::debug!(
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
            Message::Stop => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return crate::ControlOutcome::Stop;
                }
            }
            Message::EndOfData => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return crate::ControlOutcome::Finish;
                }
            }
        }
        crate::ControlOutcome::Continue
    }
    #[must_use]
    async fn checkpoint(
        &mut self,
        checkpoint_barrier: arroyo_types::CheckpointBarrier,
        ctx: &mut crate::engine::Context<OutKey, OutT>,
    ) -> bool {
        {}
        let __tracing_attr_span = tracing::span!(target:module_path!(),tracing::Level::TRACE,"checkpoint",checkpoint_barrier = tracing::field::debug(&checkpoint_barrier),name = self.name(),operator_id = ctx.task_info.operator_id,subtask_idx = ctx.task_info.task_index,);
        let __tracing_instrument_future = async move {
            #[allow(
                unknown_lints,
                unreachable_code,
                clippy::diverging_sub_expression,
                clippy::let_unit_value,
                clippy::unreachable,
                clippy::let_with_type_underscore,
                clippy::empty_loop
            )]
            if false {
                let __tracing_attr_fake_return: bool = loop {};
                return __tracing_attr_fake_return;
            }
            {
                crate::process_fn::ProcessFnUtils::send_checkpoint_event(
                    checkpoint_barrier,
                    ctx,
                    arroyo_rpc::grpc::TaskCheckpointEventType::StartedCheckpointing,
                )
                .await;
                self.handle_checkpoint(&checkpoint_barrier, ctx).await;
                crate::process_fn::ProcessFnUtils::send_checkpoint_event(
                    checkpoint_barrier,
                    ctx,
                    arroyo_rpc::grpc::TaskCheckpointEventType::FinishedOperatorSetup,
                )
                .await;
                let watermark = ctx.watermarks.last_present_watermark();
                ctx.state.checkpoint(checkpoint_barrier, watermark).await;
                crate::process_fn::ProcessFnUtils::send_checkpoint_event(
                    checkpoint_barrier,
                    ctx,
                    arroyo_rpc::grpc::TaskCheckpointEventType::FinishedSync,
                )
                .await;
                self.deque.push_back(Some(arroyo_types::Message::Barrier(checkpoint_barrier)));
                checkpoint_barrier.then_stop
            }
        };
        if !__tracing_attr_span.is_disabled() {
            tracing::Instrument::instrument(__tracing_instrument_future, __tracing_attr_span).await
        } else {
            __tracing_instrument_future.await
        }
    }
    async fn handle_watermark_int(
        &mut self,
        watermark: arroyo_types::Watermark,
        ctx: &mut crate::engine::Context<OutKey, OutT>,
    ) {
        tracing::trace!(
            "handling watermark {:?} for {}-{}",
            watermark,
            ctx.task_info.operator_name,
            ctx.task_info.task_index
        );
        if let arroyo_types::Watermark::EventTime(t) = watermark {
            let finished = crate::process_fn::ProcessFnUtils::finished_timers(t, ctx).await;
            for (k, tv) in finished {
                self.handle_timer(k, tv.data, ctx).await;
            }
        }
        self.handle_watermark(watermark, ctx).await;
    }
    async fn handle_checkpoint(
        &mut self,
        checkpoint_barrier: &arroyo_types::CheckpointBarrier,
        ctx: &mut crate::engine::Context<OutKey, OutT>,
    ) {
    }

    async fn on_start(&mut self, ctx: &mut crate::engine::Context<OutKey, OutT>) {}

    async fn on_close(&mut self, ctx: &mut crate::engine::Context<OutKey, OutT>) {
        
    }

    async fn handle_timer(
        &mut self,
        key: OutKey,
        tv: (),
        ctx: &mut crate::engine::Context<OutKey, OutT>,
    ) {
    }

    async fn handle_tick(&mut self, tick: u64, ctx: &mut crate::engine::Context<OutKey, OutT>) {}

    async fn handle_watermark(
        &mut self,
        watermark: arroyo_types::Watermark,
        ctx: &mut crate::engine::Context<OutKey, OutT>,
    ) {
        self.deque.push_back(Some(arroyo_types::Message::Watermark(watermark)));
    }
    async fn handle_commit(
        &mut self,
        epoch: u32,
        commit_data: std::collections::HashMap<char, std::collections::HashMap<u32, Vec<u8>>>,
        ctx: &mut Context<OutKey, OutT>,
    ) {
        tracing::warn!("default handling of commit with epoch {:?}", epoch);
    }
    fn tables(&self) -> Vec<arroyo_rpc::grpc::TableDescriptor> {
        vec![]
    }
}


async fn with_index<Fut, T>(fut: Fut, i: usize) -> (usize, T)
where
    Fut: std::future::Future<Output = T>,
{
    let result = fut.await;
    (i, result)
}