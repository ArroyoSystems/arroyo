use std::collections::HashMap;

use arroyo_df::meta::SchemaRefWithMeta;
use arroyo_rpc::grpc::CheckpointMetadata;

use arroyo_types::{Data, Key, Message, Record, RecordBatchData, TaskInfo, Watermark, Window};

use bincode::config;
use futures::StreamExt;

use tracing::Instrument;

use crate::{
    engine::{CheckpointCounter, Context, StreamNode},
    ControlOutcome,
};

impl<T> StreamNode for T
where
    T: ProcessFuncTrait,
    T::InKey: Key,
    T::InT: Data,
    T::OutKey: Key,
    T::OutT: Data,
{
    fn node_name(&self) -> String {
        self.name()
    }

    fn start(
        mut self: Box<Self>,
        task_info: TaskInfo,
        checkpoint_metadata: Option<CheckpointMetadata>,
        control_rx: tokio::sync::mpsc::Receiver<arroyo_rpc::ControlMessage>,
        control_tx: tokio::sync::mpsc::Sender<arroyo_rpc::ControlResp>,
        in_qs: Vec<Vec<tokio::sync::mpsc::Receiver<crate::engine::QueueItem>>>,
        out_qs: Vec<Vec<crate::engine::OutQueue>>,
    ) -> tokio::task::JoinHandle<()> {
        if in_qs.is_empty() {
            panic!(
                "Wrong number of logical inputs for node {} (expected {}, found {})",
                task_info.operator_name,
                1usize,
                in_qs.len()
            );
        }
        let in_qs: Vec<_> = in_qs.into_iter().flatten().collect();
        let tables = self.tables();
        tokio::spawn(async move {
            let mut ctx = crate::engine::Context::<T::OutKey, T::OutT>::new(
                task_info,
                checkpoint_metadata,
                control_rx,
                control_tx,
                in_qs.len(),
                out_qs,
                tables,
                self.table_schemas(),
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
            let mut final_message = None;
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
                        match idx/in_partitions{
                          0usize => {
                            let message = match item {
                              crate::engine::QueueItem::Data(datum) => {
                                *datum.downcast().unwrap_or_else(|_| panic!("failed to downcast data in {}",self.name()))
                              }crate::engine::QueueItem::Bytes(bs) => {
                                crate::metrics::TaskCounters::BytesReceived.for_task(&ctx.task_info).inc_by(bs.len()as u64);
                                bincode::decode_from_slice(&bs,config::standard()).expect("Failed to deserialize message (expected <Self::InKey, Self::InT>)").0
                              }
                            };
                            let local_idx = idx-in_partitions*0usize;
                            tracing::debug!("[{}] Received message {}-{}, {:?} [{:?}]",ctx.task_info.operator_name,0usize,local_idx,message,stacker::remaining_stack());
                            match &message {
                              arroyo_types::Message::Record(record) => {
                                self.process_element(record, &mut ctx).instrument(tracing::trace_span!("handle_fn",name,operator_id = task_info.operator_id,subtask_idx = task_info.task_index)).await;
                              },
                              arroyo_types::Message::RecordBatch(batch) => {
                                self.process_record_batch(batch, &mut ctx).await;
                              }
                              _ => {
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
                              }
                            }
                            tracing::debug!("[{}] Handled message {}-{}, {:?} [{:?}]",ctx.task_info.operator_name,0usize,local_idx,message,stacker::remaining_stack());
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
}

#[async_trait::async_trait]
pub trait ProcessFuncTrait: Send + 'static {
    type InKey: Key;
    type InT: Data;
    type OutKey: Key;
    type OutT: Data;
    fn name(&self) -> String;

    async fn process_element(
        &mut self,
        record: &Record<Self::InKey, Self::InT>,
        ctx: &mut Context<Self::OutKey, Self::OutT>,
    );

    async fn process_record_batch(
        &mut self,
        record_batch: &RecordBatchData,
        ctx: &mut Context<Self::OutKey, Self::OutT>,
    );

    async fn handle_control_message(
        &mut self,
        idx: usize,
        message: &arroyo_types::Message<Self::InKey, Self::InT>,
        counter: &mut CheckpointCounter,
        closed: &mut std::collections::HashSet<usize>,
        in_partitions: usize,
        ctx: &mut Context<Self::OutKey, Self::OutT>,
    ) -> ControlOutcome {
        match message {
            Message::Record(_record) => {
                unreachable!();
            }
            Message::RecordBatch(_batch) => {
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

                if counter.mark(idx, t) {
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

    #[tracing::instrument(
            level = "trace",
            skip(self, ctx),
            fields(
                name=self.name(),
                operator_id=ctx.task_info.operator_id,
                subtask_idx=ctx.task_info.task_index,
            ),
        )]
    #[must_use]
    async fn checkpoint(
        &mut self,
        checkpoint_barrier: arroyo_types::CheckpointBarrier,
        ctx: &mut Context<Self::OutKey, Self::OutT>,
    ) -> bool {
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

        ctx.broadcast(arroyo_types::Message::Barrier(checkpoint_barrier))
            .await;

        checkpoint_barrier.then_stop
    }

    async fn handle_watermark_int(
        &mut self,
        watermark: arroyo_types::Watermark,
        ctx: &mut Context<Self::OutKey, Self::OutT>,
    ) {
        // process timers
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
        _checkpoint_barrier: &arroyo_types::CheckpointBarrier,
        _ctx: &mut Context<Self::OutKey, Self::OutT>,
    ) {
    }
    async fn on_close(&mut self, _ctx: &mut Context<Self::OutKey, Self::OutT>) {}

    async fn handle_timer(
        &mut self,
        _key: Self::OutKey,
        _tv: Window,
        _ctx: &mut Context<Self::OutKey, Self::OutT>,
    ) {
    }

    async fn handle_watermark(
        &mut self,
        watermark: arroyo_types::Watermark,
        ctx: &mut Context<Self::OutKey, Self::OutT>,
    ) {
        // by default, just pass watermarks on down
        ctx.broadcast(arroyo_types::Message::Watermark(watermark))
            .await;
    }

    async fn handle_commit(
        &mut self,
        epoch: u32,
        _commit_data: HashMap<char, HashMap<u32, Vec<u8>>>,
        _ctx: &mut Context<Self::OutKey, Self::OutT>,
    ) {
        tracing::warn!("default handling of commit with epoch {:?}", epoch);
    }

    fn tables(&self) -> Vec<arroyo_rpc::grpc::TableDescriptor> {
        vec![]
    }

    fn table_schemas(&self) -> HashMap<char, SchemaRefWithMeta> {
        HashMap::new()
    }

    async fn on_start(&mut self, _ctx: &mut crate::engine::Context<Self::OutKey, Self::OutT>) {}
}
