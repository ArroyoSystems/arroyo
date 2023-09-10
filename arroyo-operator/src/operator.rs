use std::hash::Hash;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::SystemTime,
};

use crate::inq_reader::InQReader;
use crate::{CheckpointCounter, ControlOutcome, WatermarkHolder};
use arrow::datatypes::{Schema, SchemaRef};
use arrow_array::iterator::ArrayIter;
use arrow_array::{downcast_primitive_array, Array, ArrayAccessor, ArrayRef, RecordBatch};
use arroyo_datastream::logical::ArrowSchema;
use arroyo_metrics::{counter_for_task, gauge_for_task};
use arroyo_rpc::{
    grpc::{
        CheckpointMetadata, TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior,
        TaskCheckpointEventType,
    },
    ControlMessage, ControlResp,
};
use arroyo_state::{hash_key, BackingStore, StateBackend, StateStore};
use arroyo_types::{
    from_micros, ArrowMessage, ArrowRecord, CheckpointBarrier, TaskInfo, Watermark, BYTES_RECV,
    BYTES_SENT, MESSAGES_RECV, MESSAGES_SENT,
};
use datafusion_physical_expr::hash_utils;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use prometheus::{labels, IntCounter, IntGauge};
use rand::{random, Rng};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

pub fn server_for_hash(x: u64, n: usize) -> usize {
    let range_size = u64::MAX / (n as u64);
    (n - 1).min((x / range_size) as usize)
}

pub static TIMER_TABLE: char = '[';

pub struct ArrowContext<S: BackingStore = StateBackend> {
    pub task_info: TaskInfo,
    pub control_rx: Receiver<ControlMessage>,
    pub control_tx: Sender<ControlResp>,
    pub watermarks: WatermarkHolder,
    pub state: StateStore<S>,
    pub in_schemas: Vec<ArrowSchema>,
    pub out_schema: Option<ArrowSchema>,
    pub collector: ArrowCollector,
    pub counters: HashMap<&'static str, IntCounter>,
}

impl ArrowContext {
    pub async fn new(
        task_info: TaskInfo,
        restore_from: Option<CheckpointMetadata>,
        control_rx: Receiver<ControlMessage>,
        control_tx: Sender<ControlResp>,
        input_partitions: usize,
        in_schemas: Vec<ArrowSchema>,
        out_schema: Option<ArrowSchema>,
        out_qs: Vec<Vec<Sender<ArrowMessage>>>,
        mut tables: Vec<TableDescriptor>,
    ) -> ArrowContext {
        tables.push(TableDescriptor {
            name: TIMER_TABLE.to_string(),
            description: "timer state".to_string(),
            table_type: TableType::TimeKeyMap as i32,
            delete_behavior: TableDeleteBehavior::None as i32,
            write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
            retention_micros: 0,
        });

        let (state, watermark) = if let Some(metadata) = restore_from {
            let watermark = {
                let metadata = StateBackend::load_operator_metadata(
                    &task_info.job_id,
                    &task_info.operator_id,
                    metadata.epoch,
                )
                .await;
                metadata
                    .expect("require metadata")
                    .min_watermark
                    .map(from_micros)
            };
            let state = StateStore::<StateBackend>::from_checkpoint(
                &task_info,
                metadata,
                tables,
                control_tx.clone(),
            )
            .await;

            (state, watermark)
        } else {
            (
                StateStore::<StateBackend>::new(&task_info, tables, control_tx.clone()).await,
                None,
            )
        };

        let mut counters = HashMap::new();

        if let Some(c) = counter_for_task(
            &task_info,
            MESSAGES_RECV,
            "Count of messages received by this subtask",
            HashMap::new(),
        ) {
            counters.insert(MESSAGES_RECV, c);
        }

        if let Some(c) = counter_for_task(
            &task_info,
            MESSAGES_SENT,
            "Count of messages sent by this subtask",
            HashMap::new(),
        ) {
            counters.insert(MESSAGES_SENT, c);
        }

        if let Some(c) = counter_for_task(
            &task_info,
            BYTES_RECV,
            "Count of bytes received by this subtask",
            HashMap::new(),
        ) {
            counters.insert(BYTES_RECV, c);
        }

        if let Some(c) = counter_for_task(
            &task_info,
            BYTES_SENT,
            "Count of bytes sent by this subtask",
            HashMap::new(),
        ) {
            counters.insert(BYTES_SENT, c);
        }

        let tx_queue_size_gauges = out_qs
            .iter()
            .enumerate()
            .map(|(i, qs)| {
                qs.iter()
                    .enumerate()
                    .map(|(j, _)| {
                        gauge_for_task(
                            &task_info,
                            "arroyo_worker_tx_queue_size",
                            "Size of a tx queue",
                            labels! {
                                "next_node".to_string() => format!("{}", i),
                                "next_node_idx".to_string() => format!("{}", j)
                            },
                        )
                    })
                    .collect()
            })
            .collect();

        let tx_queue_rem_gauges = out_qs
            .iter()
            .enumerate()
            .map(|(i, qs)| {
                qs.iter()
                    .enumerate()
                    .map(|(j, _)| {
                        gauge_for_task(
                            &task_info,
                            "arroyo_worker_tx_queue_rem",
                            "Remaining space in a tx queue",
                            labels! {
                                "next_node".to_string() => format!("{}", i),
                                "next_node_idx".to_string() => format!("{}", j)
                            },
                        )
                    })
                    .collect()
            })
            .collect();

        ArrowContext {
            task_info,
            control_rx,
            control_tx,
            watermarks: WatermarkHolder::new(vec![
                watermark.map(Watermark::EventTime);
                input_partitions
            ]),
            collector: ArrowCollector {
                out_qs,
                out_schema: out_schema.clone(),
                sent_messages: counters.remove(MESSAGES_SENT),
                sent_bytes: counters.remove(BYTES_SENT),
                tx_queue_rem_gauges,
                tx_queue_size_gauges,
            },
            in_schemas,
            out_schema,
            state,
            counters,
        }
    }

    pub async fn collect(&mut self, record: ArrowRecord) {
        self.collector.collect(record).await;
    }

    pub async fn broadcast(&mut self, message: ArrowMessage) {
        self.collector.broadcast(message).await;
    }

    async fn send_checkpoint_event(
        &mut self,
        barrier: CheckpointBarrier,
        event_type: TaskCheckpointEventType,
    ) {
        self.control_tx
            .send(arroyo_rpc::ControlResp::CheckpointEvent(
                arroyo_rpc::CheckpointEvent {
                    checkpoint_epoch: barrier.epoch,
                    operator_id: self.task_info.operator_id.clone(),
                    subtask_index: self.task_info.task_index as u32,
                    time: std::time::SystemTime::now(),
                    event_type,
                },
            ))
            .await
            .unwrap();
    }

    pub fn watermark(&self) -> Option<Watermark> {
        self.watermarks.watermark()
    }

    pub fn last_present_watermark(&self) -> Option<SystemTime> {
        self.watermarks.last_present_watermark()
    }
}

pub struct ArrowCollector {
    out_qs: Vec<Vec<Sender<ArrowMessage>>>,
    out_schema: Option<ArrowSchema>,
    sent_bytes: Option<IntCounter>,
    sent_messages: Option<IntCounter>,
    tx_queue_rem_gauges: Vec<Vec<Option<IntGauge>>>,
    tx_queue_size_gauges: Vec<Vec<Option<IntGauge>>>,
}

impl ArrowCollector {
    pub async fn collect(&mut self, record: ArrowRecord) {
        assert_eq!(record.count, 1);

        fn out_idx(keys: Option<ArrayRef>, qs: usize) -> usize {
            let hash = if let Some(keys) = keys {
                let mut buf = vec![0];
                let result =
                    hash_utils::create_hashes(&[keys], &ahash::RandomState::new(), &mut buf)
                        .unwrap();
                result[0]
            } else {
                // TODO: do we want this be random or deterministic?
                random()
            };

            server_for_hash(hash, qs)
        }

        self.sent_messages.iter().for_each(|c| c.inc());

        let keys = Some(record.columns[0].clone());

        if self.out_qs.len() == 1 {
            let idx = out_idx(keys, self.out_qs[0].len());

            // self.tx_queue_rem_gauges[0][idx]
            //     .iter()
            //     .for_each(|g| g.set(self.out_qs[0][idx].tx.capacity() as i64));
            //
            // self.tx_queue_size_gauges[0][idx]
            //     .iter()
            //     .for_each(|g| g.set(QUEUE_SIZE as i64));

            self.out_qs[0][idx]
                .send(ArrowMessage::Record(record))
                .await
                .unwrap();
        } else {
            todo!("multi output nodes")
            // let key = record.key.clone();
            // let message = Message::Record(record);

            // for (i, out_node_qs) in self.out_qs.iter().enumerate() {
            //     let idx = out_idx(&key, out_node_qs.len());
            //     self.tx_queue_rem_gauges[i][idx]
            //         .iter()
            //         .for_each(|c| c.set(self.out_qs[i][idx].tx.capacity() as i64));

            //     self.tx_queue_size_gauges[i][idx]
            //         .iter()
            //         .for_each(|c| c.set(QUEUE_SIZE as i64));

            //     out_node_qs[idx]
            //         .send(message.clone(), &self.sent_bytes)
            //         .await;
            // }
        }
    }

    pub async fn broadcast(&mut self, message: ArrowMessage) {
        for out_node in &self.out_qs {
            for q in out_node {
                q.send(message.clone()).await.unwrap()
            }
        }
    }
}

pub trait ArrowOperatorConstructor: ArrowOperator {
    fn from_config(config: Vec<u8>) -> Box<dyn ArrowOperator>;
}

#[async_trait::async_trait]
pub trait ArrowOperator: Send + 'static {
    fn start(
        mut self: Box<Self>,
        task_info: TaskInfo,
        restore_from: Option<CheckpointMetadata>,
        control_rx: Receiver<ControlMessage>,
        control_tx: Sender<ControlResp>,
        in_schemas: Vec<ArrowSchema>,
        out_schema: Option<ArrowSchema>,
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
                out_qs,
                tables,
            )
            .await;

            self.on_start(&mut ctx).await;

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

            loop {
                tokio::select! {
                    Some(control_message) = ctx.control_rx.recv() => {
                        self.handle_controller_message(control_message, &mut ctx).await;
                    }


                    p = sel.next() => {
                        match p {
                            Some(((idx, message), s)) => {
                                let local_idx = idx;

                                tracing::debug!("[{}] Handling message {}-{}, {:?}",
                                    ctx.task_info.operator_name, 0, local_idx, message);

                                if let ArrowMessage::Record(record) = message {
                                    ctx.counters.get("arroyo_worker_messages_recv").expect("msg received").inc();
                                    Self::process_batch(&mut(*self),record, &mut ctx)
                                        .instrument(tracing::trace_span!("handle_fn",
                                            name,
                                            operator_id = task_info.operator_id,
                                            subtask_idx = task_info.task_index)
                                    ).await;
                                } else {
                                    match self.handle_control_message(idx, message, &mut counter, &mut closed, in_partitions, &mut ctx).await {
                                        ControlOutcome::Continue => {}
                                        ControlOutcome::Stop => {
                                            ctx.broadcast(ArrowMessage::Stop).await;
                                            break;
                                        }crate::ControlOutcome::Finish => {
                                            ctx.broadcast(ArrowMessage::EndOfData).await;
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
                }
            }
        })
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
            ControlMessage::Commit { epoch } => {
                self.handle_commit(epoch, ctx).await;
            }
        }
    }

    async fn handle_control_message(
        &mut self,
        idx: usize,
        message: ArrowMessage,
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
                    if self.checkpoint(t, ctx).await {
                        return ControlOutcome::Stop;
                    }
                }
            }
            ArrowMessage::Watermark(watermark) => {
                tracing::debug!(
                    "received watermark {:?} in {}-{}",
                    watermark,
                    self.name(),
                    ctx.task_info.task_index
                );
                let watermark = ctx
                    .watermarks
                    .set(idx, watermark)
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
                    ctx.broadcast(ArrowMessage::Stop).await;
                    return ControlOutcome::Stop;
                }
            }
            ArrowMessage::EndOfData => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    ctx.broadcast(ArrowMessage::EndOfData).await;
                    return ControlOutcome::Finish;
                }
            }
        }
        ControlOutcome::Continue
    }

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

        ctx.state.checkpoint(checkpoint_barrier, watermark).await;

        ctx.send_checkpoint_event(checkpoint_barrier, TaskCheckpointEventType::FinishedSync)
            .await;

        ctx.broadcast(ArrowMessage::Barrier(checkpoint_barrier))
            .await;

        checkpoint_barrier.then_stop
    }

    async fn handle_watermark_int(
        &mut self,
        watermark: arroyo_types::Watermark,
        ctx: &mut ArrowContext,
    ) {
        tracing::trace!(
            "handling watermark {:?} for {}-{}",
            watermark,
            ctx.task_info.operator_name,
            ctx.task_info.task_index
        );
        if let arroyo_types::Watermark::EventTime(t) = watermark {
            /*let mut state = ctx
                .state
                .get_time_key_map(TIMER_TABLE, ctx.last_present_watermark())
                .await;
            let finished = state.evict_all_before_watermark(t);

            for (k, tv) in finished {
                self.handle_timer(k, tv.data, ctx).await;
            }*/

            // TODO: handle timers
        }
        self.handle_watermark(watermark, ctx).await;
    }

    fn name(&self) -> String;
    fn tables(&self) -> Vec<TableDescriptor> {
        vec![]
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {}

    async fn process_batch(&mut self, batch: ArrowRecord, ctx: &mut ArrowContext);

    //async fn handle_timer(&mut self, k: ArrowData, tv: (), ctx: &mut ArrowContext) {}

    async fn handle_watermark(
        &mut self,
        watermark: arroyo_types::Watermark,
        ctx: &mut ArrowContext,
    ) {
        ctx.broadcast(ArrowMessage::Watermark(watermark)).await;
    }

    async fn handle_checkpoint(&mut self, b: CheckpointBarrier, ctx: &mut ArrowContext) {}

    async fn handle_commit(&mut self, epoch: u32, ctx: &mut ArrowContext) {
        tracing::warn!("default haindling of commit with epoch {:?}", epoch);
    }

    async fn on_close(&mut self, ctx: &mut ArrowContext) {}
}
