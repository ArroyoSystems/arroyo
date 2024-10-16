use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arroyo_operator::{context::ArrowContext, operator::ArrowOperator};
use arroyo_rpc::{
    grpc::rpc::{GlobalKeyedTableConfig, TableConfig, TableEnum},
    CheckpointEvent, ControlMessage,
};
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::{Data, SignalMessage, TaskInfo, Watermark};
use async_trait::async_trait;
use bincode::config;
use prost::Message;
use std::fmt::Debug;
use std::{collections::HashMap, time::SystemTime};
use tracing::{info, warn};

pub struct TwoPhaseCommitterOperator<TPC: TwoPhaseCommitter> {
    committer: TPC,
    pre_commits: Vec<TPC::PreCommit>,
}

/// A trait representing a two-phase committer for a stream processing system.
///
/// This trait defines the interface for a two-phase committer, which is responsible for committing
/// records to a persistent store in a fault-tolerant manner. The two-phase commit protocol is used
/// to ensure that all records are either committed or rolled back in the event of a failure.
///
/// The trait defines methods for initializing the committer, inserting records, committing the
/// records, and performing a checkpoint. Implementations of this trait must be `Send` and `'static`.
///
/// The trait is generic over two types: `K`, which represents the key type of the records being
/// committed, and `T`, which represents the data type of the records being committed. The trait
/// also defines two associated types: `DataRecovery`, which represents the type of data that can
/// be recovered in the event of a failure, and `PreCommit`, which represents the type of data that
/// is pre-committed before the final commit.
///
///
#[async_trait]
pub trait TwoPhaseCommitter: Send + 'static {
    type DataRecovery: Data;
    type PreCommit: Data;

    fn name(&self) -> String;
    async fn init(
        &mut self,
        task_info: &mut ArrowContext,
        data_recovery: Vec<Self::DataRecovery>,
    ) -> Result<()>;
    async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()>;
    // TODO: figure out how to have the relevant vectors be of pointers across async boundaries.
    async fn commit(
        &mut self,
        task_info: &TaskInfo,
        pre_commit: Vec<Self::PreCommit>,
    ) -> Result<()>;
    async fn checkpoint(
        &mut self,
        task_info: &TaskInfo,
        watermark: Option<SystemTime>,
        stopping: bool,
    ) -> Result<(Self::DataRecovery, HashMap<String, Self::PreCommit>)>;
    fn commit_strategy(&self) -> CommitStrategy {
        CommitStrategy::PerSubtask
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CommitStrategy {
    // Per subtask uses the subtask itself as the committer, writing the pre-commit messages to state for restoration.
    PerSubtask,
    // Per operator uses subtask 0 as the committer, passing all PreCommit data through the control system/checkpoint metadata.
    PerOperator,
}

impl<TPC: TwoPhaseCommitter> TwoPhaseCommitterOperator<TPC> {
    pub(crate) fn new(committer: TPC) -> Self {
        Self {
            committer,
            pre_commits: Vec::new(),
        }
    }
    async fn handle_commit(
        &mut self,
        epoch: u32,
        mut commit_data: HashMap<String, HashMap<u32, Vec<u8>>>,
        ctx: &mut ArrowContext,
    ) {
        info!("received commit message");
        let pre_commits = match self.committer.commit_strategy() {
            CommitStrategy::PerSubtask => std::mem::take(&mut self.pre_commits),
            CommitStrategy::PerOperator => {
                // only subtask 0 should be committing
                if ctx.task_info.task_index == 0 {
                    commit_data
                        .remove("p")
                        .unwrap_or_default()
                        .into_values()
                        .flat_map(|serialized_data| Self::map_from_serialized_data(serialized_data))
                        .collect()
                } else {
                    vec![]
                }
            }
        };

        self.committer
            .commit(&ctx.task_info, pre_commits)
            .await
            .expect("committer committed");
        let checkpoint_event = arroyo_rpc::ControlResp::CheckpointEvent(CheckpointEvent {
            checkpoint_epoch: epoch,
            operator_id: ctx.task_info.operator_id.clone(),
            subtask_index: ctx.task_info.task_index as u32,
            time: SystemTime::now(),
            event_type: arroyo_rpc::grpc::rpc::TaskCheckpointEventType::FinishedCommit,
        });
        ctx.control_tx
            .send(checkpoint_event)
            .await
            .expect("sent commit event");
    }

    fn map_from_serialized_data(serialized_data: Vec<u8>) -> Vec<TPC::PreCommit> {
        let map: HashMap<String, TPC::PreCommit> =
            bincode::decode_from_slice(&serialized_data, config::standard())
                .unwrap()
                .0;
        map.into_values().collect()
    }
}

#[async_trait]
impl<TPC: TwoPhaseCommitter> ArrowOperator for TwoPhaseCommitterOperator<TPC> {
    fn name(&self) -> String {
        self.committer.name()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        let mut tables = arroyo_state::global_table_config("r", "recovery data");
        tables.insert(
            "p".into(),
            TableConfig {
                table_type: TableEnum::GlobalKeyValue.into(),
                config: GlobalKeyedTableConfig {
                    table_name: "p".into(),
                    description: "pre-commit data".into(),
                    uses_two_phase_commit: true,
                }
                .encode_to_vec(),
            },
        );
        tables
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        let tracking_key_state: &mut GlobalKeyedView<usize, TPC::DataRecovery> = ctx
            .table_manager
            .get_global_keyed_state("r")
            .await
            .expect("should be able to get table");

        let state_vec = tracking_key_state.get_all().values().cloned().collect();
        self.committer
            .init(ctx, state_vec)
            .await
            .expect("committer initialized");

        // subtask 0 is responsible for finishing commits if we were interrupted mid commit.
        if ctx.task_info.task_index == 0 {
            let pre_commit_state: &mut GlobalKeyedView<String, TPC::PreCommit> = ctx
                .table_manager
                .get_global_keyed_state("p")
                .await
                .expect("should be able to get table");
            self.pre_commits = pre_commit_state.get_all().values().cloned().collect();
        }
    }

    async fn process_batch(&mut self, batch: RecordBatch, _ctx: &mut ArrowContext) {
        self.committer
            .insert_batch(batch)
            .await
            .expect("record inserted");
    }

    async fn on_close(&mut self, _final_mesage: &Option<SignalMessage>, ctx: &mut ArrowContext) {
        if let Some(ControlMessage::Commit { epoch, commit_data }) = ctx.control_rx.recv().await {
            self.handle_commit(epoch, commit_data, ctx).await;
        } else {
            warn!("no commit message received, not committing")
        }
    }

    async fn handle_commit(
        &mut self,
        epoch: u32,
        commit_data: &HashMap<String, HashMap<u32, Vec<u8>>>,
        ctx: &mut ArrowContext,
    ) {
        self.handle_commit(epoch, commit_data.clone(), ctx).await;
    }

    async fn handle_checkpoint(
        &mut self,
        checkpoint_barrier: arroyo_types::CheckpointBarrier,
        ctx: &mut ArrowContext,
    ) {
        let (recovery_data, pre_commits) = self
            .committer
            .checkpoint(
                &ctx.task_info,
                ctx.watermark().and_then(|watermark| match watermark {
                    Watermark::EventTime(watermark) => Some(watermark),
                    arroyo_types::Watermark::Idle => None,
                }),
                checkpoint_barrier.then_stop,
            )
            .await
            .unwrap();

        let recovery_data_state: &mut GlobalKeyedView<usize, _> = ctx
            .table_manager
            .get_global_keyed_state("r")
            .await
            .expect("should be able to get table");
        recovery_data_state
            .insert(ctx.task_info.task_index, recovery_data)
            .await;
        self.pre_commits.clear();
        if pre_commits.is_empty() {
            return;
        }
        let commit_strategy = self.committer.commit_strategy();
        match commit_strategy {
            CommitStrategy::PerSubtask => {
                let pre_commit_state = ctx
                    .table_manager
                    .get_global_keyed_state("p")
                    .await
                    .expect("should be able to get table");
                for (key, value) in pre_commits {
                    self.pre_commits.push(value.clone());
                    pre_commit_state.insert(key, value).await;
                }
                ctx.table_manager
                    .insert_committing_data("p", vec![])
                    .await
                    .expect("should be able to send committing data");
            }
            CommitStrategy::PerOperator => {
                let serialized_pre_commits =
                    bincode::encode_to_vec(&pre_commits, config::standard()).unwrap();
                ctx.table_manager
                    .insert_committing_data("p", serialized_pre_commits)
                    .await
                    .expect("should be able to send committing data");
            }
        }
    }
}
