use std::{collections::HashMap, marker::PhantomData, time::SystemTime};

use crate::engine::Context;
use anyhow::Result;
use arroyo_macro::{process_fn, StreamNode};
use arroyo_rpc::{
    grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior},
    CheckpointEvent, ControlMessage,
};
use arroyo_state::judy::tables::global_keyed_map::GlobalKeyedState;
use arroyo_types::{Data, Key, Record, TaskInfo};
use async_trait::async_trait;
use tracing::warn;

#[derive(StreamNode)]
pub struct TwoPhaseCommitterOperator<K: Key, T: Data + Sync, TPC: TwoPhaseCommitter<K, T>> {
    committer: TPC,
    pre_commits: Vec<TPC::PreCommit>,
    phantom: PhantomData<(K, T)>,
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
pub trait TwoPhaseCommitter<K: Key, T: Data + Sync>: Send + 'static {
    type DataRecovery: Data;
    type PreCommit: Data;

    fn name(&self) -> String;
    async fn init(
        &mut self,
        task_info: &TaskInfo,
        data_recovery: Vec<Self::DataRecovery>,
    ) -> Result<()>;
    async fn insert_record(&mut self, record: &Record<K, T>) -> Result<()>;
    // TODO: figure out how to have the relevant vectors be of pointers across async boundaries.
    async fn commit(
        &mut self,
        task_info: &TaskInfo,
        pre_commit: Vec<Self::PreCommit>,
    ) -> Result<()>;
    async fn checkpoint(
        &mut self,
        task_info: &TaskInfo,
        stopping: bool,
    ) -> Result<(Self::DataRecovery, HashMap<String, Self::PreCommit>)>;
}

#[process_fn(in_k = K, in_t = T)]
impl<K: Key, T: Data + Sync, TPC: TwoPhaseCommitter<K, T>> TwoPhaseCommitterOperator<K, T, TPC> {
    pub(crate) fn new(committer: TPC) -> Self {
        Self {
            committer,
            pre_commits: Vec::new(),
            phantom: PhantomData,
        }
    }

    fn name(&self) -> String {
        self.committer.name()
    }

    fn tables(&self) -> Vec<arroyo_rpc::grpc::TableDescriptor> {
        vec![
            arroyo_state::global_table("r", "recovery data"),
            TableDescriptor {
                name: "p".into(),
                description: "pre-commit data".into(),
                table_type: TableType::Global as i32,
                delete_behavior: TableDeleteBehavior::None as i32,
                write_behavior: TableWriteBehavior::CommitWrites as i32,
                retention_micros: 0,
            },
        ]
    }

    async fn on_start(&mut self, ctx: &mut Context<(), ()>) {
        let mut tracking_key_state: &mut GlobalKeyedState<
            usize,
            <TPC as TwoPhaseCommitter<K, T>>::DataRecovery,
        > = ctx.state.get_global_keyed_state('r').await;
        // take the max of all values
        let state_vec = tracking_key_state
            .get_all()
            .into_iter()
            .map(|state| state.into_owned())
            .collect();
        self.committer
            .init(&ctx.task_info, state_vec)
            .await
            .expect("committer initialized");

        // subtask 0 is responsible for finishing commits if we were interrupted mid commit.
        if ctx.task_info.task_index == 0 {
            let mut pre_commit_state: &mut GlobalKeyedState<
                String,
                <TPC as TwoPhaseCommitter<K, T>>::PreCommit,
            > = ctx.state.get_global_keyed_state('p').await;
            self.pre_commits = pre_commit_state
                .get_all()
                .into_iter()
                .map(|state| state.into_owned())
                .collect();
        }
    }

    async fn process_element(&mut self, record: &Record<K, T>, _ctx: &mut Context<(), ()>) {
        self.committer
            .insert_record(record)
            .await
            .expect("record inserted");
    }

    async fn on_close(&mut self, ctx: &mut crate::engine::Context<(), ()>) {
        if let Some(ControlMessage::Commit { epoch }) = ctx.control_rx.recv().await {
            self.handle_commit(epoch, ctx).await;
        } else {
            warn!("no commit message received, not committing")
        }
    }

    async fn handle_checkpoint(
        &mut self,
        checkpoint_barrier: &arroyo_types::CheckpointBarrier,
        ctx: &mut crate::engine::Context<(), ()>,
    ) {
        let (recovery_data, pre_commits) = self
            .committer
            .checkpoint(&ctx.task_info, checkpoint_barrier.then_stop)
            .await
            .unwrap();
        let recovery_data_state: &mut GlobalKeyedState<usize, _> =
            ctx.state.get_global_keyed_state('r').await;
        recovery_data_state.insert(ctx.task_info.task_index, recovery_data);
        let pre_commit_state: &mut GlobalKeyedState<String, _> =
            ctx.state.get_global_keyed_state('p').await;
        self.pre_commits.clear();
        for (key, value) in pre_commits {
            self.pre_commits.push(value.clone());
            pre_commit_state.insert(key, value);
        }
    }
    async fn handle_commit(&mut self, epoch: u32, ctx: &mut crate::engine::Context<(), ()>) {
        let pre_commits = self.pre_commits.clone();
        self.pre_commits.clear();
        self.committer
            .commit(&ctx.task_info, pre_commits)
            .await
            .expect("committer committed");
        let checkpoint_event = arroyo_rpc::ControlResp::CheckpointEvent(CheckpointEvent {
            checkpoint_epoch: epoch,
            operator_id: ctx.task_info.operator_id.clone(),
            subtask_index: ctx.task_info.task_index as u32,
            time: SystemTime::now(),
            event_type: arroyo_rpc::grpc::TaskCheckpointEventType::FinishedCommit.into(),
        });
        ctx.control_tx
            .send(checkpoint_event)
            .await
            .expect("sent commit event");
    }
}
