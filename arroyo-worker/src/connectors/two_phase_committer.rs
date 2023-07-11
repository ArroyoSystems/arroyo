use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    io::Write,
    marker::PhantomData,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{bail, Result};
use arroyo_macro::{process_fn, StreamNode};
use arroyo_rpc::{
    grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior},
    CheckpointEvent, ControlMessage,
};
use arroyo_state::tables::GlobalKeyedState;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use futures::{stream::FuturesUnordered, Future};
use futures::{stream::StreamExt, TryStreamExt};
use object_store::{
    aws::{AmazonS3Builder, AwsCredential},
    local::LocalFileSystem,
    path::Path,
    CredentialProvider, MultipartId, ObjectStore, UploadPart,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use rusoto_core::credential::{DefaultCredentialsProvider, ProvideAwsCredentials};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{info, warn};
use typify::import_types;

import_types!(schema = "../connector-schemas/parquet/table.json");

use crate::engine::Context;
use arroyo_types::*;

#[derive(StreamNode)]
pub struct TwoPhaseCommitterOperator<K: Key, T: Data + Sync, TPC: TwoPhaseCommitter<K, T>> {
    committer: TPC,
    pre_commits: Vec<TPC::PreCommit>,
    phantom: PhantomData<(K, T)>,
}

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
    // TODO: figure out how to have these be vecs of pointers across async boundaries.
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
    async fn close(&mut self, and_commit: bool) -> Result<()>;
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
        let mut tracking_key_state: GlobalKeyedState<
            usize,
            <TPC as TwoPhaseCommitter<K, T>>::DataRecovery,
            _,
        > = ctx.state.get_global_keyed_state('r').await;
        // take the max of all values
        let state_vec = tracking_key_state
            .get_all()
            .into_iter()
            .map(|state| state.clone())
            .collect();
        self.committer
            .init(&ctx.task_info, state_vec)
            .await
            .expect("committer initialized");
    }

    async fn process_element(&mut self, record: &Record<K, T>, _ctx: &mut Context<(), ()>) {
        self.committer
            .insert_record(record)
            .await
            .expect("record inserted");
    }

    async fn on_close(&mut self, ctx: &mut crate::engine::Context<(), ()>) {
        info!("waiting for commit message");
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
        info!("received checkpoint barrier {:?}", checkpoint_barrier);
        let (recovery_data, pre_commits) = self
            .committer
            .checkpoint(&ctx.task_info, checkpoint_barrier.then_stop)
            .await
            .unwrap();
        let mut recovery_data_state: GlobalKeyedState<usize, _, _> =
            ctx.state.get_global_keyed_state('r').await;
        recovery_data_state
            .insert(ctx.task_info.task_index, recovery_data)
            .await;
        let mut pre_commit_state: GlobalKeyedState<String, _, _> =
            ctx.state.get_global_keyed_state('p').await;
        for (key, value) in pre_commits {
            self.pre_commits.push(value.clone());
            pre_commit_state.insert(key, value).await;
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
        info!("sent commit event")
    }

    async fn handle_raw_control_message(
        &mut self,
        control_message: arroyo_rpc::ControlMessage,
        ctx: &mut Context<(), ()>,
    ) {
        info!("received control message {:?}", control_message);
        match control_message {
            arroyo_rpc::ControlMessage::Checkpoint(_) => warn!("shouldn't receive checkpoint"),
            arroyo_rpc::ControlMessage::Stop { mode: _ } => warn!("shouldn't receive stop"),
            arroyo_rpc::ControlMessage::Commit { epoch } => {
                self.handle_commit(epoch, ctx).await;
            }
        }
    }
}
