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
    pub fn from_config(config: &str) -> Self {
        todo!()
    }

    fn name(&self) -> String {
        self.committer.name()
    }

    fn tables(&self) -> Vec<arroyo_rpc::grpc::TableDescriptor> {
        vec![
            arroyo_state::global_table("r", "recovery data"),
            arroyo_state::global_table("p", "pre-commit data"),
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

        self.handle_commit(ctx).await;
    }

    async fn process_element(&mut self, record: &Record<K, T>, _ctx: &mut Context<(), ()>) {
        self.committer
            .insert_record(record)
            .await
            .expect("record inserted");
    }

    async fn on_close(&mut self, _ctx: &mut crate::engine::Context<(), ()>) {
        self.committer.close(false).await.unwrap();
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
    async fn handle_commit(&mut self, ctx: &mut crate::engine::Context<(), ()>) {
        let pre_commits = self.pre_commits.clone();
        self.pre_commits.clear();
        self.committer
            .commit(&ctx.task_info, pre_commits)
            .await
            .expect("committer committed");
    }
}
