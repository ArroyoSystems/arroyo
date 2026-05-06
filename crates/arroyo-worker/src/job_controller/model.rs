use crate::job_controller::checkpoint_state::CheckpointState;
use crate::job_controller::committing_state::{CheckpointIdOrRef, CommittingState};
use crate::job_controller::{
    CHECKPOINTS_TO_KEEP, COMPACT_EVERY, RetireWorkerLeader, RunningMessage, TaskFailedEvent,
};
use anyhow::bail;
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::api_types::checkpoints::{JobCheckpointEventType, JobCheckpointSpan};
use arroyo_rpc::checkpoints::{
    CheckpointMetadataStore, CheckpointStatus, CreateCheckpointReq, FinishCheckpointReq,
    UpdateCheckpointReq,
};
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, CheckpointReq, CommitReq, JobFinishedReq, LoadCompactedDataReq,
    OperatorCheckpointMetadata, TaskCheckpointEventType,
};
use arroyo_rpc::identity::WorkerClient;
use arroyo_rpc::public_ids::{IdTypes, generate_id};
use arroyo_state::parquet::ParquetBackend;
use arroyo_state::{BackingStore, StateBackend, StorageProviderFor, get_storage_provider};
use arroyo_state_protocol::ProtocolPaths;
use arroyo_state_protocol::types::{CheckpointRef, Epoch, Generation, GenerationManifest};
use arroyo_state_protocol::workflow::{
    CheckpointPublication, CommitAuthorization, PublishCheckpointRequest, complete_commit,
    prepare_commit, publish_checkpoint,
};
use arroyo_types::{JobId, PipelineId, WorkerId, to_micros};
use futures::future::try_join_all;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::task::JoinHandle;
use tonic::Request;
use tracing::{debug, error, info, warn};

pub struct RunningJobModel {
    pub pipeline_id: PipelineId,
    pub job_id: JobId,
    pub generation: u64,
    pub state: JobState,
    pub program: Arc<LogicalProgram>,
    pub checkpoint_state: Option<CheckpointingOrCommittingState>,
    pub epoch: u32,
    pub min_epoch: u32,
    pub last_checkpoint: Instant,
    pub workers: HashMap<WorkerId, WorkerStatus>,
    pub tasks: HashMap<(u32, u32), TaskStatus>,
    pub operator_parallelism: HashMap<u32, usize>,
    pub metric_update_task: Option<JoinHandle<()>>,
    pub last_updated_metrics: Instant,
    pub protocol_paths: ProtocolPaths,
    pub checkpoint_parent_ref: Option<CheckpointRef>,
    pub generation_manifest: Option<GenerationManifest>,

    pub finished_operators: Vec<OperatorCheckpointMetadata>,

    pub worker_leader_mode: bool,

    /// Identifies which process / role is running this model and where its
    /// checkpoint storage lives.
    ///
    /// On the worker-leader process, this is [`StorageProviderFor::Worker`]:
    /// the worker resolves its URL from `config().checkpoint_url` (set
    /// per-pipeline via `ARROYO__CHECKPOINT_URL` at worker startup).
    ///
    /// On the controller process, this is
    /// [`StorageProviderFor::Controller`] carrying the pipeline's
    /// `state_url`. The controller's own `config().checkpoint_url` is a
    /// global default that doesn't differ across pipelines, so the
    /// per-pipeline override is needed to route storage operations to the
    /// right URL.
    pub storage_role: StorageProviderFor,

    // checkpoint-wide events
    pub checkpoint_spans: Vec<JobCheckpointSpan>,
}

impl std::fmt::Debug for RunningJobModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningJobModel")
            .field("job_id", &self.job_id)
            .field("state", &self.state)
            .field("checkpointing", &self.checkpoint_state.is_some())
            .field("epoch", &self.epoch)
            .field("min_epoch", &self.min_epoch)
            .field("last_checkpoint", &self.last_checkpoint)
            .finish()
    }
}

impl RunningJobModel {
    async fn update_db(&self, store: &dyn CheckpointMetadataStore) -> anyhow::Result<()> {
        if let Some(CheckpointingOrCommittingState::Checkpointing(checkpoint_state)) =
            &self.checkpoint_state
        {
            store
                .update_checkpoint(UpdateCheckpointReq {
                    checkpoint_id: checkpoint_state.checkpoint_id.clone(),
                    operator_details: serde_json::to_value(&checkpoint_state.operator_details)
                        .unwrap(),
                    finish_time: None,
                    status: CheckpointStatus::InProgress,
                    event_spans: serde_json::to_value(&self.checkpoint_spans).unwrap(),
                })
                .await?;
        }

        Ok(())
    }

    async fn update_checkpoint_in_db(
        &self,
        checkpoint_state: &CheckpointState,
        store: &dyn CheckpointMetadataStore,
        status: CheckpointStatus,
    ) -> anyhow::Result<()> {
        let finish_time = if status == CheckpointStatus::Ready {
            Some(SystemTime::now())
        } else {
            None
        };
        store
            .update_checkpoint(UpdateCheckpointReq {
                checkpoint_id: checkpoint_state.checkpoint_id.clone(),
                operator_details: serde_json::to_value(&checkpoint_state.operator_details).unwrap(),
                finish_time,
                status,
                event_spans: serde_json::to_value(&self.checkpoint_spans).unwrap(),
            })
            .await?;

        Ok(())
    }

    pub async fn finish_committing(
        &self,
        checkpoint_id: &str,
        store: &dyn CheckpointMetadataStore,
    ) -> anyhow::Result<()> {
        info!(
            message = "Finishing committing",
            epoch = self.epoch,
            job_id = *self.job_id,
        );

        store
            .finish_checkpoint(FinishCheckpointReq {
                checkpoint_id: checkpoint_id.to_string(),
                finish_time: SystemTime::now(),
                event_spans: serde_json::to_value(&self.checkpoint_spans).unwrap(),
            })
            .await?;

        Ok(())
    }

    pub async fn handle_message(
        &mut self,
        msg: RunningMessage,
        store: &dyn CheckpointMetadataStore,
    ) -> anyhow::Result<()> {
        let storage_role = self.storage_role.clone();
        match msg {
            RunningMessage::TaskCheckpointEvent(c) => {
                if let Some(checkpoint_state) = &mut self.checkpoint_state {
                    if c.epoch != self.epoch {
                        warn!(
                            message = "Received checkpoint event for wrong epoch",
                            epoch = c.epoch,
                            expected = self.epoch,
                            job_id = *self.job_id,
                        );
                    } else {
                        match checkpoint_state {
                            CheckpointingOrCommittingState::Checkpointing(checkpoint_state) => {
                                checkpoint_state.checkpoint_event(c)?;
                            }
                            CheckpointingOrCommittingState::Committing(committing_state) => {
                                if matches!(c.event_type(), TaskCheckpointEventType::FinishedCommit)
                                {
                                    committing_state
                                        .subtask_committed(c.operator_id.clone(), c.subtask_idx);
                                    self.compact_state().await?;
                                } else {
                                    warn!("unexpected checkpoint event type {:?}", c.event_type())
                                }
                            }
                        };
                        self.update_db(store).await?;
                    }
                } else {
                    debug!(
                        message = "Received checkpoint event but not checkpointing",
                        job_id = *self.job_id,
                        event = format!("{:?}", c)
                    )
                }
            }
            RunningMessage::TaskCheckpointFinished(c) => {
                if let Some(checkpoint_state) = &mut self.checkpoint_state {
                    if c.epoch != self.epoch {
                        warn!(
                            message = "Received checkpoint finished for wrong epoch",
                            epoch = c.epoch,
                            expected = self.epoch,
                            job_id = *self.job_id,
                        );
                    } else {
                        let CheckpointingOrCommittingState::Checkpointing(checkpoint_state) =
                            checkpoint_state
                        else {
                            bail!("Received checkpoint finished but not checkpointing");
                        };
                        if let Some(operator_metadata) = checkpoint_state.checkpoint_finished(c)? {
                            if self.worker_leader_mode {
                                self.finished_operators.push(operator_metadata);
                            } else {
                                StateBackend::write_operator_checkpoint_metadata(
                                    &storage_role,
                                    operator_metadata,
                                )
                                .await?;
                            }
                        }

                        if checkpoint_state.done()
                            && let Some(e) = self
                                .checkpoint_spans
                                .iter_mut()
                                .find(|e| e.event == JobCheckpointEventType::CheckpointingOperators)
                        {
                            e.finish()
                        }

                        self.update_db(store).await?;
                    }
                } else {
                    warn!(
                        message = "Received checkpoint finished but not checkpointing",
                        job_id = *self.job_id
                    )
                }
            }
            RunningMessage::TaskFinished {
                worker_id: _,
                time: _,
                task_id,
                subtask_idx,
            } => {
                let key = (task_id, subtask_idx);
                if let Some(status) = self.tasks.get_mut(&key) {
                    status.state = TaskState::Finished;
                } else {
                    warn!(
                        message = "Received task finished for unknown task",
                        job_id = *self.job_id,
                        task_id = key.0,
                        subtask_idx
                    );
                }
            }
            RunningMessage::TaskFailed(event) => {
                let key = (event.task_id, event.subtask_idx);
                if let Some(status) = self.tasks.get_mut(&key) {
                    status.state = TaskState::Failed(event);
                } else {
                    warn!(
                        message = "Received task failed message for unknown task",
                        job_id = *self.job_id,
                        task_id = key.0,
                        subtask_idx = key.1,
                        reason = event.reason,
                    );
                }
            }
            RunningMessage::WorkerHeartbeat { worker_id, time } => {
                if let Some(worker) = self.workers.get_mut(&worker_id) {
                    worker.last_heartbeat = time;
                } else {
                    warn!(
                        message = "Received heartbeat for unknown worker",
                        job_id = *self.job_id,
                        worker_id = worker_id.0
                    );
                }
            }
            RunningMessage::WorkerFinished { worker_id } => {
                if let Some(worker) = self.workers.get_mut(&worker_id) {
                    worker.state = WorkerState::Stopped;
                } else {
                    warn!(
                        message = "Received finish message for unknown worker",
                        job_id = *self.job_id,
                        worker_id = worker_id.0
                    );
                }
            }
            RunningMessage::Stop { .. } => {
                // should have been already handled by the job controller
            }
        }

        if self.state == JobState::Running
            && self.all_tasks_finished()
            && self.checkpoint_state.is_none()
        {
            for w in &mut self.workers.values_mut() {
                if let Err(e) = w.connect.job_finished(JobFinishedReq {}).await {
                    warn!(
                        message = "Failed to connect to worker to send job finish",
                        job_id = *self.job_id,
                        worker_id = w.id.0,
                        error = format!("{:?}", e),
                    )
                }
            }
            self.state = JobState::Stopped;
        }

        Ok(())
    }

    pub async fn start_checkpoint(
        &mut self,
        store: &dyn CheckpointMetadataStore,
        then_stop: bool,
    ) -> anyhow::Result<()> {
        self.epoch += 1;

        info!(
            message = "Starting checkpointing",
            job_id = *self.job_id,
            epoch = self.epoch,
            then_stop
        );

        self.checkpoint_spans.clear();
        self.start_or_get_span(JobCheckpointEventType::Checkpointing);
        self.start_or_get_span(JobCheckpointEventType::CheckpointingOperators);

        let checkpoints = self.workers.values_mut().map(|worker| {
            worker.connect.checkpoint(Request::new(CheckpointReq {
                epoch: self.epoch,
                timestamp: to_micros(SystemTime::now()),
                min_epoch: self.min_epoch,
                then_stop,
                is_commit: false,
            }))
        });

        try_join_all(checkpoints).await?;

        let checkpoint_id = generate_id(IdTypes::Checkpoint);

        store
            .create_checkpoint(CreateCheckpointReq {
                checkpoint_id: checkpoint_id.clone(),
                epoch: self.epoch,
                min_epoch: self.min_epoch,
                start_time: SystemTime::now(),
            })
            .await?;

        let state = CheckpointState::new(
            self.job_id.0.clone(),
            checkpoint_id,
            self.epoch,
            self.min_epoch,
            self.program.clone(),
        );

        self.checkpoint_state = Some(CheckpointingOrCommittingState::Checkpointing(state));

        Ok(())
    }

    async fn compact_state(&mut self) -> anyhow::Result<()> {
        if self.worker_leader_mode {
            // TODO: compaction for leader mode
            return Ok(());
        }

        if !config().pipeline.compaction.enabled {
            debug!("Compaction is disabled, skipping compaction");
            return Ok(());
        }

        self.start_or_get_span(JobCheckpointEventType::Compacting);
        info!(
            message = "Compacting state",
            job_id = *self.job_id,
            epoch = self.epoch,
        );

        let storage_role = self.storage_role.clone();
        let mut worker_clients: Vec<WorkerClient> =
            self.workers.values().map(|w| w.connect.clone()).collect();
        for node in self.program.graph.node_weights() {
            for (op, _) in node.operator_chain.iter() {
                let compacted_tables = ParquetBackend::compact_operator(
                    // compact the operator's state and notify the workers to load the new files
                    &storage_role,
                    self.job_id.0.clone(),
                    &op.operator_id,
                    self.epoch,
                )
                .await?;

                if compacted_tables.is_empty() {
                    continue;
                }

                // TODO: these should be put on separate tokio tasks.
                for worker_client in &mut worker_clients {
                    worker_client
                        .load_compacted_data(LoadCompactedDataReq {
                            operator_id: op.operator_id.clone(),
                            compacted_metadata: compacted_tables.clone(),
                        })
                        .await?;
                }
            }
        }
        self.start_or_get_span(JobCheckpointEventType::Compacting)
            .finish();

        info!(
            message = "Finished compaction",
            job_id = *self.job_id,
            epoch = self.epoch,
        );
        Ok(())
    }

    async fn finish_checkpoint_leader(
        &mut self,
        store: &dyn CheckpointMetadataStore,
    ) -> anyhow::Result<()> {
        let state = self.checkpoint_state.take().unwrap();
        let storage = get_storage_provider(&self.storage_role).await?;
        match state {
            CheckpointingOrCommittingState::Checkpointing(mut checkpointing) => {
                let pipeline_id = (*self.pipeline_id).clone();
                let generation = self.generation;
                let operators = self.finished_operators.drain(..).collect();
                let metadata_span = Self::start_or_get_spans_static(
                    &mut self.checkpoint_spans,
                    JobCheckpointEventType::WritingMetadata,
                );

                let metadata = checkpointing.build_metadata();

                let manifest = CheckpointManifest {
                    pipeline_id,
                    job_id: metadata.job_id,
                    generation,
                    epoch: metadata.epoch as u64,
                    min_epoch: metadata.min_epoch as u64,
                    start_time: metadata.start_time,
                    finish_time: metadata.finish_time,
                    needs_commit: checkpointing.needs_commit(),
                    operators,
                    parent_checkpoint_ref: self
                        .checkpoint_parent_ref
                        .as_ref()
                        .map(|t| t.to_string()),
                };

                let checkpoint_ref = self
                    .protocol_paths
                    .checkpoint_manifest(Generation(generation), Epoch(metadata.epoch as u64));

                let publish_req = PublishCheckpointRequest {
                    generation_manifest: self
                        .generation_manifest
                        .as_ref()
                        .expect("generation manifest not set in leader mode"),
                    checkpoint_ref: &checkpoint_ref,
                    checkpoint: &manifest,
                    created_at: SystemTime::now(),
                };

                let res = publish_checkpoint(storage.as_ref(), publish_req).await?;

                self.checkpoint_parent_ref = Some(checkpoint_ref);

                metadata_span.finish();

                let duration = checkpointing
                    .start_time()
                    .elapsed()
                    .unwrap_or(Duration::ZERO)
                    .as_secs_f32();

                let (checkpoint_ref, commit_permit) = match res {
                    CheckpointPublication::Ready { .. } => {
                        // we're done
                        self.start_or_get_span(JobCheckpointEventType::Checkpointing)
                            .finish();
                        self.last_checkpoint = Instant::now();
                        self.checkpoint_state = None;
                        info!(
                            message = "Finished checkpointing",
                            job_id = *self.job_id,
                            epoch = self.epoch,
                            duration
                        );
                        self.update_checkpoint_in_db(
                            &checkpointing,
                            store,
                            CheckpointStatus::Ready,
                        )
                        .await?;

                        store.notify_checkpoint_complete();
                        return Ok(());
                    }
                    CheckpointPublication::CommitRequired {
                        checkpoint_ref,
                        commit_permit,
                    } => (checkpoint_ref, commit_permit),
                    r @ CheckpointPublication::StopOrphaned { .. }
                    | r @ CheckpointPublication::StaleGeneration => {
                        return Err(RetireWorkerLeader {
                            reason: format!(
                                "generation {} is no longer permitted to checkpoint: {:?}",
                                self.generation, r
                            ),
                        }
                        .into());
                    }
                    CheckpointPublication::Failed(f) => {
                        bail!(
                            "failed while attempting to write checkpoint metadata: {:?}",
                            f
                        );
                    }
                };

                let commit = checkpointing
                    .into_commit(CheckpointIdOrRef::CheckpointRef(commit_permit.clone()));

                let committing_data = commit.committing_data();

                info!(
                    message = "Committing checkpoint",
                    job_id = *self.job_id,
                    epoch = self.epoch,
                    generation = self.generation,
                    checkpoint_ref = checkpoint_ref.as_str(),
                );

                self.start_or_get_span(JobCheckpointEventType::Committing);

                match prepare_commit(
                    storage.as_ref(),
                    &checkpoint_ref,
                    manifest,
                    Some(commit_permit.epoch_record().clone()),
                    true,
                )
                .await?
                {
                    CommitAuthorization::Authorized { .. } => {
                        // commit to workers
                        for worker in self.workers.values_mut() {
                            worker
                                .connect
                                .commit(Request::new(CommitReq {
                                    epoch: self.epoch,
                                    // TODO: this is pretty expensive
                                    committing_data: committing_data.clone(),
                                }))
                                .await?;
                        }
                    }
                    CommitAuthorization::AlreadyCommitted { .. } => {
                        unreachable!(
                            "we are passing assume_not_committed: true, so we should not be able\
                        to end up in a state where the protocol believes we have already committed."
                        )
                    }
                    CommitAuthorization::NoCommitNeeded { checkpoint_ref } => {
                        unreachable!(
                            "RunningJobModel believes that we need to commit for {}, \
                        but checkpoint protocol disagrees; something has gone very wrong",
                            checkpoint_ref
                        );
                    }
                    CommitAuthorization::StopOrphaned { canonical_ref } => {
                        return Err(RetireWorkerLeader {
                            reason: format!(
                                "checkpoint {checkpoint_ref} is orphaned; canonical checkpoint is {canonical_ref}"
                            ),
                        }
                        .into());
                    }
                    CommitAuthorization::NotCanonical { checkpoint_ref } => {
                        return Err(RetireWorkerLeader {
                            reason: format!(
                                "attempted to commit non-canonical checkpoint {checkpoint_ref}"
                            ),
                        }
                        .into());
                    }
                    CommitAuthorization::MissingCheckpoint { checkpoint_ref } => {
                        bail!("checkpoint {checkpoint_ref} was missing when attempting to commit!");
                    }
                }

                self.checkpoint_state = Some(CheckpointingOrCommittingState::Committing(commit));
            }
            CheckpointingOrCommittingState::Committing(committing) => {
                self.start_or_get_span(JobCheckpointEventType::Committing)
                    .finish();
                self.start_or_get_span(JobCheckpointEventType::Checkpointing)
                    .finish();

                let commit_permit = committing.commit_permit();

                complete_commit(storage.as_ref(), commit_permit, Generation(self.generation))
                    .await?;

                self.finish_committing(commit_permit.checkpoint_ref().as_str(), store)
                    .await?;

                self.last_checkpoint = Instant::now();
                self.checkpoint_state = None;
                info!(
                    message = "Finished committing checkpointing",
                    job_id = *self.job_id,
                    epoch = self.epoch,
                    generation = self.generation,
                );
                store.notify_checkpoint_complete();
            }
        }

        Ok(())
    }

    async fn finish_checkpoint_controller(
        &mut self,
        store: &dyn CheckpointMetadataStore,
    ) -> anyhow::Result<()> {
        let storage_role = self.storage_role.clone();
        let state = self.checkpoint_state.take().unwrap();
        match state {
            CheckpointingOrCommittingState::Checkpointing(mut checkpointing) => {
                let metadata_span = self.start_or_get_span(JobCheckpointEventType::WritingMetadata);

                let metadata = checkpointing.build_metadata();

                StateBackend::write_checkpoint_metadata(&storage_role, metadata).await?;

                metadata_span.finish();

                let duration = checkpointing
                    .start_time()
                    .elapsed()
                    .unwrap_or(Duration::ZERO)
                    .as_secs_f32();
                // shortcut if committing is unnecessary
                if !checkpointing.needs_commit() {
                    self.start_or_get_span(JobCheckpointEventType::Checkpointing)
                        .finish();
                    self.update_checkpoint_in_db(&checkpointing, store, CheckpointStatus::Ready)
                        .await?;
                    self.last_checkpoint = Instant::now();
                    self.checkpoint_state = None;
                    self.compact_state().await?;

                    info!(
                        message = "Finished checkpointing",
                        job_id = *self.job_id,
                        epoch = self.epoch,
                        duration
                    );
                    store.notify_checkpoint_complete();
                } else {
                    self.update_checkpoint_in_db(
                        &checkpointing,
                        store,
                        CheckpointStatus::Committing,
                    )
                    .await?;

                    let id = CheckpointIdOrRef::CheckpointId(checkpointing.checkpoint_id.clone());
                    let committing = checkpointing.into_commit(id);
                    info!(
                        message = "Committing checkpoint",
                        job_id = *self.job_id,
                        epoch = self.epoch,
                    );

                    self.start_or_get_span(JobCheckpointEventType::Committing);

                    // TODO: this should be done in parallel, but we're being conservative for now
                    //  about changing existing behaviorÏ
                    for worker in self.workers.values_mut() {
                        worker
                            .connect
                            .commit(Request::new(CommitReq {
                                epoch: self.epoch,
                                committing_data: committing.committing_data().clone(),
                            }))
                            .await?;
                    }

                    self.checkpoint_state =
                        Some(CheckpointingOrCommittingState::Committing(committing));
                }
            }
            CheckpointingOrCommittingState::Committing(committing) => {
                self.start_or_get_span(JobCheckpointEventType::Committing)
                    .finish();
                self.start_or_get_span(JobCheckpointEventType::Checkpointing)
                    .finish();
                self.finish_committing(committing.checkpoint_id(), store)
                    .await?;
                self.last_checkpoint = Instant::now();
                self.checkpoint_state = None;
                info!(
                    message = "Finished committing checkpointing",
                    job_id = *self.job_id,
                    epoch = self.epoch,
                );
                store.notify_checkpoint_complete();
            }
        }

        Ok(())
    }

    pub async fn finish_checkpoint_if_done(
        &mut self,
        store: &dyn CheckpointMetadataStore,
    ) -> anyhow::Result<()> {
        if self.checkpoint_state.as_ref().unwrap().done() {
            if self.worker_leader_mode {
                self.finish_checkpoint_leader(store).await?;
            } else {
                self.finish_checkpoint_controller(store).await?;
            }
        }
        Ok(())
    }

    pub fn cleanup_needed(&self) -> Option<u32> {
        if self.epoch - self.min_epoch > CHECKPOINTS_TO_KEEP
            && self.epoch.is_multiple_of(COMPACT_EVERY)
        {
            Some(self.epoch - CHECKPOINTS_TO_KEEP)
        } else {
            None
        }
    }

    pub fn worker_timedout(&self) -> bool {
        for (worker, status) in &self.workers {
            if status.heartbeat_timeout() {
                error!(
                    message = "worker failed to heartbeat",
                    job_id = *self.job_id,
                    worker_id = worker.0
                );
                return true;
            }
        }

        false
    }

    pub fn task_failed(&self) -> Option<TaskFailedEvent> {
        for status in self.tasks.values() {
            if let TaskState::Failed(reason) = &status.state {
                return Some(reason.clone());
            }
        }

        None
    }

    pub fn any_finished_sources(&self) -> bool {
        let source_tasks = self.program.sources();

        self.tasks.iter().any(|((operator, _), t)| {
            source_tasks.contains(operator) && t.state == TaskState::Finished
        })
    }

    pub fn all_tasks_finished(&self) -> bool {
        self.tasks
            .iter()
            .all(|(_, t)| t.state == TaskState::Finished)
    }

    fn start_or_get_spans_static(
        spans: &mut Vec<JobCheckpointSpan>,
        event: JobCheckpointEventType,
    ) -> &mut JobCheckpointSpan {
        if let Some(idx) = spans.iter().position(|e| e.event == event) {
            return &mut spans[idx];
        }

        spans.push(JobCheckpointSpan::now(event));
        spans.last_mut().unwrap()
    }

    pub fn start_or_get_span(&mut self, event: JobCheckpointEventType) -> &mut JobCheckpointSpan {
        Self::start_or_get_spans_static(&mut self.checkpoint_spans, event)
    }
}

pub enum CheckpointingOrCommittingState {
    Checkpointing(CheckpointState),
    Committing(CommittingState),
}

impl CheckpointingOrCommittingState {
    pub fn done(&self) -> bool {
        match self {
            CheckpointingOrCommittingState::Checkpointing(checkpointing) => checkpointing.done(),
            CheckpointingOrCommittingState::Committing(committing) => committing.done(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerState {
    Running,
    Stopped,
}

#[allow(unused)]
pub struct WorkerStatus {
    pub id: WorkerId,
    pub connect: WorkerClient,
    pub last_heartbeat: Instant,
    pub state: WorkerState,
}

impl WorkerStatus {
    fn heartbeat_timeout(&self) -> bool {
        self.last_heartbeat.elapsed() > *config().pipeline.worker_heartbeat_timeout
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum TaskState {
    Running,
    Finished,
    Failed(TaskFailedEvent),
}

#[derive(Debug)]
pub struct TaskStatus {
    pub state: TaskState,
}

// Stores a model of the current state of a running job to use in the state machine
#[derive(Debug, PartialEq, Eq)]
pub enum JobState {
    Running,
    Stopped,
}
