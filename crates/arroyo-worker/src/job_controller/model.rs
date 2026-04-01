use crate::job_controller::checkpoint_state::CheckpointState;
use crate::job_controller::{CHECKPOINTS_TO_KEEP, COMPACT_EVERY, RunningMessage, TaskFailedEvent};
use anyhow::bail;
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::api_types::checkpoints::{JobCheckpointEventType, JobCheckpointSpan};
use arroyo_rpc::checkpoints::{
    CheckpointMetadataStore, CheckpointStatus, CreateCheckpointReq, FinishCheckpointReq,
    UpdateCheckpointReq,
};
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc::worker_grpc_client::WorkerGrpcClient;
use arroyo_rpc::grpc::rpc::{
    CheckpointReq, CommitReq, JobFinishedReq, LoadCompactedDataReq, TaskCheckpointEventType,
};
use arroyo_rpc::public_ids::{IdTypes, generate_id};
use arroyo_state::committing_state::CommittingState;
use arroyo_state::parquet::ParquetBackend;
use arroyo_state::{BackingStore, StateBackend};
use arroyo_types::{WorkerId, to_micros};
use futures::future::try_join_all;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::task::JoinHandle;
use tonic::Request;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

pub struct RunningJobModel {
    pub job_id: Arc<String>,
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
                    checkpoint_id: checkpoint_state.checkpoint_id().to_string(),
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
                checkpoint_id: checkpoint_state.checkpoint_id().to_string(),
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
                            StateBackend::write_operator_checkpoint_metadata(operator_metadata)
                                .await?;
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
            self.job_id.clone(),
            checkpoint_id,
            self.epoch,
            self.min_epoch,
            self.program.clone(),
        );

        self.checkpoint_state = Some(CheckpointingOrCommittingState::Checkpointing(state));

        Ok(())
    }

    async fn compact_state(&mut self) -> anyhow::Result<()> {
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

        let mut worker_clients: Vec<WorkerGrpcClient<Channel>> =
            self.workers.values().map(|w| w.connect.clone()).collect();
        for node in self.program.graph.node_weights() {
            for (op, _) in node.operator_chain.iter() {
                let compacted_tables = ParquetBackend::compact_operator(
                    // compact the operator's state and notify the workers to load the new files
                    self.job_id.clone(),
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

    pub async fn finish_checkpoint_if_done(
        &mut self,
        store: &dyn CheckpointMetadataStore,
    ) -> anyhow::Result<()> {
        if self.checkpoint_state.as_ref().unwrap().done() {
            let state = self.checkpoint_state.take().unwrap();
            match state {
                CheckpointingOrCommittingState::Checkpointing(mut checkpointing) => {
                    let metadata_span =
                        self.start_or_get_span(JobCheckpointEventType::WritingMetadata);
                    let checkpoint_metadata = checkpointing.build_metadata();
                    StateBackend::write_checkpoint_metadata(checkpoint_metadata).await?;
                    metadata_span.finish();

                    let committing_state = checkpointing.committing_state();
                    let duration = checkpointing
                        .start_time()
                        .elapsed()
                        .unwrap_or(Duration::ZERO)
                        .as_secs_f32();
                    // shortcut if committing is unnecessary
                    if committing_state.done() {
                        self.start_or_get_span(JobCheckpointEventType::Checkpointing)
                            .finish();
                        self.update_checkpoint_in_db(
                            &checkpointing,
                            store,
                            CheckpointStatus::Ready,
                        )
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

                        let committing_data = committing_state.committing_data();
                        self.checkpoint_state =
                            Some(CheckpointingOrCommittingState::Committing(committing_state));
                        info!(
                            message = "Committing checkpoint",
                            job_id = *self.job_id,
                            epoch = self.epoch,
                        );

                        self.start_or_get_span(JobCheckpointEventType::Committing);

                        for worker in self.workers.values_mut() {
                            worker
                                .connect
                                .commit(Request::new(CommitReq {
                                    epoch: self.epoch,
                                    committing_data: committing_data.clone(),
                                }))
                                .await?;
                        }
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

    pub fn start_or_get_span(&mut self, event: JobCheckpointEventType) -> &mut JobCheckpointSpan {
        if let Some(idx) = self.checkpoint_spans.iter().position(|e| e.event == event) {
            return &mut self.checkpoint_spans[idx];
        }

        self.checkpoint_spans.push(JobCheckpointSpan::now(event));
        self.checkpoint_spans.last_mut().unwrap()
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
    pub connect: WorkerGrpcClient<Channel>,
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
