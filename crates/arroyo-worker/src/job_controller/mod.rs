use anyhow::bail;
use arroyo_rpc::api_types::checkpoints::{JobCheckpointEventType, JobCheckpointSpan};
use arroyo_rpc::checkpoints::{
    CheckpointMetadataStore, CreateCheckpointReq, FinishCheckpointReq, UpdateCheckpointReq,
};
use arroyo_rpc::config::config;
use arroyo_rpc::errors::{ErrorDomain, RetryHint};
use arroyo_rpc::grpc::api::OperatorCheckpointDetail;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::{
    JobFailure, JobStatus, SubtaskCheckpointMetadata, TaskCheckpointEventType,
};
use arroyo_rpc::grpc_channel_builder;
use arroyo_rpc::identity::{WorkerClient, worker_client};
use arroyo_state::{BackingStore, StateBackend};
use arroyo_types::WorkerId;
use arroyo_types::to_micros;
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tracing::{error, info};

// Re-export shared types from arroyo-rpc
pub use arroyo_rpc::worker_types::{RunningMessage, TaskFailedEvent, WorkerContext};

pub const CHECKPOINTS_TO_KEEP: u32 = 10;
pub const COMPACT_EVERY: u32 = 2;

#[derive(Debug, Clone)]
pub struct StoredCheckpointMetadata {
    checkpoint_id: String,
    epoch: u64,
    state_backend: String,
    start_time: SystemTime,
    finish_time: Option<SystemTime>,
    event_spans: Vec<JobCheckpointSpan>,
    operator_details: HashMap<String, OperatorCheckpointDetail>,
    status: arroyo_rpc::checkpoints::CheckpointStatus,
}

#[derive(Debug, Default)]
pub struct CheckpointHistory {
    checkpoints: VecDeque<StoredCheckpointMetadata>,
}

fn checkpoint_event_description(event: JobCheckpointEventType) -> &'static str {
    match event {
        JobCheckpointEventType::Checkpointing => "The entire checkpointing process",
        JobCheckpointEventType::CheckpointingOperators => {
            "The time spent checkpointing operator states"
        }
        JobCheckpointEventType::WritingMetadata => "Writing the final checkpoint metadata",
        JobCheckpointEventType::Compacting => "Compacting old checkpoints",
        JobCheckpointEventType::Committing => {
            "Running two-phase commit for transactional connectors"
        }
    }
}

fn checkpoint_span_to_rpc(span: JobCheckpointSpan) -> rpc::JobCheckpointEventSpan {
    rpc::JobCheckpointEventSpan {
        start_time: span.start_time,
        finish_time: span.finish_time.unwrap_or_default(),
        event: format!("{:?}", span.event),
        description: checkpoint_event_description(span.event).to_string(),
    }
}

impl CheckpointHistory {
    fn prune(&mut self) {
        while self.checkpoints.len() > CHECKPOINTS_TO_KEEP as usize {
            self.checkpoints.pop_front();
        }
    }

    fn create_checkpoint(&mut self, req: &CreateCheckpointReq) {
        let checkpoint = StoredCheckpointMetadata {
            checkpoint_id: req.checkpoint_id.clone(),
            epoch: req.epoch as u64,
            state_backend: StateBackend::name().to_string(),
            start_time: req.start_time,
            finish_time: None,
            event_spans: vec![],
            operator_details: HashMap::new(),
            status: arroyo_rpc::checkpoints::CheckpointStatus::InProgress,
        };

        if let Some(existing) = self
            .checkpoints
            .iter_mut()
            .find(|c| c.checkpoint_id == req.checkpoint_id)
        {
            *existing = checkpoint;
        } else {
            self.checkpoints.push_back(checkpoint);
        }

        self.prune();
    }

    fn update_checkpoint(
        &mut self,
        checkpoint_id: &str,
        operator_details: HashMap<String, OperatorCheckpointDetail>,
        event_spans: Vec<JobCheckpointSpan>,
        finish_time: Option<SystemTime>,
        status: arroyo_rpc::checkpoints::CheckpointStatus,
    ) {
        if let Some(checkpoint) = self
            .checkpoints
            .iter_mut()
            .find(|c| c.checkpoint_id == checkpoint_id)
        {
            checkpoint.operator_details = operator_details;
            checkpoint.event_spans = event_spans;
            checkpoint.finish_time = finish_time;
            checkpoint.status = status;
        }
    }

    fn finish_checkpoint(
        &mut self,
        checkpoint_id: &str,
        finish_time: SystemTime,
        event_spans: Vec<JobCheckpointSpan>,
    ) {
        if let Some(checkpoint) = self
            .checkpoints
            .iter_mut()
            .find(|c| c.checkpoint_id == checkpoint_id)
        {
            checkpoint.finish_time = Some(finish_time);
            checkpoint.event_spans = event_spans;
            checkpoint.status = arroyo_rpc::checkpoints::CheckpointStatus::Ready;
        }
    }

    pub fn checkpoints(&self) -> Vec<rpc::JobCheckpointMetadata> {
        self.checkpoints
            .iter()
            .filter(|c| {
                !matches!(
                    c.status,
                    arroyo_rpc::checkpoints::CheckpointStatus::Failed
                        | arroyo_rpc::checkpoints::CheckpointStatus::Compacted
                )
            })
            .map(|c| rpc::JobCheckpointMetadata {
                epoch: c.epoch as u32,
                state_backend: c.state_backend.clone(),
                start_time: to_micros(c.start_time),
                finish_time: c.finish_time.map(to_micros),
                event_spans: c
                    .event_spans
                    .iter()
                    .cloned()
                    .map(checkpoint_span_to_rpc)
                    .collect(),
            })
            .collect()
    }

    pub fn operators_for_epoch(
        &self,
        epoch: u64,
    ) -> Option<HashMap<String, OperatorCheckpointDetail>> {
        self.checkpoints
            .iter()
            .find(|c| {
                c.epoch == epoch
                    && !matches!(c.status, arroyo_rpc::checkpoints::CheckpointStatus::Failed)
            })
            .map(|c| c.operator_details.clone())
    }
}

pub mod checkpoint_state;
pub mod committing_state;
pub mod controller;
pub mod model;

#[derive(Debug)]
pub struct RetireWorkerLeader {
    pub reason: String,
}

impl Display for RetireWorkerLeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "retiring worker leader: {}", self.reason)
    }
}

impl std::error::Error for RetireWorkerLeader {}

#[derive(Debug, Clone)]
pub struct WorkerRegistration {
    #[allow(dead_code)]
    pub context: WorkerContext,
    pub rpc_address: String,
    pub data_address: String,
    pub slots: u64,
    pub resources_slots: u64,
}

#[derive(Debug, Clone)]
pub struct WorkerInitResult {
    #[allow(dead_code)]
    pub context: WorkerContext,
    pub success: bool,
    pub error_message: Option<String>,
    pub time: SystemTime,
}

#[derive(Debug, Clone)]
pub enum JobControllerEvent {
    TaskStarted {
        time: SystemTime,
        operator_idx: u32,
        subtask_index: u64,
    },
    TaskFinished {
        time: SystemTime,
        operator_idx: u32,
        subtask_index: u64,
    },
    TaskFailed {
        time: SystemTime,
        operator_id: String,
        subtask_index: u64,
        message: String,
        details: String,
        error_domain: ErrorDomain,
        retry_hint: RetryHint,
    },
    CheckpointEvent {
        time: SystemTime,
        operator_id: String,
        subtask_index: u32,
        epoch: u32,
        event_type: TaskCheckpointEventType,
    },
    CheckpointCompleted {
        time: SystemTime,
        operator_id: String,
        epoch: u32,
        metadata: SubtaskCheckpointMetadata,
        needs_commit: bool,
    },
    NonfatalError {
        time: SystemTime,
        operator_id: String,
        subtask_index: u64,
        message: String,
        details: String,
        error_domain: ErrorDomain,
        retry_hint: RetryHint,
    },
}

pub(crate) async fn connect_to_worker(id: WorkerId, addr: String) -> anyhow::Result<WorkerClient> {
    info!(
        message = "connecting to worker from job leader",
        worker_id =? id,
        addr,
    );

    for i in 0..3 {
        match grpc_channel_builder(
            "controller",
            addr.clone(),
            &config().worker.tls,
            &config().worker.tls,
        )
        .await?
        .timeout(Duration::from_secs(15))
        .connect()
        .await
        {
            Ok(channel) => return Ok(worker_client(channel, id)),
            Err(e) => {
                error!(
                    message = "Failed to connect to worker",
                    worker_id =? id,
                    error = format!("{:?}", e),
                    addr,
                    retry = i
                );
                tokio::time::sleep(Duration::from_millis((i + 1) * 100)).await;
            }
        }
    }
    bail!("failed to connect to worker {:?} at {}", id, addr)
}

#[derive(Clone)]
pub struct JobControllerStatus {
    pub job_status: Arc<Mutex<JobStatus>>,
    pub checkpoint_history: Arc<Mutex<CheckpointHistory>>,
}

impl JobControllerStatus {
    pub fn transition(&self, next: rpc::JobState) -> anyhow::Result<()> {
        let mut status = self.job_status.lock().unwrap();
        let cur = rpc::JobState::try_from(status.job_state)?;
        match (cur, next) {
            (rpc::JobState::JobUnknown, _) | (_, rpc::JobState::JobUnknown) => {
                bail!("invalid job status transition involving JOB_UNKNOWN");
            }
            (rpc::JobState::JobInitializing, rpc::JobState::JobRunning)
            | (rpc::JobState::JobInitializing, rpc::JobState::JobStopping)
            | (rpc::JobState::JobInitializing, rpc::JobState::JobFailing)
            | (rpc::JobState::JobRunning, rpc::JobState::JobStopping)
            | (rpc::JobState::JobRunning, rpc::JobState::JobFinishing)
            | (rpc::JobState::JobRunning, rpc::JobState::JobFailing)
            | (rpc::JobState::JobStopping, rpc::JobState::JobStopped)
            | (rpc::JobState::JobStopping, rpc::JobState::JobFailing)
            | (rpc::JobState::JobFinishing, rpc::JobState::JobFinished)
            | (rpc::JobState::JobFinishing, rpc::JobState::JobFailing)
            | (rpc::JobState::JobFailing, rpc::JobState::JobFailed) => {
                status.job_state = next.into();
                status.transitioned_at = to_micros(SystemTime::now());
            }
            (cur, next) if cur == next => {
                // no-op
            }
            _ => {
                bail!("invalid job status transition from {:?} to {:?}", cur, next);
            }
        }

        status.updated_at = to_micros(SystemTime::now());

        Ok(())
    }

    pub fn to_failing(&self, failure: JobFailure) -> anyhow::Result<()> {
        {
            let mut status = self.job_status.lock().unwrap();
            status.job_failure = Some(failure);
        }
        self.transition(rpc::JobState::JobFailing)
    }

    pub fn retire(&self) -> anyhow::Result<()> {
        let mut status = self.job_status.lock().unwrap();
        status.job_state = rpc::JobState::JobStopped.into();
        status.transitioned_at = to_micros(SystemTime::now());
        status.updated_at = status.transitioned_at;
        Ok(())
    }
}

#[async_trait]
impl CheckpointMetadataStore for JobControllerStatus {
    async fn create_checkpoint(&self, req: CreateCheckpointReq) -> anyhow::Result<()> {
        self.checkpoint_history
            .lock()
            .unwrap()
            .create_checkpoint(&req);

        Ok(())
    }

    async fn update_checkpoint(&self, req: UpdateCheckpointReq) -> anyhow::Result<()> {
        let operator_details = serde_json::from_value(req.operator_details.clone())?;
        let event_spans = serde_json::from_value(req.event_spans.clone())?;

        self.checkpoint_history.lock().unwrap().update_checkpoint(
            &req.checkpoint_id,
            operator_details,
            event_spans,
            req.finish_time,
            req.status,
        );

        Ok(())
    }

    async fn finish_checkpoint(&self, req: FinishCheckpointReq) -> anyhow::Result<()> {
        let event_spans = serde_json::from_value(req.event_spans.clone())?;

        self.job_status.lock().unwrap().last_checkpointed_at = Some(to_micros(req.finish_time));

        self.checkpoint_history.lock().unwrap().finish_checkpoint(
            &req.checkpoint_id,
            req.finish_time,
            event_spans,
        );

        Ok(())
    }

    async fn mark_compacting(
        &self,
        _job_id: &str,
        _min_epoch: u32,
        _new_min: u32,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn mark_checkpoints_compacted(&self, _job_id: &str, _epoch: u32) -> anyhow::Result<()> {
        Ok(())
    }

    async fn drop_old_checkpoint_rows(&self, _job_id: &str, _epoch: u32) -> anyhow::Result<()> {
        Ok(())
    }

    fn notify_checkpoint_complete(&self) {
        // no-op
    }
}
