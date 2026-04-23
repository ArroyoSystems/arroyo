use anyhow::{anyhow, bail};
use arroyo_rpc::checkpoints::{
    CheckpointMetadataStore, CreateCheckpointReq, FinishCheckpointReq, UpdateCheckpointReq,
};
use arroyo_rpc::config::config;
use arroyo_rpc::errors::{ErrorDomain, RetryHint};
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::{
    JobFailure, JobStatus, SubtaskCheckpointMetadata, TaskCheckpointEventType,
};
use arroyo_rpc::grpc_channel_builder;
use arroyo_rpc::identity::{WorkerClient, worker_client};
use arroyo_types::WorkerId;
use arroyo_types::to_micros;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tracing::{error, info};

// Re-export shared types from arroyo-rpc
pub use arroyo_rpc::worker_types::{RunningMessage, TaskFailedEvent, WorkerContext};

pub const CHECKPOINTS_TO_KEEP: u32 = 4;
pub const COMPACT_EVERY: u32 = 2;

pub mod checkpoint_state;
pub mod controller;
pub mod model;

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
            Ok(channel) => return Ok(worker_client(channel, *id)),
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
}

fn checkpoint_size_bytes(operator_details: &Value) -> u64 {
    operator_details
        .as_object()
        .into_iter()
        .flat_map(|operators| operators.values())
        .filter_map(|operator| operator.get("tasks"))
        .filter_map(Value::as_object)
        .flat_map(|tasks| tasks.values())
        .filter_map(|task| task.get("bytes"))
        .filter_map(Value::as_u64)
        .sum()
}

fn update_last_checkpoint(
    job_status: &Arc<Mutex<JobStatus>>,
    f: impl FnOnce(&mut rpc::CheckpointStatus),
) -> anyhow::Result<()> {
    let mut status = job_status
        .lock()
        .map_err(|e| anyhow!("job status mutex poisoned: {e}"))?;

    status.updated_at = to_micros(SystemTime::now());

    let checkpoint = status
        .last_checkpoint
        .as_mut()
        .ok_or_else(|| anyhow!("job status missing last checkpoint"))?;

    f(checkpoint);
    Ok(())
}

#[async_trait]
impl CheckpointMetadataStore for JobControllerStatus {
    async fn create_checkpoint(&self, req: CreateCheckpointReq) -> anyhow::Result<()> {
        let mut status = self
            .job_status
            .lock()
            .map_err(|e| anyhow!("job status mutex poisoned: {e}"))?;
        status.last_checkpoint = Some(rpc::CheckpointStatus {
            epoch: req.epoch,
            started_at: to_micros(req.start_time),
            finished_at: None,
            size_bytes: 0,
        });
        Ok(())
    }

    async fn update_checkpoint(&self, req: UpdateCheckpointReq) -> anyhow::Result<()> {
        let size_bytes = checkpoint_size_bytes(&req.operator_details);
        update_last_checkpoint(&self.job_status, |checkpoint| {
            checkpoint.size_bytes = size_bytes;
            checkpoint.finished_at = req.finish_time.map(to_micros);
        })
    }

    async fn finish_checkpoint(&self, req: FinishCheckpointReq) -> anyhow::Result<()> {
        update_last_checkpoint(&self.job_status, |checkpoint| {
            checkpoint.finished_at = Some(to_micros(req.finish_time));
        })
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
