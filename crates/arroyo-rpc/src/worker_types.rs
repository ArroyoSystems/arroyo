use crate::errors;
use crate::grpc::rpc;
use crate::grpc::rpc::{JobStopMode, TaskCheckpointCompletedReq, TaskCheckpointEventReq};
use arroyo_types::{JobId, MachineId, WorkerId};
use std::time::{Instant, SystemTime};

#[derive(Debug, Clone)]
pub struct WorkerContext {
    pub machine_id: MachineId,
    pub worker_id: WorkerId,
    pub job_id: JobId,
    pub run_id: u64,
}

impl WorkerContext {
    pub fn as_proto(&self) -> rpc::WorkerContext {
        rpc::WorkerContext {
            machine_id: self.machine_id.to_string(),
            worker_id: *self.worker_id,
            job_id: (*self.job_id).clone(),
            run_id: self.run_id,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TaskFailedEvent {
    pub worker_id: WorkerId,
    pub operator_id: String,
    pub task_id: u32,
    pub subtask_idx: u32,
    pub reason: String,
    pub details: String,
    pub error_domain: errors::ErrorDomain,
    pub retry_hint: errors::RetryHint,
}

impl From<TaskFailedEvent> for rpc::JobFailure {
    fn from(value: TaskFailedEvent) -> Self {
        Self {
            operator_id: Some(value.operator_id),
            task_id: Some(value.task_id),
            subtask_index: Some(value.subtask_idx),
            message: value.reason,
            error_domain: rpc::ErrorDomain::from(value.error_domain).into(),
            retry_hint: rpc::RetryHint::from(value.retry_hint).into(),
        }
    }
}

#[derive(Debug)]
pub enum RunningMessage {
    TaskCheckpointEvent(TaskCheckpointEventReq),
    TaskCheckpointFinished(TaskCheckpointCompletedReq),
    TaskFinished {
        worker_id: WorkerId,
        time: SystemTime,
        task_id: u32,
        subtask_idx: u32,
    },
    TaskFailed(TaskFailedEvent),
    WorkerHeartbeat {
        worker_id: WorkerId,
        time: Instant,
    },
    WorkerFinished {
        worker_id: WorkerId,
    },
    Stop {
        stop_mode: JobStopMode,
    },
}
