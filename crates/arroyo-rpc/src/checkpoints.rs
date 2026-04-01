use anyhow::Result;
use serde_json::Value as JsonValue;
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointStatus {
    InProgress,
    Committing,
    Ready,
    Failed,
    Compacting,
    Compacted,
}

#[derive(Debug, Clone)]
pub struct CreateCheckpointReq {
    pub checkpoint_id: String,
    pub epoch: u32,
    pub min_epoch: u32,
    pub start_time: SystemTime,
}

#[derive(Debug, Clone)]
pub struct UpdateCheckpointReq {
    pub checkpoint_id: String,
    pub operator_details: JsonValue,
    pub finish_time: Option<SystemTime>,
    pub status: CheckpointStatus,
    pub event_spans: JsonValue,
}

#[derive(Debug, Clone)]
pub struct FinishCheckpointReq {
    pub checkpoint_id: String,
    pub finish_time: SystemTime,
    pub event_spans: JsonValue,
}

#[async_trait::async_trait]
pub trait CheckpointMetadataStore: Send + Sync {
    async fn create_checkpoint(&self, req: CreateCheckpointReq) -> Result<()>;

    async fn update_checkpoint(&self, req: UpdateCheckpointReq) -> Result<()>;

    async fn finish_checkpoint(&self, req: FinishCheckpointReq) -> Result<()>;

    async fn mark_compacting(&self, job_id: &str, min_epoch: u32, new_min: u32) -> Result<()>;

    async fn mark_checkpoints_compacted(&self, job_id: &str, epoch: u32) -> Result<()>;

    async fn drop_old_checkpoint_rows(&self, job_id: &str, epoch: u32) -> Result<()>;

    fn notify_checkpoint_complete(&self);
}
