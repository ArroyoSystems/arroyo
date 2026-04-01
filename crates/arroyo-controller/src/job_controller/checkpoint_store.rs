use anyhow::Result;
use arroyo_rpc::checkpoints::{
    CheckpointMetadataStore, CheckpointStatus, CreateCheckpointReq, FinishCheckpointReq,
    UpdateCheckpointReq,
};
use arroyo_rpc::notify_db;
use cornucopia_async::DatabaseSource;
use std::sync::Arc;
use time::OffsetDateTime;

use crate::queries::controller_queries;
use crate::types::public::CheckpointState as DbCheckpointState;

pub struct DbCheckpointMetadataStore {
    pub organization_id: String,
    pub job_id: Arc<String>,
    pub db: DatabaseSource,
    pub state_backend: &'static str,
}

fn to_db_state(status: CheckpointStatus) -> DbCheckpointState {
    match status {
        CheckpointStatus::InProgress => DbCheckpointState::inprogress,
        CheckpointStatus::Committing => DbCheckpointState::committing,
        CheckpointStatus::Ready => DbCheckpointState::ready,
        CheckpointStatus::Failed => DbCheckpointState::failed,
        CheckpointStatus::Compacting => DbCheckpointState::compacting,
        CheckpointStatus::Compacted => DbCheckpointState::compacted,
    }
}

#[async_trait::async_trait]
impl CheckpointMetadataStore for DbCheckpointMetadataStore {
    async fn create_checkpoint(&self, req: CreateCheckpointReq) -> Result<()> {
        let c = self.db.client().await?;
        controller_queries::execute_create_checkpoint(
            &c,
            &req.checkpoint_id,
            &self.organization_id,
            &*self.job_id,
            &self.state_backend,
            &(req.epoch as i32),
            &(req.min_epoch as i32),
            &OffsetDateTime::from(req.start_time),
        )
        .await?;
        Ok(())
    }

    async fn update_checkpoint(&self, req: UpdateCheckpointReq) -> Result<()> {
        let c = self.db.client().await?;
        let finish_time: Option<OffsetDateTime> = req.finish_time.map(OffsetDateTime::from);
        controller_queries::execute_update_checkpoint(
            &c,
            &req.operator_details,
            &finish_time,
            &to_db_state(req.status),
            &req.event_spans,
            &req.checkpoint_id,
        )
        .await?;
        Ok(())
    }

    async fn finish_checkpoint(&self, req: FinishCheckpointReq) -> Result<()> {
        let c = self.db.client().await?;
        controller_queries::execute_commit_checkpoint(
            &c,
            &OffsetDateTime::from(req.finish_time),
            &req.event_spans,
            &req.checkpoint_id,
        )
        .await?;
        Ok(())
    }

    async fn mark_compacting(&self, job_id: &str, min_epoch: u32, new_min: u32) -> Result<()> {
        let c = self.db.client().await?;
        controller_queries::execute_mark_compacting(
            &c,
            &job_id,
            &(min_epoch as i32),
            &(new_min as i32),
        )
        .await?;
        Ok(())
    }

    async fn mark_checkpoints_compacted(&self, job_id: &str, epoch: u32) -> Result<()> {
        let c = self.db.client().await?;
        controller_queries::execute_mark_checkpoints_compacted(&c, &job_id, &(epoch as i32))
            .await?;
        Ok(())
    }

    async fn drop_old_checkpoint_rows(&self, job_id: &str, epoch: u32) -> Result<()> {
        let c = self.db.client().await?;
        controller_queries::execute_drop_old_checkpoint_rows(&c, &job_id, &(epoch as i32)).await?;
        Ok(())
    }

    fn notify_checkpoint_complete(&self) {
        notify_db();
    }
}
