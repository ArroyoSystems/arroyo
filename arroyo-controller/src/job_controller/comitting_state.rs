use crate::queries::controller_queries;
use deadpool_postgres::Pool;
use std::collections::HashSet;
use std::time::SystemTime;

pub struct CommittingState {
    checkpoint_id: i64,
    subtasks_to_commit: HashSet<(String, u32)>,
}

impl CommittingState {
    pub fn new(checkpoint_id: i64, subtasks_to_commit: HashSet<(String, u32)>) -> Self {
        Self {
            checkpoint_id,
            subtasks_to_commit,
        }
    }
    pub fn subtask_committed(&mut self, operator_id: String, subtask_index: u32) {
        self.subtasks_to_commit
            .remove(&(operator_id, subtask_index));
    }

    pub fn done(&self) -> bool {
        self.subtasks_to_commit.is_empty()
    }

    pub async fn finish(self, pool: &Pool) -> anyhow::Result<()> {
        let finish_time = SystemTime::now();

        let c = pool.get().await?;
        controller_queries::commit_checkpoint()
            .bind(&c, &finish_time.into(), &self.checkpoint_id)
            .await?;

        Ok(())
    }
}

impl From<(i64, HashSet<(String, u32)>)> for CommittingState {
    fn from((checkpoint_id, subtasks_to_commit): (i64, HashSet<(String, u32)>)) -> Self {
        Self {
            checkpoint_id,
            subtasks_to_commit,
        }
    }
}
