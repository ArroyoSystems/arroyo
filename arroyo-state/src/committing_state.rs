use std::collections::HashSet;

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

    pub fn checkpoint_id(&self) -> i64 {
        self.checkpoint_id
    }

    pub fn subtask_committed(&mut self, operator_id: String, subtask_index: u32) {
        self.subtasks_to_commit
            .remove(&(operator_id, subtask_index));
    }

    pub fn done(&self) -> bool {
        self.subtasks_to_commit.is_empty()
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
