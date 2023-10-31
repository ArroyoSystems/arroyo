use std::collections::{HashMap, HashSet};

use arroyo_rpc::grpc::{OperatorCommitData, TableCommitData};

pub struct CommittingState {
    checkpoint_id: i64,
    subtasks_to_commit: HashSet<(String, u32)>,
    committing_data: HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>>,
}

impl CommittingState {
    pub fn new(
        checkpoint_id: i64,
        subtasks_to_commit: HashSet<(String, u32)>,
        committing_data: HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>>,
    ) -> Self {
        Self {
            checkpoint_id,
            subtasks_to_commit,
            committing_data,
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

    pub fn committing_data(&self) -> HashMap<String, OperatorCommitData> {
        let operators_to_commit: HashSet<_> = self
            .subtasks_to_commit
            .iter()
            .map(|(operator_id, _subtask_id)| operator_id.clone())
            .collect();
        operators_to_commit
            .into_iter()
            .map(|operator_id| {
                let committing_data = self
                    .committing_data
                    .get(&operator_id)
                    .map(|table_map| {
                        table_map
                            .iter()
                            .map(|(table_name, subtask_to_commit_data)| {
                                (
                                    table_name.clone(),
                                    TableCommitData {
                                        commit_data_by_subtask: subtask_to_commit_data.clone(),
                                    },
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (operator_id, OperatorCommitData { committing_data })
            })
            .collect()
    }
}
