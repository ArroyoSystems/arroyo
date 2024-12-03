use std::collections::{HashMap, HashSet};

use arroyo_rpc::grpc::rpc::{OperatorCommitData, TableCommitData};

pub struct CommittingState {
    checkpoint_id: String,
    subtasks_to_commit: HashSet<(u32, u32)>,
    committing_data: HashMap<u32, HashMap<String, HashMap<u32, Vec<u8>>>>,
}

impl CommittingState {
    pub fn new(
        checkpoint_id: String,
        subtasks_to_commit: HashSet<(u32, u32)>,
        committing_data: HashMap<u32, HashMap<String, HashMap<u32, Vec<u8>>>>,
    ) -> Self {
        Self {
            checkpoint_id,
            subtasks_to_commit,
            committing_data,
        }
    }

    pub fn checkpoint_id(&self) -> &str {
        &self.checkpoint_id
    }

    pub fn subtask_committed(&mut self, node_id: u32, subtask_index: u32) {
        self.subtasks_to_commit.remove(&(node_id, subtask_index));
    }

    pub fn done(&self) -> bool {
        self.subtasks_to_commit.is_empty()
    }

    pub fn committing_data(&self) -> HashMap<u32, OperatorCommitData> {
        let operators_to_commit: HashSet<_> = self
            .subtasks_to_commit
            .iter()
            .map(|(node_id, _subtask_id)| *node_id)
            .collect();

        operators_to_commit
            .into_iter()
            .map(|node_id| {
                let committing_data = self
                    .committing_data
                    .get(&node_id)
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
                (node_id, OperatorCommitData { committing_data })
            })
            .collect()
    }
}
