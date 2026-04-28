use std::collections::{HashMap, HashSet};
use arroyo_rpc::grpc::rpc::{OperatorCommitData, TableCommitData};
use arroyo_state_protocol::types::CheckpointRef;

pub struct CommittingState {
    checkpoint_ref: CheckpointRef,
    subtasks_to_commit: HashSet<(String, u32)>,
    committing_data: HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>>,
}

impl CommittingState {
    pub fn new(
        checkpoint_ref: CheckpointRef,
        subtasks_to_commit: HashSet<(String, u32)>,
        committing_data: HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>>,
    ) -> Self {
        Self {
            checkpoint_ref,
            subtasks_to_commit,
            committing_data,
        }
    }

    pub fn checkpoint_id(&self) -> &str {
        self.checkpoint_ref.as_str()
    }
    
    pub fn checkpoint_ref(&self) -> &CheckpointRef {
        &self.checkpoint_ref
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