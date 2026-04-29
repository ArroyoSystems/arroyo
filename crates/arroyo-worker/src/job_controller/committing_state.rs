use crate::job_controller::checkpoint_state::CommitData;
use arroyo_rpc::grpc::rpc::{OperatorCommitData, TableCommitData};
use arroyo_state_protocol::types::{CheckpointRef, EpochRecord};
use std::collections::{HashMap, HashSet};

pub enum CheckpointIdOrRef {
    // used by database-backed mode, checkpoint metadata v0
    CheckpointId(String),
    // used by checkpoint metadata v1
    CheckpointRef(CheckpointRef, EpochRecord),
}

pub struct CommittingState {
    checkpoint_id: CheckpointIdOrRef,
    subtasks_to_commit: HashSet<(String, u32)>,
    committing_data: CommitData,
}

impl CommittingState {
    pub fn new(
        checkpoint_id: CheckpointIdOrRef,
        subtasks_to_commit: HashSet<(String, u32)>,
        committing_data: CommitData,
    ) -> Self {
        Self {
            checkpoint_id,
            subtasks_to_commit,
            committing_data,
        }
    }

    pub fn checkpoint_id(&self) -> &str {
        match &self.checkpoint_id {
            CheckpointIdOrRef::CheckpointId(id) => id.as_str(),
            CheckpointIdOrRef::CheckpointRef(_, _) => {
                panic!("asked for checkpoint id in leader mode, which is not allowed");
            }
        }
    }

    pub fn checkpoint_ref(&self) -> (&CheckpointRef, &EpochRecord) {
        match &self.checkpoint_id {
            CheckpointIdOrRef::CheckpointId(_) => {
                panic!("asked for checkpoint ref in controller mode, which is not allowed");
            }
            CheckpointIdOrRef::CheckpointRef(a, b) => (a, b),
        }
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
