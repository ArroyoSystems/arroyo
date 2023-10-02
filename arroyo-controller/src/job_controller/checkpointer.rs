use crate::job_controller::checkpoint_state::CheckpointState;
use crate::job_controller::comitting_state::CommittingState;

pub enum CheckpointingOrCommittingState {
    Checkpointing(CheckpointState),
    Committing(CommittingState),
}

impl CheckpointingOrCommittingState {
    pub(crate) fn done(&self) -> bool {
        match self {
            CheckpointingOrCommittingState::Checkpointing(checkpointing) => checkpointing.done(),
            CheckpointingOrCommittingState::Committing(committing) => committing.done(),
        }
    }
}
