use arroyo_state::checkpoint_state::CheckpointState;
use arroyo_state::committing_state::CommittingState;

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
