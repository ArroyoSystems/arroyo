use crate::types::{
    CheckpointRef, CommittedMarker, EpochRecord, ProtocolError, checkpoint_epoch,
    validate_committed_marker_matches_checkpoint, validate_epoch_record_matches_checkpoint,
};
use arroyo_rpc::grpc::rpc::CheckpointManifest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointState {
    Invisible,
    Unclaimed,
    Orphaned { canonical_ref: CheckpointRef },
    Ready,
    Committing,
}

impl CheckpointState {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub fn is_canonical(&self) -> bool {
        matches!(self, Self::Ready | Self::Committing)
    }

    pub fn requires_commit_replay(&self) -> bool {
        matches!(self, Self::Committing)
    }
}

pub fn derive_checkpoint_state(
    checkpoint_ref: &CheckpointRef,
    checkpoint: Option<&CheckpointManifest>,
    epoch_record: Option<&EpochRecord>,
    committed_marker: Option<&CommittedMarker>,
) -> Result<CheckpointState, ProtocolError> {
    let Some(checkpoint) = checkpoint else {
        return Ok(CheckpointState::Invisible);
    };

    let Some(epoch_record) = epoch_record else {
        return Ok(CheckpointState::Unclaimed);
    };

    let checkpoint_epoch = checkpoint_epoch(checkpoint);
    if epoch_record.epoch != checkpoint_epoch {
        return Err(ProtocolError::EpochMismatch {
            checkpoint_epoch,
            record_epoch: epoch_record.epoch,
        });
    }

    if &epoch_record.checkpoint_ref != checkpoint_ref {
        return Ok(CheckpointState::Orphaned {
            canonical_ref: epoch_record.checkpoint_ref.clone(),
        });
    }

    validate_epoch_record_matches_checkpoint(checkpoint_ref, checkpoint, epoch_record)?;

    if !checkpoint.needs_commit {
        return Ok(CheckpointState::Ready);
    }

    match committed_marker {
        Some(marker) => {
            validate_committed_marker_matches_checkpoint(checkpoint_ref, checkpoint, marker)?;
            Ok(CheckpointState::Ready)
        }
        None => Ok(CheckpointState::Committing),
    }
}
