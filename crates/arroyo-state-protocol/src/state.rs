use crate::types::{
    CheckpointRef, CommittedMarker, Epoch, EpochRecord, ProtocolError,
    validate_committed_marker_matches_checkpoint, validate_epoch_record_matches_checkpoint,
};
use arroyo_rpc::grpc::rpc::CheckpointManifest;

/// Derived state of a checkpoint from the checkpoint manifest, epoch record,
/// and commit marker observed in object storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointState {
    /// The checkpoint manifest does not exist yet.
    Invisible,
    /// The checkpoint manifest exists, but no epoch record exists for its epoch.
    Unclaimed,
    /// The epoch record exists but points at another checkpoint.
    Orphaned { canonical_ref: CheckpointRef },
    /// The checkpoint is canonical and safe to use for normal recovery.
    Ready,
    /// The checkpoint is canonical but still requires external commit replay.
    Committing { epoch_record: EpochRecord },
}

impl CheckpointState {
    /// Returns true when normal execution can restore from this checkpoint.
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Returns true when the checkpoint owns its epoch record.
    pub fn is_canonical(&self) -> bool {
        matches!(self, Self::Ready | Self::Committing { .. })
    }

    /// Returns true when recovery must replay external commit before continuing.
    pub fn requires_commit_replay(&self) -> bool {
        matches!(self, Self::Committing { .. })
    }
}

/// Computes checkpoint state from already-read protocol objects.
///
/// This function performs no I/O. Callers that need storage access should use
/// the workflow functions, which read the objects and then call this logic.
pub fn derive_checkpoint_state(
    checkpoint_ref: &CheckpointRef,
    checkpoint: Option<&CheckpointManifest>,
    epoch_record: Option<EpochRecord>,
    committed_marker: Option<&CommittedMarker>,
) -> Result<CheckpointState, ProtocolError> {
    let Some(checkpoint) = checkpoint else {
        return Ok(CheckpointState::Invisible);
    };

    let Some(epoch_record) = epoch_record else {
        return Ok(CheckpointState::Unclaimed);
    };

    let checkpoint_epoch = Epoch(checkpoint.epoch);
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

    validate_epoch_record_matches_checkpoint(checkpoint_ref, checkpoint, &epoch_record)?;

    if !checkpoint.needs_commit {
        return Ok(CheckpointState::Ready);
    }

    match committed_marker {
        Some(marker) => {
            validate_committed_marker_matches_checkpoint(checkpoint_ref, checkpoint, marker)?;
            Ok(CheckpointState::Ready)
        }
        None => Ok(CheckpointState::Committing { epoch_record }),
    }
}
