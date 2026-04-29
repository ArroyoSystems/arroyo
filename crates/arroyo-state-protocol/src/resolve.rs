use crate::state::{CheckpointState, derive_checkpoint_state};
use crate::types::{
    CheckpointRef, CommittedMarker, EpochRecord, GenerationManifest, ProtocolError,
    checkpoint_parent_checkpoint_ref,
};
use arroyo_rpc::grpc::rpc::CheckpointManifest;

/// State of a candidate checkpoint's parent when resolving recovery safety.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentCheckpointStatus {
    NoParent,
    ReadyCanonical,
    NotReadyCanonical,
}

/// Pure decision returned when resolving a generation manifest candidate.
///
/// Storage-facing callers usually use `workflow::resolve_generation_manifest`;
/// this enum is useful for tests and for code that has already read all inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveDecision {
    Ready { checkpoint_ref: CheckpointRef },
    ReplayCommit { checkpoint_ref: CheckpointRef },
    ClaimUnclaimed { checkpoint_ref: CheckpointRef },
    FallbackToBase,
    StopOrphaned { canonical_ref: CheckpointRef },
    Failed(ResolveFailure),
}

/// Reason a candidate could not be resolved into a recoverable checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveFailure {
    NoCandidate,
    InvisibleBase,
    UnclaimedBase,
    ParentNotReadyCanonical,
}

/// Result of interpreting an epoch-record conditional-create outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochClaimOutcome {
    Owned { record: EpochRecord },
    Orphaned { canonical_ref: CheckpointRef },
}

/// Resolves a generation manifest candidate from already-read protocol objects.
///
/// The generation manifest's latest/base pointer is only a candidate. This
/// function checks the epoch record and commit marker to decide whether the
/// checkpoint is ready, needs commit replay, should be claimed by the current
/// generation, should fall back to the base checkpoint, or is orphaned.
pub fn resolve_candidate(
    manifest: &GenerationManifest,
    candidate_ref: &CheckpointRef,
    checkpoint: Option<&CheckpointManifest>,
    epoch_record: Option<&EpochRecord>,
    committed_marker: Option<&CommittedMarker>,
    parent_status: ParentCheckpointStatus,
    is_current_generation: bool,
) -> Result<ResolveDecision, ProtocolError> {
    let Some(checkpoint) = checkpoint else {
        if manifest.base_checkpoint_ref.as_ref() == Some(candidate_ref) {
            return Ok(ResolveDecision::Failed(ResolveFailure::InvisibleBase));
        }

        return Ok(ResolveDecision::FallbackToBase);
    };

    if checkpoint_parent_checkpoint_ref(checkpoint)?.is_some()
        && parent_status != ParentCheckpointStatus::ReadyCanonical
    {
        return Ok(ResolveDecision::Failed(
            ResolveFailure::ParentNotReadyCanonical,
        ));
    }

    let state = derive_checkpoint_state(
        candidate_ref,
        Some(checkpoint),
        epoch_record,
        committed_marker,
    )?;

    Ok(match state {
        CheckpointState::Invisible => unreachable!("handled above"),
        CheckpointState::Ready => ResolveDecision::Ready {
            checkpoint_ref: candidate_ref.clone(),
        },
        CheckpointState::Committing => ResolveDecision::ReplayCommit {
            checkpoint_ref: candidate_ref.clone(),
        },
        CheckpointState::Orphaned { canonical_ref } => {
            ResolveDecision::StopOrphaned { canonical_ref }
        }
        CheckpointState::Unclaimed if is_current_generation => ResolveDecision::ClaimUnclaimed {
            checkpoint_ref: candidate_ref.clone(),
        },
        CheckpointState::Unclaimed => {
            if manifest.base_checkpoint_ref.as_ref() == Some(candidate_ref) {
                ResolveDecision::Failed(ResolveFailure::UnclaimedBase)
            } else {
                ResolveDecision::FallbackToBase
            }
        }
    })
}
