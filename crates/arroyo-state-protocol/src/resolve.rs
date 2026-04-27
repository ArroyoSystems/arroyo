use crate::{
    CheckpointManifest, CheckpointRef, CheckpointState, CommittedMarker, EpochRecord,
    GenerationManifest, ProtocolError, derive_checkpoint_state,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentCheckpointStatus {
    NoParent,
    ReadyCanonical,
    NotReadyCanonical,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveDecision {
    Ready { checkpoint_ref: CheckpointRef },
    ReplayCommit { checkpoint_ref: CheckpointRef },
    ClaimUnclaimed { checkpoint_ref: CheckpointRef },
    FallbackToBase,
    StopOrphaned { canonical_ref: CheckpointRef },
    Failed(ResolveFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveFailure {
    NoCandidate,
    InvisibleBase,
    UnclaimedBase,
    ParentNotReadyCanonical,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochClaimOutcome {
    Owned,
    Orphaned { canonical_ref: CheckpointRef },
}

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

    if checkpoint.parent_checkpoint_ref.is_some()
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

pub fn classify_epoch_record_claim(
    checkpoint_ref: &CheckpointRef,
    existing_record: Option<&EpochRecord>,
) -> EpochClaimOutcome {
    match existing_record {
        None => EpochClaimOutcome::Owned,
        Some(record) if &record.checkpoint_ref == checkpoint_ref => EpochClaimOutcome::Owned,
        Some(record) => EpochClaimOutcome::Orphaned {
            canonical_ref: record.checkpoint_ref.clone(),
        },
    }
}
