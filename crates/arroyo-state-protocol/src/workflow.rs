use crate::ProtocolPaths;
use crate::resolve::{
    EpochClaimOutcome, ParentCheckpointStatus, ResolveDecision, ResolveFailure,
    classify_epoch_record_claim, resolve_candidate,
};
use crate::state::{CheckpointState, derive_checkpoint_state};
use crate::store::{
    CreateResult, ProtocolStore, StoreError, create_json, read_json, read_protobuf,
};
use crate::types::{
    CheckpointRef, CommittedMarker, CurrentGeneration, Epoch, EpochRecord, Generation,
    GenerationManifest, ProtocolError, checkpoint_epoch, checkpoint_parent_checkpoint_ref,
};
use arroyo_rpc::grpc::rpc::CheckpointManifest;
use arroyo_types::{JobId, PipelineId};

#[derive(Debug, Clone)]
pub struct ClaimEpochRecordRequest<'a> {
    pub epoch_record_path: &'a CheckpointRef,
    pub pipeline_id: &'a PipelineId,
    pub generation: Generation,
    pub checkpoint_ref: &'a CheckpointRef,
    pub checkpoint: &'a CheckpointManifest,
    pub created_at_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommittedMarkerOutcome {
    Created,
    AlreadyCommitted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationResolution {
    Ready { checkpoint_ref: CheckpointRef },
    ReplayCommit { checkpoint_ref: CheckpointRef },
    StopOrphaned { canonical_ref: CheckpointRef },
    Failed(ResolveFailure),
}

pub async fn resolve_generation_manifest<S>(
    store: &S,
    manifest: &GenerationManifest,
    runner_generation: Generation,
) -> Result<GenerationResolution, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let Some(candidate_ref) = manifest.candidate_checkpoint_ref().cloned() else {
        return Ok(GenerationResolution::Failed(ResolveFailure::NoCandidate));
    };

    let paths = ProtocolPaths::new(manifest.pipeline_id.clone(), manifest.job_id.clone());
    let is_current_generation =
        read_json::<_, CurrentGeneration>(store, &paths.current_generation())
            .await?
            .is_some_and(|current_generation| current_generation.generation == runner_generation);

    let mut candidate_ref = candidate_ref;

    loop {
        match resolve_candidate_from_store(
            store,
            &paths,
            manifest,
            &candidate_ref,
            is_current_generation,
        )
        .await?
        {
            CandidateResolution::Done(resolution) => return Ok(resolution),
            CandidateResolution::FallbackToBase => {
                let Some(base_checkpoint_ref) = &manifest.base_checkpoint_ref else {
                    return Ok(GenerationResolution::Failed(ResolveFailure::NoCandidate));
                };

                candidate_ref = base_checkpoint_ref.clone();
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CandidateResolution {
    Done(GenerationResolution),
    FallbackToBase,
}

async fn resolve_candidate_from_store<S>(
    store: &S,
    paths: &ProtocolPaths,
    manifest: &GenerationManifest,
    candidate_ref: &CheckpointRef,
    is_current_generation: bool,
) -> Result<CandidateResolution, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let checkpoint: Option<CheckpointManifest> = read_protobuf(store, candidate_ref).await?;
    let parent_status = parent_status(store, paths, checkpoint.as_ref()).await?;
    let epoch_record = match &checkpoint {
        Some(checkpoint) => {
            read_json(store, &paths.epoch_record(checkpoint_epoch(checkpoint))).await?
        }
        None => None,
    };
    let committed_marker = match (&checkpoint, &epoch_record) {
        (Some(checkpoint), Some(_)) if checkpoint.needs_commit => {
            let path =
                paths.committed_marker(Generation(checkpoint.generation), Epoch(checkpoint.epoch));
            read_json(store, &path).await?
        }
        _ => None,
    };

    let decision = resolve_candidate(
        manifest,
        candidate_ref,
        checkpoint.as_ref(),
        epoch_record.as_ref(),
        committed_marker.as_ref(),
        parent_status,
        is_current_generation,
    )?;

    match decision {
        ResolveDecision::Ready { checkpoint_ref } => {
            Ok(CandidateResolution::Done(GenerationResolution::Ready {
                checkpoint_ref,
            }))
        }
        ResolveDecision::ReplayCommit { checkpoint_ref } => Ok(CandidateResolution::Done(
            GenerationResolution::ReplayCommit { checkpoint_ref },
        )),
        ResolveDecision::StopOrphaned { canonical_ref } => Ok(CandidateResolution::Done(
            GenerationResolution::StopOrphaned { canonical_ref },
        )),
        ResolveDecision::Failed(failure) => Ok(CandidateResolution::Done(
            GenerationResolution::Failed(failure),
        )),
        ResolveDecision::FallbackToBase => Ok(CandidateResolution::FallbackToBase),
        ResolveDecision::ClaimUnclaimed { checkpoint_ref } => {
            let checkpoint = checkpoint.expect("unclaimed checkpoints must have a manifest");
            let outcome = claim_epoch_record(
                store,
                ClaimEpochRecordRequest {
                    epoch_record_path: &paths.epoch_record(checkpoint_epoch(&checkpoint)),
                    pipeline_id: &manifest.pipeline_id,
                    generation: manifest.generation,
                    checkpoint_ref: &checkpoint_ref,
                    checkpoint: &checkpoint,
                    created_at_micros: 0,
                },
            )
            .await?;

            match outcome {
                EpochClaimOutcome::Owned if checkpoint.needs_commit => {
                    let committed_marker_path = paths.committed_marker(
                        Generation(checkpoint.generation),
                        Epoch(checkpoint.epoch),
                    );
                    let committed_marker: Option<CommittedMarker> =
                        read_json(store, &committed_marker_path).await?;

                    if committed_marker.is_some() {
                        Ok(CandidateResolution::Done(GenerationResolution::Ready {
                            checkpoint_ref,
                        }))
                    } else {
                        Ok(CandidateResolution::Done(
                            GenerationResolution::ReplayCommit { checkpoint_ref },
                        ))
                    }
                }
                EpochClaimOutcome::Owned => {
                    Ok(CandidateResolution::Done(GenerationResolution::Ready {
                        checkpoint_ref,
                    }))
                }
                EpochClaimOutcome::Orphaned { canonical_ref } => Ok(CandidateResolution::Done(
                    GenerationResolution::StopOrphaned { canonical_ref },
                )),
            }
        }
    }
}

async fn parent_status<S>(
    store: &S,
    paths: &ProtocolPaths,
    checkpoint: Option<&CheckpointManifest>,
) -> Result<ParentCheckpointStatus, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let Some(checkpoint) = checkpoint else {
        return Ok(ParentCheckpointStatus::NoParent);
    };
    let Some(parent_checkpoint_ref) = checkpoint_parent_checkpoint_ref(checkpoint)? else {
        return Ok(ParentCheckpointStatus::NoParent);
    };

    let Some(parent_checkpoint): Option<CheckpointManifest> =
        read_protobuf(store, &parent_checkpoint_ref).await?
    else {
        return Ok(ParentCheckpointStatus::NotReadyCanonical);
    };
    let parent_epoch_record: Option<EpochRecord> = read_json(
        store,
        &paths.epoch_record(checkpoint_epoch(&parent_checkpoint)),
    )
    .await?;
    let parent_committed_marker = if parent_checkpoint.needs_commit {
        let marker_path = paths.committed_marker(
            Generation(parent_checkpoint.generation),
            Epoch(parent_checkpoint.epoch),
        );
        read_json(store, &marker_path).await?
    } else {
        None
    };

    let state = derive_checkpoint_state(
        &parent_checkpoint_ref,
        Some(&parent_checkpoint),
        parent_epoch_record.as_ref(),
        parent_committed_marker.as_ref(),
    )?;

    match state {
        CheckpointState::Ready => Ok(ParentCheckpointStatus::ReadyCanonical),
        _ => Ok(ParentCheckpointStatus::NotReadyCanonical),
    }
}

pub async fn claim_epoch_record<S>(
    store: &S,
    request: ClaimEpochRecordRequest<'_>,
) -> Result<EpochClaimOutcome, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let record = EpochRecord::for_checkpoint(
        request.pipeline_id.clone(),
        request.generation,
        request.checkpoint_ref.clone(),
        request.checkpoint,
        request.created_at_micros,
    )?;

    match create_json(store, request.epoch_record_path, &record).await? {
        CreateResult::Created => Ok(EpochClaimOutcome::Owned),
        CreateResult::AlreadyExists(existing) => {
            let outcome = classify_epoch_record_claim(request.checkpoint_ref, Some(&existing));

            if outcome == EpochClaimOutcome::Owned {
                derive_checkpoint_state(
                    request.checkpoint_ref,
                    Some(request.checkpoint),
                    Some(&existing),
                    None,
                )?;
            }

            Ok(outcome)
        }
    }
}

pub async fn mark_committed<S>(
    store: &S,
    committed_marker_path: &CheckpointRef,
    marker: &CommittedMarker,
    checkpoint: &CheckpointManifest,
) -> Result<CommittedMarkerOutcome, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    validate_marker(marker, checkpoint)?;

    match create_json(store, committed_marker_path, marker).await? {
        CreateResult::Created => Ok(CommittedMarkerOutcome::Created),
        CreateResult::AlreadyExists(existing) => {
            validate_marker(&existing, checkpoint)?;

            if existing.checkpoint_ref == marker.checkpoint_ref {
                Ok(CommittedMarkerOutcome::AlreadyCommitted)
            } else {
                Err(StoreError::Protocol(ProtocolError::CommittedMarkerMismatch))
            }
        }
    }
}

fn validate_marker(
    marker: &CommittedMarker,
    checkpoint: &CheckpointManifest,
) -> Result<(), ProtocolError> {
    if *marker.job_id != checkpoint.job_id || marker.epoch != checkpoint_epoch(checkpoint) {
        return Err(ProtocolError::CommittedMarkerMismatch);
    }

    Ok(())
}
