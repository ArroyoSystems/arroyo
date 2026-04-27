use crate::ProtocolPaths;
use crate::resolve::{
    EpochClaimOutcome, ParentCheckpointStatus, ResolveDecision, ResolveFailure,
    classify_epoch_record_claim, resolve_candidate,
};
use crate::state::{CheckpointState, derive_checkpoint_state};
use crate::store::{
    CreateResult, ProtocolStore, StoreError, create_json, create_protobuf, put_json, read_json,
    read_protobuf,
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

#[derive(Debug, Clone)]
pub struct InitializeGenerationRequest {
    pub pipeline_id: PipelineId,
    pub job_id: JobId,
    pub generation: Generation,
    pub updated_at_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationRecovery {
    NoCheckpoint,
    Ready { checkpoint_ref: CheckpointRef },
    ReplayCommit { checkpoint_ref: CheckpointRef },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationInitialization {
    Initialized {
        generation_manifest: GenerationManifest,
        recovery: GenerationRecovery,
    },
    StaleGeneration,
    StopOrphaned {
        canonical_ref: CheckpointRef,
    },
    Failed(ResolveFailure),
}

#[derive(Debug, Clone)]
pub struct PublishCheckpointRequest<'a> {
    pub generation_manifest: &'a GenerationManifest,
    pub checkpoint_ref: &'a CheckpointRef,
    pub checkpoint: &'a CheckpointManifest,
    pub created_at_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointPublication {
    Ready { checkpoint_ref: CheckpointRef },
    CommitRequired { checkpoint_ref: CheckpointRef },
    StopOrphaned { canonical_ref: CheckpointRef },
    StaleGeneration,
    Failed(ResolveFailure),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CommitAuthorization {
    Authorized {
        checkpoint_ref: CheckpointRef,
        checkpoint: CheckpointManifest,
    },
    AlreadyCommitted {
        checkpoint_ref: CheckpointRef,
    },
    NoCommitNeeded {
        checkpoint_ref: CheckpointRef,
    },
    StopOrphaned {
        canonical_ref: CheckpointRef,
    },
    NotCanonical {
        checkpoint_ref: CheckpointRef,
    },
    MissingCheckpoint {
        checkpoint_ref: CheckpointRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitCompletion {
    Created { checkpoint_ref: CheckpointRef },
    AlreadyCommitted { checkpoint_ref: CheckpointRef },
    NoCommitNeeded { checkpoint_ref: CheckpointRef },
    StopOrphaned { canonical_ref: CheckpointRef },
    NotCanonical { checkpoint_ref: CheckpointRef },
    MissingCheckpoint { checkpoint_ref: CheckpointRef },
}

pub async fn initialize_generation<S>(
    store: &S,
    request: InitializeGenerationRequest,
) -> Result<GenerationInitialization, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let paths = ProtocolPaths::new(request.pipeline_id.clone(), request.job_id.clone());
    let is_current_generation =
        read_json::<_, CurrentGeneration>(store, &paths.current_generation())
            .await?
            .is_some_and(|current_generation| current_generation.generation == request.generation);

    if !is_current_generation {
        return Ok(GenerationInitialization::StaleGeneration);
    }

    let recovery = find_recovery_checkpoint(store, &paths, request.generation).await?;
    let base_checkpoint_ref = match &recovery {
        RecoverySearch::Found(recovery) => match recovery {
            GenerationRecovery::NoCheckpoint => None,
            GenerationRecovery::Ready { checkpoint_ref }
            | GenerationRecovery::ReplayCommit { checkpoint_ref } => Some(checkpoint_ref.clone()),
        },
        RecoverySearch::StopOrphaned { canonical_ref } => {
            return Ok(GenerationInitialization::StopOrphaned {
                canonical_ref: canonical_ref.clone(),
            });
        }
        RecoverySearch::Failed(failure) => {
            return Ok(GenerationInitialization::Failed(failure.clone()));
        }
    };

    let generation_manifest = GenerationManifest::new(
        request.pipeline_id,
        request.job_id,
        request.generation,
        base_checkpoint_ref,
        request.updated_at_micros,
    );

    put_json(
        store,
        &paths.generation_manifest(request.generation),
        &generation_manifest,
    )
    .await?;

    let RecoverySearch::Found(recovery) = recovery else {
        unreachable!("handled non-found recovery results above")
    };

    Ok(GenerationInitialization::Initialized {
        generation_manifest,
        recovery,
    })
}

async fn find_recovery_checkpoint<S>(
    store: &S,
    paths: &ProtocolPaths,
    generation: Generation,
) -> Result<RecoverySearch, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let Some(previous_generation) = generation.0.checked_sub(1) else {
        return Ok(RecoverySearch::Found(GenerationRecovery::NoCheckpoint));
    };

    for previous_generation in (0..=previous_generation).rev() {
        let manifest_ref = paths.generation_manifest(Generation(previous_generation));
        let Some(manifest): Option<GenerationManifest> = read_json(store, &manifest_ref).await?
        else {
            continue;
        };

        match resolve_generation_manifest(store, &manifest, generation).await? {
            GenerationResolution::Ready { checkpoint_ref } => {
                return Ok(RecoverySearch::Found(GenerationRecovery::Ready {
                    checkpoint_ref,
                }));
            }
            GenerationResolution::ReplayCommit { checkpoint_ref } => {
                return Ok(RecoverySearch::Found(GenerationRecovery::ReplayCommit {
                    checkpoint_ref,
                }));
            }
            GenerationResolution::StopOrphaned { canonical_ref } => {
                return resolve_canonical_recovery_ref(store, paths, &canonical_ref).await;
            }
            GenerationResolution::Failed(
                ResolveFailure::NoCandidate
                | ResolveFailure::InvisibleBase
                | ResolveFailure::UnclaimedBase,
            ) => continue,
            GenerationResolution::Failed(failure) => {
                return Ok(RecoverySearch::Failed(failure));
            }
        }
    }

    Ok(RecoverySearch::Found(GenerationRecovery::NoCheckpoint))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RecoverySearch {
    Found(GenerationRecovery),
    StopOrphaned { canonical_ref: CheckpointRef },
    Failed(ResolveFailure),
}

async fn resolve_canonical_recovery_ref<S>(
    store: &S,
    paths: &ProtocolPaths,
    checkpoint_ref: &CheckpointRef,
) -> Result<RecoverySearch, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let Some(checkpoint): Option<CheckpointManifest> = read_protobuf(store, checkpoint_ref).await?
    else {
        return Ok(RecoverySearch::Failed(ResolveFailure::InvisibleBase));
    };

    if parent_status(store, paths, Some(&checkpoint)).await?
        == ParentCheckpointStatus::NotReadyCanonical
    {
        return Ok(RecoverySearch::Failed(
            ResolveFailure::ParentNotReadyCanonical,
        ));
    }

    let epoch_record: Option<EpochRecord> =
        read_json(store, &paths.epoch_record(checkpoint_epoch(&checkpoint))).await?;
    let committed_marker = if checkpoint.needs_commit {
        let committed_marker_path = committed_marker_path(paths, &checkpoint);
        read_json(store, &committed_marker_path).await?
    } else {
        None
    };

    match derive_checkpoint_state(
        checkpoint_ref,
        Some(&checkpoint),
        epoch_record.as_ref(),
        committed_marker.as_ref(),
    )? {
        CheckpointState::Ready => Ok(RecoverySearch::Found(GenerationRecovery::Ready {
            checkpoint_ref: checkpoint_ref.clone(),
        })),
        CheckpointState::Committing => {
            Ok(RecoverySearch::Found(GenerationRecovery::ReplayCommit {
                checkpoint_ref: checkpoint_ref.clone(),
            }))
        }
        CheckpointState::Orphaned { canonical_ref } => {
            Ok(RecoverySearch::StopOrphaned { canonical_ref })
        }
        CheckpointState::Invisible => unreachable!("checkpoint was read above"),
        CheckpointState::Unclaimed => Ok(RecoverySearch::Failed(ResolveFailure::UnclaimedBase)),
    }
}

pub async fn prepare_commit<S>(
    store: &S,
    checkpoint_ref: &CheckpointRef,
) -> Result<CommitAuthorization, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let Some(checkpoint): Option<CheckpointManifest> = read_protobuf(store, checkpoint_ref).await?
    else {
        return Ok(CommitAuthorization::MissingCheckpoint {
            checkpoint_ref: checkpoint_ref.clone(),
        });
    };

    let paths = ProtocolPaths::new(
        PipelineId::new(&checkpoint.pipeline_id),
        checkpoint_job_id(&checkpoint),
    );
    let epoch_record: Option<EpochRecord> =
        read_json(store, &paths.epoch_record(checkpoint_epoch(&checkpoint))).await?;
    let committed_marker_path = committed_marker_path(&paths, &checkpoint);
    let committed_marker: Option<CommittedMarker> =
        read_json(store, &committed_marker_path).await?;

    match derive_checkpoint_state(
        checkpoint_ref,
        Some(&checkpoint),
        epoch_record.as_ref(),
        committed_marker.as_ref(),
    )? {
        CheckpointState::Invisible => unreachable!("checkpoint was read above"),
        CheckpointState::Unclaimed => Ok(CommitAuthorization::NotCanonical {
            checkpoint_ref: checkpoint_ref.clone(),
        }),
        CheckpointState::Orphaned { canonical_ref } => {
            Ok(CommitAuthorization::StopOrphaned { canonical_ref })
        }
        CheckpointState::Ready if checkpoint.needs_commit => {
            Ok(CommitAuthorization::AlreadyCommitted {
                checkpoint_ref: checkpoint_ref.clone(),
            })
        }
        CheckpointState::Ready => Ok(CommitAuthorization::NoCommitNeeded {
            checkpoint_ref: checkpoint_ref.clone(),
        }),
        CheckpointState::Committing => Ok(CommitAuthorization::Authorized {
            checkpoint_ref: checkpoint_ref.clone(),
            checkpoint,
        }),
    }
}

pub async fn complete_commit<S>(
    store: &S,
    checkpoint_ref: &CheckpointRef,
    writer_generation: Generation,
) -> Result<CommitCompletion, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let Some(checkpoint): Option<CheckpointManifest> = read_protobuf(store, checkpoint_ref).await?
    else {
        return Ok(CommitCompletion::MissingCheckpoint {
            checkpoint_ref: checkpoint_ref.clone(),
        });
    };

    let paths = ProtocolPaths::new(
        PipelineId::new(&checkpoint.pipeline_id),
        checkpoint_job_id(&checkpoint),
    );
    let committed_marker_path = committed_marker_path(&paths, &checkpoint);

    match prepare_commit(store, checkpoint_ref).await? {
        CommitAuthorization::Authorized { .. } => {
            let marker = CommittedMarker::new(
                PipelineId::new(&checkpoint.pipeline_id),
                checkpoint_job_id(&checkpoint),
                checkpoint_epoch(&checkpoint),
                Generation(checkpoint.generation),
                writer_generation,
                checkpoint_ref.clone(),
            );

            match mark_committed(store, &committed_marker_path, &marker, &checkpoint).await? {
                CommittedMarkerOutcome::Created => Ok(CommitCompletion::Created {
                    checkpoint_ref: checkpoint_ref.clone(),
                }),
                CommittedMarkerOutcome::AlreadyCommitted => {
                    Ok(CommitCompletion::AlreadyCommitted {
                        checkpoint_ref: checkpoint_ref.clone(),
                    })
                }
            }
        }
        CommitAuthorization::AlreadyCommitted { .. } => Ok(CommitCompletion::AlreadyCommitted {
            checkpoint_ref: checkpoint_ref.clone(),
        }),
        CommitAuthorization::NoCommitNeeded { .. } => Ok(CommitCompletion::NoCommitNeeded {
            checkpoint_ref: checkpoint_ref.clone(),
        }),
        CommitAuthorization::StopOrphaned { canonical_ref } => {
            Ok(CommitCompletion::StopOrphaned { canonical_ref })
        }
        CommitAuthorization::NotCanonical { .. } => Ok(CommitCompletion::NotCanonical {
            checkpoint_ref: checkpoint_ref.clone(),
        }),
        CommitAuthorization::MissingCheckpoint { .. } => Ok(CommitCompletion::MissingCheckpoint {
            checkpoint_ref: checkpoint_ref.clone(),
        }),
    }
}

pub async fn publish_checkpoint<S>(
    store: &S,
    request: PublishCheckpointRequest<'_>,
) -> Result<CheckpointPublication, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    validate_checkpoint_for_generation(request.generation_manifest, request.checkpoint)?;

    let paths = ProtocolPaths::new(
        request.generation_manifest.pipeline_id.clone(),
        request.generation_manifest.job_id.clone(),
    );

    let is_current_generation =
        read_json::<_, CurrentGeneration>(store, &paths.current_generation())
            .await?
            .is_some_and(|current_generation| {
                current_generation.generation == request.generation_manifest.generation
            });

    if !is_current_generation {
        return Ok(CheckpointPublication::StaleGeneration);
    }

    match create_protobuf(store, request.checkpoint_ref, request.checkpoint).await? {
        CreateResult::Created => {}
        CreateResult::AlreadyExists(existing) if existing == *request.checkpoint => {}
        CreateResult::AlreadyExists(_) => {
            return Err(StoreError::Protocol(
                ProtocolError::CheckpointManifestMismatch,
            ));
        }
    }

    if parent_status(store, &paths, Some(request.checkpoint)).await?
        == ParentCheckpointStatus::NotReadyCanonical
    {
        return Ok(CheckpointPublication::Failed(
            ResolveFailure::ParentNotReadyCanonical,
        ));
    }

    let mut updated_generation_manifest = request.generation_manifest.clone();
    updated_generation_manifest.latest_checkpoint_ref = Some(request.checkpoint_ref.clone());
    put_json(
        store,
        &paths.generation_manifest(request.generation_manifest.generation),
        &updated_generation_manifest,
    )
    .await?;

    let outcome = claim_epoch_record(
        store,
        ClaimEpochRecordRequest {
            epoch_record_path: &paths.epoch_record(checkpoint_epoch(request.checkpoint)),
            pipeline_id: &request.generation_manifest.pipeline_id,
            generation: request.generation_manifest.generation,
            checkpoint_ref: request.checkpoint_ref,
            checkpoint: request.checkpoint,
            created_at_micros: request.created_at_micros,
        },
    )
    .await?;

    match outcome {
        EpochClaimOutcome::Owned if request.checkpoint.needs_commit => {
            Ok(CheckpointPublication::CommitRequired {
                checkpoint_ref: request.checkpoint_ref.clone(),
            })
        }
        EpochClaimOutcome::Owned => Ok(CheckpointPublication::Ready {
            checkpoint_ref: request.checkpoint_ref.clone(),
        }),
        EpochClaimOutcome::Orphaned { canonical_ref } => {
            Ok(CheckpointPublication::StopOrphaned { canonical_ref })
        }
    }
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
                    generation: Generation(checkpoint.generation),
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

fn validate_checkpoint_for_generation(
    generation_manifest: &GenerationManifest,
    checkpoint: &CheckpointManifest,
) -> Result<(), ProtocolError> {
    if *generation_manifest.pipeline_id != checkpoint.pipeline_id
        || *generation_manifest.job_id != checkpoint.job_id
        || generation_manifest.generation.0 != checkpoint.generation
    {
        return Err(ProtocolError::CheckpointManifestMismatch);
    }

    Ok(())
}

fn checkpoint_job_id(checkpoint: &CheckpointManifest) -> JobId {
    JobId::new(&checkpoint.job_id)
}

fn committed_marker_path(paths: &ProtocolPaths, checkpoint: &CheckpointManifest) -> CheckpointRef {
    paths.committed_marker(
        Generation(checkpoint.generation),
        checkpoint_epoch(checkpoint),
    )
}
