//! Pure types and decision logic for Arroyo's object-store checkpoint protocol.
//!
//! Object-store I/O is kept at the edge: storage code reads protocol objects,
//! passes the observed facts into pure decision functions, and then executes the
//! returned decision.

pub mod resolve;
pub mod state;
pub mod store;
pub mod types;
pub mod workflow;

use crate::types::{CheckpointRef, Epoch, Generation};
use arroyo_types::{JobId, PipelineId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolPaths {
    pipeline_id: PipelineId,
    job_id: JobId,
}

impl ProtocolPaths {
    pub fn new(pipeline_id: PipelineId, job_id: JobId) -> Self {
        Self {
            pipeline_id,
            job_id,
        }
    }

    pub fn pipeline_id(&self) -> &PipelineId {
        &self.pipeline_id
    }

    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    pub fn current_generation(&self) -> CheckpointRef {
        self.path("current-generation.json")
    }

    pub fn generation_manifest(&self, generation: Generation) -> CheckpointRef {
        self.path(format!("generations/{generation}/generation-manifest.json"))
    }

    pub fn checkpoint_dir(&self, generation: Generation, epoch: Epoch) -> CheckpointRef {
        self.path(format!(
            "generations/{generation}/checkpoints/checkpoint-{epoch:07}"
        ))
    }

    pub fn checkpoint_manifest(&self, generation: Generation, epoch: Epoch) -> CheckpointRef {
        self.path(format!(
            "generations/{generation}/checkpoints/checkpoint-{epoch:07}/checkpoint-manifest.pb"
        ))
    }

    pub fn committed_marker(&self, generation: Generation, epoch: Epoch) -> CheckpointRef {
        self.path(format!(
            "generations/{generation}/checkpoints/checkpoint-{epoch:07}/committed.json"
        ))
    }

    pub fn epoch_record(&self, epoch: Epoch) -> CheckpointRef {
        self.path(format!("epochs/epoch-{epoch:07}.record"))
    }

    fn path(&self, suffix: impl AsRef<str>) -> CheckpointRef {
        CheckpointRef::from_validated(format!(
            "{}/{}/{}",
            self.pipeline_id,
            self.job_id,
            suffix.as_ref()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolve::{
        EpochClaimOutcome, ParentCheckpointStatus, ResolveDecision, ResolveFailure,
        classify_epoch_record_claim, resolve_candidate,
    };
    use crate::state::{CheckpointState, derive_checkpoint_state};
    use crate::store::tests::MemoryProtocolStore;
    use crate::store::{
        CreateResult, StoreError, create_json, put_json, put_protobuf, read_json, read_protobuf,
    };
    use crate::types::{
        CommittedMarker, CurrentGeneration, EpochRecord, GenerationManifest, ProtocolError,
    };
    use crate::workflow::{
        CheckpointPublication, ClaimEpochRecordRequest, CommitAuthorization, CommitCompletion,
        CommittedMarkerOutcome, GenerationInitialization, GenerationRecovery, GenerationResolution,
        InitializeGenerationRequest, PublishCheckpointRequest, claim_epoch_record, complete_commit,
        initialize_generation, mark_committed, prepare_commit, publish_checkpoint,
        resolve_generation_manifest,
    };
    use arroyo_rpc::grpc::rpc::CheckpointManifest;
    use arroyo_types::{JobId, PipelineId};

    fn checkpoint_ref(path: &str) -> CheckpointRef {
        CheckpointRef::new(path).unwrap()
    }

    fn checkpoint(
        epoch: u64,
        parent_checkpoint_ref: Option<CheckpointRef>,
        needs_commit: bool,
    ) -> CheckpointManifest {
        checkpoint_for_generation(Generation(1), epoch, parent_checkpoint_ref, needs_commit)
    }

    fn checkpoint_for_generation(
        generation: Generation,
        epoch: u64,
        parent_checkpoint_ref: Option<CheckpointRef>,
        needs_commit: bool,
    ) -> CheckpointManifest {
        CheckpointManifest {
            pipeline_id: "P".to_string(),
            job_id: "J".to_string(),
            generation: generation.0,
            epoch,
            min_epoch: epoch,
            start_time: 0,
            finish_time: 0,
            needs_commit,
            operators: vec![],
            parent_checkpoint_ref: parent_checkpoint_ref
                .map(|checkpoint_ref| checkpoint_ref.to_string()),
        }
    }

    fn epoch_record(checkpoint_ref: CheckpointRef, checkpoint: &CheckpointManifest) -> EpochRecord {
        EpochRecord::for_checkpoint(
            PipelineId::new("P"),
            Generation(1),
            checkpoint_ref,
            checkpoint,
            0,
        )
        .unwrap()
    }

    fn committed_marker(checkpoint_ref: CheckpointRef, epoch: u64) -> CommittedMarker {
        CommittedMarker::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Epoch(epoch),
            Generation(1),
            Generation(2),
            checkpoint_ref,
        )
    }

    fn generation_manifest(
        base_checkpoint_ref: Option<CheckpointRef>,
        latest_checkpoint_ref: Option<CheckpointRef>,
    ) -> GenerationManifest {
        generation_manifest_for_generation(
            Generation(1),
            base_checkpoint_ref,
            latest_checkpoint_ref,
        )
    }

    fn generation_manifest_for_generation(
        generation: Generation,
        base_checkpoint_ref: Option<CheckpointRef>,
        latest_checkpoint_ref: Option<CheckpointRef>,
    ) -> GenerationManifest {
        let mut manifest = GenerationManifest::new(
            PipelineId::new("P"),
            JobId::new("J"),
            generation,
            base_checkpoint_ref,
            0,
        );
        manifest.latest_checkpoint_ref = latest_checkpoint_ref;
        manifest
    }

    async fn write_current_generation(
        store: &MemoryProtocolStore,
        paths: &ProtocolPaths,
        generation: Generation,
    ) {
        let current_generation = CurrentGeneration::new(
            PipelineId::new("P"),
            JobId::new("J"),
            generation,
            paths.generation_manifest(generation),
            0,
        );

        put_json(store, &paths.current_generation(), &current_generation)
            .await
            .unwrap();
    }

    async fn write_canonical_checkpoint(
        store: &MemoryProtocolStore,
        paths: &ProtocolPaths,
        checkpoint_ref: &CheckpointRef,
        checkpoint: &CheckpointManifest,
    ) {
        put_protobuf(store, checkpoint_ref, checkpoint)
            .await
            .unwrap();
        put_json(
            store,
            &paths.epoch_record(Epoch(checkpoint.epoch)),
            &epoch_record(checkpoint_ref.clone(), checkpoint),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn initialize_generation_without_prior_checkpoint_writes_empty_manifest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(1),
                updated_at_micros: 123,
            },
        )
        .await
        .unwrap();

        let expected_manifest = GenerationManifest::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(1),
            None,
            123,
        );
        assert_eq!(
            initialization,
            GenerationInitialization::Initialized {
                generation_manifest: expected_manifest.clone(),
                recovery: GenerationRecovery::NoCheckpoint
            }
        );

        let written_manifest: GenerationManifest =
            read_json(&store, &paths.generation_manifest(Generation(1)))
                .await
                .unwrap()
                .expect("new generation manifest should be written");
        assert_eq!(written_manifest, expected_manifest);
    }

    #[tokio::test]
    async fn initialize_generation_restores_previous_ready_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(2)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint_for_generation(Generation(1), 1, None, false);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;
        let previous_manifest =
            generation_manifest_for_generation(Generation(1), None, Some(checkpoint_ref.clone()));
        put_json(
            &store,
            &paths.generation_manifest(Generation(1)),
            &previous_manifest,
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(2),
                updated_at_micros: 456,
            },
        )
        .await
        .unwrap();

        let expected_manifest = GenerationManifest::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(2),
            Some(checkpoint_ref.clone()),
            456,
        );
        assert_eq!(
            initialization,
            GenerationInitialization::Initialized {
                generation_manifest: expected_manifest.clone(),
                recovery: GenerationRecovery::Ready {
                    checkpoint_ref: checkpoint_ref.clone()
                }
            }
        );

        let written_manifest: GenerationManifest =
            read_json(&store, &paths.generation_manifest(Generation(2)))
                .await
                .unwrap()
                .expect("new generation manifest should be written");
        assert_eq!(written_manifest, expected_manifest);
    }

    #[tokio::test]
    async fn initialize_generation_restores_previous_checkpoint_requiring_commit_replay() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(2)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint_for_generation(Generation(1), 1, None, true);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;
        let previous_manifest =
            generation_manifest_for_generation(Generation(1), None, Some(checkpoint_ref.clone()));
        put_json(
            &store,
            &paths.generation_manifest(Generation(1)),
            &previous_manifest,
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(2),
                updated_at_micros: 456,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            initialization,
            GenerationInitialization::Initialized {
                generation_manifest: GenerationManifest::new(
                    PipelineId::new("P"),
                    JobId::new("J"),
                    Generation(2),
                    Some(checkpoint_ref.clone()),
                    456,
                ),
                recovery: GenerationRecovery::ReplayCommit { checkpoint_ref }
            }
        );
    }

    #[tokio::test]
    async fn initialize_generation_skips_missing_previous_manifest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(3)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint_for_generation(Generation(1), 1, None, false);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;
        put_json(
            &store,
            &paths.generation_manifest(Generation(1)),
            &generation_manifest_for_generation(Generation(1), None, Some(checkpoint_ref.clone())),
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(3),
                updated_at_micros: 789,
            },
        )
        .await
        .unwrap();

        assert!(matches!(
            initialization,
            GenerationInitialization::Initialized {
                recovery: GenerationRecovery::Ready { .. },
                ..
            }
        ));
        let written_manifest: GenerationManifest =
            read_json(&store, &paths.generation_manifest(Generation(3)))
                .await
                .unwrap()
                .expect("new generation manifest should be written");
        assert_eq!(written_manifest.base_checkpoint_ref, Some(checkpoint_ref));
    }

    #[tokio::test]
    async fn initialize_generation_claims_unclaimed_previous_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(2)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint_for_generation(Generation(1), 1, None, false);
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();
        put_json(
            &store,
            &paths.generation_manifest(Generation(1)),
            &generation_manifest_for_generation(Generation(1), None, Some(checkpoint_ref.clone())),
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(2),
                updated_at_micros: 456,
            },
        )
        .await
        .unwrap();

        assert!(matches!(
            initialization,
            GenerationInitialization::Initialized {
                recovery: GenerationRecovery::Ready { .. },
                ..
            }
        ));
        let record: EpochRecord = read_json(&store, &paths.epoch_record(Epoch(1)))
            .await
            .unwrap()
            .expect("unclaimed checkpoint should be claimed during initialization");
        assert_eq!(record.checkpoint_ref, checkpoint_ref);
        assert_eq!(record.generation, Generation(1));
    }

    #[tokio::test]
    async fn initialize_generation_recovers_canonical_checkpoint_from_orphaned_previous_manifest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(3)).await;

        let winner_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let winner_checkpoint = checkpoint_for_generation(Generation(1), 1, None, false);
        let loser_checkpoint = checkpoint_for_generation(Generation(2), 1, None, false);
        put_protobuf(&store, &winner_ref, &winner_checkpoint)
            .await
            .unwrap();
        put_protobuf(&store, &loser_ref, &loser_checkpoint)
            .await
            .unwrap();
        put_json(
            &store,
            &paths.epoch_record(Epoch(1)),
            &epoch_record(winner_ref.clone(), &winner_checkpoint),
        )
        .await
        .unwrap();
        put_json(
            &store,
            &paths.generation_manifest(Generation(2)),
            &generation_manifest_for_generation(Generation(2), None, Some(loser_ref)),
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(3),
                updated_at_micros: 456,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            initialization,
            GenerationInitialization::Initialized {
                generation_manifest: GenerationManifest::new(
                    PipelineId::new("P"),
                    JobId::new("J"),
                    Generation(3),
                    Some(winner_ref.clone()),
                    456,
                ),
                recovery: GenerationRecovery::Ready {
                    checkpoint_ref: winner_ref.clone()
                }
            }
        );
        let written_manifest: GenerationManifest =
            read_json(&store, &paths.generation_manifest(Generation(3)))
                .await
                .unwrap()
                .expect("replacement generation manifest should be written");
        assert_eq!(written_manifest.base_checkpoint_ref, Some(winner_ref));
    }

    #[tokio::test]
    async fn initialize_generation_replays_commit_for_canonical_checkpoint_from_orphaned_manifest()
    {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(3)).await;

        let winner_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let winner_checkpoint = checkpoint_for_generation(Generation(1), 1, None, true);
        let loser_checkpoint = checkpoint_for_generation(Generation(2), 1, None, true);
        put_protobuf(&store, &winner_ref, &winner_checkpoint)
            .await
            .unwrap();
        put_protobuf(&store, &loser_ref, &loser_checkpoint)
            .await
            .unwrap();
        put_json(
            &store,
            &paths.epoch_record(Epoch(1)),
            &epoch_record(winner_ref.clone(), &winner_checkpoint),
        )
        .await
        .unwrap();
        put_json(
            &store,
            &paths.generation_manifest(Generation(2)),
            &generation_manifest_for_generation(Generation(2), None, Some(loser_ref)),
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(3),
                updated_at_micros: 456,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            initialization,
            GenerationInitialization::Initialized {
                generation_manifest: GenerationManifest::new(
                    PipelineId::new("P"),
                    JobId::new("J"),
                    Generation(3),
                    Some(winner_ref.clone()),
                    456,
                ),
                recovery: GenerationRecovery::ReplayCommit {
                    checkpoint_ref: winner_ref
                }
            }
        );
    }

    #[tokio::test]
    async fn initialize_generation_exits_when_stale() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(3)).await;

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(2),
                updated_at_micros: 456,
            },
        )
        .await
        .unwrap();

        assert_eq!(initialization, GenerationInitialization::StaleGeneration);
        assert!(
            read_json::<_, GenerationManifest>(&store, &paths.generation_manifest(Generation(2)))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn non_committing_checkpoint_is_unclaimed_until_epoch_record_exists() {
        let checkpoint_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let checkpoint = checkpoint(1, None, false);

        let state =
            derive_checkpoint_state(&checkpoint_ref, Some(&checkpoint), None, None).unwrap();

        assert_eq!(state, CheckpointState::Unclaimed);
        assert!(!state.is_ready());
    }

    #[test]
    fn epoch_record_makes_non_committing_checkpoint_ready() {
        let checkpoint_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let checkpoint = checkpoint(1, None, false);
        let record = epoch_record(checkpoint_ref.clone(), &checkpoint);

        let state =
            derive_checkpoint_state(&checkpoint_ref, Some(&checkpoint), Some(&record), None)
                .unwrap();

        assert_eq!(state, CheckpointState::Ready);
        assert!(state.is_canonical());
    }

    #[test]
    fn orphaning_applies_to_non_committing_checkpoints() {
        let loser_ref = checkpoint_ref(
            "P/J/generations/2/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let winner_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let checkpoint = checkpoint(1, None, false);
        let record = epoch_record(winner_ref.clone(), &checkpoint);

        let state =
            derive_checkpoint_state(&loser_ref, Some(&checkpoint), Some(&record), None).unwrap();

        assert_eq!(
            state,
            CheckpointState::Orphaned {
                canonical_ref: winner_ref
            }
        );
    }

    #[test]
    fn committing_checkpoint_requires_committed_marker_to_be_ready() {
        let checkpoint_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let checkpoint = checkpoint(1, None, true);
        let record = epoch_record(checkpoint_ref.clone(), &checkpoint);

        let state =
            derive_checkpoint_state(&checkpoint_ref, Some(&checkpoint), Some(&record), None)
                .unwrap();
        assert_eq!(state, CheckpointState::Committing);
        assert!(state.requires_commit_replay());

        let marker = committed_marker(checkpoint_ref.clone(), 1);
        let state = derive_checkpoint_state(
            &checkpoint_ref,
            Some(&checkpoint),
            Some(&record),
            Some(&marker),
        )
        .unwrap();
        assert_eq!(state, CheckpointState::Ready);
    }

    #[test]
    fn resolve_current_generation_claims_unclaimed_candidate() {
        let checkpoint_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let manifest = generation_manifest(None, Some(checkpoint_ref.clone()));
        let checkpoint = checkpoint(1, None, false);

        let decision = resolve_candidate(
            &manifest,
            &checkpoint_ref,
            Some(&checkpoint),
            None,
            None,
            ParentCheckpointStatus::NoParent,
            true,
        )
        .unwrap();

        assert_eq!(decision, ResolveDecision::ClaimUnclaimed { checkpoint_ref });
    }

    #[test]
    fn resolve_stale_generation_falls_back_from_unclaimed_latest() {
        let base_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let latest_ref = checkpoint_ref(
            "P/J/generations/2/checkpoints/checkpoint-0000002/checkpoint-manifest.pb",
        );
        let manifest = generation_manifest(Some(base_ref), Some(latest_ref.clone()));
        let checkpoint = checkpoint(2, None, false);

        let decision = resolve_candidate(
            &manifest,
            &latest_ref,
            Some(&checkpoint),
            None,
            None,
            ParentCheckpointStatus::NoParent,
            false,
        )
        .unwrap();

        assert_eq!(decision, ResolveDecision::FallbackToBase);
    }

    #[test]
    fn resolve_orphaned_candidate_stops_generation() {
        let loser_ref = checkpoint_ref(
            "P/J/generations/2/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let winner_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let manifest = generation_manifest(None, Some(loser_ref.clone()));
        let checkpoint = checkpoint(1, None, false);
        let record = epoch_record(winner_ref.clone(), &checkpoint);

        let decision = resolve_candidate(
            &manifest,
            &loser_ref,
            Some(&checkpoint),
            Some(&record),
            None,
            ParentCheckpointStatus::NoParent,
            true,
        )
        .unwrap();

        assert_eq!(
            decision,
            ResolveDecision::StopOrphaned {
                canonical_ref: winner_ref
            }
        );
    }

    #[test]
    fn resolve_canonical_uncommitted_checkpoint_requires_replay() {
        let checkpoint_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let manifest = generation_manifest(None, Some(checkpoint_ref.clone()));
        let checkpoint = checkpoint(1, None, true);
        let record = epoch_record(checkpoint_ref.clone(), &checkpoint);

        let decision = resolve_candidate(
            &manifest,
            &checkpoint_ref,
            Some(&checkpoint),
            Some(&record),
            None,
            ParentCheckpointStatus::NoParent,
            true,
        )
        .unwrap();

        assert_eq!(decision, ResolveDecision::ReplayCommit { checkpoint_ref });
    }

    #[test]
    fn resolve_rejects_child_when_parent_is_not_ready_canonical() {
        let parent_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let child_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000002/checkpoint-manifest.pb",
        );
        let manifest = generation_manifest(None, Some(child_ref.clone()));
        let child = checkpoint(2, Some(parent_ref), false);

        let decision = resolve_candidate(
            &manifest,
            &child_ref,
            Some(&child),
            None,
            None,
            ParentCheckpointStatus::NotReadyCanonical,
            true,
        )
        .unwrap();

        assert_eq!(
            decision,
            ResolveDecision::Failed(ResolveFailure::ParentNotReadyCanonical)
        );
    }

    #[test]
    fn classify_claim_treats_existing_same_record_as_success() {
        let checkpoint_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let checkpoint = checkpoint(1, None, false);
        let record = epoch_record(checkpoint_ref.clone(), &checkpoint);

        assert_eq!(
            classify_epoch_record_claim(&checkpoint_ref, None),
            EpochClaimOutcome::Owned
        );
        assert_eq!(
            classify_epoch_record_claim(&checkpoint_ref, Some(&record)),
            EpochClaimOutcome::Owned
        );
    }

    #[test]
    fn classify_claim_orphans_different_existing_record() {
        let loser_ref = checkpoint_ref(
            "P/J/generations/2/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let winner_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let checkpoint = checkpoint(1, None, false);
        let record = epoch_record(winner_ref.clone(), &checkpoint);

        assert_eq!(
            classify_epoch_record_claim(&loser_ref, Some(&record)),
            EpochClaimOutcome::Orphaned {
                canonical_ref: winner_ref
            }
        );
    }

    #[tokio::test]
    async fn claim_epoch_record_creates_canonical_record() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let epoch_record_path = paths.epoch_record(Epoch(1));

        let outcome = claim_epoch_record(
            &store,
            ClaimEpochRecordRequest {
                epoch_record_path: &epoch_record_path,
                pipeline_id: &PipelineId::new("P"),
                generation: Generation(1),
                checkpoint_ref: &checkpoint_ref,
                checkpoint: &checkpoint,
                created_at_micros: 123,
            },
        )
        .await
        .unwrap();

        assert_eq!(outcome, EpochClaimOutcome::Owned);

        let record: EpochRecord = read_json(&store, &epoch_record_path)
            .await
            .unwrap()
            .expect("epoch record should have been written");
        assert_eq!(record.checkpoint_ref, checkpoint_ref);
        assert_eq!(record.epoch, Epoch(1));
    }

    #[tokio::test]
    async fn claim_epoch_record_treats_existing_same_record_as_owned() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let epoch_record_path = paths.epoch_record(Epoch(1));
        let request = ClaimEpochRecordRequest {
            epoch_record_path: &epoch_record_path,
            pipeline_id: &PipelineId::new("P"),
            generation: Generation(1),
            checkpoint_ref: &checkpoint_ref,
            checkpoint: &checkpoint,
            created_at_micros: 123,
        };

        claim_epoch_record(&store, request.clone()).await.unwrap();
        let outcome = claim_epoch_record(&store, request).await.unwrap();

        assert_eq!(outcome, EpochClaimOutcome::Owned);
    }

    #[tokio::test]
    async fn claim_epoch_record_orphans_different_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let winner_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let epoch_record_path = paths.epoch_record(Epoch(1));

        claim_epoch_record(
            &store,
            ClaimEpochRecordRequest {
                epoch_record_path: &epoch_record_path,
                pipeline_id: &PipelineId::new("P"),
                generation: Generation(1),
                checkpoint_ref: &winner_ref,
                checkpoint: &checkpoint,
                created_at_micros: 123,
            },
        )
        .await
        .unwrap();

        let outcome = claim_epoch_record(
            &store,
            ClaimEpochRecordRequest {
                epoch_record_path: &epoch_record_path,
                pipeline_id: &PipelineId::new("P"),
                generation: Generation(2),
                checkpoint_ref: &loser_ref,
                checkpoint: &checkpoint,
                created_at_micros: 124,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            EpochClaimOutcome::Orphaned {
                canonical_ref: winner_ref
            }
        );
    }

    #[tokio::test]
    async fn mark_committed_is_idempotent_for_same_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let committed_marker_path = paths.committed_marker(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        let marker = committed_marker(checkpoint_ref, 1);

        let outcome = mark_committed(&store, &committed_marker_path, &marker, &checkpoint)
            .await
            .unwrap();
        assert_eq!(outcome, CommittedMarkerOutcome::Created);

        let outcome = mark_committed(&store, &committed_marker_path, &marker, &checkpoint)
            .await
            .unwrap();
        assert_eq!(outcome, CommittedMarkerOutcome::AlreadyCommitted);
    }

    #[tokio::test]
    async fn mark_committed_rejects_existing_marker_for_different_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let winner_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let committed_marker_path = paths.committed_marker(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        let winner_marker = committed_marker(winner_ref, 1);
        let loser_marker = committed_marker(loser_ref, 1);

        mark_committed(&store, &committed_marker_path, &winner_marker, &checkpoint)
            .await
            .unwrap();

        let err = mark_committed(&store, &committed_marker_path, &loser_marker, &checkpoint)
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::Protocol(ProtocolError::CommittedMarkerMismatch)
        ));
    }

    #[tokio::test]
    async fn prepare_commit_authorizes_canonical_uncommitted_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;

        let authorization = prepare_commit(&store, &checkpoint_ref).await.unwrap();

        assert_eq!(
            authorization,
            CommitAuthorization::Authorized {
                checkpoint_ref,
                checkpoint
            }
        );
    }

    #[tokio::test]
    async fn prepare_commit_reports_already_committed() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;
        put_json(
            &store,
            &paths.committed_marker(Generation(1), Epoch(1)),
            &committed_marker(checkpoint_ref.clone(), 1),
        )
        .await
        .unwrap();

        let authorization = prepare_commit(&store, &checkpoint_ref).await.unwrap();

        assert_eq!(
            authorization,
            CommitAuthorization::AlreadyCommitted { checkpoint_ref }
        );
    }

    #[tokio::test]
    async fn prepare_commit_rejects_unclaimed_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();

        let authorization = prepare_commit(&store, &checkpoint_ref).await.unwrap();

        assert_eq!(
            authorization,
            CommitAuthorization::NotCanonical { checkpoint_ref }
        );
    }

    #[tokio::test]
    async fn prepare_commit_stops_orphaned_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let winner_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        put_protobuf(&store, &loser_ref, &checkpoint).await.unwrap();
        put_json(
            &store,
            &paths.epoch_record(Epoch(1)),
            &epoch_record(winner_ref.clone(), &checkpoint),
        )
        .await
        .unwrap();

        let authorization = prepare_commit(&store, &loser_ref).await.unwrap();

        assert_eq!(
            authorization,
            CommitAuthorization::StopOrphaned {
                canonical_ref: winner_ref
            }
        );
    }

    #[tokio::test]
    async fn prepare_commit_skips_non_committing_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;

        let authorization = prepare_commit(&store, &checkpoint_ref).await.unwrap();

        assert_eq!(
            authorization,
            CommitAuthorization::NoCommitNeeded { checkpoint_ref }
        );
    }

    #[tokio::test]
    async fn complete_commit_writes_committed_marker() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;

        let completion = complete_commit(&store, &checkpoint_ref, Generation(2))
            .await
            .unwrap();

        assert_eq!(
            completion,
            CommitCompletion::Created {
                checkpoint_ref: checkpoint_ref.clone()
            }
        );
        let marker: CommittedMarker =
            read_json(&store, &paths.committed_marker(Generation(1), Epoch(1)))
                .await
                .unwrap()
                .expect("committed marker should be written");
        assert_eq!(marker.checkpoint_ref, checkpoint_ref);
        assert_eq!(marker.writer_generation, Generation(2));
    }

    #[tokio::test]
    async fn complete_commit_is_idempotent() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;

        complete_commit(&store, &checkpoint_ref, Generation(2))
            .await
            .unwrap();
        let completion = complete_commit(&store, &checkpoint_ref, Generation(3))
            .await
            .unwrap();

        assert_eq!(
            completion,
            CommitCompletion::AlreadyCommitted { checkpoint_ref }
        );
    }

    #[tokio::test]
    async fn complete_commit_does_not_mark_unclaimed_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();

        let completion = complete_commit(&store, &checkpoint_ref, Generation(2))
            .await
            .unwrap();

        assert_eq!(
            completion,
            CommitCompletion::NotCanonical {
                checkpoint_ref: checkpoint_ref.clone()
            }
        );
        assert!(
            read_json::<_, CommittedMarker>(
                &store,
                &paths.committed_marker(Generation(1), Epoch(1)),
            )
            .await
            .unwrap()
            .is_none()
        );
    }

    #[tokio::test]
    async fn storage_provider_store_round_trips_conditional_json_create() {
        let temp_dir = std::env::temp_dir().join(format!(
            "arroyo-state-protocol-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let storage =
            arroyo_storage::StorageProvider::for_url(&format!("file://{}", temp_dir.display()))
                .await
                .unwrap();
        let store = storage;
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let record = epoch_record(checkpoint_ref, &checkpoint);
        let epoch_record_path = paths.epoch_record(Epoch(1));

        let created = create_json(&store, &epoch_record_path, &record)
            .await
            .unwrap();
        assert_eq!(created, CreateResult::Created);

        let existing = create_json(&store, &epoch_record_path, &record)
            .await
            .unwrap();
        assert_eq!(existing, CreateResult::AlreadyExists(record));

        let _ = tokio::fs::remove_dir_all(temp_dir).await;
    }

    #[tokio::test]
    async fn resolve_generation_manifest_claims_unclaimed_current_latest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();

        let manifest = generation_manifest(None, Some(checkpoint_ref.clone()));

        let resolution = resolve_generation_manifest(&store, &manifest, Generation(1))
            .await
            .unwrap();

        assert_eq!(resolution, GenerationResolution::Ready { checkpoint_ref });
        assert!(
            read_json::<_, EpochRecord>(&store, &paths.epoch_record(Epoch(1)))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn resolve_generation_manifest_claims_unclaimed_commit_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();

        let manifest = generation_manifest(None, Some(checkpoint_ref.clone()));

        let resolution = resolve_generation_manifest(&store, &manifest, Generation(1))
            .await
            .unwrap();

        assert_eq!(
            resolution,
            GenerationResolution::ReplayCommit { checkpoint_ref }
        );
    }

    #[tokio::test]
    async fn resolve_generation_manifest_falls_back_to_base_for_stale_unclaimed_latest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(3)).await;

        let base_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let base = checkpoint(1, None, false);
        put_protobuf(&store, &base_ref, &base).await.unwrap();
        put_json(
            &store,
            &paths.epoch_record(Epoch(1)),
            &epoch_record(base_ref.clone(), &base),
        )
        .await
        .unwrap();

        let latest_ref = paths.checkpoint_manifest(Generation(2), Epoch(2));
        let latest = checkpoint(2, Some(base_ref.clone()), false);
        put_protobuf(&store, &latest_ref, &latest).await.unwrap();

        let manifest = generation_manifest(Some(base_ref.clone()), Some(latest_ref));

        let resolution = resolve_generation_manifest(&store, &manifest, Generation(2))
            .await
            .unwrap();

        assert_eq!(
            resolution,
            GenerationResolution::Ready {
                checkpoint_ref: base_ref
            }
        );
    }

    #[tokio::test]
    async fn resolve_generation_manifest_stops_on_orphaned_latest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(2)).await;

        let winner_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        put_protobuf(&store, &loser_ref, &checkpoint).await.unwrap();
        put_json(
            &store,
            &paths.epoch_record(Epoch(1)),
            &epoch_record(winner_ref.clone(), &checkpoint),
        )
        .await
        .unwrap();

        let manifest = generation_manifest(None, Some(loser_ref));

        let resolution = resolve_generation_manifest(&store, &manifest, Generation(2))
            .await
            .unwrap();

        assert_eq!(
            resolution,
            GenerationResolution::StopOrphaned {
                canonical_ref: winner_ref
            }
        );
    }

    #[tokio::test]
    async fn resolve_generation_manifest_rejects_checkpoint_with_unready_parent() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let parent_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let child_ref = paths.checkpoint_manifest(Generation(1), Epoch(2));
        let child = checkpoint(2, Some(parent_ref), false);
        put_protobuf(&store, &child_ref, &child).await.unwrap();

        let manifest = generation_manifest(None, Some(child_ref));

        let resolution = resolve_generation_manifest(&store, &manifest, Generation(1))
            .await
            .unwrap();

        assert_eq!(
            resolution,
            GenerationResolution::Failed(ResolveFailure::ParentNotReadyCanonical)
        );
    }

    #[tokio::test]
    async fn publish_non_committing_checkpoint_writes_protocol_objects() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let manifest = generation_manifest(None, None);

        let publication = publish_checkpoint(
            &store,
            PublishCheckpointRequest {
                generation_manifest: &manifest,
                checkpoint_ref: &checkpoint_ref,
                checkpoint: &checkpoint,
                created_at_micros: 42,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            publication,
            CheckpointPublication::Ready {
                checkpoint_ref: checkpoint_ref.clone()
            }
        );

        let written_checkpoint: CheckpointManifest = read_protobuf(&store, &checkpoint_ref)
            .await
            .unwrap()
            .expect("checkpoint manifest should be written");
        assert_eq!(written_checkpoint, checkpoint);

        let written_generation_manifest: GenerationManifest =
            read_json(&store, &paths.generation_manifest(Generation(1)))
                .await
                .unwrap()
                .expect("generation manifest should be updated");
        assert_eq!(
            written_generation_manifest.latest_checkpoint_ref,
            Some(checkpoint_ref.clone())
        );

        let record: EpochRecord = read_json(&store, &paths.epoch_record(Epoch(1)))
            .await
            .unwrap()
            .expect("epoch record should be written");
        assert_eq!(record.checkpoint_ref, checkpoint_ref);
    }

    #[tokio::test]
    async fn publish_committing_checkpoint_requires_commit() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        let manifest = generation_manifest(None, None);

        let publication = publish_checkpoint(
            &store,
            PublishCheckpointRequest {
                generation_manifest: &manifest,
                checkpoint_ref: &checkpoint_ref,
                checkpoint: &checkpoint,
                created_at_micros: 42,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            publication,
            CheckpointPublication::CommitRequired { checkpoint_ref }
        );
    }

    #[tokio::test]
    async fn publish_checkpoint_exits_when_generation_is_stale() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(2)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let manifest = generation_manifest(None, None);

        let publication = publish_checkpoint(
            &store,
            PublishCheckpointRequest {
                generation_manifest: &manifest,
                checkpoint_ref: &checkpoint_ref,
                checkpoint: &checkpoint,
                created_at_micros: 42,
            },
        )
        .await
        .unwrap();

        assert_eq!(publication, CheckpointPublication::StaleGeneration);
        assert!(
            read_protobuf::<_, CheckpointManifest>(&store, &checkpoint_ref)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn publish_checkpoint_is_idempotent_for_existing_same_manifest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let manifest = generation_manifest(None, None);
        let request = PublishCheckpointRequest {
            generation_manifest: &manifest,
            checkpoint_ref: &checkpoint_ref,
            checkpoint: &checkpoint,
            created_at_micros: 42,
        };

        publish_checkpoint(&store, request.clone()).await.unwrap();
        let publication = publish_checkpoint(&store, request).await.unwrap();

        assert_eq!(publication, CheckpointPublication::Ready { checkpoint_ref });
    }

    #[tokio::test]
    async fn publish_checkpoint_stops_when_epoch_record_points_elsewhere() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let winner_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        put_json(
            &store,
            &paths.epoch_record(Epoch(1)),
            &epoch_record(winner_ref.clone(), &checkpoint),
        )
        .await
        .unwrap();

        let manifest = generation_manifest(None, None);
        let publication = publish_checkpoint(
            &store,
            PublishCheckpointRequest {
                generation_manifest: &manifest,
                checkpoint_ref: &loser_ref,
                checkpoint: &checkpoint,
                created_at_micros: 42,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            publication,
            CheckpointPublication::StopOrphaned {
                canonical_ref: winner_ref
            }
        );
    }

    #[tokio::test]
    async fn publish_checkpoint_rejects_unready_parent() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let parent_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let child_ref = paths.checkpoint_manifest(Generation(1), Epoch(2));
        let child = checkpoint(2, Some(parent_ref), false);
        let manifest = generation_manifest(None, None);

        let publication = publish_checkpoint(
            &store,
            PublishCheckpointRequest {
                generation_manifest: &manifest,
                checkpoint_ref: &child_ref,
                checkpoint: &child,
                created_at_micros: 42,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            publication,
            CheckpointPublication::Failed(ResolveFailure::ParentNotReadyCanonical)
        );
        assert!(
            read_json::<_, EpochRecord>(&store, &paths.epoch_record(Epoch(2)))
                .await
                .unwrap()
                .is_none()
        );
    }
}
