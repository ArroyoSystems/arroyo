//! Pure types and decision logic for Arroyo's object-store checkpoint protocol.
//!
//! This crate intentionally keeps object-store I/O out of the core protocol
//! logic. Callers read protocol objects, pass the observed facts into these
//! functions, and then execute the returned decision.

mod paths;
mod resolve;
mod state;
mod types;

pub use paths::ProtocolPaths;
pub use resolve::{
    EpochClaimOutcome, ParentCheckpointStatus, ResolveDecision, ResolveFailure,
    classify_epoch_record_claim, resolve_candidate,
};
pub use state::{CheckpointState, derive_checkpoint_state};
pub use types::{
    CheckpointManifest, CheckpointRef, CommittedMarker, CurrentGeneration, Epoch, EpochRecord,
    Generation, GenerationManifest, PROTOCOL_VERSION, ProtocolError,
};

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint_ref(path: &str) -> CheckpointRef {
        CheckpointRef::new(path).unwrap()
    }

    fn checkpoint(
        epoch: u64,
        parent_checkpoint_ref: Option<CheckpointRef>,
        needs_commit: bool,
    ) -> CheckpointManifest {
        CheckpointManifest::new(
            "J",
            Epoch(epoch),
            Epoch(epoch),
            parent_checkpoint_ref,
            needs_commit,
        )
    }

    fn epoch_record(checkpoint_ref: CheckpointRef, checkpoint: &CheckpointManifest) -> EpochRecord {
        EpochRecord::for_checkpoint("P", Generation(1), checkpoint_ref, checkpoint, 0)
    }

    fn committed_marker(checkpoint_ref: CheckpointRef, epoch: u64) -> CommittedMarker {
        CommittedMarker::new(
            "P",
            "J",
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
        let mut manifest = GenerationManifest::new("P", "J", Generation(1), base_checkpoint_ref, 0);
        manifest.latest_checkpoint_ref = latest_checkpoint_ref;
        manifest
    }

    #[test]
    fn protocol_paths_are_canonical_relative_paths() {
        let paths = ProtocolPaths::new("P", "J").unwrap();

        assert_eq!(
            paths.current_generation().as_str(),
            "P/J/current-generation.json"
        );
        assert_eq!(
            paths.generation_manifest(Generation(17)).as_str(),
            "P/J/generations/17/generation-manifest.json"
        );
        assert_eq!(
            paths
                .checkpoint_manifest(Generation(17), Epoch(44))
                .as_str(),
            "P/J/generations/17/checkpoints/checkpoint-0000044/checkpoint-manifest.pb"
        );
        assert_eq!(
            paths.committed_marker(Generation(17), Epoch(44)).as_str(),
            "P/J/generations/17/checkpoints/checkpoint-0000044/committed.json"
        );
        assert_eq!(
            paths.epoch_record(Epoch(44)).as_str(),
            "P/J/epochs/epoch-0000044.record"
        );
    }

    #[test]
    fn checkpoint_refs_must_be_relative_object_paths() {
        assert!(CheckpointRef::new("P/J/checkpoint-manifest.pb").is_ok());
        assert!(CheckpointRef::new("/P/J/checkpoint-manifest.pb").is_err());
        assert!(CheckpointRef::new("P//J/checkpoint-manifest.pb").is_err());
        assert!(CheckpointRef::new("P/../checkpoint-manifest.pb").is_err());
        assert!(CheckpointRef::new("P/J/").is_err());
        assert!(ProtocolPaths::new("P/extra", "J").is_err());
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

    #[test]
    fn protocol_records_serialize_with_expected_version() {
        let paths = ProtocolPaths::new("P", "J").unwrap();
        let current = CurrentGeneration::new(
            "P",
            "J",
            Generation(17),
            paths.generation_manifest(Generation(17)),
            123,
        );

        let encoded = serde_json::to_value(&current).unwrap();

        assert_eq!(encoded["version"], PROTOCOL_VERSION);
        assert_eq!(encoded["generation"], 17);
        assert_eq!(
            encoded["generation_manifest_ref"],
            "P/J/generations/17/generation-manifest.json"
        );
    }
}
