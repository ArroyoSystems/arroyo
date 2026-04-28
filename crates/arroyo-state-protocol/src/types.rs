use std::fmt::{Display, Formatter};
use std::time::SystemTime;
/// Protobuf checkpoint manifest used as the publication point for checkpoint data.
pub use arroyo_rpc::grpc::rpc::CheckpointManifest;
use arroyo_types::{to_micros, JobId, PipelineId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Current version for JSON protocol records written by this crate.
pub const PROTOCOL_VERSION: u32 = 1;

/// Errors returned when observed protocol objects violate protocol invariants.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("invalid checkpoint ref `{path}`: {reason}")]
    InvalidCheckpointRef { path: String, reason: &'static str },
    #[error("invalid path component `{name}` with value `{value}`: {reason}")]
    InvalidPathComponent {
        name: &'static str,
        value: String,
        reason: &'static str,
    },
    #[error(
        "epoch record for epoch {record_epoch} cannot describe checkpoint at epoch {checkpoint_epoch}"
    )]
    EpochMismatch {
        checkpoint_epoch: Epoch,
        record_epoch: Epoch,
    },
    #[error("epoch record parent does not match checkpoint manifest parent")]
    ParentMismatch,
    #[error("committed marker does not match checkpoint")]
    CommittedMarkerMismatch,
    #[error("checkpoint manifest does not match protocol record")]
    CheckpointManifestMismatch,
}

/// Monotonic identifier for a worker cluster generation of a job.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Generation(pub u64);

impl Display for Generation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Monotonic identifier for a checkpoint epoch.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Epoch(pub u64);

impl Display for Epoch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Relative object-store path to a checkpoint/protocol object.
///
/// Use [`CheckpointRef::new`] for externally supplied paths. It rejects absolute
/// paths and parent-directory traversal so protocol records can be safely
/// relocated under a checkpoint URI.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckpointRef(String);

impl CheckpointRef {
    /// Validates and constructs a checkpoint reference from a relative path.
    pub fn new(path: impl Into<String>) -> Result<Self, ProtocolError> {
        let path = path.into();
        validate_ref(&path)?;
        Ok(Self(path))
    }

    pub(crate) fn from_validated(path: String) -> Self {
        debug_assert!(validate_ref(&path).is_ok());
        Self(path)
    }

    /// Returns the underlying relative path string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for CheckpointRef {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for CheckpointRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

fn validate_ref(path: &str) -> Result<(), ProtocolError> {
    if path.is_empty() {
        return Err(invalid_ref(path, "path is empty"));
    }

    if path.starts_with('/') {
        return Err(invalid_ref(path, "path must be relative"));
    }

    if path.ends_with('/') {
        return Err(invalid_ref(path, "path must identify an object"));
    }

    if path.contains('\\') {
        return Err(invalid_ref(path, "path must use `/` separators"));
    }

    if path
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(invalid_ref(path, "path contains an invalid segment"));
    }

    Ok(())
}

fn invalid_ref(path: &str, reason: &'static str) -> ProtocolError {
    ProtocolError::InvalidCheckpointRef {
        path: path.to_string(),
        reason,
    }
}

/// Controller-written fence naming the current generation for a job.
///
/// Workers should read this before generation initialization, publication, and
/// other ownership-sensitive operations. It is advisory; canonical ownership is
/// still determined by epoch records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentGeneration {
    pub version: u32,
    pub pipeline_id: PipelineId,
    pub job_id: JobId,
    pub generation: Generation,
    pub generation_manifest_ref: CheckpointRef,
    pub updated_at_micros: u64,
}

impl CurrentGeneration {
    /// Builds a current-generation record with the current protocol version.
    pub fn new(
        pipeline_id: PipelineId,
        job_id: JobId,
        generation: Generation,
        generation_manifest_ref: CheckpointRef,
        updated_at_micros: u64,
    ) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            pipeline_id,
            job_id,
            generation,
            generation_manifest_ref,
            updated_at_micros,
        }
    }
}

/// Per-generation candidate recovery manifest.
///
/// `latest_checkpoint_ref` is only a candidate pointer. Callers must resolve it
/// through `workflow::resolve_generation_manifest` before restoring from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationManifest {
    pub version: u32,
    pub pipeline_id: PipelineId,
    pub job_id: JobId,
    pub generation: Generation,
    pub base_checkpoint_ref: Option<CheckpointRef>,
    pub latest_checkpoint_ref: Option<CheckpointRef>,
    pub updated_at_micros: u64,
}

impl GenerationManifest {
    /// Creates a new generation manifest with `latest_checkpoint_ref` unset.
    ///
    /// New generations should set `base_checkpoint_ref` to the checkpoint
    /// returned by `workflow::initialize_generation`, if any.
    pub fn new(
        pipeline_id: PipelineId,
        job_id: JobId,
        generation: Generation,
        base_checkpoint_ref: Option<CheckpointRef>,
        updated_at_micros: u64,
    ) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            pipeline_id,
            job_id,
            generation,
            base_checkpoint_ref,
            latest_checkpoint_ref: None,
            updated_at_micros,
        }
    }

    /// Returns `latest_checkpoint_ref` if present, otherwise `base_checkpoint_ref`.
    ///
    /// This value is a candidate only; do not restore from it without resolving
    /// it against epoch records.
    pub fn candidate_checkpoint_ref(&self) -> Option<&CheckpointRef> {
        self.latest_checkpoint_ref
            .as_ref()
            .or(self.base_checkpoint_ref.as_ref())
    }
}

/// Marker written after all external commit work for a checkpoint completes.
///
/// This object is immutable and should be created conditionally. If creation
/// races with a retry, an existing marker for the same checkpoint is success.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedMarker {
    pub version: u32,
    pub pipeline_id: PipelineId,
    pub job_id: JobId,
    pub epoch: Epoch,
    pub checkpoint_generation: Generation,
    pub writer_generation: Generation,
    pub checkpoint_ref: CheckpointRef,
}

impl CommittedMarker {
    /// Builds a commit-completion marker with the current protocol version.
    pub fn new(
        pipeline_id: PipelineId,
        job_id: JobId,
        epoch: Epoch,
        checkpoint_generation: Generation,
        writer_generation: Generation,
        checkpoint_ref: CheckpointRef,
    ) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            pipeline_id,
            job_id,
            epoch,
            checkpoint_generation,
            writer_generation,
            checkpoint_ref,
        }
    }
}

/// Canonical ownership record for an epoch.
///
/// Exactly one checkpoint may own an epoch record. For non-committing
/// checkpoints, this makes the checkpoint recoverable. For committing
/// checkpoints, it also authorizes sending external commit requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochRecord {
    pub version: u32,
    pub pipeline_id: PipelineId,
    pub job_id: JobId,
    pub epoch: Epoch,
    pub generation: Generation,
    pub parent_checkpoint_ref: Option<CheckpointRef>,
    pub checkpoint_ref: CheckpointRef,
    pub created_at_micros: u64,
}

impl EpochRecord {
    /// Builds the epoch record that canonically assigns `checkpoint_ref` to the
    /// checkpoint's epoch.
    ///
    /// Callers should write this with conditional-create semantics, normally via
    /// `workflow::claim_epoch_record` rather than writing it directly.
    pub fn for_checkpoint(
        pipeline_id: PipelineId,
        generation: Generation,
        checkpoint_ref: CheckpointRef,
        checkpoint: &CheckpointManifest,
        created_at: SystemTime,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            version: PROTOCOL_VERSION,
            pipeline_id,
            job_id: JobId::new(&checkpoint.job_id),
            epoch: Epoch(checkpoint.epoch),
            generation,
            parent_checkpoint_ref: checkpoint_parent_checkpoint_ref(checkpoint)?,
            checkpoint_ref,
            created_at_micros: to_micros(created_at),
        })
    }
}

pub(crate) fn checkpoint_parent_checkpoint_ref(
    checkpoint: &CheckpointManifest,
) -> Result<Option<CheckpointRef>, ProtocolError> {
    checkpoint
        .parent_checkpoint_ref
        .as_ref()
        .map(|checkpoint_ref| CheckpointRef::new(checkpoint_ref.clone()))
        .transpose()
}

pub(crate) fn validate_epoch_record_matches_checkpoint(
    checkpoint_ref: &CheckpointRef,
    checkpoint: &CheckpointManifest,
    record: &EpochRecord,
) -> Result<(), ProtocolError> {
    let checkpoint_epoch = Epoch(checkpoint.epoch);
    if checkpoint_epoch != record.epoch {
        return Err(ProtocolError::EpochMismatch {
            checkpoint_epoch,
            record_epoch: record.epoch,
        });
    }

    if checkpoint_parent_checkpoint_ref(checkpoint)? != record.parent_checkpoint_ref {
        return Err(ProtocolError::ParentMismatch);
    }

    debug_assert_eq!(checkpoint_ref, &record.checkpoint_ref);
    Ok(())
}

pub(crate) fn validate_committed_marker_matches_checkpoint(
    checkpoint_ref: &CheckpointRef,
    checkpoint: &CheckpointManifest,
    marker: &CommittedMarker,
) -> Result<(), ProtocolError> {
    if *marker.job_id != checkpoint.job_id
        || marker.epoch != Epoch(checkpoint.epoch)
        || &marker.checkpoint_ref != checkpoint_ref
    {
        return Err(ProtocolError::CommittedMarkerMismatch);
    }

    Ok(())
}
