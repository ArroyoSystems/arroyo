use std::fmt::{Display, Formatter};

pub use arroyo_rpc::grpc::rpc::CheckpointManifest;
use arroyo_types::{JobId, PipelineId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PROTOCOL_VERSION: u32 = 1;

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckpointRef(String);

impl CheckpointRef {
    pub fn new(path: impl Into<String>) -> Result<Self, ProtocolError> {
        let path = path.into();
        validate_ref(&path)?;
        Ok(Self(path))
    }

    pub(crate) fn from_validated(path: String) -> Self {
        debug_assert!(validate_ref(&path).is_ok());
        Self(path)
    }

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

    pub fn candidate_checkpoint_ref(&self) -> Option<&CheckpointRef> {
        self.latest_checkpoint_ref
            .as_ref()
            .or(self.base_checkpoint_ref.as_ref())
    }
}

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
    pub fn for_checkpoint(
        pipeline_id: PipelineId,
        generation: Generation,
        checkpoint_ref: CheckpointRef,
        checkpoint: &CheckpointManifest,
        created_at_micros: u64,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            version: PROTOCOL_VERSION,
            pipeline_id,
            job_id: JobId::new(&checkpoint.job_id),
            epoch: checkpoint_epoch(checkpoint),
            generation,
            parent_checkpoint_ref: checkpoint_parent_checkpoint_ref(checkpoint)?,
            checkpoint_ref,
            created_at_micros,
        })
    }
}

pub(crate) fn checkpoint_epoch(checkpoint: &CheckpointManifest) -> Epoch {
    Epoch(checkpoint.epoch)
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
    let checkpoint_epoch = checkpoint_epoch(checkpoint);
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
        || marker.epoch != checkpoint_epoch(checkpoint)
        || &marker.checkpoint_ref != checkpoint_ref
    {
        return Err(ProtocolError::CommittedMarkerMismatch);
    }

    Ok(())
}
