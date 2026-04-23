//! JSON protocol files used for the object-storage-backed checkpoint and
//! two-phase commit protocol
//!
//! These manifests are written only in `JobControllerMode::Worker` mode and
//! are the source of truth for recovery, committing, and garbage collection.
//!
//! # Layout
//!
//! For pipeline `P`, job `J`, generation `G`, and epoch `E`:
//!
//! - `P/J/control/current_generation.json`
//! - `P/J/generations/G/generation_manifest.json`
//! - `P/J/epochs/epoch-{E:07}.lock`

use serde::{Deserialize, Serialize};

/// Current version of the metadata files used for the checkpoint protocol
pub const METADATA_VERSION: u32 = 1;


/// Written by the controller to identify the latest generation for a job.
///
/// Acts as an advisory fence: a leader reads this on startup and before
/// external commit to detect that it has been superseded by a newer
/// generation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentGeneration {
    pub version: u32,
    pub pipeline_id: String,
    pub job_id: String,
    pub job_generation: u64,
    pub generation_manifest_ref: String,
    pub updated_at_micros: u64,
}

/// Written by the job leader on startup and updated as checkpoints are
/// published by this generation.
///
/// - `base_checkpoint_ref` is the checkpoint this generation restored from
///   (`None` for a fresh job).
/// - `latest_checkpoint_ref` is the newest published checkpoint for this
///   generation (`None` until the first checkpoint is published).
/// - Recovery uses `latest_checkpoint_ref` if present; otherwise
///   `base_checkpoint_ref`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationManifest {
    pub version: u32,
    pub pipeline_id: String,
    pub job_id: String,
    pub job_generation: u64,
    #[serde(default)]
    pub base_checkpoint_ref: Option<String>,
    #[serde(default)]
    pub latest_checkpoint_ref: Option<String>,
    pub updated_at_micros: u64,
}

/// The epoch lock. Created with a conditional `put-if-not-exists` write; this
/// creates a permanent ownership over the epoch for this checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochLock {
    pub version: u32,
    pub pipeline_id: String,
    pub job_id: String,
    pub epoch: u64,
    pub job_generation: u64,
    pub checkpoint_ref: String,
    pub created_at_micros: u64,
}

pub mod paths {
    /// `P/J/control/current_generation.json`
    pub fn current_generation(pipeline_id: &str, job_id: &str) -> String {
        format!("{pipeline_id}/{job_id}/control/current-generation.json")
    }

    /// `P/J/generations/G/generation_manifest.json`
    pub fn generation_manifest(pipeline_id: &str, job_id: &str, generation: u64) -> String {
        format!("{pipeline_id}/{job_id}/generations/{generation}/generation-manifest.json")
    }

    /// `P/J/generations/G/checkpoints/checkpoint-{E:07}`
    pub fn checkpoint_dir(pipeline_id: &str, job_id: &str, generation: u64, epoch: u64) -> String {
        format!(
            "{pipeline_id}/{job_id}/generations/{generation}/checkpoints/checkpoint-{epoch:0>7}"
        )
    }

    /// `P/J/generations/G/checkpoints/checkpoint-{E:07}/checkpoint_manifest.pb`
    pub fn checkpoint_manifest(pipeline_id: &str, job_id: &str, generation: u64, epoch: u64) -> String {
        format!(
            "{}/manifest",
            checkpoint_dir(pipeline_id, job_id, generation, epoch)
        )
    }

    /// `P/J/generations/G/checkpoints/checkpoint-{E:07}/operator-{operator_id}`
    pub fn operator_dir(
        pipeline_id: &str,
        job_id: &str,
        generation: u64,
        epoch: u64,
        operator_id: &str,
    ) -> String {
        format!(
            "{}/operator-{operator_id}",
            checkpoint_dir(pipeline_id, job_id, generation, epoch)
        )
    }

    /// `P/J/epochs/epoch-{E:07}.lock`
    pub fn epoch_lock(pipeline_id: &str, job_id: &str, epoch: u32) -> String {
        format!("{pipeline_id}/{job_id}/epochs/epoch-{epoch:0>7}.lock")
    }
}