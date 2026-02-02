use super::migration;
use crate::filesystem::sink::iceberg::metadata::IcebergFileMetadata;
use crate::filesystem::sink::{FileSystemDataRecovery, FileToFinish};
use arroyo_rpc::SerializableBytes;
use arroyo_rpc::errors::StateError;
use arroyo_state::tables::MigratableState;
use bincode::{Decode, Encode};
use std::fmt::Debug;

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct FilesCheckpointV2 {
    pub open_files: Vec<InProgressFile>,
    pub file_index: usize,
    pub delta_version: i64,
}

impl MigratableState for FilesCheckpointV2 {
    const VERSION: u32 = 1;
    type PreviousVersion = FileSystemDataRecovery;

    fn migrate(previous: FileSystemDataRecovery) -> Result<Self, StateError> {
        migration::migrate_recovery_v1_to_v2(previous).map_err(|e| StateError::MigrationFailed {
            table: "r".to_string(),
            error: e.to_string(),
        })
    }
}

impl MigratableState for FileToCommit {
    const VERSION: u32 = 1;
    type PreviousVersion = FileToFinish;

    fn migrate(previous: FileToFinish) -> Result<Self, StateError> {
        Ok(migration::migrate_precommit_v1_to_v2(previous))
    }
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct InProgressFile {
    pub path: String,
    pub total_size: usize,
    pub data: SerializableBytes,
    pub metadata: Option<IcebergFileMetadata>,
    pub state: InProgressFileState,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub enum InProgressFileState {
    New,
    MultipartStarted {
        multipart_id: String,
        parts: Vec<String>,
    },
    SingleFileToFinish,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub enum FileToCommitType {
    Multipart {
        multipart_id: String,
        parts: Vec<String>,
        total_size: usize,
    },
    Single {
        total_size: usize,
    },
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct FileToCommit {
    pub path: String,
    pub typ: FileToCommitType,
    pub iceberg_metadata: Option<IcebergFileMetadata>,
}
