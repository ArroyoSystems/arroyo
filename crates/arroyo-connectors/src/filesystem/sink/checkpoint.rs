use crate::filesystem::sink::iceberg::metadata::IcebergFileMetadata;
use arroyo_rpc::SerializableBytes;
use bincode::{Decode, Encode};
use std::fmt::Debug;

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct FilesCheckpointV2 {
    pub open_files: Vec<InProgressFile>,
    pub file_index: usize,
    pub delta_version: i64,
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
