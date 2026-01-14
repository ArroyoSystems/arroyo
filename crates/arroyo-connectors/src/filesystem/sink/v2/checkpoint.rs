use crate::filesystem::sink::iceberg::metadata::IcebergFileMetadata;
use bincode::{Decode, Encode};
use std::fmt::{Debug, Formatter};

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
    pub data: Vec<u8>,
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
}

#[derive(Clone, Encode, Decode, PartialEq, Eq)]
pub enum FileToCommitType {
    Multipart {
        multipart_id: String,
        parts: Vec<String>,
        total_size: usize,
    },
    Single {
        data: Vec<u8>,
    },
}

impl Debug for FileToCommitType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileToCommitType::Multipart {
                multipart_id,
                parts,
                total_size,
            } => f
                .debug_struct("Multipart")
                .field("multipart_id", multipart_id)
                .field("parts", parts)
                .field("total_size", total_size)
                .finish(),
            FileToCommitType::Single { data } => f
                .debug_struct("Single")
                .field("data.len", &data.len())
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct FileToCommit {
    pub path: String,
    pub typ: FileToCommitType,
    pub iceberg_metadata: Option<IcebergFileMetadata>,
}
