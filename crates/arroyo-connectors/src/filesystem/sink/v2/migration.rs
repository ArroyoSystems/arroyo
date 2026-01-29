use arroyo_rpc::connector_err;
use arroyo_rpc::errors::DataflowResult;
use tracing::info;

use super::checkpoint::{
    FileToCommit, FileToCommitType, FilesCheckpointV2, InProgressFile, InProgressFileState,
};
use crate::filesystem::sink::{
    FileCheckpointData, FileSystemDataRecovery, FileToFinish, InFlightPartCheckpoint,
    InProgressFileCheckpoint,
};

pub fn migrate_recovery_v1_to_v2(v1: FileSystemDataRecovery) -> DataflowResult<FilesCheckpointV2> {
    info!(
        "Migrating filesystem sink recovery state from V1 to V2 ({} files)",
        v1.active_files.len()
    );

    let mut open_files = Vec::with_capacity(v1.active_files.len());

    for file in v1.active_files {
        open_files.push(migrate_file_checkpoint(file)?);
    }

    Ok(FilesCheckpointV2 {
        open_files,
        file_index: v1.next_file_index,
        delta_version: v1.delta_version,
    })
}

fn migrate_file_checkpoint(v1: InProgressFileCheckpoint) -> DataflowResult<InProgressFile> {
    let (state, data, metadata) = match v1.data {
        FileCheckpointData::Empty => (InProgressFileState::New, vec![], None),

        FileCheckpointData::MultiPartNotCreated {
            parts_to_add,
            trailing_bytes,
            metadata,
        } => {
            let mut data: Vec<u8> = parts_to_add.into_iter().flatten().collect();
            if let Some(tb) = trailing_bytes {
                data.extend(tb);
            }
            (InProgressFileState::New, data, metadata)
        }

        FileCheckpointData::MultiPartInFlight {
            multi_part_upload_id,
            in_flight_parts,
            trailing_bytes,
            metadata,
        } => {
            let (parts, has_in_progress) = extract_completed_parts(&in_flight_parts);
            if has_in_progress {
                return Err(connector_err!(
                    Internal,
                    NoRetry,
                    "Cannot migrate V1 state with in-progress parts (file: {}). \
                     This state should not occur during normal checkpoint.",
                    v1.filename
                ));
            }
            (
                InProgressFileState::MultipartStarted {
                    multipart_id: multi_part_upload_id,
                    parts,
                },
                trailing_bytes.unwrap_or_default(),
                metadata,
            )
        }

        FileCheckpointData::MultiPartWriterClosed {
            multi_part_upload_id,
            in_flight_parts,
            metadata,
        } => {
            let (parts, has_in_progress) = extract_completed_parts(&in_flight_parts);
            if has_in_progress {
                return Err(connector_err!(
                    Internal,
                    NoRetry,
                    "Cannot migrate V1 closed state with in-progress parts (file: {}). \
                     This state should not occur during normal checkpoint.",
                    v1.filename
                ));
            }
            (
                InProgressFileState::MultipartStarted {
                    multipart_id: multi_part_upload_id,
                    parts,
                },
                vec![],
                metadata,
            )
        }

        FileCheckpointData::MultiPartWriterUploadCompleted { .. } => {
            return Err(connector_err!(
                Internal,
                NoRetry,
                "Unexpected MultiPartWriterUploadCompleted in V1 recovery state (file: {})",
                v1.filename
            ));
        }
    };

    Ok(InProgressFile {
        path: v1.filename,
        total_size: v1.pushed_size,
        data,
        metadata,
        state,
    })
}

fn extract_completed_parts(parts: &[InFlightPartCheckpoint]) -> (Vec<String>, bool) {
    let mut completed = Vec::new();
    let mut has_in_progress = false;

    for part in parts {
        match part {
            InFlightPartCheckpoint::FinishedPart { content_id, .. } => {
                completed.push(content_id.clone());
            }
            InFlightPartCheckpoint::InProgressPart { .. } => {
                has_in_progress = true;
            }
        }
    }

    (completed, has_in_progress)
}

pub fn migrate_precommit_v1_to_v2(v1: FileToFinish) -> FileToCommit {
    FileToCommit {
        path: v1.filename,
        typ: FileToCommitType::Multipart {
            multipart_id: v1.multi_part_upload_id,
            parts: v1.completed_parts,
            total_size: v1.size,
        },
        iceberg_metadata: v1.metadata,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::sink::iceberg::metadata::{ColumnAccum, IcebergFileMetadata};
    use std::collections::HashMap;

    fn make_iceberg_metadata() -> IcebergFileMetadata {
        let mut columns = HashMap::new();
        columns.insert(
            1,
            ColumnAccum {
                compressed_size: 512,
                num_values: 100,
                null_values: 5,
                bounds: Default::default(),
            },
        );
        IcebergFileMetadata {
            row_count: 100,
            split_offsets: vec![0, 1024],
            columns,
        }
    }

    // Tests for migrate_precommit_v1_to_v2

    #[test]
    fn test_migrate_precommit_basic() {
        let v1 = FileToFinish {
            filename: "path/to/file.parquet".to_string(),
            partition: Some("partition_key".to_string()),
            multi_part_upload_id: "upload-123".to_string(),
            completed_parts: vec!["part1".to_string(), "part2".to_string()],
            size: 2048,
            metadata: None,
        };

        let v2 = migrate_precommit_v1_to_v2(v1);

        assert_eq!(v2.path, "path/to/file.parquet");
        assert!(matches!(
            v2.typ,
            FileToCommitType::Multipart {
                multipart_id,
                parts,
                total_size,
            } if multipart_id == "upload-123"
                && parts == vec!["part1", "part2"]
                && total_size == 2048
        ));
        assert!(v2.iceberg_metadata.is_none());
    }

    #[test]
    fn test_migrate_precommit_with_iceberg_metadata() {
        let metadata = make_iceberg_metadata();
        let v1 = FileToFinish {
            filename: "iceberg/data/file.parquet".to_string(),
            partition: None,
            multi_part_upload_id: "upload-456".to_string(),
            completed_parts: vec!["etag1".to_string()],
            size: 4096,
            metadata: Some(metadata.clone()),
        };

        let v2 = migrate_precommit_v1_to_v2(v1);

        assert_eq!(v2.path, "iceberg/data/file.parquet");
        assert_eq!(v2.iceberg_metadata, Some(metadata));
    }

    #[test]
    fn test_migrate_precommit_empty_parts() {
        let v1 = FileToFinish {
            filename: "empty.parquet".to_string(),
            partition: None,
            multi_part_upload_id: "upload-789".to_string(),
            completed_parts: vec![],
            size: 0,
            metadata: None,
        };

        let v2 = migrate_precommit_v1_to_v2(v1);

        assert!(matches!(
            v2.typ,
            FileToCommitType::Multipart { parts, total_size, .. }
            if parts.is_empty() && total_size == 0
        ));
    }

    #[test]
    fn test_migrate_recovery_empty() {
        let v1 = FileSystemDataRecovery {
            next_file_index: 5,
            active_files: vec![],
            delta_version: 10,
        };

        let v2 = migrate_recovery_v1_to_v2(v1).unwrap();

        assert_eq!(v2.file_index, 5);
        assert_eq!(v2.delta_version, 10);
        assert!(v2.open_files.is_empty());
    }

    #[test]
    fn test_migrate_recovery_multipart_not_created() {
        let v1 = FileSystemDataRecovery {
            next_file_index: 2,
            active_files: vec![InProgressFileCheckpoint {
                filename: "file2.parquet".to_string(),
                partition: Some("part=1".to_string()),
                data: FileCheckpointData::MultiPartNotCreated {
                    parts_to_add: vec![vec![1, 2, 3], vec![4, 5, 6]],
                    trailing_bytes: Some(vec![7, 8]),
                    metadata: None,
                },
                pushed_size: 100,
            }],
            delta_version: 1,
        };

        let v2 = migrate_recovery_v1_to_v2(v1).unwrap();

        assert_eq!(v2.open_files.len(), 1);
        let file = &v2.open_files[0];
        assert_eq!(file.path, "file2.parquet");
        assert_eq!(file.total_size, 100);
        // parts_to_add flattened + trailing_bytes
        assert_eq!(file.data, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(file.state, InProgressFileState::New);
    }

    #[test]
    fn test_migrate_recovery_multipart_in_flight_all_finished() {
        let v1 = FileSystemDataRecovery {
            next_file_index: 3,
            active_files: vec![InProgressFileCheckpoint {
                filename: "inflight.parquet".to_string(),
                partition: None,
                data: FileCheckpointData::MultiPartInFlight {
                    multi_part_upload_id: "upload-abc".to_string(),
                    in_flight_parts: vec![
                        InFlightPartCheckpoint::FinishedPart {
                            part: 1,
                            content_id: "etag1".to_string(),
                        },
                        InFlightPartCheckpoint::FinishedPart {
                            part: 2,
                            content_id: "etag2".to_string(),
                        },
                    ],
                    trailing_bytes: Some(vec![99, 100]),
                    metadata: None,
                },
                pushed_size: 500,
            }],
            delta_version: 5,
        };

        let v2 = migrate_recovery_v1_to_v2(v1).unwrap();

        let file = &v2.open_files[0];
        assert_eq!(file.path, "inflight.parquet");
        assert_eq!(file.total_size, 500);
        assert_eq!(file.data, vec![99, 100]); // trailing bytes become data
        assert_eq!(
            file.state,
            InProgressFileState::MultipartStarted {
                multipart_id: "upload-abc".to_string(),
                parts: vec!["etag1".to_string(), "etag2".to_string()],
            }
        );
    }

    #[test]
    fn test_migrate_recovery_multipart_writer_closed_all_finished() {
        let v1 = FileSystemDataRecovery {
            next_file_index: 1,
            active_files: vec![InProgressFileCheckpoint {
                filename: "closed.parquet".to_string(),
                partition: None,
                data: FileCheckpointData::MultiPartWriterClosed {
                    multi_part_upload_id: "upload-closed".to_string(),
                    in_flight_parts: vec![InFlightPartCheckpoint::FinishedPart {
                        part: 1,
                        content_id: "closed-etag1".to_string(),
                    }],
                    metadata: Some(make_iceberg_metadata()),
                },
                pushed_size: 1000,
            }],
            delta_version: 2,
        };

        let v2 = migrate_recovery_v1_to_v2(v1).unwrap();

        let file = &v2.open_files[0];
        assert_eq!(file.path, "closed.parquet");
        assert_eq!(file.total_size, 1000);
        assert!(file.data.is_empty()); // closed writer has no trailing bytes
        assert!(file.metadata.is_some());
        assert_eq!(
            file.state,
            InProgressFileState::MultipartStarted {
                multipart_id: "upload-closed".to_string(),
                parts: vec!["closed-etag1".to_string()],
            }
        );
    }
}
