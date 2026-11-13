use ::arrow::record_batch::RecordBatch;
use arroyo_rpc::errors::{DataflowError, DataflowResult, ErrorDomain, RetryHint, StorageError};
use arroyo_rpc::{df::ArroyoSchemaRef, formats::Format, log_trace_event};
use arroyo_types::*;
use bincode::{Decode, Encode};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use deltalake::DeltaTable;
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
pub mod arrow;
mod checkpoint;
mod delta;
pub(crate) mod iceberg;
pub mod json;
pub mod local;
mod open_file;
pub mod operator;
pub mod parquet;
pub(crate) mod partitioning;
mod two_phase_committer;
mod uploads;

use self::{json::JsonLocalWriter, local::LocalFileSystemWriter, parquet::ParquetLocalWriter};

use self::iceberg::metadata::IcebergFileMetadata;
use crate::filesystem::sink::iceberg::IcebergTable;
use crate::filesystem::{FilenameStrategy, config};

const MAX_CONFLICT_RETRIES: u32 = 3;

pub type LocalParquetFileSystemSink = LocalFileSystemWriter<ParquetLocalWriter>;

pub type LocalJsonFileSystemSink = LocalFileSystemWriter<JsonLocalWriter>;

fn map_storage_error(storage_error: StorageError) -> DataflowError {
    let (domain, retry) = classify_storage_error(&storage_error);

    DataflowError::ConnectorError {
        domain,
        retry,
        error: storage_error_message(&storage_error),
        source: None,
    }
}

fn storage_error_message(storage_error: &StorageError) -> String {
    match storage_error {
        StorageError::ObjectStore(obj_err) => obj_err.to_string(),
        storage_error => storage_error.to_string(),
    }
}

fn classify_storage_error(storage_error: &StorageError) -> (ErrorDomain, RetryHint) {
    match storage_error {
        // User errors: configuration/input mistakes by the user
        StorageError::InvalidUrl
        | StorageError::PathError(_)
        | StorageError::NoKeyInUrl
        | StorageError::AlreadyExists { .. }
        | StorageError::CredentialsError(_) => (ErrorDomain::User, RetryHint::NoRetry),
        // Object store errors need to be inspected to determine the domain
        StorageError::ObjectStore(obj_err) => classify_object_store_error(obj_err),
    }
}

fn classify_object_store_error(obj_err: &object_store::Error) -> (ErrorDomain, RetryHint) {
    use object_store::Error;
    match obj_err {
        // AlreadyExists can wrap spurious upstream 409s; retry a few times before
        // failing with a user error.
        Error::AlreadyExists { .. } => (
            ErrorDomain::User,
            RetryHint::WithBackoffLimited(MAX_CONFLICT_RETRIES),
        ),
        // User errors: authentication, authorization, bad paths, misconfiguration
        Error::NotFound { .. }
        | Error::InvalidPath { .. }
        | Error::Unauthenticated { .. }
        | Error::PermissionDenied { .. } => (ErrorDomain::User, RetryHint::NoRetry),
        // External errors: permanent issues that won't be fixed by retrying
        Error::NotSupported { .. } | Error::NotModified { .. } | Error::NotImplemented { .. } => {
            (ErrorDomain::External, RetryHint::NoRetry)
        }
        // External errors: transient issues worth retrying
        _ => (ErrorDomain::External, RetryHint::WithBackoff),
    }
}

#[derive(Clone)]
pub struct FsEventLogger {
    task_info: Option<Arc<TaskInfo>>,
    connection_id: Arc<String>,
    output_format: &'static str,
    table_format: &'static str,
}

impl FsEventLogger {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn log_fs_event(
        &self,
        bytes: usize,
        files: u64,
        write_time: Duration,
        failures: u64,
        failure_message: Option<String>,
        row_groups_written: u64,
        uncompressed_bytes_written: u64,
        rows_written: u64,
    ) {
        let task_info = self.task_info.as_ref().unwrap();
        log_trace_event!("filesystem_write",
        {
            "operator_id": task_info.operator_id,
            "operator_name": task_info.operator_name,
            "connection_id": self.connection_id.as_str(),
            "write_error_reason": failure_message.as_deref().unwrap_or(""),
            "subtask_idx": task_info.task_index,
            "output_format": self.output_format,
            "table_format": self.table_format,
        }, [
            "bytes_written" => bytes as f64,
            "files_written" => files as f64,
            "write_time_ms" => write_time.as_millis() as f64,
            "write_failures" => failures as f64,
            "row_groups_written" => row_groups_written as f64,
            "uncompressed_bytes_written" => uncompressed_bytes_written as f64,
            "rows_written" => rows_written as f64,
        ]);
    }
}

#[derive(Debug)]
pub enum CommitState {
    DeltaLake {
        last_version: i64,
        table: Box<DeltaTable>,
    },
    // None while the sink is committing in the background
    Iceberg(Option<Box<IcebergTable>>),
    VanillaParquet,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct FinishedFile {
    filename: String,
    // unused, retained for backwards compatibility
    partition: Option<String>,
    size: usize,
    metadata: Option<IcebergFileMetadata>,
}

#[derive(Debug)]
enum RollingPolicy {
    PartLimit(usize),
    SizeLimit(usize),
    InactivityDuration(Duration),
    RolloverDuration(Duration),
    WatermarkExpiration { pattern: String },
}

impl RollingPolicy {
    fn should_roll(&self, stats: &MultiPartWriterStats, watermark: Option<SystemTime>) -> bool {
        match self {
            RollingPolicy::PartLimit(part_limit) => stats.parts_written >= *part_limit,
            RollingPolicy::SizeLimit(size_limit) => stats.bytes_written >= *size_limit,
            RollingPolicy::InactivityDuration(duration) => {
                stats.last_write_at.elapsed() >= *duration
            }
            RollingPolicy::RolloverDuration(duration) => {
                stats.first_write_at.elapsed() >= *duration
            }
            RollingPolicy::WatermarkExpiration { pattern } => {
                let Some(watermark) = watermark else {
                    return false;
                };
                if watermark <= stats.representative_timestamp {
                    return false;
                }
                // if the watermark is greater than the representative and has a different string format,
                // then this partition is closed and should be rolled over.
                let datetime: DateTime<Utc> = DateTime::from(watermark);
                let formatted_time = datetime.format(pattern).to_string();
                let representative_datetime: DateTime<Utc> =
                    DateTime::from(stats.representative_timestamp);
                let representative_formatted_time =
                    representative_datetime.format(pattern).to_string();
                formatted_time != representative_formatted_time
            }
        }
    }

    fn from_file_settings(table: &config::FileSystemSink) -> Vec<RollingPolicy> {
        let mut policies = vec![];
        let part_size_limit = table.multipart.max_parts.map(|d| d.get()).unwrap_or(1000) as usize;

        // this is a hard limit, so will always be present.
        policies.push(RollingPolicy::PartLimit(part_size_limit));
        if let Some(file_size_target) = table.rolling_policy.file_size_bytes {
            policies.push(RollingPolicy::SizeLimit(file_size_target as usize))
        }

        if let Some(s) = table.rolling_policy.inactivity_seconds {
            policies.push(RollingPolicy::InactivityDuration(Duration::from_secs(s)))
        }

        if let Some(s) = table.rolling_policy.interval_seconds {
            policies.push(RollingPolicy::RolloverDuration(Duration::from_secs(s)));
        }

        if table.rolling_policy.watermark_expiration
            && let Some(pattern) = &table.partitioning.time_pattern
        {
            policies.push(RollingPolicy::WatermarkExpiration {
                pattern: pattern.clone(),
            });
        }

        policies
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct MultiPartWriterStats {
    bytes_written: usize,
    parts_written: usize,
    last_write_at: Instant,
    first_write_at: Instant,
    representative_timestamp: SystemTime,
}

pub trait BatchBufferingWriter: Send {
    fn new(
        config: &config::FileSystemSink,
        format: Format,
        schema: ArroyoSchemaRef,
        iceberg_schema: Option<::iceberg::spec::SchemaRef>,
        event_logger: FsEventLogger,
    ) -> DataflowResult<Self>
    where
        Self: Sized;

    /// Returns the file suffix based on the format configuration.
    /// This allows writers to customize the suffix based on format options
    /// (e.g., "json.gz" for compressed JSON, "json" for uncompressed).
    fn suffix_for_format(format: &Format) -> &str;

    fn add_batch_data(&mut self, data: &RecordBatch);
    /// approximate number of bytes that are internally buffered in the underlying writer;
    /// unflushed_bytes() + buffered_bytes() should give approximately the size of the buffer
    /// that would be produced by calling close()
    fn unflushed_bytes(&self) -> usize;
    fn buffered_bytes(&self) -> usize;
    /// Splits currently written data from the buffer (up to pos) and returns it. Note there may
    /// still be additional interally-buffered data that has not been written out into the
    /// actual buffer, for example in-progress row groups. This method does not flush those
    /// in progress writes.
    ///
    /// Panics if pos > `self.buffered_bytes()`
    fn split_to(&mut self, pos: usize) -> Bytes;
    fn get_trailing_bytes_for_checkpoint(&mut self) -> (Vec<u8>, Option<IcebergFileMetadata>);
    fn close(&mut self) -> (Bytes, Option<IcebergFileMetadata>);
}

pub(crate) fn split_into_parts(mut bytes: Bytes, part_size: usize) -> Vec<Bytes> {
    let mut parts = Vec::new();
    while bytes.len() > part_size {
        let mut part = bytes;
        let remainder = part.split_off(part_size);
        parts.push(part);
        bytes = remainder;
    }
    if !bytes.is_empty() {
        parts.push(bytes);
    }
    parts
}

pub(crate) fn add_suffix_prefix(
    filename: String,
    prefix: Option<&String>,
    suffix: &String,
) -> String {
    match prefix {
        None => format!("{filename}.{suffix}"),
        Some(prefix) => format!("{prefix}-{filename}.{suffix}"),
    }
}

#[cfg(test)]
mod tests {
    use crate::filesystem::sink::split_into_parts;
    use bytes::Bytes;

    #[test]
    fn test_split_into_parts() {
        let data: Bytes = (0..10).collect();
        let parts = split_into_parts(data.clone(), 3);

        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], vec![0, 1, 2]);
        assert_eq!(parts[1], vec![3, 4, 5]);
        assert_eq!(parts[2], vec![6, 7, 8]);
        assert_eq!(parts[3], vec![9]);

        let combined: Vec<u8> = parts.concat();
        assert_eq!(combined, data);

        let data: Bytes = vec![1, 2].into();
        let parts = split_into_parts(data.clone(), 10);
        assert_eq!(parts, vec![data]);

        let data: Bytes = (0..6).collect();
        let parts = split_into_parts(data.clone(), 3);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts.concat(), data);
    }
}
