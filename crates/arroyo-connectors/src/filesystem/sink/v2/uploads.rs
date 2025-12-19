//! Upload futures with retry logic for the filesystem sink.
//!
//! This module provides functions to create futures for various upload operations
//! (multipart init, part upload, single-file upload) with built-in retry logic.

// Some variants and functions are not yet used but kept for future use
#![allow(dead_code)]

use super::map_storage_error;
use crate::filesystem::sink::FsEventLogger;
use arroyo_rpc::connector_err;
use arroyo_rpc::errors::{DataflowError, DataflowResult, StorageError};
use arroyo_storage::StorageProvider;
use bytes::Bytes;
use object_store::MultipartId;
use object_store::path::Path;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Maximum number of retries for upload operations.
const UPLOAD_MAX_RETRIES: usize = 10;

/// Base delay between retries (uses exponential backoff).
const UPLOAD_BASE_DELAY: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub enum FsResponseData {
    MultipartInitialized {
        multipart_id: MultipartId,
    },
    PartFinished {
        part_index: usize,
        content_id: String,
    },
    SingleFileFinished,
    MultipartFinalized,
}

#[derive(Debug)]
pub struct FsResponse {
    pub path: Arc<Path>,
    pub data: FsResponseData,
}

fn handle_error(started: Instant, logger: &FsEventLogger, error: StorageError) -> DataflowError {
    logger.log_fs_event(0, 0, started.elapsed(), 1, Some(error.to_string()), 0, 0, 0);

    map_storage_error(error)
}

/// Type alias for upload futures.
pub type UploadFuture = Pin<Box<dyn Future<Output = DataflowResult<FsResponse>> + Send>>;

/// Create a future that initializes a multipart upload.
pub fn create_multipart_init_future(
    storage: Arc<StorageProvider>,
    path: Arc<Path>,
    logger: FsEventLogger,
) -> UploadFuture {
    Box::pin(async move {
        let started = Instant::now();
        let multipart_id = storage
            .start_multipart(&path)
            .await
            .map_err(|e| handle_error(started, &logger, e))?;

        Ok(FsResponse {
            path,
            data: FsResponseData::MultipartInitialized { multipart_id },
        })
    })
}

/// Create a future that uploads a part of a multipart upload.
pub fn create_part_upload_future(
    storage: Arc<StorageProvider>,
    path: Arc<Path>,
    multipart_id: Arc<MultipartId>,
    part_index: usize,
    data: Bytes,
    logger: FsEventLogger,
) -> UploadFuture {
    Box::pin(async move {
        let started = Instant::now();
        let part_id = storage
            .add_multipart(&path, &multipart_id, part_index, data.clone())
            .await
            .map_err(|e| handle_error(started, &logger, e))?;

        logger.log_fs_event(data.len(), 0, started.elapsed(), 0, None, 0, 0, 0);

        Ok(FsResponse {
            path,
            data: FsResponseData::PartFinished {
                part_index,
                content_id: part_id.content_id,
            },
        })
    })
}

pub fn create_single_file_upload_future(
    storage: Arc<StorageProvider>,
    path: Arc<Path>,
    data: Bytes,
    logger: FsEventLogger,
) -> UploadFuture {
    Box::pin(async move {
        let started = Instant::now();
        storage
            .put_bytes(&path, data.clone())
            .await
            .map_err(|e| handle_error(started, &logger, e))?;

        logger.log_fs_event(data.len(), 1, started.elapsed(), 0, None, 0, 0, 0);

        Ok(FsResponse {
            path,
            data: FsResponseData::SingleFileFinished,
        })
    })
}

pub fn create_multipart_finalize_future(
    storage: Arc<StorageProvider>,
    path: Arc<Path>,
    multipart_id: Arc<MultipartId>,
    part_ids: Vec<String>,
    expected_size: usize,
    logger: FsEventLogger,
) -> UploadFuture {
    Box::pin(async move {
        let parts: Vec<_> = part_ids
            .into_iter()
            .map(|content_id| object_store::multipart::PartId { content_id })
            .collect();

        let started = Instant::now();
        if let Err(e) = storage
            .close_multipart(&path, &multipart_id, parts.clone())
            .await
        {
            // check if the file is already there with the correct size -- in which case it means
            // that we've already finalized it
            if let Ok(meta) = storage
                .head(path.as_ref().clone())
                .await
                .map_err(map_storage_error)
            {
                if meta.size != expected_size as u64 {
                    return Err(connector_err!(
                        External,
                        NoRetry,
                        "file written to {} should have length of {}, not {}",
                        path,
                        meta.size,
                        expected_size
                    ));
                }
            } else {
                return Err(handle_error(started, &logger, e));
            }
        }

        logger.log_fs_event(0, 1, started.elapsed(), 0, None, 0, 0, 0);

        Ok(FsResponse {
            path,
            data: FsResponseData::MultipartFinalized,
        })
    })
}
