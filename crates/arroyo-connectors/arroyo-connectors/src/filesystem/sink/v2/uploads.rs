use super::{classify_storage_error, map_storage_error};
use crate::filesystem::sink::{FsEventLogger, storage_error_message};
use arroyo_rpc::connector_err;
use arroyo_rpc::errors::{DataflowError, DataflowResult, ErrorDomain, StorageError};
use arroyo_storage::StorageProvider;
use bytes::Bytes;
use futures::future::try_join_all;
use object_store::MultipartId;
use object_store::path::Path;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

#[derive(Debug)]
pub enum FsResponseData {
    MultipartInitialized {
        multipart_id: MultipartId,
    },
    PartFinished {
        part_index: usize,
        content_id: String,
    },
    PartsFinished {
        first_part_index: usize,
        content_ids: Vec<String>,
    },
    SingleFileFinished,
    MultipartFinalized,
    MultipartAlreadyComplete,
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

pub type UploadFuture = Pin<Box<dyn Future<Output = DataflowResult<FsResponse>> + Send>>;

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

// writes (possibly) multiple final parts of a file, gracefully handling the case that in
// recovery the file may have already been completed
pub fn create_last_parts_upload_future(
    storage: Arc<StorageProvider>,
    path: Arc<Path>,
    multipart_id: Arc<MultipartId>,
    first_part_index: usize,
    data: Vec<Bytes>,
    existing_size: u64,
    logger: FsEventLogger,
) -> UploadFuture {
    debug!("uploading last parts for {:?}", path);
    let new_bytes = data.iter().map(|f| f.len()).sum();

    Box::pin(async move {
        let started = Instant::now();

        let uploads = data.into_iter().enumerate().map(|(i, data)| {
            storage.add_multipart(&path, &multipart_id, first_part_index + i, data)
        });

        match try_join_all(uploads).await {
            Ok(parts) => {
                logger.log_fs_event(new_bytes, 0, started.elapsed(), 0, None, 0, 0, 0);

                Ok(FsResponse {
                    path,
                    data: FsResponseData::PartsFinished {
                        first_part_index,
                        content_ids: parts.into_iter().map(|p| p.content_id).collect(),
                    },
                })
            }
            Err(e) => {
                validate_already_there(
                    &storage,
                    &path,
                    existing_size + new_bytes as u64,
                    started,
                    &logger,
                    e,
                )
                .await?;
                Ok(FsResponse {
                    path,
                    data: FsResponseData::MultipartAlreadyComplete,
                })
            }
        }
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

async fn validate_already_there(
    storage: &StorageProvider,
    path: &Path,
    expected_size: u64,
    started: Instant,
    logger: &FsEventLogger,
    e: StorageError,
) -> DataflowResult<()> {
    // check if the file is already there with the correct size -- in which case it means
    // that we've already finalized it
    match storage.head(path.as_ref()).await {
        Ok(meta) => {
            if meta.size != expected_size {
                Err(connector_err!(
                    External,
                    NoRetry,
                    "file written to {} should have length of {}, not {}",
                    path,
                    expected_size,
                    meta.size,
                ))
            } else {
                debug!(
                    path = %path,
                    size = expected_size,
                    original_error = %e,
                    "file already exists with expected size; \
                     treating as successful recovery from prior finalization"
                );
                Ok(())
            }
        }
        Err(head_error) => {
            Err(map_recovery_head_error(head_error)
                .unwrap_or_else(|| handle_error(started, logger, e)))
        }
    }
}

fn map_recovery_head_error(head_error: StorageError) -> Option<DataflowError> {
    if matches!(
        head_error,
        StorageError::ObjectStore(object_store::Error::NotFound { .. })
    ) {
        return None;
    }

    let (domain, retry) = classify_storage_error(&head_error);
    let error = match domain {
        ErrorDomain::User => storage_error_message(&head_error),
        ErrorDomain::External | ErrorDomain::Internal => {
            "failed to validate whether failed multipart write successfully completed".to_string()
        }
    };
    Some(DataflowError::ConnectorError {
        domain,
        retry,
        error,
        source: Some(head_error.into()),
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
            // if we fail to close the multipart, check if it's because we've already finalized this
            // multipart
            validate_already_there(&storage, &path, expected_size as u64, started, &logger, e)
                .await?;
        }

        logger.log_fs_event(0, 1, started.elapsed(), 0, None, 0, 0, 0);
        Ok(FsResponse {
            path,
            data: FsResponseData::MultipartFinalized,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::map_recovery_head_error;
    use arroyo_rpc::errors::{DataflowError, ErrorDomain, RetryHint, StorageError, TaskError};
    use object_store::Error;
    use std::io;

    fn source() -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(io::Error::other("test error"))
    }

    #[test]
    fn recovery_head_auth_errors_preserve_original_message_and_details() {
        for error in [
            Error::PermissionDenied {
                path: "test.parquet".to_string(),
                source: Box::new(io::Error::other("403 Forbidden")),
            },
            Error::Unauthenticated {
                path: "test.parquet".to_string(),
                source: Box::new(io::Error::other("401 Unauthorized")),
            },
        ] {
            let expected_message = error.to_string();
            let head_error = StorageError::ObjectStore(error);
            let expected_details = head_error.to_string();
            let mapped: TaskError = map_recovery_head_error(head_error).unwrap().into();

            assert_eq!(mapped.domain, ErrorDomain::User);
            assert_eq!(mapped.retry_hint, RetryHint::NoRetry);
            assert_eq!(
                mapped.message,
                format!("error in connector: {expected_message}")
            );
            let details = mapped
                .details
                .expect("HEAD error should retain diagnostic details");
            assert!(details.contains(&expected_details));
            assert!(details.contains(&expected_message));
        }
    }

    #[test]
    fn recovery_head_transient_errors_remain_retryable() {
        let error = StorageError::ObjectStore(Error::Generic {
            store: "test",
            source: source(),
        });

        let expected_source = error.to_string();
        let mapped = map_recovery_head_error(error).unwrap();
        let DataflowError::ConnectorError {
            domain,
            retry,
            error,
            source,
        } = mapped
        else {
            panic!("expected connector error, got {mapped:?}");
        };

        assert_eq!(domain, ErrorDomain::External);
        assert_eq!(retry, RetryHint::WithBackoff);
        assert_eq!(
            error,
            "failed to validate whether failed multipart write successfully completed"
        );
        assert_eq!(source.unwrap().to_string(), expected_source);
    }

    #[test]
    fn recovery_head_not_found_uses_original_write_error() {
        let error = StorageError::ObjectStore(Error::NotFound {
            path: "test.parquet".to_string(),
            source: source(),
        });

        assert!(map_recovery_head_error(error).is_none());
    }
}
