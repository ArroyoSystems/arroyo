use super::checkpoint::{FileToCommit, FileToCommitType, InProgressFile, InProgressFileState};
use crate::filesystem::sink::iceberg::metadata::IcebergFileMetadata;
use crate::filesystem::sink::v2::SinkConfig;
use crate::filesystem::sink::v2::uploads::{
    FsResponseData, UploadFuture, create_multipart_finalize_future, create_multipart_init_future,
    create_part_upload_future, create_single_file_upload_future,
};
use crate::filesystem::sink::{
    BatchBufferingWriter, FsEventLogger, MultiPartWriterStats, split_into_parts,
};
use arrow::record_batch::RecordBatch;
use arroyo_rpc::connector_err;
use arroyo_rpc::errors::DataflowResult;
use arroyo_storage::StorageProvider;
use bytes::Bytes;
use object_store::MultipartId;
use object_store::path::Path;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct Part {
    pub part_index: usize,
    // Some if finished, None if inflight
    pub content_id: Option<String>,
}

#[derive(Default)]
pub enum OpenFileState<BBW: BatchBufferingWriter> {
    New {
        writer: BBW,
    },
    MultipartStarting {
        writer: BBW,
        then_close: bool,
    },
    MultipartStarted {
        writer: BBW,
        multipart_id: Arc<MultipartId>,
        parts: Vec<Part>,
    },
    Recovering {
        multipart_id: Arc<MultipartId>,
        parts: Vec<Part>,
        trailing_bytes: Bytes,
        iceberg_metadata: Option<IcebergFileMetadata>,
        total_size: usize,
    },
    ClosingMulti {
        multipart_id: Arc<MultipartId>,
        parts: Vec<Part>,
        iceberg_metadata: Option<IcebergFileMetadata>,
        total_size: usize,
    },
    ClosingSingle {
        bytes: Bytes,
        iceberg_metadata: Option<IcebergFileMetadata>,
    },
    Finishing {
        iceberg_metadata: Option<IcebergFileMetadata>,
        total_size: usize,
    },
    Closed {
        iceberg_metadata: Option<IcebergFileMetadata>,
        total_size: usize,
    },

    #[default]
    Failed,
}

impl<BBW: BatchBufferingWriter> Debug for OpenFileState<BBW> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenFileState::New { writer } => write!(
                f,
                "OpenFileState::New ( buffered: {} bytes )",
                writer.buffered_bytes()
            ),
            OpenFileState::MultipartStarting { writer, then_close } => write!(
                f,
                "OpenFileState::MultipartStarting ( buffered: {} bytes, closing: {} )",
                writer.buffered_bytes(),
                then_close
            ),
            OpenFileState::MultipartStarted {
                writer,
                parts,
                multipart_id,
            } => {
                write!(
                    f,
                    "OpenFileState::MultipartStarted ( buffered: {} bytes, parts: {:?}, multipart_id: {} )",
                    writer.buffered_bytes(),
                    parts,
                    multipart_id
                )
            }
            OpenFileState::Recovering {
                multipart_id,
                parts,
                trailing_bytes,
                ..
            } => {
                write!(
                    f,
                    "OpenFileState::Recovering ( parts: {:?}, multipart_id: {:?}, trailing_bytes: {} bytes )",
                    parts,
                    multipart_id,
                    trailing_bytes.len()
                )
            }
            OpenFileState::ClosingMulti {
                multipart_id,
                parts,
                ..
            } => {
                write!(
                    f,
                    "OpenFileState::ClosingMulti ( parts: {:?}, multipart_id: {:?} )",
                    parts, multipart_id
                )
            }
            OpenFileState::ClosingSingle { bytes, .. } => {
                write!(
                    f,
                    "OpenFileState::ClosingSingle ( buffered: {} bytes )",
                    bytes.len()
                )
            }
            OpenFileState::Finishing { .. } => {
                write!(f, "OpenFileState::FinishingMultipart")
            }
            OpenFileState::Closed { .. } => {
                write!(f, "OpenFileState::Closed")
            }
            OpenFileState::Failed => {
                write!(f, "OpenFileState::Failed")
            }
        }
    }
}

pub struct OpenFile<BBW: BatchBufferingWriter> {
    pub path: Arc<Path>,
    pub stats: MultiPartWriterStats,
    pub logger: FsEventLogger,
    pub state: OpenFileState<BBW>,
    pub target_part_size_bytes: usize,
    pub minimum_multipart_size: usize,
    pub storage_provider: Arc<StorageProvider>,
}

impl<BBW: BatchBufferingWriter> OpenFile<BBW> {
    pub fn new(
        path: Arc<Path>,
        writer: BBW,
        logger: FsEventLogger,
        storage_provider: Arc<StorageProvider>,
        representative_timestamp: SystemTime,
        config: &SinkConfig,
    ) -> Self {
        OpenFile {
            path,
            stats: MultiPartWriterStats {
                bytes_written: 0,
                parts_written: 0,
                last_write_at: Instant::now(),
                first_write_at: Instant::now(),
                representative_timestamp,
            },
            logger,
            state: OpenFileState::New { writer },
            target_part_size_bytes: config.target_part_size(),
            minimum_multipart_size: config.minimum_multipart_size(),
            storage_provider,
        }
    }

    pub fn from_checkpoint(
        file: InProgressFile,
        storage_provider: Arc<StorageProvider>,
        config: &SinkConfig,
        logger: FsEventLogger,
    ) -> DataflowResult<Self> {
        let path = Path::parse(&file.path).map_err(|e| {
            connector_err!(
                Internal,
                NoRetry,
                "invalid path ({}) in checkpoint file: {:?}",
                file.path,
                e
            )
        })?;

        let trailing_bytes: Bytes = file.data.into();

        let state = match file.state {
            InProgressFileState::New => OpenFileState::ClosingSingle {
                bytes: trailing_bytes,
                iceberg_metadata: file.metadata,
            },
            InProgressFileState::MultipartStarted {
                multipart_id,
                parts,
            } => {
                let completed_parts: Vec<Part> = parts
                    .into_iter()
                    .enumerate()
                    .map(|(i, content_id)| Part {
                        part_index: i,
                        content_id: Some(content_id),
                    })
                    .collect();

                if trailing_bytes.is_empty() {
                    // all data already uploaded as parts, we can immediately move to finalization
                    OpenFileState::ClosingMulti {
                        multipart_id: Arc::new(multipart_id),
                        parts: completed_parts,
                        iceberg_metadata: file.metadata,
                        total_size: file.total_size,
                    }
                } else {
                    // we need to upload the last parts
                    OpenFileState::Recovering {
                        multipart_id: Arc::new(multipart_id),
                        parts: completed_parts,
                        trailing_bytes,
                        iceberg_metadata: file.metadata,
                        total_size: file.total_size,
                    }
                }
            }
        };

        Ok(Self {
            path: Arc::new(path),
            stats: MultiPartWriterStats {
                bytes_written: file.total_size,
                parts_written: 0,
                last_write_at: Instant::now(),
                first_write_at: Instant::now(),
                representative_timestamp: SystemTime::UNIX_EPOCH,
            },
            logger,
            state,
            target_part_size_bytes: config.target_part_size(),
            minimum_multipart_size: config.minimum_multipart_size(),
            storage_provider,
        })
    }

    pub fn from_commit(
        file: FileToCommit,
        storage_provider: Arc<StorageProvider>,
        config: &SinkConfig,
        logger: FsEventLogger,
    ) -> DataflowResult<Self> {
        let path = Path::parse(&file.path).map_err(|e| {
            connector_err!(
                Internal,
                NoRetry,
                "invalid path ({}) in commit file: {:?}",
                file.path,
                e
            )
        })?;

        Ok(Self {
            path: Arc::new(path),
            stats: MultiPartWriterStats {
                bytes_written: 0,
                parts_written: 0,
                last_write_at: Instant::now(),
                first_write_at: Instant::now(),
                representative_timestamp: SystemTime::UNIX_EPOCH,
            },
            logger,
            state: match file.typ {
                FileToCommitType::Multipart {
                    multipart_id,
                    parts,
                    total_size,
                } => OpenFileState::ClosingMulti {
                    total_size,
                    multipart_id: multipart_id.into(),
                    parts: parts
                        .into_iter()
                        .enumerate()
                        .map(|(i, p)| Part {
                            part_index: i,
                            content_id: Some(p),
                        })
                        .collect(),
                    iceberg_metadata: file.iceberg_metadata,
                },
                FileToCommitType::Single { data } => OpenFileState::ClosingSingle {
                    bytes: data.into(),
                    iceberg_metadata: file.iceberg_metadata,
                },
            },
            target_part_size_bytes: config.target_part_size(),
            minimum_multipart_size: config.minimum_multipart_size(),
            storage_provider,
        })
    }

    pub fn is_writable(&self) -> bool {
        matches!(
            self.state,
            OpenFileState::New { .. }
                | OpenFileState::MultipartStarting {
                    then_close: false,
                    ..
                }
                | OpenFileState::MultipartStarted { .. }
        )
    }

    pub fn metadata_for_closed(self) -> DataflowResult<(usize, Option<IcebergFileMetadata>)> {
        let OpenFileState::Closed {
            iceberg_metadata,
            total_size,
        } = self.state
        else {
            return Err(connector_err!(
                Internal,
                WithBackoff,
                "file {} was not closed after attempting in commit",
                self.path
            ));
        };

        Ok((total_size, iceberg_metadata))
    }

    pub fn ready_to_finalize(&self) -> bool {
        if let OpenFileState::ClosingMulti { parts, .. } = &self.state {
            // we're ready to finalize once all parts have been uploaded
            parts.iter().all(|p| p.content_id.is_some())
        } else {
            matches!(self.state, OpenFileState::ClosingSingle { .. })
        }
    }

    fn should_flush(&self) -> bool {
        match &self.state {
            OpenFileState::New { writer }
            | OpenFileState::MultipartStarting { writer, .. }
            | OpenFileState::MultipartStarted { writer, .. } => {
                writer.buffered_bytes() >= self.target_part_size_bytes
            }
            _ => false,
        }
    }

    fn maybe_flush(&mut self) -> DataflowResult<Option<UploadFuture>> {
        let should_flush = self.should_flush();

        let mut future = None;

        self.state = match mem::take(&mut self.state) {
            OpenFileState::New { writer } => {
                if should_flush
                    || writer.unflushed_bytes() + writer.buffered_bytes()
                        >= self.minimum_multipart_size
                {
                    future = Some(create_multipart_init_future(
                        self.storage_provider.clone(),
                        self.path.clone(),
                        self.logger.clone(),
                    ));
                    OpenFileState::MultipartStarting {
                        writer,
                        then_close: false,
                    }
                } else {
                    OpenFileState::New { writer }
                }
            }
            s @ OpenFileState::MultipartStarting { .. } => {
                // nothing to do yet, we need to wait for the multipart to be ready
                s
            }
            OpenFileState::MultipartStarted {
                mut writer,
                multipart_id,
                mut parts,
            } if should_flush => {
                // TODO: plug in to a global max in flight parts limit
                let buf = writer.split_to(self.target_part_size_bytes);
                assert!(!buf.is_empty(), "Trying to write empty part file");

                self.stats.bytes_written += buf.len();
                self.stats.parts_written += 1;

                // TODO: it's possible that we could start multiple uploads at this point and get
                //  a bit better throughput
                future = Some(create_part_upload_future(
                    self.storage_provider.clone(),
                    self.path.clone(),
                    multipart_id.clone(),
                    parts.len(),
                    buf,
                    self.logger.clone(),
                ));

                parts.push(Part {
                    part_index: parts.len(),
                    content_id: None,
                });

                OpenFileState::MultipartStarted {
                    writer,
                    multipart_id,
                    parts,
                }
            }
            s @ OpenFileState::MultipartStarted { .. } => {
                // we need more data to start the next file
                s
            }
            OpenFileState::Finishing { .. } => {
                return Err(connector_err!(
                    Internal,
                    WithBackoff,
                    "tried to push data to a file {:?} waiting to finish its multipart upload",
                    self.path
                ));
            }
            OpenFileState::Recovering { .. }
            | OpenFileState::ClosingSingle { .. }
            | OpenFileState::ClosingMulti { .. } => {
                return Err(connector_err!(
                    Internal,
                    WithBackoff,
                    "tried to push data to a closing/recovering file {:?}",
                    self.path
                ));
            }
            OpenFileState::Closed { .. } => {
                return Err(connector_err!(
                    Internal,
                    WithBackoff,
                    "tried to push data to a closed file {:?}",
                    self.path
                ));
            }
            OpenFileState::Failed => {
                return Err(connector_err!(
                    Internal,
                    WithBackoff,
                    "tried to push data to a failed file {:?}",
                    self.path
                ));
            }
        };

        Ok(future)
    }

    pub fn add_batch(&mut self, batch: &RecordBatch) -> DataflowResult<Option<UploadFuture>> {
        match &mut self.state {
            OpenFileState::New { writer }
            | OpenFileState::MultipartStarting {
                writer,
                then_close: false,
            }
            | OpenFileState::MultipartStarted { writer, .. } => {
                writer.add_batch_data(batch);
                self.maybe_flush()
            }
            s => Err(connector_err!(
                Internal,
                WithBackoff,
                "tried to push data to a file {} in an invalid state {:?}",
                self.path,
                s
            )),
        }
    }

    pub fn handle_event(&mut self, event: FsResponseData) -> DataflowResult<Vec<UploadFuture>> {
        debug!(path = ?self.path, event = ?event, "received FS event");

        match event {
            FsResponseData::MultipartInitialized { multipart_id } => {
                self.multipart_initialized(multipart_id)
            }
            FsResponseData::PartFinished {
                part_index,
                content_id,
            } => {
                self.part_upload_completed(part_index, content_id)?;
                Ok(vec![])
            }
            FsResponseData::SingleFileFinished => {
                let OpenFileState::Finishing {
                    iceberg_metadata,
                    total_size,
                } = &mut self.state
                else {
                    return Err(connector_err!(
                        Internal,
                        NoRetry,
                        "received single file finish event in invalid file state: {:?}",
                        self.state
                    ));
                };

                self.state = OpenFileState::Closed {
                    iceberg_metadata: iceberg_metadata.take(),
                    total_size: *total_size,
                };
                Ok(vec![])
            }
            FsResponseData::MultipartFinalized => {
                self.state = match mem::take(&mut self.state) {
                    OpenFileState::Finishing {
                        iceberg_metadata,
                        total_size,
                        ..
                    } => OpenFileState::Closed {
                        iceberg_metadata,
                        total_size,
                    },
                    s @ OpenFileState::Closed { .. } => {
                        warn!(data = ?event, file = ?self.path, "received duplicate finish event for already-closed file");
                        s
                    }
                    s => {
                        return Err(connector_err!(
                            Internal,
                            NoRetry,
                            "received finish event in invalid file state: {:?}",
                            s
                        ));
                    }
                };

                Ok(vec![])
            }
        }
    }

    pub fn multipart_initialized(&mut self, id: MultipartId) -> DataflowResult<Vec<UploadFuture>> {
        let mut close_after = false;
        self.state = match mem::take(&mut self.state) {
            OpenFileState::New { .. } => {
                return Err(connector_err!(
                    Internal,
                    WithBackoff,
                    "received multipart initialized response for file ({}) that is in the New state",
                    self.path
                ));
            }
            OpenFileState::MultipartStarting { writer, then_close } => {
                close_after = then_close;
                OpenFileState::MultipartStarted {
                    writer,
                    multipart_id: Arc::new(id),
                    parts: vec![],
                }
            }
            OpenFileState::MultipartStarted {
                writer,
                multipart_id,
                parts,
            } => {
                if **multipart_id != id {
                    return Err(connector_err!(
                        Internal,
                        WithBackoff,
                        "received multipart initialized response for file ({}) that was already \
                        initialized with a different id",
                        self.path
                    ));
                }
                warn!(file = ?self.path, "received multiple multipart initialized responses for file");
                OpenFileState::MultipartStarted {
                    writer,
                    multipart_id,
                    parts,
                }
            }
            s => {
                warn!(file = ?self.path, state = ?s, "received multipart initialized responses in invalid state");
                s
            }
        };

        if close_after {
            self.close()
        } else {
            Ok(self.maybe_flush()?.into_iter().collect())
        }
    }

    pub fn part_upload_completed(
        &mut self,
        part_index: usize,
        content_id: String,
    ) -> DataflowResult<()> {
        match &mut self.state {
            s @ OpenFileState::New { .. }
            | s @ OpenFileState::MultipartStarting { .. }
            | s @ OpenFileState::ClosingSingle { .. }
            | s @ OpenFileState::Recovering { .. } => {
                return Err(connector_err!(
                    Internal,
                    WithBackoff,
                    "received part upload notification in the {:?} phase for {}, which should not happen",
                    s,
                    self.path
                ));
            }
            OpenFileState::MultipartStarted { parts, .. } => {
                let part = parts.get_mut(part_index).ok_or_else(||
                    connector_err!(Internal, WithBackoff,
                        "received part upload notification for unknown part {} for {}, which should not happen", part_index, self.path))?;
                assert_eq!(part.part_index, part_index);

                part.content_id = Some(content_id);
            }
            OpenFileState::ClosingMulti { parts, .. } => {
                let part = parts.get_mut(part_index).ok_or_else(||
                    connector_err!(Internal, WithBackoff,
                        "received part upload notification for unknown part {} for {}, which should not happen", part_index, self.path))?;
                assert_eq!(part.part_index, part_index);

                part.content_id = Some(content_id);
            }
            OpenFileState::Finishing { .. }
            | OpenFileState::Closed { .. }
            | OpenFileState::Failed => {
                warn!(
                    "Received part upload complete for closed file ({})",
                    self.path
                );
            }
        };

        Ok(())
    }

    fn write_last_parts(
        &mut self,
        bytes: Bytes,
        multipart_id: Arc<MultipartId>,
        parts: &mut Vec<Part>,
    ) -> Vec<UploadFuture> {
        let len = bytes.len();

        if len == 0 {
            return vec![];
        }

        let futures = if self.storage_provider.requires_same_part_sizes()
            && bytes.len() > self.target_part_size_bytes
        {
            // our last part is bigger than our part size, which isn't allowed by some object stores
            // so we need to split it up

            let part_bytes = split_into_parts(bytes, self.target_part_size_bytes);

            debug!(
                "final multipart upload ({}) is bigger than part size ({}) so splitting into {}",
                len,
                self.target_part_size_bytes,
                part_bytes.len()
            );

            part_bytes
                .into_iter()
                .enumerate()
                .map(|(i, part)| {
                    create_part_upload_future(
                        self.storage_provider.clone(),
                        self.path.clone(),
                        multipart_id.clone(),
                        parts.len() + i,
                        part,
                        self.logger.clone(),
                    )
                })
                .collect()
        } else {
            vec![create_part_upload_future(
                self.storage_provider.clone(),
                self.path.clone(),
                multipart_id.clone(),
                parts.len(),
                bytes,
                self.logger.clone(),
            )]
        };

        self.stats.parts_written += futures.len();
        self.stats.bytes_written += len;

        for _ in futures.iter() {
            parts.push(Part {
                part_index: parts.len(),
                content_id: None,
            });
        }

        futures
    }

    pub fn close(&mut self) -> DataflowResult<Vec<UploadFuture>> {
        let mut futures = vec![];

        self.state = match mem::take(&mut self.state) {
            OpenFileState::New { mut writer } => {
                // we haven't started a multipart, so we just write a single file
                // TODO: handle the (implausible but possible) case that the single file is larger
                //  than the max part size for the object store (typically 5GB)

                let (bytes, iceberg_metadata) = writer.close();

                OpenFileState::ClosingSingle {
                    bytes,
                    iceberg_metadata,
                }
            }
            OpenFileState::MultipartStarting { writer, .. } => {
                // we need to wait for the multipart to finish creating before we can write our part
                OpenFileState::MultipartStarting {
                    writer,
                    then_close: true,
                }
            }
            OpenFileState::MultipartStarted {
                mut writer,
                multipart_id,
                mut parts,
            } => {
                // write our last parts
                let (bytes, iceberg_metadata) = writer.close();
                futures = self.write_last_parts(bytes, multipart_id.clone(), &mut parts);

                OpenFileState::ClosingMulti {
                    multipart_id: multipart_id.clone(),
                    parts,
                    iceberg_metadata,
                    total_size: self.stats.bytes_written,
                }
            }
            OpenFileState::Recovering {
                multipart_id,
                mut parts,
                trailing_bytes,
                iceberg_metadata,
                total_size,
            } => {
                futures =
                    self.write_last_parts(trailing_bytes.clone(), multipart_id.clone(), &mut parts);

                OpenFileState::ClosingMulti {
                    multipart_id,
                    parts,
                    iceberg_metadata,
                    total_size: total_size + trailing_bytes.len(),
                }
            }
            s @ OpenFileState::ClosingSingle { .. }
            | s @ OpenFileState::ClosingMulti { .. }
            | s @ OpenFileState::Closed { .. }
            | s @ OpenFileState::Finishing { .. } => {
                // this shouldn't happen, but it also shouldn't cause a failure
                warn!(file = ?self.path, "close called on an already-closed file");

                s
            }
            OpenFileState::Failed => {
                return Err(connector_err!(
                    Internal,
                    WithBackoff,
                    "close called on a failed file {}",
                    self.path
                ));
            }
        };

        Ok(futures)
    }

    // Callers are responsible for first calling read_to_finalize() to determine that the file is
    // ready to be finalized
    pub fn into_commit_file(self) -> DataflowResult<FileToCommit> {
        Ok(match self.state {
            OpenFileState::ClosingMulti {
                multipart_id,
                parts,
                iceberg_metadata,
                total_size,
            } => FileToCommit {
                path: self.path.to_string(),
                typ: FileToCommitType::Multipart {
                    multipart_id: multipart_id.to_string(),
                    parts: parts
                        .into_iter()
                        .map(|p| {
                            p.content_id
                                .expect("file was not ready before to_commit_file was called!")
                        })
                        .collect(),
                    total_size,
                },
                iceberg_metadata,
            },
            OpenFileState::ClosingSingle {
                bytes,
                iceberg_metadata,
            } => FileToCommit {
                path: self.path.to_string(),
                typ: FileToCommitType::Single {
                    data: bytes.to_vec(),
                },
                iceberg_metadata,
            },
            s => {
                return Err(connector_err!(
                    Internal,
                    WithBackoff,
                    "tried to create commit file {} in invalid state {:?}",
                    self.path,
                    s
                ));
            }
        })
    }

    pub fn finalize(&mut self) -> DataflowResult<UploadFuture> {
        let (fut, total_size, meta) = match &mut self.state {
            OpenFileState::ClosingMulti {
                multipart_id,
                parts,
                total_size,
                iceberg_metadata,
                ..
            } => {
                let future = create_multipart_finalize_future(
                    self.storage_provider.clone(),
                    self.path.clone(),
                    multipart_id.clone(),
                    parts.drain(..).map(|p| p.content_id.unwrap()).collect(),
                    *total_size,
                    self.logger.clone(),
                );

                (future, *total_size, iceberg_metadata.take())
            }
            OpenFileState::ClosingSingle {
                bytes,
                iceberg_metadata,
            } => {
                let future = create_single_file_upload_future(
                    self.storage_provider.clone(),
                    self.path.clone(),
                    bytes.clone(),
                    self.logger.clone(),
                );

                (future, bytes.len(), iceberg_metadata.take())
            }
            s => {
                return Err(connector_err!(
                    Internal,
                    WithBackoff,
                    "tried to finalize file {} in invalid state {:?}",
                    self.path,
                    s
                ));
            }
        };

        self.state = OpenFileState::Finishing {
            iceberg_metadata: meta,
            total_size,
        };

        Ok(fut)
    }

    pub fn as_checkpoint(&mut self) -> DataflowResult<InProgressFile> {
        let (data, metadata, state) = match &mut self.state {
            OpenFileState::New { writer } => {
                let (bytes, metadata) = writer.get_trailing_bytes_for_checkpoint();

                (bytes, metadata, InProgressFileState::New)
            }
            OpenFileState::MultipartStarting { .. } => {
                return Err(connector_err!(
                    Internal,
                    WithBackoff,
                    "as_checkpoint called on a file ({}) without \
                waiting for multipart creation operations to be completed",
                    self.path
                ))?;
            }
            OpenFileState::MultipartStarted {
                multipart_id,
                parts,
                writer,
            } => {
                let parts: DataflowResult<Vec<String>> = parts
                    .iter()
                    .map(|p| {
                        p.content_id.as_ref().map(|p| p.to_string()).ok_or_else(|| {
                            connector_err!(
                                Internal,
                                WithBackoff,
                                "as_checkpoint called on a file ({}) without \
                waiting for all part uploads to be complete ({})",
                                self.path,
                                p.part_index
                            )
                        })
                    })
                    .collect();

                let (bytes, metadata) = writer.get_trailing_bytes_for_checkpoint();

                (
                    bytes,
                    metadata,
                    InProgressFileState::MultipartStarted {
                        multipart_id: multipart_id.to_string(),
                        parts: parts?,
                    },
                )
            }
            OpenFileState::Recovering { .. }
            | OpenFileState::ClosingSingle { .. }
            | OpenFileState::ClosingMulti { .. }
            | OpenFileState::Finishing { .. }
            | OpenFileState::Closed { .. }
            | OpenFileState::Failed => {
                return Err(connector_err!(
                    Internal,
                    WithBackoff,
                    "invalid state for as_checkpoint call on file {}: {:?}",
                    self.path,
                    self.state
                ));
            }
        };

        Ok(InProgressFile {
            path: self.path.to_string(),
            total_size: self.stats.bytes_written,
            data,
            metadata,
            state,
        })
    }
}
