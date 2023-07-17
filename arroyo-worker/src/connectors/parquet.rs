use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    io::Write,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{bail, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use futures::{stream::FuturesUnordered, Future};
use futures::{stream::StreamExt, TryStreamExt};
use object_store::{
    aws::{AmazonS3Builder, AwsCredential},
    local::LocalFileSystem,
    path::Path,
    CredentialProvider, MultipartId, ObjectStore, UploadPart,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use rusoto_core::credential::{DefaultCredentialsProvider, ProvideAwsCredentials};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{info, warn};
use typify::import_types;

import_types!(schema = "../connector-schemas/parquet/table.json");

use arroyo_types::*;

use super::{
    two_phase_committer::{TwoPhaseCommitter, TwoPhaseCommitterOperator},
    OperatorConfig,
};

pub struct ParquetSink<K: Key, T: Data + Sync, R: MultiPartWriter<InputType = T> + Send + 'static> {
    sender: Sender<ParquetMessages<T>>,
    checkpoint_receiver: Receiver<CheckpointData<T>>,
    _ts: PhantomData<(K, R)>,
}

impl<
        K: Key,
        T: Data + Sync,
        R: MultiPartWriter<InputType = T, ConfigT = ParquetWriterProperties> + Send + 'static,
    > ParquetSink<K, T, R>
{
    pub fn generic_from_config(config: &str) -> TwoPhaseCommitterOperator<K, T, Self> {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for Parquet");
        let table: ParquetTable =
            serde_json::from_value(config.table).expect("Invalid table config for Parquet");
        let write_target = table.write_target;
        let file_settings = table.file_settings.unwrap_or(FileSettings {
            inactivity_rollover_seconds: None,
            max_parts: None,
            rollover_seconds: None,
            target_file_size: None,
            target_part_size: None,
        });
        let format_settings = table.format_settings.unwrap_or(FormatSettings {
            compression: None,
            row_batch_size: None,
            row_group_size: None,
        });
        let committer = match write_target {
            Destination::LocalFilesystem { local_directory } => {
                Self::new_local(&local_directory, file_settings, format_settings)
            }
            Destination::S3Bucket {
                s3_bucket,
                s3_directory,
            } => Self::new_s3(&s3_bucket, &s3_directory, file_settings, format_settings),
        };
        TwoPhaseCommitterOperator::new(committer)
    }

    pub fn new_local(
        local_path: &str,
        file_settings: FileSettings,
        format_settings: FormatSettings,
    ) -> Self {
        Self::new_from_object_store(
            Box::new(LocalFileSystem::new()),
            local_path,
            file_settings,
            format_settings,
        )
    }

    pub fn new_s3(
        bucket: &str,
        key: &str,
        file_settings: FileSettings,
        format_settings: FormatSettings,
    ) -> Self {
        let object_store = Box::new(
            // use default credentialsp
            AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_credentials(Arc::new(S3Credentialing::try_new().unwrap()))
                .with_region("us-west-2")
                .build()
                .unwrap(),
        );
        Self::new_from_object_store(object_store, key, file_settings, format_settings)
    }

    fn new_from_object_store(
        object_store: Box<dyn ObjectStore>,
        path: &str,
        file_settings: FileSettings,
        format_settings: FormatSettings,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(10000);
        let (checkpoint_sender, checkpoint_receiver) = tokio::sync::mpsc::channel(10000);
        let path = path.into();
        tokio::spawn(async move {
            let mut writer = AsyncMultipartParquetWriter::<T, R>::new(
                path,
                Arc::new(object_store),
                receiver,
                checkpoint_sender,
                file_settings,
                format_settings,
            );
            writer.run().await.unwrap();
        });

        Self {
            sender,
            checkpoint_receiver,
            _ts: PhantomData,
        }
    }
}

#[derive(Debug)]
enum ParquetMessages<T: Data> {
    Data {
        value: T,
        time: SystemTime,
    },
    Init {
        max_file_index: usize,
        subtask_id: usize,
        recovered_files: Vec<InProgressFileCheckpoint<T>>,
    },
    Checkpoint {
        subtask_id: usize,
        then_stop: bool,
    },
    FilesToFinish(Vec<FileToFinish>),
}

#[derive(Debug)]
enum CheckpointData<T: Data> {
    InProgressFileCheckpoint(InProgressFileCheckpoint<T>),
    Finished { max_file_index: usize },
}

#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq)]
struct InProgressFileCheckpoint<T: Data> {
    filename: String,
    data: FileCheckpointData,
    buffered_data: Vec<T>,
}

#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq)]
pub enum FileCheckpointData {
    Empty,
    MultiPartNotCreated {
        parts_to_add: Vec<Vec<u8>>,
        trailing_bytes: Option<Vec<u8>>,
    },
    MultiPartInFlight {
        multi_part_upload_id: String,
        in_flight_parts: Vec<InFlightPartCheckpoint>,
        trailing_bytes: Option<Vec<u8>>,
    },
    MultiPartWriterClosed {
        multi_part_upload_id: String,
        in_flight_parts: Vec<InFlightPartCheckpoint>,
    },
    MultiPartWriterUploadCompleted {
        multi_part_upload_id: String,
        completed_parts: Vec<String>,
    },
}

#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq)]
pub enum InFlightPartCheckpoint {
    FinishedPart { part: usize, content_id: String },
    InProgressPart { part: usize, data: Vec<u8> },
}

struct S3Credentialing {
    credentials_provider: DefaultCredentialsProvider,
}

impl Debug for S3Credentialing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Credentialing").finish()
    }
}

impl S3Credentialing {
    fn try_new() -> Result<Self> {
        Ok(Self {
            credentials_provider: DefaultCredentialsProvider::new()?,
        })
    }
}

#[async_trait::async_trait]
impl CredentialProvider for S3Credentialing {
    #[doc = " The type of credential returned by this provider"]
    type Credential = AwsCredential;

    /// Return a credential
    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let credentials = self
            .credentials_provider
            .credentials()
            .await
            .map_err(|err| object_store::Error::Generic {
                store: "s3",
                source: Box::new(err),
            })?;
        Ok(Arc::new(AwsCredential {
            key_id: credentials.aws_access_key_id().to_string(),
            secret_key: credentials.aws_secret_access_key().to_string(),
            token: credentials.token().clone(),
        }))
    }
}

struct AsyncMultipartParquetWriter<T: Data + Sync, R: MultiPartWriter> {
    path: Path,
    current_writer_name: String,
    max_file_index: usize,
    subtask_id: usize,
    object_store: Arc<dyn ObjectStore>,
    writers: HashMap<String, R>,
    receiver: Receiver<ParquetMessages<T>>,
    checkpoint_sender: Sender<CheckpointData<T>>,
    futures: FuturesUnordered<BoxedTryFuture<MultipartCallbackWithName>>,
    files_to_finish: Vec<FileToFinish>,
    properties: ParquetWriterProperties,
    rolling_policy: RollingPolicy,
}

#[async_trait]
pub trait MultiPartWriter {
    type InputType: Data;
    type ConfigT;
    fn new(object_store: Arc<dyn ObjectStore>, path: Path, config: Self::ConfigT) -> Self;

    fn name(&self) -> String;

    async fn insert_value(
        &mut self,
        value: Self::InputType,
        time: SystemTime,
    ) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>>;

    fn handle_initialization(
        &mut self,
        multipart_id: String,
    ) -> Result<Vec<BoxedTryFuture<MultipartCallbackWithName>>>;

    fn handle_completed_part(
        &mut self,
        part_idx: usize,
        upload_part: UploadPart,
    ) -> Result<Option<FileToFinish>>;

    fn get_in_progress_checkpoint(&mut self) -> FileCheckpointData;

    fn currently_buffered_data(&mut self) -> Vec<Self::InputType>;

    fn close(&mut self) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>>;

    fn stats(&self) -> Option<MultiPartWriterStats>;
}

async fn from_checkpoint(
    path: &Path,
    checkpoint_data: FileCheckpointData,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Option<FileToFinish>> {
    let mut parts = vec![];
    let multipart_id = match checkpoint_data {
        FileCheckpointData::Empty => {
            return Ok(None);
        }
        FileCheckpointData::MultiPartNotCreated {
            parts_to_add,
            trailing_bytes,
        } => {
            let multipart_id = object_store
                .start_multipart(path)
                .await
                .expect("failed to create multipart upload");
            let mut parts = vec![];
            for (part_index, data) in parts_to_add.into_iter().enumerate() {
                let upload_part = object_store
                    .add_multipart(path, &multipart_id, part_index, data.into())
                    .await
                    .unwrap();
                parts.push(upload_part);
            }
            if let Some(trailing_bytes) = trailing_bytes {
                let upload_part = object_store
                    .add_multipart(path, &multipart_id, parts.len(), trailing_bytes.into())
                    .await?;
                parts.push(upload_part);
            }
            multipart_id
        }
        FileCheckpointData::MultiPartInFlight {
            multi_part_upload_id,
            in_flight_parts,
            trailing_bytes,
        } => {
            for data in in_flight_parts.into_iter() {
                match data {
                    InFlightPartCheckpoint::FinishedPart {
                        part: _,
                        content_id,
                    } => parts.push(UploadPart { content_id }),
                    InFlightPartCheckpoint::InProgressPart { part, data } => {
                        let upload_part = object_store
                            .add_multipart(path, &multi_part_upload_id, part, data.into())
                            .await
                            .unwrap();
                        parts.push(upload_part);
                    }
                }
            }
            if let Some(trailing_bytes) = trailing_bytes {
                let upload_part = object_store
                    .add_multipart(
                        path,
                        &multi_part_upload_id,
                        parts.len(),
                        trailing_bytes.into(),
                    )
                    .await?;
                parts.push(upload_part);
            }
            multi_part_upload_id
        }
        FileCheckpointData::MultiPartWriterClosed {
            multi_part_upload_id,
            in_flight_parts,
        } => {
            for (part_index, data) in in_flight_parts.into_iter().enumerate() {
                match data {
                    InFlightPartCheckpoint::FinishedPart {
                        part: _,
                        content_id,
                    } => parts.push(UploadPart { content_id }),
                    InFlightPartCheckpoint::InProgressPart { part: _, data } => {
                        let upload_part = object_store
                            .add_multipart(path, &multi_part_upload_id, part_index, data.into())
                            .await
                            .unwrap();
                        parts.push(upload_part);
                    }
                }
            }
            multi_part_upload_id
        }
        FileCheckpointData::MultiPartWriterUploadCompleted {
            multi_part_upload_id,
            completed_parts,
        } => {
            for content_id in completed_parts {
                parts.push(UploadPart { content_id })
            }
            multi_part_upload_id
        }
    };
    Ok(Some(FileToFinish {
        filename: path.to_string(),
        multi_part_upload_id: multipart_id,
        completed_parts: parts.into_iter().map(|p| p.content_id).collect(),
    }))
}

#[derive(Debug, Clone)]
pub struct ParquetWriterProperties {
    parquet_options: WriterProperties,
    row_batch_size: usize,
    target_part_size: usize,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct FileToFinish {
    filename: String,
    multi_part_upload_id: String,
    completed_parts: Vec<String>,
}

enum RollingPolicy {
    PartLimit(usize),
    SizeLimit(usize),
    InactivityDuration(Duration),
    RolloverDuration(Duration),
    AnyPolicy(Vec<RollingPolicy>),
}

impl RollingPolicy {
    fn should_roll<MPW: MultiPartWriter>(&self, writer: &MPW) -> bool {
        let Some(stats) = writer.stats() else {
            return false
        };
        match self {
            RollingPolicy::PartLimit(part_limit) => stats.parts_written >= *part_limit,
            RollingPolicy::SizeLimit(size_limit) => stats.bytes_written >= *size_limit,
            RollingPolicy::InactivityDuration(duration) => {
                stats.last_write_at.elapsed() >= *duration
            }
            RollingPolicy::RolloverDuration(duration) => {
                stats.first_write_at.elapsed() >= *duration
            }
            RollingPolicy::AnyPolicy(policies) => {
                policies.iter().any(|policy| policy.should_roll(writer))
            }
        }
    }

    fn from_file_settings(file_settings: FileSettings) -> RollingPolicy {
        let mut policies = vec![];
        let part_size_limit = file_settings.max_parts.unwrap_or(1000) as usize;
        // this is a hard limit, so will always be present.
        policies.push(RollingPolicy::PartLimit(part_size_limit));
        if let Some(file_size_target) = file_settings.target_file_size {
            policies.push(RollingPolicy::SizeLimit(file_size_target as usize))
        }
        if let Some(inactivity_timeout) = file_settings
            .inactivity_rollover_seconds
            .map(|seconds| Duration::from_secs(seconds as u64))
        {
            policies.push(RollingPolicy::InactivityDuration(inactivity_timeout))
        }
        if let Some(rollover_timeout) = file_settings
            .rollover_seconds
            .map(|seconds| Duration::from_secs(seconds as u64))
        {
            policies.push(RollingPolicy::RolloverDuration(rollover_timeout))
        }
        if policies.len() == 1 {
            policies.pop().unwrap()
        } else {
            RollingPolicy::AnyPolicy(policies)
        }
    }
}

#[derive(Debug, Clone)]
pub struct MultiPartWriterStats {
    bytes_written: usize,
    parts_written: usize,
    last_write_at: Instant,
    first_write_at: Instant,
}

impl<T, R> AsyncMultipartParquetWriter<T, R>
where
    T: Data + std::marker::Sync,
    R: MultiPartWriter<InputType = T, ConfigT = ParquetWriterProperties>,
{
    fn new(
        path: Path,
        object_store: Arc<dyn ObjectStore>,
        receiver: Receiver<ParquetMessages<T>>,
        checkpoint_sender: Sender<CheckpointData<T>>,
        file_settings: FileSettings,
        format_settings: FormatSettings,
    ) -> Self {
        let row_batch_size = format_settings.row_batch_size.unwrap_or(10_000) as usize;
        let parquet_options = format_settings_to_parquet_writer_options(format_settings);
        let target_part_size = file_settings
            .target_part_size
            .unwrap_or(5 * 1024 * 1024)
            .max(5 * 1024 * 1024) as usize;
        let properties = ParquetWriterProperties {
            parquet_options,
            row_batch_size,
            target_part_size,
        };
        Self {
            path,
            current_writer_name: "".to_string(),
            max_file_index: 0,
            subtask_id: 0,
            object_store,
            writers: HashMap::new(),
            receiver,
            checkpoint_sender,
            futures: FuturesUnordered::new(),
            files_to_finish: Vec::new(),
            properties,
            rolling_policy: RollingPolicy::from_file_settings(file_settings),
        }
    }

    fn add_part_to_finish(&mut self, file_to_finish: FileToFinish) {
        self.files_to_finish.push(file_to_finish);
    }

    async fn run(&mut self) -> Result<()> {
        let mut next_policy_check = tokio::time::Instant::now();
        loop {
            tokio::select! {
                Some(message) = self.receiver.recv() => {
                    match message {
                        ParquetMessages::Data{value, time} => {
                            let Some(writer) = self.writers.get_mut(&self.current_writer_name) else {
                                bail!("expect the current parquet writer to be initialized");
                            };
                            if let Some(future) = writer.insert_value(value, time).await? {
                                self.futures.push(future);
                            }
                        },
                        ParquetMessages::Init {max_file_index, subtask_id, recovered_files } => {
                            if let Some(writer) = self.writers.get_mut(&self.current_writer_name) {
                                if let Some(future) = writer.close()? {
                                    self.futures.push(future);
                                }
                            }
                            self.max_file_index = max_file_index;
                            self.subtask_id = subtask_id;
                            let new_writer = R::new(
                                self.object_store.clone(),
                                // take base path, add max_file_index and subtask_id, plus parquet suffix
                                format!("{}/{}-{}.parquet", self.path, self.max_file_index, self.subtask_id).into(),
                                self.properties.clone(),
                            );
                            self.current_writer_name = new_writer.name();
                            self.writers.insert(new_writer.name(), new_writer);
                            for recovered_file in recovered_files {
                                if let Some(file_to_finish) = from_checkpoint(
                                     &Path::parse(&recovered_file.filename)?, recovered_file.data, self.object_store.clone()).await? {
                                        self.add_part_to_finish(file_to_finish);
                                     }

                                for value in recovered_file.buffered_data {
                                    let Some(writer) = self.writers.get_mut(&self.current_writer_name) else {
                                        bail!("expect the current parquet writer to be initialized");
                                    };
                                    if let Some(future) = writer.insert_value(value, SystemTime::now()).await? {
                                        self.futures.push(future);
                                    }
                                }
                            }
                        },
                        ParquetMessages::Checkpoint { subtask_id, then_stop } => {
                            self.flush_futures().await?;
                            if then_stop {
                                self.stop().await?;
                            }
                            self.take_checkpoint( subtask_id).await?;
                            self.checkpoint_sender.send(CheckpointData::Finished {  max_file_index: self.max_file_index}).await?;
                        },
                        ParquetMessages::FilesToFinish(files_to_finish) =>{
                            for file_to_finish in files_to_finish {
                                info!("finishing {}", file_to_finish.filename);
                                self.finish_file(file_to_finish).await?;
                            }
                            self.checkpoint_sender.send(CheckpointData::Finished {  max_file_index: self.max_file_index}).await?;
                        }
                    }
                }
                Some(result) = self.futures.next() => {
                    let MultipartCallbackWithName { callback, name } = result?;
                    self.process_callback(name, callback)?;
                }
                _ = tokio::time::sleep_until(next_policy_check) => {
                    next_policy_check = tokio::time::Instant::now() + Duration::from_millis(100);
                    if let Some(writer) = self.writers.get_mut(&self.current_writer_name) {
                        if self.rolling_policy.should_roll(writer) {
                            if let Some(future) = writer.close()? {
                                self.futures.push(future);
                            }
                            self.max_file_index += 1;
                            let new_writer = R::new(
                                self.object_store.clone(),
                                // take base path, add max_file_index and subtask_id, plus parquet suffix
                                format!("{}/{}-{}.parquet", self.path, self.max_file_index, self.subtask_id).into(),
                                self.properties.clone(),
                            );
                            self.current_writer_name = new_writer.name();
                            self.writers.insert(new_writer.name(), new_writer);
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn flush_futures(&mut self) -> Result<()> {
        while let Some(MultipartCallbackWithName { callback, name }) =
            self.futures.try_next().await?
        {
            self.process_callback(name, callback)?;
        }
        Ok(())
    }

    fn process_callback(&mut self, name: String, callback: MultipartCallback) -> Result<()> {
        info!("got callback {:?} for writer {}", callback, name);
        let writer = self.writers.get_mut(&name).ok_or_else(|| {
            anyhow::anyhow!(
                "missing parquet writer {} for callback {:?}",
                name,
                callback
            )
        })?;
        match callback {
            MultipartCallback::InitializedMultipart { multipart_id } => {
                self.futures
                    .extend(writer.handle_initialization(multipart_id)?);
                Ok(())
            }
            MultipartCallback::CompletedPart {
                part_idx,
                upload_part,
            } => {
                if let Some(file_to_write) = writer.handle_completed_part(part_idx, upload_part)? {
                    // need the file to finish to be checkpointed first.
                    self.add_part_to_finish(file_to_write);
                    self.writers.remove(&name);
                }
                Ok(())
            }
        }
    }

    async fn finish_file(&mut self, file_to_finish: FileToFinish) -> Result<()> {
        let FileToFinish {
            filename,
            multi_part_upload_id,
            completed_parts,
        } = file_to_finish;
        if completed_parts.len() == 0 {
            warn!("no parts to finish for file {}", filename);
            return Ok(());
        }
        let parts: Vec<_> = completed_parts
            .into_iter()
            .map(|content_id| UploadPart {
                content_id: content_id.clone(),
            })
            .collect();
        let location = Path::parse(&filename)?;
        self.object_store
            .close_multipart(&location, &multi_part_upload_id, parts)
            .await
            .unwrap();
        info!("finished file {}", filename);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(writer) = self.writers.get_mut(&self.current_writer_name) {
            let close_future: Option<BoxedTryFuture<MultipartCallbackWithName>> = writer.close()?;
            if let Some(future) = close_future {
                info!("current writer has close future");
                self.futures.push(future);
            }
        }
        while let Some(result) = self.futures.next().await {
            let MultipartCallbackWithName { callback, name } = result?;
            self.process_callback(name, callback)?;
        }
        Ok(())
    }

    async fn take_checkpoint(&mut self, _subtask_id: usize) -> Result<()> {
        for (filename, writer) in self.writers.iter_mut() {
            let in_progress_checkpoint =
                CheckpointData::InProgressFileCheckpoint(InProgressFileCheckpoint {
                    filename: filename.clone(),
                    data: writer.get_in_progress_checkpoint(),
                    buffered_data: writer.currently_buffered_data(),
                });
            self.checkpoint_sender.send(in_progress_checkpoint).await?;
        }
        for file_to_finish in &self.files_to_finish {
            self.checkpoint_sender
                .send(CheckpointData::InProgressFileCheckpoint(
                    InProgressFileCheckpoint {
                        filename: file_to_finish.filename.clone(),
                        data: FileCheckpointData::MultiPartWriterUploadCompleted {
                            multi_part_upload_id: file_to_finish.multi_part_upload_id.clone(),
                            completed_parts: file_to_finish.completed_parts.clone(),
                        },
                        buffered_data: vec![],
                    },
                ))
                .await?;
        }
        self.files_to_finish.clear();
        Ok(())
    }
}

type BoxedTryFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

pub struct BufferingMultipartWriter<B: BufferedBuilder> {
    builder: B,
    max_record_batch_size: usize,
    target_part_size: usize,
    closed: bool,
    multipart_manager: MultipartManager,
    stats: Option<MultiPartWriterStats>,
}

struct MultipartManager {
    object_store: Arc<dyn ObjectStore>,
    location: Path,
    multipart_id: Option<MultipartId>,
    pushed_parts: Vec<UploadPartOrBufferedData>,
    uploaded_parts: usize,
    pushed_size: usize,
    parts_to_add: Vec<PartToUpload>,
    closed: bool,
}
impl MultipartManager {
    fn new(object_store: Arc<dyn ObjectStore>, location: Path) -> Self {
        Self {
            object_store,
            location,
            multipart_id: None,
            pushed_parts: vec![],
            uploaded_parts: 0,
            pushed_size: 0,
            parts_to_add: vec![],
            closed: false,
        }
    }

    fn name(&self) -> String {
        self.location.to_string()
    }

    fn write_next_part(
        &mut self,
        data: Vec<u8>,
    ) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>> {
        match &self.multipart_id {
            Some(_multipart_id) => Ok(Some(self.get_part_upload_future(PartToUpload {
                part_index: self.pushed_parts.len(),
                byte_data: data,
            })?)),
            None => {
                let is_first_part = self.parts_to_add.is_empty();
                self.parts_to_add.push(PartToUpload {
                    byte_data: data,
                    part_index: self.parts_to_add.len(),
                });
                if is_first_part {
                    // start a new multipart upload
                    Ok(Some(self.get_initialize_multipart_future()?))
                } else {
                    Ok(None)
                }
            }
        }
    }
    // Future for uploading a part of a multipart upload.
    // Argument is either from a newly flushed part or from parts_to_add.
    fn get_part_upload_future(
        &mut self,
        part_to_upload: PartToUpload,
    ) -> Result<BoxedTryFuture<MultipartCallbackWithName>> {
        self.pushed_parts
            .push(UploadPartOrBufferedData::BufferedData {
                // TODO: use Bytes to avoid clone
                data: part_to_upload.byte_data.clone(),
            });
        let location = self.location.clone();
        let multipart_id = self
            .multipart_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("missing multipart id"))?;
        let object_store = self.object_store.clone();
        Ok(Box::pin(async move {
            let upload_part = object_store
                .add_multipart(
                    &location,
                    &multipart_id,
                    part_to_upload.part_index,
                    part_to_upload.byte_data.into(),
                )
                .await?;
            Ok(MultipartCallbackWithName {
                name: location.to_string(),
                callback: MultipartCallback::CompletedPart {
                    part_idx: part_to_upload.part_index,
                    upload_part,
                },
            })
        }))
    }

    fn get_initialize_multipart_future(
        &mut self,
    ) -> Result<BoxedTryFuture<MultipartCallbackWithName>> {
        let object_store = self.object_store.clone();
        let location = self.location.clone();
        Ok(Box::pin(async move {
            let multipart_id = object_store.start_multipart(&location).await?;
            Ok(MultipartCallbackWithName {
                name: location.to_string(),
                callback: MultipartCallback::InitializedMultipart { multipart_id },
            })
        }))
    }

    fn handle_initialization(
        &mut self,
        multipart_id: String,
    ) -> Result<Vec<BoxedTryFuture<MultipartCallbackWithName>>> {
        // for each part in parts_to_add, start a new part upload
        self.multipart_id = Some(multipart_id);
        std::mem::take(&mut self.parts_to_add)
            .into_iter()
            .map(|part_to_upload| self.get_part_upload_future(part_to_upload))
            .collect::<Result<Vec<_>>>()
    }

    fn handle_completed_part(
        &mut self,
        part_idx: usize,
        upload_part: UploadPart,
    ) -> Result<Option<FileToFinish>> {
        self.pushed_parts[part_idx] = UploadPartOrBufferedData::UploadPart(upload_part);
        self.uploaded_parts += 1;

        if !self.all_uploads_finished() {
            Ok(None)
        } else {
            Ok(Some(FileToFinish {
                filename: self.name(),
                multi_part_upload_id: self
                    .multipart_id
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("need multipart id to complete"))?
                    .clone(),
                completed_parts: self
                    .pushed_parts
                    .iter()
                    .map(|part| match part {
                        UploadPartOrBufferedData::UploadPart(upload_part) => {
                            Ok(upload_part.content_id.clone())
                        }
                        UploadPartOrBufferedData::BufferedData { .. } => {
                            bail!("unfinished part in get_complete_multipart_future")
                        }
                    })
                    .collect::<Result<Vec<_>>>()?,
            }))
        }
    }
    fn all_uploads_finished(&self) -> bool {
        self.closed && self.uploaded_parts == self.pushed_parts.len()
    }

    fn get_closed_file_checkpoint_data(&mut self) -> FileCheckpointData {
        if !self.closed {
            unreachable!("get_closed_file_checkpoint_data called on open file");
        }
        let Some(ref multipart_id) =  self.multipart_id else  {
            if self.pushed_size == 0 {
            return FileCheckpointData::Empty;
            } else {
            return FileCheckpointData::MultiPartNotCreated {
                parts_to_add: self
                    .parts_to_add
                    .iter()
                    .map(|val| val.byte_data.clone())
                    .collect(),
                trailing_bytes: None,
            };
        }
    };
        if self.all_uploads_finished() {
            return FileCheckpointData::MultiPartWriterUploadCompleted {
                multi_part_upload_id: multipart_id.clone(),
                completed_parts: self
                    .pushed_parts
                    .iter()
                    .map(|val| match val {
                        UploadPartOrBufferedData::UploadPart(upload_part) => {
                            upload_part.content_id.clone()
                        }
                        UploadPartOrBufferedData::BufferedData { .. } => {
                            unreachable!("unfinished part in get_closed_file_checkpoint_data")
                        }
                    })
                    .collect(),
            };
        } else {
            let in_flight_parts = self
                .pushed_parts
                .iter()
                .enumerate()
                .map(|(part_index, part)| match part {
                    UploadPartOrBufferedData::UploadPart(upload_part) => {
                        InFlightPartCheckpoint::FinishedPart {
                            part: part_index,
                            content_id: upload_part.content_id.clone(),
                        }
                    }
                    UploadPartOrBufferedData::BufferedData { data } => {
                        InFlightPartCheckpoint::InProgressPart {
                            part: part_index,
                            data: data.clone(),
                        }
                    }
                })
                .collect();
            FileCheckpointData::MultiPartWriterClosed {
                multi_part_upload_id: multipart_id.clone(),
                in_flight_parts,
            }
        }
    }

    fn get_in_progress_checkpoint(
        &mut self,
        trailing_bytes: Option<Vec<u8>>,
    ) -> FileCheckpointData {
        if self.closed {
            unreachable!("get_in_progress_checkpoint called on closed file");
        }
        if self.multipart_id.is_none() {
            return FileCheckpointData::MultiPartNotCreated {
                parts_to_add: self
                    .parts_to_add
                    .iter()
                    .map(|val| val.byte_data.clone())
                    .collect(),
                trailing_bytes,
            };
        }
        let multi_part_id = self.multipart_id.as_ref().unwrap().clone();
        let in_flight_parts = self
            .pushed_parts
            .iter()
            .enumerate()
            .map(|(part_index, part)| match part {
                UploadPartOrBufferedData::UploadPart(upload_part) => {
                    InFlightPartCheckpoint::FinishedPart {
                        part: part_index,
                        content_id: upload_part.content_id.clone(),
                    }
                }
                UploadPartOrBufferedData::BufferedData { data } => {
                    InFlightPartCheckpoint::InProgressPart {
                        part: part_index,
                        data: data.clone(),
                    }
                }
            })
            .collect();
        FileCheckpointData::MultiPartInFlight {
            multi_part_upload_id: multi_part_id,
            in_flight_parts,
            trailing_bytes,
        }
    }
}

pub trait BufferedBuilder: Default + Send {
    type InputType: Data;
    type BatchData;
    type BuilderConfigT;
    fn new(config: Self::BuilderConfigT) -> Self;
    fn insert(&mut self, value: Self::InputType);
    fn buffer_len(&self) -> usize;
    fn buffered_inputs(&self) -> Vec<Self::InputType>;
    fn flush_buffer(&mut self) -> Self::BatchData;
    fn byte_buffer_length(&mut self) -> usize;
    fn add_batch_data(&mut self, data: Self::BatchData);
    fn get_buffered_bytes(&mut self) -> Vec<u8>;
    fn get_trailing_bytes_for_checkpoint(&mut self) -> Option<Vec<u8>>;
    fn close(&mut self) -> Vec<u8>;
}

pub struct BufferedRecordBatchBuilder<R: RecordBatchBuilder> {
    builder: R,
    buffer: Vec<R::Data>,
    arrow_writer: Option<ArrowWriter<SharedBuffer>>,
    shared_buffer: SharedBuffer,
}

impl<R: RecordBatchBuilder> Default for BufferedRecordBatchBuilder<R> {
    fn default() -> Self {
        Self {
            builder: R::default(),
            buffer: vec![],
            arrow_writer: None,
            shared_buffer: SharedBuffer::new(10 * 1024 * 1024),
        }
    }
}

impl<R: RecordBatchBuilder> BufferedBuilder for BufferedRecordBatchBuilder<R> {
    type InputType = R::Data;
    type BatchData = RecordBatch;
    type BuilderConfigT = WriterProperties;

    fn new(config: Self::BuilderConfigT) -> Self {
        let builder = R::default();
        let shared_buffer = SharedBuffer::new(10 * 1024 * 1024);
        let arrow_writer = ArrowWriter::try_new(
            shared_buffer.clone(),
            builder.schema(),
            // TODO: add props
            Some(config),
        )
        .unwrap();
        Self {
            builder: R::default(),
            buffer: Vec::new(),
            arrow_writer: Some(arrow_writer),
            shared_buffer,
        }
    }
    fn insert(&mut self, value: Self::InputType) {
        self.buffer.push(value.clone());
        self.builder.add_data(Some(value));
    }
    fn buffer_len(&self) -> usize {
        self.buffer.len()
    }
    fn buffered_inputs(&self) -> Vec<Self::InputType> {
        self.buffer.clone()
    }
    fn flush_buffer(&mut self) -> RecordBatch {
        self.buffer.clear();
        self.builder.flush()
    }
    fn byte_buffer_length(&mut self) -> usize {
        self.shared_buffer.buffer.try_lock().unwrap().len()
    }

    fn add_batch_data(&mut self, data: Self::BatchData) {
        self.arrow_writer.as_mut().unwrap().write(&data).unwrap();
    }

    fn get_buffered_bytes(&mut self) -> Vec<u8> {
        let mut buffer = self.shared_buffer.buffer.try_lock().unwrap();
        let bytes_copy = buffer.clone();
        buffer.clear();
        bytes_copy
    }

    fn close(&mut self) -> Vec<u8> {
        info!("closing buffered writer");
        let record_batch = self.flush_buffer();
        self.add_batch_data(record_batch);
        self.arrow_writer.take().unwrap().close().unwrap();
        self.get_buffered_bytes()
    }

    fn get_trailing_bytes_for_checkpoint(&mut self) -> Option<Vec<u8>> {
        // record the current size written to the shared buffer
        let writer = self.arrow_writer.as_mut()?;
        let result = writer.get_trailing_bytes(SharedBuffer::new(0)).unwrap();
        let bytes = result.buffer.try_lock().unwrap().to_vec();
        // copy out the current bytes in the shared buffer, plus the trailing bytes
        let mut copied_bytes = self.shared_buffer.buffer.try_lock().unwrap().to_vec();
        copied_bytes.extend(bytes.iter().cloned());
        Some(copied_bytes)
    }
}

impl<K: Key, T: Data + Sync, R: RecordBatchBuilder<Data = T>>
    ParquetSink<K, T, BufferingMultipartWriter<BufferedRecordBatchBuilder<R>>>
{
    pub fn from_config(config: &str) -> TwoPhaseCommitterOperator<K, T, Self> {
        ParquetSink::generic_from_config(config)
    }
}

#[async_trait]
impl<B: BufferedBuilder<BuilderConfigT = WriterProperties>> MultiPartWriter
    for BufferingMultipartWriter<B>
{
    type InputType = B::InputType;
    type ConfigT = ParquetWriterProperties;

    fn new(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        config: ParquetWriterProperties,
    ) -> Self {
        let row_batch_size = config.row_batch_size;
        let parquet_options = config.parquet_options;
        let target_part_size = config.target_part_size;
        Self {
            builder: B::new(parquet_options),
            max_record_batch_size: row_batch_size,
            target_part_size,
            closed: false,
            multipart_manager: MultipartManager::new(object_store, path),
            stats: None,
        }
    }

    fn name(&self) -> String {
        self.multipart_manager.name()
    }

    async fn insert_value(
        &mut self,
        value: Self::InputType,
        _time: SystemTime,
    ) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>> {
        if self.stats.is_none() {
            self.stats = Some(MultiPartWriterStats {
                bytes_written: 0,
                parts_written: 0,
                last_write_at: Instant::now(),
                first_write_at: Instant::now(),
            });
        }
        let stats = self.stats.as_mut().unwrap();
        stats.last_write_at = Instant::now();

        self.builder.insert(value.clone());

        if self.builder.buffer_len() == self.max_record_batch_size {
            let batch = self.builder.flush_buffer();
            let prev_size = self.builder.byte_buffer_length();
            self.builder.add_batch_data(batch);
            stats.bytes_written += self.builder.byte_buffer_length() - prev_size;
            if self.builder.byte_buffer_length() > self.target_part_size {
                stats.parts_written += 1;
                self.write_multipart()
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn handle_initialization(
        &mut self,
        multipart_id: String,
    ) -> Result<Vec<BoxedTryFuture<MultipartCallbackWithName>>> {
        self.multipart_manager.handle_initialization(multipart_id)
    }

    fn handle_completed_part(
        &mut self,
        part_idx: usize,
        upload_part: UploadPart,
    ) -> Result<Option<FileToFinish>> {
        self.multipart_manager
            .handle_completed_part(part_idx, upload_part)
    }

    fn get_in_progress_checkpoint(&mut self) -> FileCheckpointData {
        if self.multipart_manager.closed {
            self.multipart_manager.get_closed_file_checkpoint_data()
        } else {
            self.multipart_manager
                .get_in_progress_checkpoint(self.builder.get_trailing_bytes_for_checkpoint())
        }
    }

    fn currently_buffered_data(&mut self) -> Vec<Self::InputType> {
        self.builder.buffered_inputs()
    }

    fn close(&mut self) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>> {
        self.closed = true;
        self.multipart_manager.closed = true;
        self.write_multipart()
    }

    fn stats(&self) -> Option<MultiPartWriterStats> {
        self.stats.clone()
    }
}

impl<B: BufferedBuilder> BufferingMultipartWriter<B> {
    fn write_multipart(&mut self) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>> {
        let data = if self.closed {
            self.builder.close()
        } else {
            self.builder.get_buffered_bytes()
        };
        self.multipart_manager.write_next_part(data)
    }
}

struct PartToUpload {
    part_index: usize,
    byte_data: Vec<u8>,
}

#[derive(Debug)]
enum UploadPartOrBufferedData {
    UploadPart(UploadPart),
    BufferedData { data: Vec<u8> },
}

fn format_settings_to_parquet_writer_options(format_settings: FormatSettings) -> WriterProperties {
    let mut parquet_writer_options = WriterProperties::builder();
    if let Some(compression) = format_settings.compression {
        let compression = match compression {
            Compression::None => parquet::basic::Compression::UNCOMPRESSED,
            Compression::Snappy => parquet::basic::Compression::SNAPPY,
            Compression::Gzip => parquet::basic::Compression::GZIP(GzipLevel::default()),
            Compression::Zstd => parquet::basic::Compression::ZSTD(ZstdLevel::default()),
            Compression::Lz4 => parquet::basic::Compression::LZ4,
        };
        parquet_writer_options = parquet_writer_options.set_compression(compression);
    }
    if let Some(row_group_size) = format_settings.row_group_size {
        parquet_writer_options =
            parquet_writer_options.set_max_row_group_size(row_group_size as usize);
    }
    parquet_writer_options.build()
}

pub struct MultipartCallbackWithName {
    callback: MultipartCallback,
    name: String,
}

pub enum MultipartCallback {
    InitializedMultipart {
        multipart_id: MultipartId,
    },
    CompletedPart {
        part_idx: usize,
        upload_part: UploadPart,
    },
}

impl Debug for MultipartCallback {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MultipartCallback::InitializedMultipart { .. } => {
                write!(f, "MultipartCallback::InitializedMultipart")
            }
            MultipartCallback::CompletedPart { part_idx, .. } => {
                write!(f, "MultipartCallback::CompletedPart({})", part_idx)
            }
        }
    }
}

/// A buffer with interior mutability shared by the [`ArrowWriter`] and
/// [`AsyncArrowWriter`]. From Arrow. This lets us write data from the buffer to S3.
#[derive(Clone)]
struct SharedBuffer {
    /// The inner buffer for reading and writing
    ///
    /// The lock is used to obtain internal mutability, so no worry about the
    /// lock contention.
    buffer: Arc<futures::lock::Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(futures::lock::Mutex::new(Vec::with_capacity(capacity))),
        }
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::write(&mut *buffer, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::flush(&mut *buffer)
    }
}

#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq)]
pub struct ParquetDataRecovery<T: Data> {
    next_file_index: usize,
    active_files: Vec<InProgressFileCheckpoint<T>>,
}

#[async_trait]
impl<
        K: Key,
        T: Data + Sync,
        R: MultiPartWriter<InputType = T, ConfigT = ParquetWriterProperties> + Send + 'static,
    > TwoPhaseCommitter<K, T> for ParquetSink<K, T, R>
{
    type DataRecovery = ParquetDataRecovery<T>;

    type PreCommit = FileToFinish;

    fn name(&self) -> String {
        "parquet_sink".to_string()
    }

    async fn init(
        &mut self,
        task_info: &TaskInfo,
        data_recovery: Vec<Self::DataRecovery>,
    ) -> Result<()> {
        let mut max_file_index = 0;
        let mut recovered_files = Vec::new();
        for parquet_data_recovery in data_recovery {
            max_file_index = max_file_index.max(parquet_data_recovery.next_file_index);
            // task 0 is responsible for recovering all files.
            // This is because the number of subtasks may have changed.
            // Recovering should be reasonably fast since it is just finishing in-flight uploads.
            if task_info.task_index == 0 {
                recovered_files.extend(parquet_data_recovery.active_files.into_iter());
            }
        }
        self.sender
            .send(ParquetMessages::Init {
                max_file_index,
                subtask_id: task_info.task_index,
                recovered_files,
            })
            .await?;
        Ok(())
    }

    async fn insert_record(&mut self, record: &Record<K, T>) -> Result<()> {
        let value = record.value.clone();
        self.sender
            .send(ParquetMessages::Data {
                value,
                time: record.timestamp,
            })
            .await?;
        Ok(())
    }

    async fn commit(
        &mut self,
        _task_info: &TaskInfo,
        pre_commit: Vec<Self::PreCommit>,
    ) -> Result<()> {
        self.sender
            .send(ParquetMessages::FilesToFinish(pre_commit))
            .await?;
        // loop over checkpoint receiver until finished received
        while let Some(checkpoint_message) = self.checkpoint_receiver.recv().await {
            match checkpoint_message {
                CheckpointData::Finished { max_file_index: _ } => return Ok(()),
                _ => {
                    bail!("unexpected checkpoint message")
                }
            }
        }
        bail!("checkpoint receiver closed unexpectedly")
    }

    async fn checkpoint(
        &mut self,
        task_info: &TaskInfo,
        stopping: bool,
    ) -> Result<(Self::DataRecovery, HashMap<String, Self::PreCommit>)> {
        self.sender
            .send(ParquetMessages::Checkpoint {
                subtask_id: task_info.task_index,
                then_stop: stopping,
            })
            .await?;
        let mut pre_commit_messages = HashMap::new();
        let mut active_files = Vec::new();
        while let Some(checkpoint_message) = self.checkpoint_receiver.recv().await {
            match checkpoint_message {
                CheckpointData::Finished { max_file_index } => {
                    return Ok((
                        ParquetDataRecovery {
                            next_file_index: max_file_index + 1,
                            active_files,
                        },
                        pre_commit_messages,
                    ))
                }
                CheckpointData::InProgressFileCheckpoint(InProgressFileCheckpoint {
                    filename,
                    data,
                    buffered_data,
                }) => {
                    if let FileCheckpointData::MultiPartWriterUploadCompleted {
                        multi_part_upload_id,
                        completed_parts,
                    } = data
                    {
                        pre_commit_messages.insert(
                            filename.clone(),
                            FileToFinish {
                                filename,
                                multi_part_upload_id,
                                completed_parts,
                            },
                        );
                    } else {
                        active_files.push(InProgressFileCheckpoint {
                            filename,
                            data,
                            buffered_data,
                        })
                    }
                }
            }
        }
        bail!("checkpoint receiver closed unexpectedly")
    }
}
