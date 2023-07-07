use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    io::Write,
    marker::PhantomData,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{bail, Result};
use arroyo_macro::{process_fn, StreamNode};
use arroyo_state::tables::GlobalKeyedState;
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
use typify::import_types;

import_types!(schema = "../connector-schemas/parquet/table.json");

use crate::engine::Context;
use arroyo_types::*;

use super::OperatorConfig;

#[derive(StreamNode)]
pub struct ParquetSink<K: Key, T: Data + Sync, R: RecordBatchBuilder<T> + Send + 'static> {
    sender: Sender<ParquetMessages<T>>,
    checkpoint_receiver: Receiver<CheckpointData<T>>,
    closed: bool,
    _ts: PhantomData<(K, R)>,
}

#[process_fn(in_k = K, in_t = T)]
impl<K: Key, T: Data + Sync, R: RecordBatchBuilder<T> + Send + 'static> ParquetSink<K, T, R> {
    pub fn from_config(config: &str) -> Self {
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
        match write_target {
            Destination::LocalFilesystem { local_directory } => {
                Self::new_local(&local_directory, file_settings, format_settings)
            }
            Destination::S3Bucket {
                s3_bucket,
                s3_directory,
            } => Self::new_s3(&s3_bucket, &s3_directory, file_settings, format_settings),
        }
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
        let path: PathBuf = path.into();
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
            closed: false,
            _ts: PhantomData,
        }
    }

    fn name(&self) -> String {
        "parquet_sink".to_string()
    }

    fn tables(&self) -> Vec<arroyo_rpc::grpc::TableDescriptor> {
        vec![
            arroyo_state::global_table("p", "parquet sink state"),
            arroyo_state::global_table("m", "max file index"),
        ]
    }

    async fn on_start(&mut self, ctx: &mut Context<(), ()>) {
        let mut tracking_key_state: GlobalKeyedState<usize, CoordinationState, _> =
            ctx.state.get_global_keyed_state('m').await;
        // take the max of all values
        let state_vec = tracking_key_state.get_all();
        let max_file_index = tracking_key_state
            .get_all()
            .into_iter()
            .map(|state| state.max_file_index)
            .max()
            .unwrap_or(1);
        let next_epoch = tracking_key_state
            .get_all()
            .first()
            .map(|state| state.epoch + 1)
            .unwrap_or(1);

        let recovered_files = if ctx.task_info.task_index == 0 {
            let mut table: GlobalKeyedState<String, InProgressFileCheckpoint<T>, _> =
                ctx.state.get_global_keyed_state('p').await;
            table.get_all().into_iter().cloned().collect()
        } else {
            vec![]
        };

        self.sender
            .send(ParquetMessages::Init {
                epoch: next_epoch,
                max_file_index,
                subtask_id: ctx.task_info.task_index,
                recovered_files,
            })
            .await
            .unwrap();
    }

    async fn process_element(&mut self, record: &Record<K, T>, _ctx: &mut Context<(), ()>) {
        let value = record.value.clone();
        self.sender
            .send(ParquetMessages::Data {
                value,
                time: record.timestamp,
            })
            .await
            .unwrap();
    }

    async fn on_close(&mut self, _ctx: &mut crate::engine::Context<(), ()>) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.sender.send(ParquetMessages::Close).await.unwrap();
        self.checkpoint_receiver.recv().await;
    }

    async fn handle_checkpoint(
        &mut self,
        checkpoint_barrier: &arroyo_types::CheckpointBarrier,
        ctx: &mut crate::engine::Context<(), ()>,
    ) {
        self.sender
            .send(ParquetMessages::Checkpoint {
                epoch: checkpoint_barrier.epoch as usize,
                subtask_id: ctx.task_info.task_index,
                then_stop: checkpoint_barrier.then_stop,
            })
            .await
            .unwrap();
        while let Some(checkpoint_data) = self.checkpoint_receiver.recv().await {
            match checkpoint_data {
                CheckpointData::InProgressFileCheckpoint(in_progress_file_checkpoint) => {
                    let mut global_keyed_state: GlobalKeyedState<
                        String,
                        InProgressFileCheckpoint<T>,
                        _,
                    > = ctx.state.get_global_keyed_state('p').await;
                    global_keyed_state
                        .insert(
                            in_progress_file_checkpoint.filename.clone(),
                            in_progress_file_checkpoint,
                        )
                        .await;
                }
                CheckpointData::Finished {
                    epoch,
                    max_file_index,
                } => {
                    let mut tracking_key_state = ctx.state.get_global_keyed_state('m').await;
                    tracking_key_state
                        .insert(
                            ctx.task_info.task_index,
                            CoordinationState {
                                epoch,
                                max_file_index: max_file_index + 1,
                            },
                        )
                        .await;
                    break;
                }
            }
        }

        if checkpoint_barrier.then_stop {
            self.closed = true;
        }
    }
}
#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
struct CoordinationState {
    epoch: usize,
    max_file_index: usize,
}

#[derive(Debug)]
enum ParquetMessages<T: Data> {
    Data {
        value: T,
        time: SystemTime,
    },
    Init {
        epoch: usize,
        max_file_index: usize,
        subtask_id: usize,
        recovered_files: Vec<InProgressFileCheckpoint<T>>,
    },
    Checkpoint {
        epoch: usize,
        subtask_id: usize,
        then_stop: bool,
    },
    Close,
}

#[derive(Debug)]
enum CheckpointData<T: Data> {
    InProgressFileCheckpoint(InProgressFileCheckpoint<T>),
    Finished { epoch: usize, max_file_index: usize },
}

#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq)]
struct InProgressFileCheckpoint<T: Data> {
    filename: String,
    data: FileCheckpointData,
    buffered_data: Vec<T>,
}

#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq)]
enum FileCheckpointData {
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
enum InFlightPartCheckpoint {
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

struct AsyncMultipartParquetWriter<T: Data + Sync, R: RecordBatchBuilder<T>> {
    path: PathBuf,
    current_writer_name: String,
    current_epoch: usize,
    max_file_index: usize,
    subtask_id: usize,
    object_store: Arc<dyn ObjectStore>,
    writers: HashMap<String, SingleMultipartParquetWriter<T, R>>,
    receiver: Receiver<ParquetMessages<T>>,
    checkpoint_sender: Sender<CheckpointData<T>>,
    futures: FuturesUnordered<BoxedTryFuture<ParquetCallbackWithName>>,
    files_to_finish: HashMap<usize, Vec<FileToFinish>>,
    rolling_policy: RollingPolicy,
    properties: ParquetWriterProperties,
}

struct ParquetWriterProperties {
    parquet_options: WriterProperties,
    row_batch_size: usize,
    target_part_size: usize,
}

#[derive(Debug)]
struct FileToFinish {
    filename: String,
    multi_part_upload_id: String,
    completed_parts: Vec<UploadPart>,
}

struct RollingPolicy {
    max_file_size: usize,
    max_parts: usize,
    check_interval: Duration,
    inactivity_duration: Option<Duration>,
    rollover_duration: Option<Duration>,
}

impl From<FileSettings> for RollingPolicy {
    fn from(file_settings: FileSettings) -> Self {
        Self {
            max_file_size: file_settings.target_file_size.unwrap_or(1024 * 1024 * 1024) as usize,
            check_interval: Duration::from_millis(10),
            max_parts: file_settings.max_parts.unwrap_or(1000) as usize,
            inactivity_duration: file_settings
                .inactivity_rollover_seconds
                .map(|seconds| Duration::from_secs(seconds as u64)),
            rollover_duration: file_settings
                .rollover_seconds
                .map(|seconds| Duration::from_secs(seconds as u64)),
        }
    }
}

impl RollingPolicy {
    fn should_close<T: Data, R: RecordBatchBuilder<T>>(
        &self,
        writer: &SingleMultipartParquetWriter<T, R>,
    ) -> bool {
        if writer.current_size() > self.max_file_size {
            return true;
        }
        if writer.pushed_parts.len() >= self.max_parts {
            return true;
        }
        if let Some(_inactivity_duration) = self.inactivity_duration {
            // TODO: implement this
        }
        if let Some(_rollover_duration) = self.rollover_duration {
            // TODO: implement this
        }

        false
    }
}

impl<T: Data + std::marker::Sync, R: RecordBatchBuilder<T>> AsyncMultipartParquetWriter<T, R> {
    fn new(
        path: PathBuf,
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
            current_epoch: 0,
            max_file_index: 0,
            subtask_id: 0,
            object_store,
            writers: HashMap::new(),
            receiver,
            checkpoint_sender,
            futures: FuturesUnordered::new(),
            files_to_finish: HashMap::new(),
            rolling_policy: file_settings.into(),
            properties,
        }
    }

    fn add_part_to_finish(&mut self, epoch: usize, file_to_finish: FileToFinish) {
        self.files_to_finish
            .entry(epoch)
            .or_default()
            .push(file_to_finish);
    }

    async fn run(&mut self) -> Result<()> {
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
                        ParquetMessages::Init {epoch,max_file_index, subtask_id,recovered_files } => {
                            if let Some(writer) = self.writers.get_mut(&self.current_writer_name) {
                                if let Some(future) = writer.close()? {
                                    self.futures.push(future);
                                }
                            }
                            self.max_file_index = max_file_index;
                            let new_writer = SingleMultipartParquetWriter::new(
                                self.path.clone(),
                                self.object_store.clone(),
                                self.max_file_index,
                                subtask_id,
                                self.properties.parquet_options.clone(),
                                self.properties.row_batch_size,
                            )?;
                            self.subtask_id = subtask_id;
                            self.current_writer_name = new_writer.name();
                            self.writers.insert(new_writer.name(), new_writer);
                            self.current_epoch = epoch;
                            for recovered_file in recovered_files {
                                let (epoch, file_to_finish) = SingleMultipartParquetWriter::<T, R>::from_checkpoint(epoch,
                                     &Path::parse(&recovered_file.filename)?, recovered_file.data, self.object_store.clone()).await?;
                                    self.add_part_to_finish(epoch, file_to_finish);
                            }
                        },
                        ParquetMessages::Checkpoint { epoch, subtask_id, then_stop } => {
                            self.flush_futures().await?;
                            if then_stop {
                                self.stop().await?;
                            }
                            self.take_checkpoint(epoch, subtask_id).await?;
                            self.checkpoint_sender.send(CheckpointData::Finished { epoch , max_file_index: self.max_file_index}).await?;
                            self.current_epoch = epoch;
                            if then_stop {
                                break;
                            }
                        },
                        ParquetMessages::Close => {
                            self.stop().await?;
                            break;
                        },
                    }
                }
                Some(result) = self.futures.next() => {
                    let ParquetCallbackWithName { callback, name } = result?;
                    self.process_callback(name, callback)?;
                }
                // duration
                _ = tokio::time::sleep(self.rolling_policy.check_interval) => {
                    if let Some(writer) = self.writers.get_mut(&self.current_writer_name) {
                        if self.rolling_policy.should_close(writer) {
                            if let Some(future) = writer.close()? {
                                self.futures.push(future);
                            }
                            self.max_file_index += 1;
                            let new_writer = SingleMultipartParquetWriter::new(
                                self.path.clone(),
                                self.object_store.clone(),
                                self.max_file_index,
                                self.subtask_id,
                                self.properties.parquet_options.clone(),
                                self.properties.row_batch_size,
                            )?;
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
        while let Some(ParquetCallbackWithName { callback, name }) = self.futures.try_next().await?
        {
            self.process_callback(name, callback)?;
        }
        Ok(())
    }

    fn process_callback(&mut self, name: String, callback: ParquetCallback) -> Result<()> {
        let writer = self.writers.get_mut(&name).ok_or_else(|| {
            anyhow::anyhow!(
                "missing parquet writer {} for callback {:?}",
                name,
                callback
            )
        })?;
        match callback {
            ParquetCallback::InitializedMultipart { multipart_id } => {
                self.futures
                    .extend(writer.handle_initialization(multipart_id)?);
                Ok(())
            }
            ParquetCallback::CompletedPart {
                part_idx,
                upload_part,
            } => {
                if let Some(file_to_write) = writer.handle_completed_part(part_idx, upload_part)? {
                    // need the file to finish to be checkpointed first.
                    self.add_part_to_finish(self.current_epoch + 2, file_to_write);
                    self.writers.remove(&name);
                }
                Ok(())
            }
        }
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(writer) = self.writers.get_mut(&self.current_writer_name) {
            if let Some(future) = writer.close()? {
                self.futures.push(future);
            }
        }
        while let Some(result) = self.futures.next().await {
            let ParquetCallbackWithName { callback, name } = result?;
            self.process_callback(name, callback)?;
        }
        for (_, file_to_finish) in self.files_to_finish.drain() {
            for FileToFinish {
                filename,
                multi_part_upload_id,
                completed_parts,
            } in file_to_finish
            {
                self.object_store
                    .close_multipart(
                        &Path::parse(&filename)?,
                        &multi_part_upload_id,
                        completed_parts,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn take_checkpoint(&mut self, epoch: usize, _subtask_id: usize) -> Result<()> {
        for (filename, writer) in self.writers.iter_mut() {
            let in_progress_checkpoint =
                CheckpointData::InProgressFileCheckpoint(InProgressFileCheckpoint {
                    filename: filename.clone(),
                    data: writer.get_in_progress_checkpoint(),
                    buffered_data: writer.currently_buffered_values.clone(),
                });
            self.checkpoint_sender.send(in_progress_checkpoint).await?;
        }
        if let Some(finished_files) = self.files_to_finish.remove(&epoch) {
            for FileToFinish {
                filename,
                multi_part_upload_id,
                completed_parts,
            } in finished_files
            {
                self.object_store
                    .close_multipart(
                        &Path::parse(&filename)?,
                        &multi_part_upload_id,
                        completed_parts,
                    )
                    .await?;
            }
        }

        for (_epoch, files_to_finish) in &self.files_to_finish {
            for file_to_finish in files_to_finish {
                self.checkpoint_sender
                    .send(CheckpointData::InProgressFileCheckpoint(
                        InProgressFileCheckpoint {
                            filename: file_to_finish.filename.clone(),
                            data: FileCheckpointData::MultiPartWriterUploadCompleted {
                                multi_part_upload_id: file_to_finish.multi_part_upload_id.clone(),
                                completed_parts: file_to_finish
                                    .completed_parts
                                    .iter()
                                    .map(|upload_part| upload_part.content_id.clone())
                                    .collect(),
                            },
                            buffered_data: vec![],
                        },
                    ))
                    .await?;
            }
        }
        Ok(())
    }
}

type BoxedTryFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

struct SingleMultipartParquetWriter<T: Data, R: RecordBatchBuilder<T>> {
    object_store: Arc<dyn ObjectStore>,
    path: String,
    writer: Option<MultipartId>,
    builder: R,
    records_in_current_batch: usize,
    currently_buffered_values: Vec<T>,
    // TODO: figure out a way to close this without needing an option.
    current_part_writer: Option<ArrowWriter<SharedBuffer>>,
    buffer: SharedBuffer,
    pushed_parts: Vec<UploadPartOrBufferedData>,
    uploaded_parts: usize,
    pushed_size: usize,
    max_record_batch_size: usize,
    parts_to_add: Vec<PartToUpload>,
    closed: bool,
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

impl<T: Data, R: RecordBatchBuilder<T>> SingleMultipartParquetWriter<T, R> {
    fn new(
        path: PathBuf,
        object_store: Arc<dyn ObjectStore>,
        file_index: usize,
        subtask_id: usize,
        writer_properties: WriterProperties,
        max_record_batch_size: usize,
    ) -> Result<Self> {
        let path = path.join(format!("{}-{}.parquet", file_index, subtask_id));
        let builder = R::default();
        let shared_buffer = SharedBuffer::new(10 * 1024 * 1024);
        let writer = ArrowWriter::try_new(
            shared_buffer.clone(),
            builder.schema(),
            // TODO: add props
            Some(writer_properties),
        )?;
        Ok(Self {
            object_store,
            path: path.to_string_lossy().to_string(),
            writer: None,
            builder,
            records_in_current_batch: 0,
            currently_buffered_values: Vec::new(),
            current_part_writer: Some(writer),
            buffer: shared_buffer,
            pushed_parts: Vec::new(),
            uploaded_parts: 0,
            pushed_size: 0,
            max_record_batch_size,
            parts_to_add: Vec::new(),
            closed: false,
        })
    }

    async fn from_checkpoint(
        mut epoch: usize,
        path: &Path,
        checkpoint_data: FileCheckpointData,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<(usize, FileToFinish)> {
        epoch += 1;
        let mut parts = vec![];
        let multipart_id = match checkpoint_data {
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
                epoch += 1;
                multi_part_upload_id
            }
        };
        Ok((
            epoch,
            FileToFinish {
                filename: path.to_string(),
                multi_part_upload_id: multipart_id,
                completed_parts: parts,
            },
        ))
    }

    fn name(&self) -> String {
        self.path.clone()
    }

    async fn insert_value(
        &mut self,
        value: T,
        _time: SystemTime,
    ) -> Result<Option<BoxedTryFuture<ParquetCallbackWithName>>> {
        self.builder.add_data(Some(value.clone()));
        self.currently_buffered_values.push(value);
        self.records_in_current_batch += 1;

        if self.records_in_current_batch == self.max_record_batch_size {
            self.records_in_current_batch = 0;
            self.currently_buffered_values.clear();
            self.current_part_writer
                .as_mut()
                .unwrap()
                .write(&self.builder.flush())?;
            self.current_part_writer.as_mut().unwrap().flush()?;
            if self.buffer.buffer.try_lock().unwrap().len() > 5 * 1024 * 1024 {
                self.write_multipart()
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn write_multipart(&mut self) -> Result<Option<BoxedTryFuture<ParquetCallbackWithName>>> {
        self.current_part_writer.as_mut().unwrap().flush()?;
        if self.closed {
            let writer = self.current_part_writer.take().unwrap();
            writer.close()?;
        }
        let data = {
            let mut buffer = self.buffer.buffer.try_lock().unwrap();
            let copy = buffer.as_slice().to_vec();
            buffer.clear();
            copy
        };
        self.currently_buffered_values.clear();

        self.pushed_size += data.len();

        match &self.writer {
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

    fn get_initialize_multipart_future(
        &mut self,
    ) -> Result<BoxedTryFuture<ParquetCallbackWithName>> {
        let name = self.path.clone();
        let object_store = self.object_store.clone();
        let path = self.path.clone().into();
        Ok(Box::pin(async move {
            let multipart_id = object_store.start_multipart(&path).await?;
            Ok(ParquetCallbackWithName {
                name,
                callback: ParquetCallback::InitializedMultipart { multipart_id },
            })
        }))
    }

    // Closes the upload, returning a future that should be added to the parent futures if necessary.
    fn close(&mut self) -> Result<Option<BoxedTryFuture<ParquetCallbackWithName>>> {
        self.closed = true;
        if self.records_in_current_batch > 0 {
            self.current_part_writer
                .as_mut()
                .unwrap()
                .write(&self.builder.flush())?;
            self.records_in_current_batch = 0;
        }
        self.write_multipart()
    }

    fn handle_initialization(
        &mut self,
        multipart_id: String,
    ) -> Result<Vec<BoxedTryFuture<ParquetCallbackWithName>>> {
        // for each part in parts_to_add, start a new part upload
        self.writer = Some(multipart_id);
        std::mem::take(&mut self.parts_to_add)
            .into_iter()
            .map(|part_to_upload| self.get_part_upload_future(part_to_upload))
            .collect::<Result<Vec<_>>>()
    }

    // Future for uploading a part of a multipart upload.
    // Argument is either from a newly flushed part or from parts_to_add.
    fn get_part_upload_future(
        &mut self,
        part_to_upload: PartToUpload,
    ) -> Result<BoxedTryFuture<ParquetCallbackWithName>> {
        self.pushed_parts
            .push(UploadPartOrBufferedData::BufferedData {
                // TODO: use Bytes to avoid clone
                data: part_to_upload.byte_data.clone(),
            });
        let name = self.path.clone();
        let multipart_id = self
            .writer
            .clone()
            .ok_or_else(|| anyhow::anyhow!("missing multipart id"))?;
        let object_store = self.object_store.clone();
        Ok(Box::pin(async move {
            let upload_part = object_store
                .add_multipart(
                    &Path::parse(&name)?,
                    &multipart_id,
                    part_to_upload.part_index,
                    part_to_upload.byte_data.into(),
                )
                .await?;
            Ok(ParquetCallbackWithName {
                name,
                callback: ParquetCallback::CompletedPart {
                    part_idx: part_to_upload.part_index,
                    upload_part,
                },
            })
        }))
    }

    fn all_uploads_finished(&self) -> bool {
        self.closed && self.uploaded_parts == self.pushed_parts.len()
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
                filename: self.path.clone(),
                multi_part_upload_id: self
                    .writer
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("need multipart id to complete"))?
                    .clone(),
                completed_parts: self
                    .pushed_parts
                    .iter()
                    .map(|part| match part {
                        UploadPartOrBufferedData::UploadPart(upload_part) => {
                            Ok(upload_part.clone())
                        }
                        UploadPartOrBufferedData::BufferedData { .. } => {
                            bail!("unfinished part in get_complete_multipart_future")
                        }
                    })
                    .collect::<Result<Vec<_>>>()?,
            }))
        }
    }

    fn get_trailing_bytes_for_checkpoint(&mut self) -> Option<Vec<u8>> {
        // record the current size written to the shared buffer
        let writer = self.current_part_writer.as_mut()?;
        let result = writer.get_trailing_bytes(SharedBuffer::new(0)).unwrap();
        let bytes = result.buffer.try_lock().unwrap().to_vec();
        // copy out the current bytes in the shared buffer, plus the trailing bytes
        let mut copied_bytes = self.buffer.buffer.try_lock().unwrap().to_vec();
        copied_bytes.extend(bytes.iter().cloned());
        Some(copied_bytes)
    }

    fn get_in_progress_checkpoint(&mut self) -> FileCheckpointData {
        if self.writer.is_none() {
            return FileCheckpointData::MultiPartNotCreated {
                parts_to_add: self
                    .parts_to_add
                    .iter()
                    .map(|val| val.byte_data.clone())
                    .collect(),
                trailing_bytes: self.get_trailing_bytes_for_checkpoint(),
            };
        }
        let multi_part_id = self.writer.as_ref().unwrap().clone();
        if self.all_uploads_finished() {
            return FileCheckpointData::MultiPartWriterUploadCompleted {
                multi_part_upload_id: multi_part_id,
                completed_parts: self
                    .pushed_parts
                    .iter()
                    .map(|val| match val {
                        UploadPartOrBufferedData::UploadPart(upload_part) => {
                            upload_part.content_id.clone()
                        }
                        UploadPartOrBufferedData::BufferedData { .. } => {
                            panic!("unfinished part in get_in_progress_checkpoint")
                        }
                    })
                    .collect(),
            };
        }
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
        if self.closed {
            return FileCheckpointData::MultiPartWriterClosed {
                multi_part_upload_id: multi_part_id,
                in_flight_parts,
            };
        };
        FileCheckpointData::MultiPartInFlight {
            multi_part_upload_id: multi_part_id,
            in_flight_parts,
            trailing_bytes: self.get_trailing_bytes_for_checkpoint(),
        }
    }

    fn current_size(&self) -> usize {
        self.pushed_size + self.buffer.buffer.try_lock().unwrap().len()
    }
}

struct ParquetCallbackWithName {
    callback: ParquetCallback,
    name: String,
}
enum ParquetCallback {
    InitializedMultipart {
        multipart_id: MultipartId,
    },
    CompletedPart {
        part_idx: usize,
        upload_part: UploadPart,
    },
}

impl Debug for ParquetCallback {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParquetCallback::InitializedMultipart { .. } => {
                write!(f, "ParquetCallback::InitializedMultipart")
            }
            ParquetCallback::CompletedPart { part_idx, .. } => {
                write!(f, "ParquetCallback::CompletedPart({})", part_idx)
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
