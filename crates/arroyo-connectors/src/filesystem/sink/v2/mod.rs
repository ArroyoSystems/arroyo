mod checkpoint;
pub mod migration;
mod open_file;
mod uploads;

use super::delta::{commit_files_to_delta, load_or_create_table};
use super::parquet::representitive_timestamp;
use super::partitioning::{Partitioner, PartitionerMode};
use super::v2::checkpoint::{FileToCommit, FilesCheckpointV2};
use super::v2::uploads::{FsResponse, UploadFuture};
use super::{
    BatchBufferingWriter, CommitState, FinishedFile, FsEventLogger, RollingPolicy,
    add_suffix_prefix, map_storage_error,
};
use crate::filesystem::TableFormat;
use crate::filesystem::config::{self, FilenameStrategy, NamingConfig};
use crate::filesystem::sink::v2::open_file::OpenFile;
use arrow::record_batch::RecordBatch;
use arrow::row::OwnedRow;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::errors::{DataflowError, DataflowResult, StateError};
use arroyo_rpc::grpc::rpc::{GlobalKeyedTableConfig, TableConfig, TableEnum};
use arroyo_rpc::{CheckpointEvent, connector_err, df::ArroyoSchemaRef, formats::Format};
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_storage::StorageProvider;
use arroyo_types::{CheckpointBarrier, TaskInfo, Watermark};
use async_trait::async_trait;
use bincode::config as bincode_config;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use object_store::path::Path;
use prost::Message;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{env, fs, mem};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use ulid::Ulid;
use uuid::Uuid;

const DEFAULT_TARGET_PART_SIZE: usize = 32 * 1024 * 1024;

pub struct ActiveState<BBW: BatchBufferingWriter> {
    max_file_index: usize,
    // partition -> filename
    active_partitions: HashMap<Option<OwnedRow>, Arc<Path>>,
    // filename -> file writer
    open_files: HashMap<Arc<Path>, OpenFile<BBW>>,
}

pub struct SinkConfig {
    config: config::FileSystemSink,
    format: Format,
    file_naming: NamingConfig,
    partitioner_mode: PartitionerMode,
    rolling_policies: Vec<RollingPolicy>,
    // consumed on start
    table_format: Option<TableFormat>,
}

impl SinkConfig {
    fn target_part_size(&self) -> usize {
        self.config
            .multipart
            .target_part_size_bytes
            .map(|s| s as usize)
            .unwrap_or(DEFAULT_TARGET_PART_SIZE)
    }

    fn minimum_multipart_size(&self) -> usize {
        self.config
            .multipart
            .minimum_multipart_size
            .unwrap_or_default() as usize
    }
}

pub struct SinkContext {
    storage_provider: Arc<StorageProvider>,
    partitioner: Arc<Partitioner>,
    schema: ArroyoSchemaRef,
    iceberg_schema: Option<iceberg::spec::SchemaRef>,
    task_info: Arc<TaskInfo>,
    commit_state: CommitState,
}

/// test utility to precisely cause failures
fn maybe_cause_failure(test_case: &str) {
    if !cfg!(debug_assertions) {
        return;
    }

    if env::var("FS_FAILURE_TESTING").is_ok()
        && fs::read("/tmp/fail")
            .unwrap_or_default()
            .starts_with(test_case.as_bytes())
    {
        panic!("intentionally failing due to {test_case}");
    }
}

impl SinkContext {
    pub async fn new(
        config: &mut SinkConfig,
        schema: ArroyoSchemaRef,
        task_info: Arc<TaskInfo>,
    ) -> DataflowResult<Self> {
        let mut table_format = config
            .table_format
            .take()
            .expect("table format has already been consumed");

        let provider = table_format
            .get_storage_provider(
                task_info.clone(),
                &config.config,
                &schema.schema_without_timestamp(),
            )
            .await?;

        let mut iceberg_schema = None;

        let commit_state = match table_format {
            TableFormat::Delta => CommitState::DeltaLake {
                last_version: -1,
                table: Box::new(
                    load_or_create_table(&provider, &schema.schema_without_timestamp())
                        .await
                        .map_err(|e| {
                            connector_err!(
                                User,
                                NoRetry,
                                source: e,
                                "failed to load or create delta table"
                            )
                        })?,
                ),
            },
            TableFormat::Iceberg(mut table) => {
                let t = table
                    .load_or_create(task_info.clone(), &schema.schema)
                    .await?;
                iceberg_schema = Some(t.metadata().current_schema().clone());
                CommitState::Iceberg(table)
            }
            TableFormat::None => CommitState::VanillaParquet,
        };

        Ok(SinkContext {
            storage_provider: Arc::new(provider),
            partitioner: Arc::new(Partitioner::new(
                config.partitioner_mode.clone(),
                &schema.schema,
            )?),
            schema,
            iceberg_schema,
            task_info: task_info.clone(),
            commit_state,
        })
    }
}

impl<BBW: BatchBufferingWriter> ActiveState<BBW> {
    fn next_file_path(
        &mut self,
        config: &SinkConfig,
        context: &SinkContext,
        partition: &Option<OwnedRow>,
    ) -> String {
        let filename_strategy = config.file_naming.strategy.unwrap_or_default();

        // This forms the base for naming files depending on strategy
        let filename_base = match filename_strategy {
            FilenameStrategy::Serial => {
                format!(
                    "{:>05}-{:>03}",
                    self.max_file_index, context.task_info.task_index
                )
            }
            FilenameStrategy::Ulid => Ulid::new().to_string(),
            FilenameStrategy::Uuid => Uuid::new_v4().to_string(),
            FilenameStrategy::UuidV7 => Uuid::now_v7().to_string(),
        };

        let mut filename = add_suffix_prefix(
            filename_base,
            config.file_naming.prefix.as_ref(),
            config.file_naming.suffix.as_ref().unwrap(),
        );

        if let Some(partition) = partition
            && let Some(hive) = context.partitioner.hive_path(partition)
        {
            filename = format!("{hive}/{filename}");
        }

        filename
    }

    fn get_or_create_file(
        &mut self,
        config: &SinkConfig,
        logger: FsEventLogger,
        context: &SinkContext,
        partition: &Option<OwnedRow>,
        representative_ts: SystemTime,
    ) -> &mut OpenFile<BBW> {
        let file = self
            .active_partitions
            .get(partition)
            .and_then(|f| self.open_files.get(f));

        if file.is_none() || !file.unwrap().is_writable() {
            let file_path = self.next_file_path(config, context, partition);
            let path = Arc::new(Path::from(file_path));

            let batch_writer = BBW::new(
                &config.config,
                config.format.clone(),
                context.schema.clone(),
                context.iceberg_schema.clone(),
                logger.clone(),
            );

            let open_file = OpenFile::new(
                path.clone(),
                batch_writer,
                logger,
                context.storage_provider.clone(),
                representative_ts,
                config,
            );

            self.active_partitions
                .insert(partition.clone(), path.clone());
            self.open_files.insert(path, open_file);

            self.max_file_index += 1;
        }

        let file_path = self.active_partitions.get(partition).unwrap();
        self.open_files.get_mut(file_path).unwrap()
    }
}

pub struct FileSystemSinkV2<BBW: BatchBufferingWriter> {
    config: SinkConfig,

    context: Option<SinkContext>,

    // state
    active: ActiveState<BBW>,
    upload: UploadState,

    event_logger: FsEventLogger,
    watermark: Option<SystemTime>,
}

struct UploadState {
    pending_uploads: Arc<Mutex<FuturesUnordered<UploadFuture>>>,
    files_to_commit: Vec<FileToCommit>,
}

impl UploadState {
    async fn roll_file_if_ready<BBW: BatchBufferingWriter>(
        &mut self,
        policies: &[RollingPolicy],
        watermark: Option<SystemTime>,
        f: &mut OpenFile<BBW>,
    ) -> DataflowResult<bool> {
        Ok(
            if f.is_writable() && policies.iter().any(|p| p.should_roll(&f.stats, watermark)) {
                let futures = f.close()?;

                let mut ps = self.pending_uploads.lock().await;
                ps.extend(futures.into_iter());
                true
            } else {
                false
            },
        )
    }
}

impl<BBW: BatchBufferingWriter + Send + 'static> FileSystemSinkV2<BBW> {
    pub fn new(
        config: config::FileSystemSink,
        table_format: TableFormat,
        format: Format,
        partitioner_mode: PartitionerMode,
        connection_id: Option<String>,
    ) -> Self {
        let mut file_naming = config.file_naming.clone();
        if file_naming.suffix.is_none() {
            file_naming.suffix = Some(BBW::suffix());
        }

        let connection_id_str = connection_id.clone().unwrap_or_default();

        Self {
            config: SinkConfig {
                rolling_policies: RollingPolicy::from_file_settings(&config),
                config,
                format,
                table_format: Some(table_format),
                file_naming,
                partitioner_mode,
            },
            context: None,
            active: ActiveState {
                active_partitions: HashMap::new(),
                open_files: HashMap::new(),
                max_file_index: 0,
            },
            upload: UploadState {
                pending_uploads: Arc::new(Mutex::new(FuturesUnordered::new())),
                files_to_commit: Vec::new(),
            },
            event_logger: FsEventLogger {
                task_info: None,
                connection_id: connection_id_str.into(),
            },
            watermark: None,
        }
    }

    async fn process_upload_result(&mut self, result: FsResponse) -> DataflowResult<()> {
        let Some(file) = self.active.open_files.get_mut(&result.path) else {
            warn!("received multipart init for unknown file: {}", result.path);
            return Ok(());
        };

        let futures = self.upload.pending_uploads.lock().await;
        for future in file.handle_event(result.data)? {
            futures.push(future);
        }

        if file.ready_to_finalize() {
            let file = self.active.open_files.remove(&result.path).unwrap();
            self.upload.files_to_commit.push(file.into_commit_file()?);
        }

        Ok(())
    }

    async fn flush_pending_uploads(&mut self) -> DataflowResult<()> {
        let mut count = 0;
        loop {
            let result = {
                let mut uploads = self.upload.pending_uploads.lock().await;
                if uploads.is_empty() {
                    info!("flush_pending_uploads: completed after {} uploads", count);
                    break;
                }
                debug!(
                    "flush_pending_uploads: waiting for upload (have {} pending)",
                    uploads.len()
                );
                uploads.next().await
            };

            if let Some(result) = result {
                count += 1;
                self.process_upload_result(result?).await?;
            }
        }
        Ok(())
    }

    /// Get the delta version if using Delta Lake.
    fn delta_version(&self) -> i64 {
        match &self.context.as_ref().unwrap().commit_state {
            CommitState::DeltaLake { last_version, .. } => *last_version,
            _ => -1,
        }
    }
}

#[async_trait]
impl<BBW: BatchBufferingWriter + Send + 'static> ArrowOperator for FileSystemSinkV2<BBW> {
    fn name(&self) -> String {
        "filesystem_sink".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        let mut tables = arroyo_state::global_table_config_with_version("r", "recovery data", 1);
        tables.insert(
            "p".into(),
            TableConfig {
                table_type: TableEnum::GlobalKeyValue.into(),
                config: GlobalKeyedTableConfig {
                    table_name: "p".into(),
                    description: "pre-commit data".into(),
                    uses_two_phase_commit: true,
                }
                .encode_to_vec(),
                state_version: 1,
            },
        );
        tables
    }

    fn is_committing(&self) -> bool {
        true
    }

    fn tick_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(1))
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        let schema = ctx.in_schemas.first().unwrap().clone();
        self.event_logger.task_info = Some(ctx.task_info.clone());
        self.context =
            Some(SinkContext::new(&mut self.config, schema, ctx.task_info.clone()).await?);

        let state: HashMap<usize, FilesCheckpointV2> = ctx
            .table_manager
            .get_global_keyed_state_migratable("r")
            .await?
            .take();

        for s in state.values() {
            self.active.max_file_index = self.active.max_file_index.max(s.file_index);
        }

        if ctx.task_info.task_index == 0 {
            for s in state.into_values() {
                for f in s.open_files {
                    debug!(path = f.path, buffered_size = f.data.len(),
                        state = ?f.state, "recovering and finishing open file");

                    let mut open_file = OpenFile::from_checkpoint(
                        f,
                        self.context.as_ref().unwrap().storage_provider.clone(),
                        &self.config,
                        self.event_logger.clone(),
                    )?;
                    self.upload
                        .pending_uploads
                        .lock()
                        .await
                        .extend(open_file.close()?);
                    self.active
                        .open_files
                        .insert(open_file.path.clone(), open_file);
                }
            }

            let pre_commit_state: &mut GlobalKeyedView<String, FileToCommit> = ctx
                .table_manager
                .get_global_keyed_state_migratable("p")
                .await
                .expect("should be able to get table");

            for f in pre_commit_state.take().into_values() {
                let open_file = OpenFile::from_commit(
                    f,
                    self.context.as_ref().unwrap().storage_provider.clone(),
                    &self.config,
                    self.event_logger.clone(),
                )?;
                self.active
                    .open_files
                    .insert(open_file.path.clone(), open_file);
            }
        }

        maybe_cause_failure("after_start");

        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let timestamp_index = self.context.as_ref().unwrap().schema.timestamp_index;
        let partitioner = &self.context.as_ref().unwrap().partitioner;

        let partitions: Vec<(Option<OwnedRow>, RecordBatch)> = if partitioner.is_partitioned() {
            partitioner
                .partition(&batch)?
                .into_iter()
                .map(|(k, b)| (Some(k), b))
                .collect()
        } else {
            vec![(None, batch)]
        };

        for (partition_key, sub_batch) in partitions {
            let representative_timestamp =
                representitive_timestamp(sub_batch.column(timestamp_index)).map_err(|e| {
                    connector_err!(
                        Internal,
                        NoRetry,
                        source: e,
                        "failed to get representative timestamp"
                    )
                })?;

            let file = self.active.get_or_create_file(
                &self.config,
                self.event_logger.clone(),
                self.context.as_ref().unwrap(),
                &partition_key,
                representative_timestamp,
            );
            let future = file.add_batch(&sub_batch)?;

            if let Some(future) = future {
                let futures = self.upload.pending_uploads.lock().await;
                futures.push(future);
            }

            if self
                .upload
                .roll_file_if_ready(&self.config.rolling_policies, self.watermark, file)
                .await?
            {
                self.active.active_partitions.remove(&partition_key);
            }
        }

        Ok(())
    }

    fn future_to_poll(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>> {
        let futures = self.upload.pending_uploads.clone();

        Some(Box::pin(async move {
            let mut guard = futures.lock().await;

            if guard.is_empty() {
                // No pending uploads - wait indefinitely
                // Will be canceled when checkpoint/new batch arrives
                drop(guard);
                futures::future::pending::<()>().await;
                unreachable!()
            }

            // Poll the next upload to completion
            let result: Option<DataflowResult<FsResponse>> = guard.next().await;
            Box::new(result) as Box<dyn Any + Send>
        }))
    }

    async fn handle_future_result(
        &mut self,
        result: Box<dyn Any + Send>,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let result: Option<DataflowResult<FsResponse>> = *result
            .downcast()
            .map_err(|_| connector_err!(Internal, NoRetry, "failed to downcast future result"))?;

        match result {
            None => Ok(()), // FuturesUnordered was empty
            Some(Err(e)) => Err(e),
            Some(Ok(upload_result)) => self.process_upload_result(upload_result).await,
        }
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<Option<Watermark>> {
        if let Watermark::EventTime(ts) = watermark {
            self.watermark = Some(ts);
        }

        Ok(Some(watermark))
    }

    async fn handle_checkpoint(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        // if stopping, close all open files
        if barrier.then_stop {
            let pending = self.upload.pending_uploads.lock().await;
            for f in self.active.open_files.values_mut() {
                for fut in f.close()? {
                    pending.push(fut);
                }
            }
        }

        maybe_cause_failure("checkpoint_start");

        // then we wait for all pending uploads to finish
        self.flush_pending_uploads().await?;

        maybe_cause_failure("after_flush");

        let mut open_files = vec![];
        let mut to_commit = vec![];

        for (file_path, file) in &mut self.active.open_files {
            if file.ready_to_finalize() {
                to_commit.push(file.path.clone());
            } else {
                let chk = file.as_checkpoint()?;
                debug!(
                    path = ?file_path,
                    state = ?chk.state,
                    "checkpointing in-progress file",
                );
                open_files.push(chk);
            }
        }

        maybe_cause_failure("after_finalize");

        let mut files_to_commit = vec![];
        mem::swap(&mut self.upload.files_to_commit, &mut files_to_commit);

        for path in to_commit {
            files_to_commit.push(
                self.active
                    .open_files
                    .remove(&path)
                    .unwrap()
                    .into_commit_file()?,
            );
        }

        let checkpoint = FilesCheckpointV2 {
            open_files,
            file_index: self.active.max_file_index,
            delta_version: self.delta_version(),
        };

        // Save recovery state
        let recovery_state: &mut GlobalKeyedView<usize, FilesCheckpointV2> = ctx
            .table_manager
            .get_global_keyed_state("r")
            .await
            .expect("should be able to get table");
        recovery_state
            .insert(ctx.task_info.task_index as usize, checkpoint)
            .await;

        // Handle pre-commit data
        let serialized =
            bincode::encode_to_vec(&files_to_commit, bincode_config::standard()).unwrap();
        ctx.table_manager
            .insert_committing_data("p", serialized)
            .await;

        maybe_cause_failure("after_checkpoint");
        Ok(())
    }

    async fn handle_commit(
        &mut self,
        epoch: u32,
        commit_data: &HashMap<String, HashMap<u32, Vec<u8>>>,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        maybe_cause_failure("commit_start");
        if ctx.task_info.task_index == 0 {
            let files_to_commit: Vec<_> = commit_data
                .get("p")
                .ok_or_else(|| {
                    DataflowError::StateError(StateError::NoRegisteredTable {
                        table: "p".to_string(),
                    })
                })?
                .values()
                .flat_map(|serialized| {
                    let v: Vec<FileToCommit> =
                        bincode::decode_from_slice(serialized, bincode_config::standard())
                            .unwrap()
                            .0;
                    v
                })
                .collect();

            // finalize the finals
            let mut futures = FuturesUnordered::new();
            let mut files = HashMap::new();
            for file in files_to_commit {
                debug!(path = ?file.path, data = ?file.typ, "Finalizing file");

                let mut file: OpenFile<BBW> = OpenFile::from_commit(
                    file,
                    self.context.as_ref().unwrap().storage_provider.clone(),
                    &self.config,
                    self.event_logger.clone(),
                )?;
                futures.push(file.finalize()?);
                files.insert(file.path.clone(), file);
            }

            // wait for them to be finalized
            while let Some(event) = futures.next().await {
                let event = event?;
                let file = files.get_mut(&event.path).ok_or_else(|| {
                    connector_err!(
                        Internal,
                        WithBackoff,
                        "received file event for unknown file during committing: {}",
                        event.path
                    )
                })?;
                futures.extend(file.handle_event(event.data)?);
            }

            maybe_cause_failure("commit_middle");

            let mut finished_files = vec![];
            for f in files.into_values() {
                let filename = f.path.to_string();
                let (total_size, metadata) = f.metadata_for_closed()?;

                finished_files.push(FinishedFile {
                    filename,
                    // not used
                    partition: None,
                    size: total_size,
                    metadata,
                })
            }

            // finally, commit them if we're using a table format
            match &mut self.context.as_mut().unwrap().commit_state {
                CommitState::DeltaLake {
                    last_version,
                    table,
                } => {
                    if let Some(new_version) = commit_files_to_delta(
                        &finished_files,
                        table,
                        *last_version,
                    )
                        .await
                        .map_err(
                            |e| connector_err!(External, WithBackoff, source: e, "failed to commit to delta"),
                        )? {
                        *last_version = new_version;
                    }
                }
                CommitState::Iceberg(table) => {
                    table.commit(epoch, &finished_files).await.map_err(
                        |e| connector_err!(External, WithBackoff, source: e, "failed to commit to iceberg"),
                    )?;
                }
                CommitState::VanillaParquet => {
                    // Nothing to do
                }
            }
        }

        maybe_cause_failure("commit_before_ack");

        // Send completion event
        ctx.control_tx
            .send(arroyo_rpc::ControlResp::CheckpointEvent(CheckpointEvent {
                checkpoint_epoch: epoch,
                node_id: ctx.task_info.node_id,
                operator_id: ctx.task_info.operator_id.clone(),
                subtask_index: ctx.task_info.task_index,
                time: SystemTime::now(),
                event_type: arroyo_rpc::grpc::rpc::TaskCheckpointEventType::FinishedCommit,
            }))
            .await
            .expect("sent commit event");

        maybe_cause_failure("commit_after_ack");

        Ok(())
    }

    async fn handle_tick(
        &mut self,
        _tick: u64,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let mut to_remove = vec![];
        for (partition, path) in self.active.active_partitions.iter() {
            let Some(of) = self.active.open_files.get_mut(path) else {
                warn!(file = ?path, "file referenced in active_partitions is missing from open_files!");
                to_remove.push(partition.clone());
                continue;
            };

            if self
                .upload
                .roll_file_if_ready(&self.config.rolling_policies, self.watermark, of)
                .await?
            {
                to_remove.push(partition.clone());
            }
        }

        for p in to_remove {
            self.active.active_partitions.remove(&p);
        }

        Ok(())
    }
}
