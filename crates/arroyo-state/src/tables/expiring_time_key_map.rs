use std::{
    collections::{BTreeMap, HashMap, HashSet},
    mem,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{anyhow, bail, Ok, Result};
use arrow::compute::{concat_batches, filter_record_batch, kernels::aggregate, take};
use arrow::row::OwnedRow;
use arrow_array::{
    cast::AsArray,
    types::{TimestampNanosecondType, UInt64Type},
    BooleanArray, PrimitiveArray, RecordBatch, TimestampNanosecondArray, UInt64Array,
};
use arrow_ord::{partition::partition, sort::sort_to_indices};
use arroyo_rpc::{
    df::server_for_hash_array,
    grpc::{
        ExpiringKeyedTimeSubtaskCheckpointMetadata, ExpiringKeyedTimeTableCheckpointMetadata,
        ExpiringKeyedTimeTableConfig, OperatorMetadata, ParquetTimeFile, TableEnum,
    },
    Converter,
};
use arroyo_storage::StorageProviderRef;
use arroyo_types::{
    from_micros, from_nanos, print_time, server_for_hash, to_micros, to_nanos, TaskInfoRef,
};

use futures::{StreamExt, TryStreamExt};
use parquet::{
    arrow::{async_reader::ParquetObjectReader, AsyncArrowWriter, ParquetRecordBatchStreamBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use tokio::{io::AsyncWrite, sync::mpsc::Sender};

use crate::{
    parquet::ParquetStats, schemas::SchemaWithHashAndOperation, CheckpointMessage, StateMessage,
    TableData,
};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use tracing::debug;

use super::{table_checkpoint_path, CompactionConfig, Table, TableEpochCheckpointer};

#[derive(Debug, Clone)]
pub struct ExpiringTimeKeyTable {
    table_name: String,
    task_info: TaskInfoRef,
    schema: SchemaWithHashAndOperation,
    retention: Duration,
    storage_provider: StorageProviderRef,
    checkpoint_files: Vec<ParquetTimeFile>,
}

impl ExpiringTimeKeyTable {
    pub(crate) async fn get_view(
        &self,
        state_tx: Sender<StateMessage>,
        watermark: Option<SystemTime>,
    ) -> Result<ExpiringTimeKeyView> {
        let cutoff = self.get_cutoff(watermark);
        let files = self.get_files_with_filtering(cutoff);

        let mut data: BTreeMap<SystemTime, Vec<RecordBatch>> = BTreeMap::new();
        let timestamp_index = self.schema.timestamp_index();
        let batches_by_timestamp = self
            .call_on_filtered_batches(files, |batch| {
                let timestamp_array: &PrimitiveArray<TimestampNanosecondType> = batch
                    .column(self.schema.timestamp_index())
                    .as_primitive_opt()
                    .ok_or_else(|| anyhow!("failed to find timestamp column"))?;
                let max_timestamp = from_nanos(
                    aggregate::max(timestamp_array)
                        .ok_or_else(|| anyhow!("should have max timestamp"))?
                        as u128,
                );
                let min_timestamp = from_nanos(
                    aggregate::min(timestamp_array)
                        .ok_or_else(|| anyhow!("should have min timestamp"))?
                        as u128,
                );
                let batches = if max_timestamp != min_timestamp {
                    // assume monotonic for now
                    let partitions =
                        partition(vec![batch.column(timestamp_index).clone()].as_slice())?;
                    partitions
                        .ranges()
                        .into_iter()
                        .map(|range| {
                            let timestamp = from_nanos(timestamp_array.value(range.start) as u128);
                            (timestamp, batch.slice(range.start, range.end - range.start))
                        })
                        .collect::<Vec<_>>()
                } else {
                    vec![(min_timestamp, batch)]
                };
                Ok(batches)
            })
            .await?;

        for (timestamp, batch) in batches_by_timestamp {
            if cutoff <= timestamp {
                data.entry(timestamp).or_default().push(batch)
            }
        }

        Ok(ExpiringTimeKeyView {
            flushed_batches_by_max_timestamp: data,
            parent: self.clone(),
            batches_to_flush: BTreeMap::new(),
            state_tx,
        })
    }
    async fn call_on_filtered_batches<T, F>(
        &self,
        files: Vec<(String, bool)>,
        batch_processor: F,
    ) -> Result<Vec<T>>
    where
        F: Fn(RecordBatch) -> Result<Vec<T>> + Send + Sync, // Ensure `F` is a closure that can be sent and synced between threads
    {
        let mut result = vec![];
        for (file, needs_filtering) in files {
            let object_meta = self
                .storage_provider
                .get_backing_store()
                .head(&(file.into()))
                .await?;
            let object_reader =
                ParquetObjectReader::new(self.storage_provider.get_backing_store(), object_meta);
            let reader_builder = ParquetRecordBatchStreamBuilder::new(object_reader).await?;
            let mut stream = reader_builder.build()?;
            // projection to trim the metadata fields. Should probably be factored out.
            let projection: Vec<_> = (0..(stream.schema().fields().len() - 2)).collect();
            while let Some(batch_result) = stream.next().await {
                let mut batch = batch_result?;
                if needs_filtering {
                    match self
                        .schema
                        .filter_by_hash_index(batch, &self.task_info.key_range)?
                    {
                        None => continue,
                        Some(filtered_batch) => batch = filtered_batch,
                    };
                }
                if batch.num_rows() == 0 {
                    continue;
                }
                batch = batch.project(&projection)?;
                result.extend(batch_processor(batch)?)
            }
        }
        Ok(result)
    }

    fn get_cutoff(&self, watermark: Option<SystemTime>) -> SystemTime {
        watermark
            .map(|watermark| watermark - self.retention)
            .unwrap_or(SystemTime::UNIX_EPOCH)
    }

    fn get_files_with_filtering(&self, cutoff: SystemTime) -> Vec<(String, bool)> {
        self.checkpoint_files
            .iter()
            .filter_map(|file| {
                // file must have some data greater than the cutoff and routing keys within the range.
                if cutoff <= from_micros(file.max_timestamp_micros)
                    && (file.max_routing_key >= *self.task_info.key_range.start()
                        && *self.task_info.key_range.end() >= file.min_routing_key)
                {
                    let needs_hash_filtering = *self.task_info.key_range.end()
                        < file.max_routing_key
                        || *self.task_info.key_range.start() > file.min_routing_key;
                    Some((file.file.clone(), needs_hash_filtering))
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) async fn get_key_time_view(
        &self,
        state_tx: Sender<StateMessage>,
        watermark: Option<SystemTime>,
    ) -> Result<KeyTimeView> {
        let cutoff = self.get_cutoff(watermark);
        let files = self.get_files_with_filtering(cutoff);

        let mut view = KeyTimeView::new(self.clone(), state_tx)?;
        let batches_to_add = self
            .call_on_filtered_batches(files, |batch| {
                let timestamp_array: &PrimitiveArray<TimestampNanosecondType> = batch
                    .column(self.schema.timestamp_index())
                    .as_primitive_opt()
                    .ok_or_else(|| anyhow!("failed to find timestamp column"))?;
                let max_timestamp = from_nanos(
                    aggregate::max(timestamp_array)
                        .ok_or_else(|| anyhow!("should have max timestamp"))?
                        as u128,
                );
                if max_timestamp < cutoff {
                    Ok(vec![])
                } else {
                    Ok(vec![batch])
                }
            })
            .await?;
        for batch in batches_to_add {
            view.insert_internal(batch)?;
        }
        Ok(view)
    }

    pub(crate) async fn get_last_key_value_view(
        &self,
        state_tx: Sender<StateMessage>,
        watermark: Option<SystemTime>,
    ) -> Result<LastKeyValueView> {
        let cutoff = self.get_cutoff(watermark);
        let files = self.get_files_with_filtering(cutoff);
        let mut view = LastKeyValueView::new(self.clone(), state_tx)?;
        let batches = self
            .call_on_filtered_batches(files, |batch| {
                let timestamp_array: &PrimitiveArray<TimestampNanosecondType> = batch
                    .column(self.schema.timestamp_index())
                    .as_primitive_opt()
                    .ok_or_else(|| anyhow!("failed to find timestamp column"))?;
                let max_timestamp = from_nanos(
                    aggregate::max(timestamp_array)
                        .ok_or_else(|| anyhow!("should have max timestamp"))?
                        as u128,
                );
                if max_timestamp < cutoff {
                    Ok(vec![])
                } else {
                    Ok(vec![batch])
                }
            })
            .await?;
        for batch in batches {
            view.insert_batch_internal(batch, true).await?;
        }
        Ok(view)
    }
}

#[async_trait::async_trait]
impl Table for ExpiringTimeKeyTable {
    type Checkpointer = ExpiringTimeKeyTableCheckpointer;

    type ConfigMessage = ExpiringKeyedTimeTableConfig;

    type TableCheckpointMessage = ExpiringKeyedTimeTableCheckpointMetadata;

    type TableSubtaskCheckpointMetadata = ExpiringKeyedTimeSubtaskCheckpointMetadata;

    fn from_config(
        config: Self::ConfigMessage,
        task_info: arroyo_types::TaskInfoRef,
        storage_provider: arroyo_storage::StorageProviderRef,
        checkpoint_message: Option<Self::TableCheckpointMessage>,
    ) -> anyhow::Result<Self> {
        let schema: ArroyoSchema = config
            .schema
            .ok_or_else(|| anyhow!("should have schema"))?
            .try_into()?;

        let schema = SchemaWithHashAndOperation::new(Arc::new(schema), config.generational);

        let mut checkpoint_files = checkpoint_message
            .map(|checkpoint_message| checkpoint_message.files)
            .unwrap_or_default();
        // sort by epoch
        checkpoint_files.sort_by_key(|file| file.epoch);
        Ok(Self {
            table_name: config.table_name,
            task_info,
            schema,
            retention: Duration::from_micros(config.retention_micros),
            storage_provider,
            checkpoint_files,
        })
    }

    fn epoch_checkpointer(
        &self,
        epoch: u32,
        previous_metadata: Option<Self::TableSubtaskCheckpointMetadata>,
    ) -> Result<Self::Checkpointer> {
        let prior_files = previous_metadata.map(|meta| meta.files).unwrap_or_default();
        ExpiringTimeKeyTableCheckpointer::new(self.clone(), epoch, prior_files)
    }

    fn merge_checkpoint_metadata(
        config: Self::ConfigMessage,
        subtask_metadata: HashMap<
            u32,
            <Self::Checkpointer as TableEpochCheckpointer>::SubTableCheckpointMessage,
        >,
    ) -> Result<Option<Self::TableCheckpointMessage>> {
        if subtask_metadata.is_empty() {
            return Ok(None);
        }
        let min_watermark = subtask_metadata
            .values()
            .filter_map(|metadata| metadata.watermark)
            .min();
        let _max_watermark = subtask_metadata
            .values()
            .filter_map(|metadata| metadata.watermark)
            .min();
        let cutoff = min_watermark
            .map(|min_watermark| min_watermark - config.retention_micros)
            .unwrap_or_default();
        let files: Vec<_> = subtask_metadata
            .into_values()
            .flat_map(|metadata: ExpiringKeyedTimeSubtaskCheckpointMetadata| metadata.files)
            .filter(|file| cutoff <= file.max_timestamp_micros)
            .collect();

        let mut seen_files = HashSet::new();
        let dedupped_files = files
            .into_iter()
            .filter_map(|file| {
                if seen_files.contains(&file.file) {
                    None
                } else {
                    seen_files.insert(file.file.to_string());
                    Some(file)
                }
            })
            .collect();

        Ok(Some(ExpiringKeyedTimeTableCheckpointMetadata {
            files: dedupped_files,
        }))
    }

    fn subtask_metadata_from_table(
        &self,
        table_metadata: Self::TableCheckpointMessage,
    ) -> anyhow::Result<Option<Self::TableSubtaskCheckpointMetadata>> {
        Ok(Some(ExpiringKeyedTimeSubtaskCheckpointMetadata {
            subtask_index: self.task_info.task_index as u32,
            watermark: None,
            files: table_metadata.files,
        }))
    }

    fn table_type() -> arroyo_rpc::grpc::TableEnum {
        TableEnum::ExpiringKeyedTimeTable
    }

    fn task_info(&self) -> TaskInfoRef {
        self.task_info.clone()
    }

    fn files_to_keep(
        _config: Self::ConfigMessage,
        checkpoint: Self::TableCheckpointMessage,
    ) -> Result<HashSet<String>> {
        Ok(checkpoint
            .files
            .into_iter()
            .map(|file: ParquetTimeFile| file.file)
            .collect())
    }
    fn apply_compacted_checkpoint(
        &self,
        epoch: u32,
        compacted_checkpoint: Self::TableSubtaskCheckpointMetadata,
        subtask_metadata: Self::TableSubtaskCheckpointMetadata,
    ) -> Result<Self::TableSubtaskCheckpointMetadata> {
        let mut current_epoch_files: Vec<_> = subtask_metadata
            .files
            .into_iter()
            .filter(|file| file.epoch == epoch)
            .collect();
        current_epoch_files.extend_from_slice(&compacted_checkpoint.files);

        Ok(Self::TableSubtaskCheckpointMetadata {
            subtask_index: subtask_metadata.subtask_index,
            watermark: subtask_metadata.watermark,
            files: current_epoch_files,
        })
    }

    async fn compact_data(
        config: Self::ConfigMessage,
        compaction_config: &CompactionConfig,
        operator_metadata: &OperatorMetadata,
        current_metadata: Self::TableCheckpointMessage,
    ) -> Result<Option<Self::TableCheckpointMessage>> {
        let mut epochs_in_generation: HashMap<u64, HashSet<u32>> = HashMap::new();
        let mut files_by_generation: BTreeMap<u64, HashMap<String, ParquetTimeFile>> =
            BTreeMap::new();
        for file in current_metadata.files {
            if compaction_config
                .compact_generations
                .contains(&file.generation)
            {
                epochs_in_generation
                    .entry(file.generation)
                    .or_default()
                    .insert(file.epoch);
            }
            files_by_generation
                .entry(file.generation)
                .or_default()
                .insert(file.file.to_string(), file);
        }
        let schema: ArroyoSchema = config
            .schema
            .ok_or_else(|| anyhow!("expect schema"))?
            .try_into()?;
        let state_schema = SchemaWithHashAndOperation::new(Arc::new(schema), config.generational);

        for (generation, epochs) in epochs_in_generation {
            if epochs.len() < compaction_config.min_compaction_epochs {
                continue;
            }
            let mut files = TimeTableCompactor::compact_files(
                config.table_name,
                epochs.into_iter().max().unwrap(),
                generation + 1,
                compaction_config.storage_provider.clone(),
                state_schema,
                Duration::from_micros(config.retention_micros),
                operator_metadata,
                files_by_generation
                    .remove(&generation)
                    .expect("will have been populated"),
            )
            .await?;
            files.extend(
                files_by_generation
                    .into_values()
                    .flat_map(|files| files.into_values()),
            );
            return Ok(Some(ExpiringKeyedTimeTableCheckpointMetadata { files }));
        }
        Ok(None)
    }
}

struct CompactedFileWriter {
    file_name: String,
    schema: SchemaWithHashAndOperation,
    writer: Option<AsyncArrowWriter<Box<dyn AsyncWrite + Send + Unpin>>>,
    parquet_stats: Option<ParquetStats>,
}

struct TimeTableCompactor {
    storage_provider: StorageProviderRef,
    schema: SchemaWithHashAndOperation,
    operator_metadata: OperatorMetadata,
    table: String,
    writers: HashMap<usize, CompactedFileWriter>,
}

impl TimeTableCompactor {
    #[allow(clippy::too_many_arguments)]
    async fn compact_files(
        table: String,
        epoch: u32,
        generation: u64,
        storage_provider: StorageProviderRef,
        schema: SchemaWithHashAndOperation,
        retention: Duration,
        operator_metadata: &OperatorMetadata,
        files: HashMap<String, ParquetTimeFile>,
    ) -> Result<Vec<ParquetTimeFile>> {
        let mut compactor = Self {
            table,
            storage_provider,
            schema: schema.clone(),
            operator_metadata: operator_metadata.clone(),
            writers: HashMap::new(),
        };
        let cutoff = operator_metadata
            .min_watermark
            .map(|min_micros| from_micros(min_micros) - retention);
        for (file_name, file) in files {
            let max_file_timestamp = from_micros(file.max_timestamp_micros);
            if cutoff
                .map(|cutoff| max_file_timestamp < cutoff)
                .unwrap_or(false)
            {
                continue;
            }
            let reader = ParquetObjectReader::new(
                compactor.storage_provider.get_backing_store(),
                compactor
                    .storage_provider
                    .get_backing_store()
                    .head(&(file_name.clone().into()))
                    .await?,
            );
            let first_partition =
                server_for_hash(file.min_routing_key, operator_metadata.parallelism as usize);
            let last_partition =
                server_for_hash(file.max_routing_key, operator_metadata.parallelism as usize);
            let multiple_partitions = first_partition != last_partition;
            let reader_builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
            let mut stream = reader_builder.build()?;
            // projection to trim the metadata fields. Should probably be factored out.
            while let Some(batch) = stream.try_next().await? {
                // Filter by _timestamp field
                let time_filtered = schema.state_schema().filter_by_time(batch, cutoff)?;
                if time_filtered.num_rows() == 0 {
                    continue;
                }
                if !multiple_partitions {
                    compactor
                        .write_batch(first_partition, time_filtered)
                        .await?;
                } else {
                    // this record batch contains data belonging to multiple partitions.
                    let partitions = server_for_hash_array(
                        time_filtered
                            .column(schema.hash_index())
                            .as_any()
                            .downcast_ref::<PrimitiveArray<UInt64Type>>()
                            .unwrap(),
                        operator_metadata.parallelism as usize,
                    )?;
                    let indices = sort_to_indices(&partitions, None, None).unwrap();
                    let columns = time_filtered
                        .columns()
                        .iter()
                        .map(|c| take(c, &indices, None).unwrap())
                        .collect();
                    let sorted =
                        RecordBatch::try_new(schema.state_schema().schema.clone(), columns)?;
                    let sorted_keys = take(&partitions, &indices, None)?;

                    let partition = partition(vec![sorted_keys.clone()].as_slice())?;
                    let typed_keys: &PrimitiveArray<UInt64Type> =
                        sorted_keys.as_any().downcast_ref().unwrap();
                    for range in partition.ranges() {
                        let partition = typed_keys.value(range.start);
                        compactor
                            .write_batch(
                                partition as usize,
                                sorted.slice(range.start, range.end - range.start),
                            )
                            .await?
                    }
                }
            }
        }
        compactor.finish(epoch, generation).await
    }

    async fn write_batch(&mut self, partition: usize, record_batch: RecordBatch) -> Result<()> {
        if let std::collections::hash_map::Entry::Vacant(e) = self.writers.entry(partition) {
            let file_name = table_checkpoint_path(
                &self.operator_metadata.job_id,
                &self.operator_metadata.operator_id,
                &self.table,
                partition,
                self.operator_metadata.epoch,
                true,
            );
            let (_multipart_id, async_writer) = self
                .storage_provider
                .get_backing_store()
                .put_multipart(&(file_name.clone().into()))
                .await?;
            let writer = Some(AsyncArrowWriter::try_new(
                async_writer,
                self.schema.state_schema().schema.clone(),
                None,
            )?);
            e.insert(CompactedFileWriter {
                file_name,
                schema: self.schema.clone(),
                writer,
                parquet_stats: None,
            });
        }
        let writer = self.writers.get_mut(&partition).unwrap();

        writer.write_batch(record_batch).await?;

        Ok(())
    }

    async fn finish(self, epoch: u32, generation: u64) -> Result<Vec<ParquetTimeFile>> {
        let mut results = vec![];
        for writer in self.writers.into_values() {
            results.push(writer.finish(epoch, generation).await?);
        }
        Ok(results)
    }
}

impl CompactedFileWriter {
    async fn write_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        let mut parquet_stats = self.schema.batch_stats_from_state_batch(&record_batch)?;
        if let Some(other) = self.parquet_stats.take() {
            parquet_stats.merge(other);
        }
        self.parquet_stats = Some(parquet_stats);
        let Some(writer) = self.writer.as_mut() else {
            bail!("should have writer");
        };
        writer.write(&record_batch).await?;
        Ok(())
    }

    async fn finish(mut self, epoch: u32, generation: u64) -> Result<ParquetTimeFile> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| anyhow!("unset compacted file writer {}", self.file_name))?;
        let _closed = writer.close().await?;
        let stats = self.parquet_stats.take().expect("should have stats");
        Ok(ParquetTimeFile {
            epoch,
            file: self.file_name,
            min_routing_key: stats.min_routing_key,
            max_routing_key: stats.max_routing_key,
            max_timestamp_micros: to_micros(stats.max_timestamp),
            generation,
        })
    }
}

pub struct ExpiringTimeKeyTableCheckpointer {
    file_name: String,
    parent: ExpiringTimeKeyTable,
    epoch: u32,
    writer: Option<AsyncArrowWriter<Box<dyn AsyncWrite + Send + Unpin>>>,
    parquet_stats: Option<ParquetStats>,
    prior_files: Vec<ParquetTimeFile>,
}

impl ExpiringTimeKeyTableCheckpointer {
    fn new(
        parent: ExpiringTimeKeyTable,
        epoch: u32,
        prior_files: Vec<ParquetTimeFile>,
    ) -> Result<Self> {
        let file_name = table_checkpoint_path(
            &parent.task_info.job_id,
            &parent.task_info.operator_id,
            &parent.table_name,
            parent.task_info.task_index,
            epoch,
            false,
        );
        Ok(Self {
            file_name,
            parent,
            epoch,
            writer: None,
            parquet_stats: None,
            prior_files,
        })
    }
    async fn init_writer(&mut self) -> Result<()> {
        let (_multipart_id, async_writer) = self
            .parent
            .storage_provider
            .get_backing_store()
            .put_multipart(&self.file_name.clone().into())
            .await?;
        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        self.writer = Some(AsyncArrowWriter::try_new(
            async_writer,
            self.parent.schema.state_schema().schema.clone(),
            Some(writer_properties),
        )?);
        Ok(())
    }
}

#[async_trait::async_trait]
impl TableEpochCheckpointer for ExpiringTimeKeyTableCheckpointer {
    type SubTableCheckpointMessage = ExpiringKeyedTimeSubtaskCheckpointMetadata;

    async fn insert_data(&mut self, data: crate::TableData) -> anyhow::Result<()> {
        let TableData::RecordBatch(batch) = data else {
            bail!("expect record batch data for expiring time key map tables")
        };
        if self.writer.is_none() {
            self.init_writer().await?;
        }
        let (annotated_batch, batch_stats) = self.annotate_record_batch(&batch)?;
        self.update_parquet_stats(batch_stats);
        self.writer
            .as_mut()
            .expect("writer should be set")
            .write(&annotated_batch)
            .await?;
        Ok(())
    }

    async fn finish(
        mut self,
        checkpoint: &CheckpointMessage,
    ) -> Result<Option<(Self::SubTableCheckpointMessage, usize)>> {
        let cutoff = checkpoint
            .watermark
            .map(|watermark| to_micros(watermark - self.parent.retention))
            .unwrap_or_default();
        let mut files: Vec<_> = self
            .prior_files
            .into_iter()
            .filter(|file| {
                // file must have some data greater than the cutoff and routing keys within the range.
                cutoff <= file.max_timestamp_micros
                    && (file.max_routing_key >= *self.parent.task_info.key_range.start()
                        && *self.parent.task_info.key_range.end() >= file.min_routing_key)
            })
            .collect();
        let mut bytes = 0;
        if let Some(writer) = self.writer.take() {
            let _result = writer.close().await?;

            let stats = self.parquet_stats.expect("should have set parquet stats");
            let meta = self
                .parent
                .storage_provider
                .get_backing_store()
                .head(&(self.file_name.clone().into()))
                .await?;
            bytes += meta.size;
            let file = ParquetTimeFile {
                epoch: self.epoch,
                file: self.file_name,
                min_routing_key: stats.min_routing_key,
                max_routing_key: stats.max_routing_key,
                max_timestamp_micros: to_micros(stats.max_timestamp),
                generation: 0,
            };
            files.push(file)
        }
        if files.is_empty() {
            Ok(None)
        } else {
            Ok(Some((
                ExpiringKeyedTimeSubtaskCheckpointMetadata {
                    subtask_index: self.parent.task_info.task_index as u32,
                    watermark: checkpoint.watermark.map(to_micros),
                    files,
                },
                bytes,
            )))
        }
    }

    fn table_type() -> arroyo_rpc::grpc::TableEnum {
        TableEnum::ExpiringKeyedTimeTable
    }

    fn subtask_index(&self) -> u32 {
        self.parent.task_info.task_index as u32
    }
}

impl ExpiringTimeKeyTableCheckpointer {
    fn annotate_record_batch(
        &mut self,
        record_batch: &RecordBatch,
    ) -> Result<(RecordBatch, ParquetStats)> {
        self.parent.schema.annotate_record_batch(record_batch)
    }

    fn update_parquet_stats(&mut self, parquet_stats: ParquetStats) {
        match self.parquet_stats.as_mut() {
            None => {
                self.parquet_stats = Some(parquet_stats);
            }
            Some(current_stats) => {
                current_stats.merge(parquet_stats);
            }
        }
    }
}

#[derive(Debug)]
pub struct ExpiringTimeKeyView {
    parent: ExpiringTimeKeyTable,
    flushed_batches_by_max_timestamp: BTreeMap<SystemTime, Vec<RecordBatch>>,
    batches_to_flush: BTreeMap<SystemTime, Vec<RecordBatch>>,
    state_tx: Sender<StateMessage>,
}

impl ExpiringTimeKeyView {
    pub async fn flush(&mut self, watermark: Option<SystemTime>) -> Result<()> {
        while let Some((max_timestamp, mut batches)) = self.batches_to_flush.pop_first() {
            if watermark
                .map(|watermark| max_timestamp < watermark - self.parent.retention)
                .unwrap_or(false)
            {
                continue;
            }
            for batch in &batches {
                self.state_tx
                    .send(StateMessage::TableData {
                        table: self.parent.table_name.to_string(),
                        data: TableData::RecordBatch(batch.clone()),
                    })
                    .await?;
            }
            self.flushed_batches_by_max_timestamp
                .entry(max_timestamp)
                .or_default()
                .append(&mut batches);
        }
        if let Some(watermark) = watermark {
            let cutoff = watermark - self.parent.retention;
            self.flushed_batches_by_max_timestamp =
                self.flushed_batches_by_max_timestamp.split_off(&cutoff);
        }
        Ok(())
    }

    pub fn insert(&mut self, max_timestamp: SystemTime, batch: RecordBatch) {
        self.batches_to_flush
            .entry(max_timestamp)
            .or_default()
            .push(batch);
    }

    pub fn all_batches_for_watermark(
        &self,
        watermark: Option<SystemTime>,
    ) -> impl Iterator<Item = (&SystemTime, &Vec<RecordBatch>)> {
        // TODO: decide how to manage hash range ownership. Previously this was done by iterating over the contents of the record batch.
        // Should we use statistics?
        let cutoff = watermark
            .map(|watermark| watermark - self.parent.retention)
            .unwrap_or_else(|| SystemTime::UNIX_EPOCH);
        debug!("CUTOFF IS {}", print_time(cutoff));
        let flushed_range = self.flushed_batches_by_max_timestamp.range(cutoff..);
        let buffered_range = self.batches_to_flush.range(cutoff..);
        flushed_range.chain(buffered_range)
    }

    pub fn expire_timestamp(&mut self, timestamp: SystemTime) -> Vec<RecordBatch> {
        let flushed_batches = self.flushed_batches_by_max_timestamp.remove(&timestamp);
        let buffered_batches = self.batches_to_flush.remove(&timestamp);
        match (flushed_batches, buffered_batches) {
            (None, None) => vec![],
            (None, Some(batches)) | (Some(batches), None) => batches,
            (Some(mut flushed_batches), Some(mut buffered_batches)) => {
                flushed_batches.append(&mut buffered_batches);
                flushed_batches
            }
        }
    }

    pub async fn flush_timestamp(&mut self, bin_start: SystemTime) -> Result<()> {
        let Some(batches_to_flush) = self.batches_to_flush.remove(&bin_start) else {
            return Ok(());
        };
        let flushed_vec = self
            .flushed_batches_by_max_timestamp
            .entry(bin_start)
            .or_default();
        for batch in batches_to_flush {
            flushed_vec.push(batch.clone());
            self.state_tx
                .send(StateMessage::TableData {
                    table: self.parent.table_name.to_string(),
                    data: TableData::RecordBatch(batch),
                })
                .await?;
        }
        Ok(())
    }

    pub fn get_min_time(&self) -> Option<SystemTime> {
        match (
            self.batches_to_flush.keys().next(),
            self.flushed_batches_by_max_timestamp.keys().next(),
        ) {
            (None, None) => None,
            (None, Some(time)) | (Some(time), None) => Some(*time),
            (Some(buffered_time), Some(flushed_time)) => Some(*buffered_time.min(flushed_time)),
        }
    }
}

#[derive(Debug)]
pub struct KeyTimeView {
    key_converter: Converter,
    parent: ExpiringTimeKeyTable,
    keyed_data: HashMap<Vec<u8>, BatchData>,
    schema: ArroyoSchemaRef,
    value_schema: ArroyoSchemaRef,
    // indices of schema that aren't keys, used for projection
    value_indices: Vec<usize>,
    state_tx: Sender<StateMessage>,
}

#[derive(Debug)]
enum BatchData {
    SingleBatch(RecordBatch),
    BatchVec(Vec<RecordBatch>),
}

impl KeyTimeView {
    fn new(parent: ExpiringTimeKeyTable, state_tx: Sender<StateMessage>) -> Result<Self> {
        let schema = parent.schema.memory_schema();
        let key_converter = schema.converter(false)?;
        let value_schema = Arc::new(schema.schema_without_keys()?);
        let value_indices = schema.value_indices(true);
        Ok(Self {
            key_converter,
            parent,
            keyed_data: HashMap::new(),
            schema,
            value_indices,
            value_schema,
            state_tx,
        })
    }

    pub fn get_batch(&mut self, row: &[u8]) -> Result<Option<&RecordBatch>> {
        if !self.keyed_data.contains_key(row) {
            return Ok(None);
        }
        let Some(value) = self.keyed_data.get_mut(row) else {
            unreachable!("just checked")
        };
        if let BatchData::BatchVec(batches) = value {
            let coalesced_batches = concat_batches(&self.value_schema.schema, batches.iter())?;
            *value = BatchData::SingleBatch(coalesced_batches);
        }
        let Some(BatchData::SingleBatch(single_batch)) = self.keyed_data.get(row) else {
            unreachable!("just inserted")
        };
        Ok(Some(single_batch))
    }

    pub async fn write_batch_to_state(&mut self, batch: RecordBatch) -> Result<()> {
        self.state_tx
            .send(StateMessage::TableData {
                table: self.parent.table_name.to_string(),
                data: TableData::RecordBatch(batch.clone()),
            })
            .await?;
        Ok(())
    }

    pub async fn insert(&mut self, batch: RecordBatch) -> Result<Vec<OwnedRow>> {
        self.state_tx
            .send(StateMessage::TableData {
                table: self.parent.table_name.to_string(),
                data: TableData::RecordBatch(batch.clone()),
            })
            .await?;
        Ok(self.insert_internal(batch)?)
    }

    fn insert_internal(&mut self, batch: RecordBatch) -> Result<Vec<OwnedRow>> {
        let sorted_batch = self.schema.sort(batch, false)?;
        let value_batch = sorted_batch.project(&self.value_indices)?;
        let mut rows = vec![];
        for range in self.schema.partition(&sorted_batch, false)? {
            let value_batch = value_batch.slice(range.start, range.end - range.start);
            let key_columns = if self.schema.key_indices.is_none() {
                vec![]
            } else {
                sorted_batch
                    .slice(range.start, 1)
                    .project(self.schema.key_indices.as_ref().unwrap())?
                    .columns()
                    .to_vec()
            };
            let key_row = self.key_converter.convert_columns(&key_columns)?;
            let contents = self.keyed_data.get_mut(key_row.as_ref());
            rows.push(key_row.clone());
            let batch = match contents {
                Some(BatchData::BatchVec(vec)) => {
                    vec.push(value_batch);
                    continue;
                }
                None => {
                    self.keyed_data.insert(
                        key_row.as_ref().to_vec(),
                        BatchData::SingleBatch(value_batch),
                    );
                    continue;
                }
                Some(BatchData::SingleBatch(single_batch)) => single_batch.clone(),
            };
            self.keyed_data.insert(
                key_row.as_ref().to_vec(),
                BatchData::BatchVec(vec![batch, value_batch]),
            );
        }
        Ok(rows)
    }
}

#[derive(Debug)]
pub struct LastKeyValueView {
    parent: ExpiringTimeKeyTable,
    key_converter: Converter,
    value_converter: Converter,
    value_indices: Vec<usize>,
    // indices of schema that aren't keys, used for projection
    backing_map: HashMap<Vec<u8>, Value>,
    expirations: BTreeMap<SystemTime, HashSet<Vec<u8>>>,
    state_tx: Sender<StateMessage>,
    key_indices: Vec<usize>,
}
#[derive(Debug)]
struct Value {
    value_row_bytes: Vec<u8>,
    timestamp: SystemTime,
    generation: u64,
}

impl LastKeyValueView {
    fn new(parent: ExpiringTimeKeyTable, state_tx: Sender<StateMessage>) -> Result<Self> {
        let schema = parent.schema.memory_schema();
        let key_converter = schema.converter(false)?;
        let Some(generation_index) = parent.schema.generation_index() else {
            bail!("should have generation index")
        };
        let value_converter = schema.value_converter(false, generation_index)?;
        let value_indices = schema
            .value_indices(false)
            .into_iter()
            .filter(|index| *index != generation_index)
            .collect();
        let backing_map = HashMap::new();
        let expirations = BTreeMap::new();
        Ok(Self {
            key_converter,
            value_converter,
            value_indices,
            backing_map,
            expirations,
            parent,
            state_tx,
            key_indices: schema.key_indices.as_ref().unwrap().clone(),
        })
    }
    pub async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.insert_batch_internal(batch, false).await
    }

    pub fn get_current_matching_values(
        &self,
        batch: &RecordBatch,
    ) -> Result<Option<(RecordBatch, BooleanArray)>> {
        if self.backing_map.is_empty() {
            return Ok(None);
        }
        let key_batch: RecordBatch = batch.project(&self.key_indices)?;
        let key_rows = self
            .key_converter
            .convert_all_columns(key_batch.columns(), key_batch.num_rows())?;
        let capacity = batch.num_rows().min(self.backing_map.len());
        let mut prior_values = Vec::with_capacity(capacity);
        let mut prior_timestamp_builder = TimestampNanosecondArray::builder(capacity);
        let mut prior_row_filter = BooleanArray::builder(capacity);
        for i in 0..batch.num_rows() {
            match self.backing_map.get(key_rows.row(i).as_ref()) {
                None => {
                    prior_row_filter.append_value(false);
                }
                Some(Value {
                    value_row_bytes,
                    timestamp,
                    generation: _,
                }) => {
                    prior_row_filter.append_value(true);
                    prior_timestamp_builder.append_value(to_nanos(*timestamp) as i64);
                    prior_values.push(value_row_bytes.as_slice());
                }
            }
        }
        let filter = prior_row_filter.finish();
        let filtered_key_batch: RecordBatch = filter_record_batch(&key_batch, &filter)?;
        if filtered_key_batch.num_rows() == 0 {
            return Ok(None);
        }
        let mut columns = filtered_key_batch.columns().to_vec();
        let value_columns = self.value_converter.convert_raw_rows(prior_values)?;
        columns.extend(value_columns);
        columns.push(Arc::new(prior_timestamp_builder.finish()));

        Ok(Some((
            RecordBatch::try_new(batch.schema(), columns)?,
            filter,
        )))
    }

    async fn insert_batch_internal(&mut self, batch: RecordBatch, is_backfill: bool) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let key_batch: RecordBatch = batch.project(&self.key_indices)?;
        let value_batch = batch.project(&self.value_indices)?;
        let key_rows = self
            .key_converter
            .convert_all_columns(key_batch.columns(), key_batch.num_rows())?;
        let value_rows = self
            .value_converter
            .convert_all_columns(value_batch.columns(), key_batch.num_rows())?;
        let timestamp_columns = batch
            .column(self.parent.schema.memory_schema().timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| anyhow!("should be able to extract timestamp array"))?;
        let mut max_timestamps = Vec::with_capacity(batch.num_rows());
        let mut new_generation_values = Vec::with_capacity(batch.num_rows());

        let generation_array = is_backfill
            .then(|| {
                batch
                    .column(
                        self.parent
                            .schema
                            .generation_index()
                            .ok_or_else(|| anyhow!("should have generation index"))?,
                    )
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("should have generation array"))
            })
            .transpose()?;
        for i in 0..batch.num_rows() {
            let (max_timestamp, generation) = self.insert_entry(
                key_rows.row(i).as_ref(),
                value_rows.row(i).as_ref(),
                from_nanos(timestamp_columns.value(i) as u128),
                generation_array.map(|generation_array| generation_array.value(i)),
            )?;
            if !is_backfill {
                new_generation_values.push(generation);
                max_timestamps.push(to_nanos(max_timestamp) as i64);
            }
        }
        if is_backfill {
            return Ok(());
        }
        let mut columns = batch.columns().to_vec();
        columns[self.parent.schema.memory_schema().timestamp_index] =
            Arc::new(TimestampNanosecondArray::from(max_timestamps));
        columns.push(Arc::new(UInt64Array::from(new_generation_values)));
        let batch =
            RecordBatch::try_new(self.parent.schema.memory_schema().schema.clone(), columns)?;
        self.state_tx
            .send(StateMessage::TableData {
                table: self.parent.table_name.to_string(),
                data: TableData::RecordBatch(batch.clone()),
            })
            .await?;
        Ok(())
    }

    fn insert_entry(
        &mut self,
        key_row: &[u8],
        value_row: &[u8],
        timestamp: SystemTime,
        generation: Option<u64>,
    ) -> Result<(SystemTime, u64)> {
        match self.backing_map.get_mut(key_row) {
            None => {
                let generation = generation.unwrap_or_default();
                self.backing_map.insert(
                    key_row.to_vec(),
                    Value {
                        value_row_bytes: value_row.to_vec(),
                        timestamp,
                        generation,
                    },
                );
                self.expirations
                    .entry(timestamp)
                    .or_default()
                    .insert(key_row.to_vec());
                Ok((timestamp, generation))
            }
            Some(Value {
                value_row_bytes: old_value,
                timestamp: existing_timestamp,
                generation: current_generation,
            }) => {
                match generation {
                    Some(generation) => {
                        // this handles out of order backfills, we only want the largest generation.
                        if generation < *current_generation {
                            return Ok((*existing_timestamp, *current_generation));
                        }
                        *current_generation = generation;
                        generation
                    }
                    // if it is new data, bump the generation by 1.
                    None => {
                        *current_generation += 1;
                        *current_generation
                    }
                };

                if *existing_timestamp < timestamp {
                    self.expirations
                        .get_mut(existing_timestamp)
                        .unwrap()
                        .remove(key_row);
                    self.expirations
                        .entry(timestamp)
                        .or_default()
                        .insert(key_row.to_vec());
                    *existing_timestamp = timestamp;
                }
                *old_value = value_row.to_vec();
                Ok((*existing_timestamp, *current_generation))
            }
        }
    }

    pub fn would_expire(&mut self, watermark: Option<SystemTime>) -> bool {
        let Some(watermark) = watermark else {
            return false;
        };
        let cutoff = watermark - self.parent.retention;
        let Some((earliest_timestamp, _value)) = self.expirations.first_key_value() else {
            return false;
        };
        *earliest_timestamp < cutoff
    }

    pub fn expire(&mut self, watermark: Option<SystemTime>) -> Result<()> {
        let Some(watermark) = watermark else {
            return Ok(());
        };
        let cutoff = watermark - self.parent.retention;
        let mut to_delete = self.expirations.split_off(&cutoff);
        mem::swap(&mut self.expirations, &mut to_delete);
        for keys in to_delete.values() {
            for key in keys {
                self.backing_map.remove(key);
            }
        }
        Ok(())
    }
}
