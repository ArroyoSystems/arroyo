use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::{Duration, SystemTime},
};


use anyhow::{anyhow, bail, Ok, Result};
use arrow::compute::{
    kernels::{aggregate},
};
use arrow_array::{
    cast::AsArray,
    types::{TimestampNanosecondType},
    PrimitiveArray, RecordBatch,
};
use arrow_ord::partition::partition;


use arroyo_rpc::{
    grpc::{
        ExpiringKeyedTimeSubtaskCheckpointMetadata, ExpiringKeyedTimeTableCheckpointMetadata,
        ExpiringKeyedTimeTableConfig, ParquetTimeFile, TableEnum,
    },
    ArroyoSchema,
};
use arroyo_storage::StorageProviderRef;
use arroyo_types::{from_micros, from_nanos, print_time, to_micros, TaskInfoRef};


use futures::StreamExt;
use parquet::arrow::{
    async_reader::ParquetObjectReader, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
};
use tokio::{io::AsyncWrite, sync::mpsc::Sender};

use crate::{
    parquet::ParquetStats, schemas::SchemaWithHashAndOperation, CheckpointMessage, StateMessage, TableData,
};
use tracing::{info, warn};

use super::{table_checkpoint_path, Table, TableEpochCheckpointer};

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
        let cutoff = watermark
            .map(|watermark| (watermark - self.retention))
            .unwrap_or_else(|| SystemTime::UNIX_EPOCH);
        info!(
            "watermark is {:?}, cutoff is {:?}",
            watermark.map(print_time),
            print_time(cutoff)
        );
        let files: Vec<_> = self
            .checkpoint_files
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
            .collect();

        let mut data: BTreeMap<SystemTime, Vec<RecordBatch>> = BTreeMap::new();
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
            let projection: Vec<_> = (0..(stream.schema().all_fields().len() - 2)).collect();
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
                batch = batch.project(&projection)?;
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
                    let partitions = partition(
                        vec![batch.column(self.schema.timestamp_index()).clone()].as_slice(),
                    )?;
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
                for (timestamp, batch) in batches {
                    if cutoff <= timestamp {
                        info!(
                            "inserting batch {:?} with timestamp {:?}",
                            batch,
                            print_time(timestamp)
                        );
                        data.entry(timestamp).or_default().push(batch)
                    } else {
                        info!(
                            "filtered batch {:?} with timestamp {:?}",
                            batch,
                            print_time(timestamp)
                        );
                    }
                }
            }
        }

        Ok(ExpiringTimeKeyView {
            flushed_batches_by_max_timestamp: data,
            parent: self.clone(),
            batches_to_flush: BTreeMap::new(),
            state_tx,
        })
    }
}

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

        let schema = SchemaWithHashAndOperation::new(schema);

        let checkpoint_files = checkpoint_message
            .map(|checkpoint_message| checkpoint_message.files)
            .unwrap_or_default();
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
        let file_name = table_checkpoint_path(&parent.task_info, &parent.table_name, epoch, false);
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
        self.writer = Some(AsyncArrowWriter::try_new(
            async_writer,
            self.parent.schema.state_schema(),
            1_000_0000,
            None,
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
    ) -> Result<Option<Self::SubTableCheckpointMessage>> {
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
        if let Some(writer) = self.writer.take() {
            // TODO: figure out how to get the size from this writer.
            let _result = writer.close().await?;

            let stats = self.parquet_stats.expect("should have set parquet stats");
            let file = ParquetTimeFile {
                epoch: self.epoch,
                file: self.file_name,
                min_routing_key: stats.min_routing_key,
                max_routing_key: stats.max_routing_key,
                max_timestamp_micros: to_micros(stats.max_timestamp),
            };
            files.push(file)
        }
        if files.is_empty() {
            Ok(None)
        } else {
            Ok(Some(ExpiringKeyedTimeSubtaskCheckpointMetadata {
                subtask_index: self.parent.task_info.task_index as u32,
                watermark: checkpoint.watermark.map(to_micros),
                files,
            }))
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
                current_stats.min_routing_key = current_stats
                    .min_routing_key
                    .min(parquet_stats.min_routing_key);
                current_stats.max_routing_key = current_stats
                    .min_routing_key
                    .max(parquet_stats.max_routing_key);
                current_stats.max_timestamp =
                    current_stats.max_timestamp.max(parquet_stats.max_timestamp);
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
                .map(|watermark| watermark - self.parent.retention < max_timestamp)
                .unwrap_or(true)
            {
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
        warn!("CUTOFF IS {}", print_time(cutoff));
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
