use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime},
};

use ahash::RandomState;
use anyhow::{anyhow, bail, Ok, Result};
use arrow::compute::kernels::{self, aggregate};
use arrow_array::{
    cast::AsArray,
    types::{TimestampNanosecondType, UInt64Type},
    PrimitiveArray, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arroyo_datastream::get_hasher;
use arroyo_rpc::{
    grpc::{
        ExpiringKeyedTimeSubtaskCheckpointMetadata, ExpiringKeyedTimeTableCheckpointMetadata,
        ExpiringKeyedTimeTableConfig, ParquetTimeFile, TableEnum,
    },
    ArroyoSchema,
};
use arroyo_storage::StorageProviderRef;
use arroyo_types::{from_nanos, to_micros, to_nanos, TaskInfoRef};
use bincode::config;
use datafusion_common::{hash_utils::create_hashes, ScalarValue};
use futures::StreamExt;
use parquet::arrow::{
    async_reader::ParquetObjectReader, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
};
use tokio::{io::AsyncWrite, sync::mpsc::Sender};

use crate::{parquet::ParquetStats, CheckpointMessage, DataOperation, StateMessage, TableData};

use super::{table_checkpoint_path, Table, TableEpochCheckpointer};

#[derive(Debug, Clone)]
pub struct ExpiringTimeKeyTable {
    table_name: String,
    task_info: TaskInfoRef,
    schema: ArroyoSchema,
    final_schema: SchemaRef,
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
            .map(|watermark| to_nanos(watermark - self.retention))
            .unwrap_or_default();
        let files: Vec<_> = self
            .checkpoint_files
            .iter()
            .filter_map(|file| {
                // file must have some data greater than the cutoff and routing keys within the range.
                if cutoff <= (file.max_timestamp_micros * 1000) as u128
                    && (file.max_routing_key >= *self.task_info.key_range.start()
                        && *self.task_info.key_range.end() >= file.min_routing_key)
                {
                    Some(file.file.clone())
                } else {
                    None
                }
            })
            .collect();

        let mut data: BTreeMap<SystemTime, Vec<RecordBatch>> = BTreeMap::new();
        for file in files {
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
                let batch = batch_result?;
                let timestamp_array: &PrimitiveArray<TimestampNanosecondType> = batch
                    .column(self.schema.timestamp_index)
                    .as_primitive_opt()
                    .ok_or_else(|| anyhow!("failed to find timestamp column"))?;
                let max_timestamp = aggregate::max(timestamp_array)
                    .ok_or_else(|| anyhow!("should have max timestamp"))?;
                if cutoff < (max_timestamp as u128) {
                    data.entry(from_nanos(max_timestamp as u128))
                        .or_default()
                        .push(batch.project(&projection)?);
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
        let schema: ArroyoSchema = config.schema.expect("should have schema").try_into()?;

        let mut fields = schema.schema.fields().to_vec();
        //TODO: we could have the additional columns at the start, rather than the end
        fields.push(Arc::new(Field::new("_key_hash", DataType::UInt64, false)));
        fields.push(Arc::new(Field::new(
            "_operation",
            arrow::datatypes::DataType::Binary,
            false,
        )));
        let final_schema = Arc::new(Schema::new_with_metadata(fields, HashMap::new()));

        let checkpoint_files = checkpoint_message
            .map(|checkpoint_message| checkpoint_message.files)
            .unwrap_or_default();
        Ok(Self {
            table_name: config.table_name,
            task_info,
            schema,
            final_schema,
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
        let max_watermark = subtask_metadata
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
            self.parent.final_schema.clone(),
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
        let (annotated_batch, batch_stats) = self.annotate_record_batch(&batch);
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
    fn annotate_record_batch(&mut self, record_batch: &RecordBatch) -> (RecordBatch, ParquetStats) {
        // TODO: figure out if we want a key column if there isn't a key.
        let key_batch = record_batch
            .project(&self.parent.schema.key_indices)
            .unwrap();

        let mut hash_buffer = vec![0u64; key_batch.num_rows()];
        let hashes = create_hashes(key_batch.columns(), &get_hasher(), &mut hash_buffer).unwrap();
        let hash_array = PrimitiveArray::<UInt64Type>::from(hash_buffer);

        let hash_min = kernels::aggregate::min(&hash_array).unwrap();
        let hash_max = kernels::aggregate::max(&hash_array).unwrap();
        let max_timestamp_nanos: i64 = kernels::aggregate::max(
            record_batch
                .column(self.parent.schema.timestamp_index)
                .as_primitive::<TimestampNanosecondType>(),
        )
        .unwrap();

        let batch_stats = ParquetStats {
            max_timestamp: from_nanos(max_timestamp_nanos as u128),
            min_routing_key: hash_min,
            max_routing_key: hash_max,
        };

        let mut columns = record_batch.columns().to_vec();
        columns.push(Arc::new(hash_array));

        // TODO: properly encode updates without using bincode
        let insert_op = ScalarValue::Binary(Some(
            bincode::encode_to_vec(DataOperation::Insert, config::standard()).unwrap(),
        ));

        // TODO: handle other types of updates
        let op_array = insert_op.to_array_of_size(record_batch.num_rows()).unwrap();
        columns.push(op_array);

        let annotated_record_batch =
            RecordBatch::try_new(self.parent.final_schema.clone(), columns).unwrap();

        (annotated_record_batch, batch_stats)
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
                .map(|watermark| watermark - self.parent.retention <= max_timestamp)
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
        let flushed_range = self.flushed_batches_by_max_timestamp.range(cutoff..);
        let buffered_range = self.batches_to_flush.range(cutoff..);
        flushed_range.chain(buffered_range)
    }
}
