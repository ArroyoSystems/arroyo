use std::{
    fs::File,
    io::Write,
    sync::Arc,
    time::{Instant, SystemTime},
};

use crate::filesystem::{Compression, FormatSettings};
use anyhow::Result;
use arrow::{
    array::{Array, RecordBatch, StringArray, TimestampNanosecondArray},
    compute::{sort_to_indices, take},
};
use arroyo_rpc::{df::ArroyoSchemaRef, formats::Format};
use arroyo_types::from_nanos;
use datafusion::physical_plan::PhysicalExpr;
use parquet::{
    arrow::ArrowWriter,
    basic::{GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use tracing::debug;
use super::{
    local::{CurrentFileRecovery, FilePreCommit, LocalWriter},
    BatchBufferingWriter, FileSettings, FileSystemTable, MultiPartWriterStats, TableType,
};

fn writer_properties_from_table(table: &FileSystemTable) -> WriterProperties {
    let mut parquet_writer_options = WriterProperties::builder();
    if let TableType::Sink {
        format_settings:
            Some(FormatSettings::Parquet {
                compression,
                row_batch_size: _,
                row_group_size,
            }),
        ..
    } = table.table_type
    {
        if let Some(compression) = compression {
            let compression = match compression {
                Compression::None => parquet::basic::Compression::UNCOMPRESSED,
                Compression::Snappy => parquet::basic::Compression::SNAPPY,
                Compression::Gzip => parquet::basic::Compression::GZIP(GzipLevel::default()),
                Compression::Zstd => parquet::basic::Compression::ZSTD(ZstdLevel::default()),
                Compression::Lz4 => parquet::basic::Compression::LZ4,
            };
            parquet_writer_options = parquet_writer_options.set_compression(compression);
        }
        if let Some(row_group_size) = row_group_size {
            parquet_writer_options =
                parquet_writer_options.set_max_row_group_size(row_group_size as usize);
        }
    }
    parquet_writer_options.build()
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

pub struct RecordBatchBufferingWriter {
    writer: Option<ArrowWriter<SharedBuffer>>,
    shared_buffer: SharedBuffer,
    target_part_size: usize,
    schema: ArroyoSchemaRef,
}

impl BatchBufferingWriter for RecordBatchBufferingWriter {
    fn new(config: &FileSystemTable, _format: Option<Format>, schema: ArroyoSchemaRef) -> Self {
        let target_part_size = if let TableType::Sink {
            file_settings:
                Some(FileSettings {
                    target_part_size: Some(target_part_size),
                    ..
                }),
            ..
        } = config.table_type
        {
            target_part_size as usize
        } else {
            5 * 1024 * 1024
        };
        
        debug!("target part size = {}", target_part_size);
        
        let shared_buffer = SharedBuffer::new(target_part_size);
        let writer_properties = writer_properties_from_table(config);
        let writer = ArrowWriter::try_new(
            shared_buffer.clone(),
            Arc::new(schema.schema_without_timestamp()),
            Some(writer_properties),
        )
        .unwrap();

        Self {
            writer: Some(writer),
            shared_buffer,
            target_part_size,
            schema,
        }
    }

    fn suffix() -> String {
        "parquet".to_string()
    }

    fn add_batch_data(&mut self, mut data: RecordBatch) -> Option<Vec<u8>> {
        let writer = self.writer.as_mut().unwrap();
        // remove timestamp column
        self.schema.remove_timestamp_column(&mut data);
        writer.write(&data).unwrap();
        if self.buffer_length() > self.target_part_size {
            Some(self.evict_current_buffer())
        } else {
            None
        }
    }

    fn buffer_length(&self) -> usize {
        self.shared_buffer.buffer.try_lock().unwrap().len()
    }

    fn evict_current_buffer(&mut self) -> Vec<u8> {
        let mut buffer = self.shared_buffer.buffer.try_lock().unwrap();
        let current_buffer_data = buffer.to_vec();
        buffer.clear();
        current_buffer_data
    }

    fn get_trailing_bytes_for_checkpoint(&mut self) -> Option<Vec<u8>> {
        let writer: &mut ArrowWriter<SharedBuffer> = self.writer.as_mut().unwrap();
        writer.flush().unwrap();
        let result = self
            .writer
            .as_mut()
            .unwrap()
            .get_trailing_bytes(SharedBuffer::new(0))
            .unwrap();
        let trailing_bytes: Vec<u8> = result.buffer.try_lock().unwrap().to_vec();
        // copy out the current bytes in the shared buffer, plus the trailing bytes
        let mut copied_bytes = self.shared_buffer.buffer.try_lock().unwrap().to_vec();
        copied_bytes.extend_from_slice(&trailing_bytes);
        Some(copied_bytes)
    }

    fn close(&mut self, final_batch: Option<RecordBatch>) -> Option<Vec<u8>> {
        let mut writer = self.writer.take().unwrap();
        if let Some(batch) = final_batch {
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();
        let buffer = self.shared_buffer.buffer.try_lock().unwrap();
        Some(buffer.to_vec())
    }
}

pub struct ParquetLocalWriter {
    writer: Option<ArrowWriter<SharedBuffer>>,
    tmp_path: String,
    file: File,
    destination_path: String,
    shared_buffer: SharedBuffer,
    stats: Option<MultiPartWriterStats>,
    schema: ArroyoSchemaRef,
}

impl LocalWriter for ParquetLocalWriter {
    fn new(
        tmp_path: String,
        final_path: String,
        table_properties: &FileSystemTable,
        _format: Option<Format>,
        schema: ArroyoSchemaRef,
    ) -> Self {
        let shared_buffer = SharedBuffer::new(0);
        let writer_properties = writer_properties_from_table(table_properties);
        let writer = ArrowWriter::try_new(
            shared_buffer.clone(),
            Arc::new(schema.schema_without_timestamp()),
            Some(writer_properties),
        )
        .unwrap();
        let file = File::create(tmp_path.clone()).unwrap();
        Self {
            writer: Some(writer),
            tmp_path,
            file,
            destination_path: final_path,
            shared_buffer,
            stats: None,
            schema,
        }
    }

    fn file_suffix() -> &'static str {
        "parquet"
    }

    fn write_batch(&mut self, mut batch: RecordBatch) -> anyhow::Result<()> {
        if self.stats.is_none() {
            self.stats = Some(MultiPartWriterStats {
                bytes_written: 0,
                parts_written: 0,
                first_write_at: Instant::now(),
                last_write_at: Instant::now(),
                representative_timestamp: representitive_timestamp(
                    batch.column(self.schema.timestamp_index),
                )?,
                part_size: None,
            });
        } else {
            self.stats.as_mut().unwrap().last_write_at = Instant::now();
        }
        self.schema.remove_timestamp_column(&mut batch);
        self.writer.as_mut().unwrap().write(&batch)?;
        Ok(())
    }

    fn sync(&mut self) -> anyhow::Result<usize> {
        let mut buffer = self.shared_buffer.buffer.try_lock().unwrap();
        self.file.write_all(&buffer)?;
        self.file.sync_all()?;
        // get size of the file
        let metadata = self.file.metadata()?;
        let size = metadata.len() as usize;
        self.stats.as_mut().unwrap().bytes_written = size;
        buffer.clear();
        Ok(size)
    }

    fn close(&mut self) -> anyhow::Result<FilePreCommit> {
        let writer = self.writer.take();
        let writer = writer.unwrap();
        writer.close()?;
        self.sync()?;
        Ok(FilePreCommit {
            tmp_file: self.tmp_path.clone(),
            destination: self.destination_path.clone(),
        })
    }

    fn checkpoint(&mut self) -> anyhow::Result<Option<CurrentFileRecovery>> {
        let writer = self.writer.as_mut().unwrap();
        writer.flush()?;
        let bytes_written = self.sync()?;
        let trailing_bytes = self
            .writer
            .as_mut()
            .unwrap()
            .get_trailing_bytes(SharedBuffer::new(0))?
            .buffer
            .try_lock()
            .unwrap()
            .to_vec();
        Ok(Some(CurrentFileRecovery {
            tmp_file: self.tmp_path.clone(),
            bytes_written,
            suffix: Some(trailing_bytes),
            destination: self.destination_path.clone(),
        }))
    }

    fn stats(&self) -> MultiPartWriterStats {
        self.stats.as_ref().unwrap().clone()
    }
}

pub(crate) fn batches_by_partition(
    batch: RecordBatch,
    partitioner: Arc<dyn PhysicalExpr>,
) -> Result<Vec<(RecordBatch, Option<String>)>> {
    let partition = partitioner.evaluate(&batch)?.into_array(batch.num_rows())?;
    // sort the partition, and then the batch, then compute partitions
    let sort_indices = sort_to_indices(&partition, None, None)?;
    let sorted_partition = take(&*partition, &sort_indices, None).unwrap();
    let sorted_batch = RecordBatch::try_new(
        batch.schema(),
        batch
            .columns()
            .iter()
            .map(|col| take(col, &sort_indices, None).unwrap())
            .collect(),
    )?;
    let partition = arrow::compute::partition(&[sorted_partition.clone()])?;
    let typed_partition = sorted_partition
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut result = Vec::with_capacity(partition.len());
    for partition in partition.ranges() {
        let partition_string = typed_partition.value(partition.start);
        let batch = sorted_batch.slice(partition.start, partition.end - partition.start);
        result.push((batch, Some(partition_string.to_string())));
    }
    Ok(result)
}

pub(crate) fn representitive_timestamp(timestamp_column: &Arc<dyn Array>) -> Result<SystemTime> {
    let time = timestamp_column
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| anyhow::anyhow!("timestamp column is not nanosecond"))?
        .value(0);
    Ok(from_nanos(time as u128))
}
