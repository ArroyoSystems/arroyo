use super::{
    local::{CurrentFileRecovery, FilePreCommit, LocalWriter},
    BatchBufferingWriter, FileMetadata, MultiPartWriterStats,
};
use crate::filesystem::config;
use crate::filesystem::sink::iceberg::add_parquet_field_ids;
use anyhow::Result;
use arrow::{
    array::{Array, RecordBatch, StringArray, TimestampNanosecondArray},
    compute::{sort_to_indices, take},
};
use arroyo_rpc::formats::{ParquetCompression, ParquetFormat};
use arroyo_rpc::{df::ArroyoSchemaRef, formats::Format};
use arroyo_types::from_nanos;
use bytes::{BufMut, Bytes, BytesMut};
use datafusion::physical_plan::PhysicalExpr;
use parquet::{
    arrow::ArrowWriter,
    basic::{GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use std::sync::Mutex;
use std::{
    fs::File,
    io::Write,
    mem,
    sync::Arc,
    time::{Instant, SystemTime},
};

const DEFAULT_ROW_GROUP_BYTES: u64 = 1024 * 1024 * 128; // 128MB

fn writer_properties_from_format(format: &ParquetFormat) -> (WriterProperties, usize) {
    let mut parquet_writer_options = WriterProperties::builder();

    parquet_writer_options = parquet_writer_options.set_compression(match format.compression {
        ParquetCompression::Uncompressed => parquet::basic::Compression::UNCOMPRESSED,
        ParquetCompression::Snappy => parquet::basic::Compression::SNAPPY,
        ParquetCompression::Gzip => parquet::basic::Compression::GZIP(GzipLevel::default()),
        ParquetCompression::Zstd => parquet::basic::Compression::ZSTD(ZstdLevel::default()),
        ParquetCompression::Lz4 => parquet::basic::Compression::LZ4,
    });

    (
        parquet_writer_options.build(),
        format.row_group_bytes.unwrap_or(DEFAULT_ROW_GROUP_BYTES) as usize,
    )
}

/// A buffer with interior mutability shared by the [`ArrowWriter`] and
/// [`AsyncArrowWriter`]. From Arrow. This lets us write data from the buffer to S3.
#[derive(Clone)]
struct SharedBuffer {
    /// The inner buffer for reading and writing
    ///
    /// The lock is used to obtain shared internal mutability, so no worry about the
    /// lock contention.
    buffer: Arc<Mutex<bytes::buf::Writer<BytesMut>>>,
}

impl SharedBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(BytesMut::with_capacity(capacity).writer())),
        }
    }

    pub fn into_inner(self) -> BytesMut {
        Arc::into_inner(self.buffer)
            .unwrap()
            .into_inner()
            .unwrap()
            .into_inner()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::write(&mut *buffer, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct ParquetBatchBufferingWriter {
    writer: Option<ArrowWriter<SharedBuffer>>,
    buffer: SharedBuffer,
    row_group_size_bytes: usize,
    schema: ArroyoSchemaRef,
}

impl BatchBufferingWriter for ParquetBatchBufferingWriter {
    fn new(_config: &config::FileSystemSink, format: Format, schema: ArroyoSchemaRef) -> Self {
        let Format::Parquet(parquet) = format else {
            panic!("ParquetBatchBufferingWriter configured with non-parquet format {format:?}");
        };

        let (writer_properties, row_group_size_bytes) = writer_properties_from_format(&parquet);

        let buffer = SharedBuffer::new(row_group_size_bytes);

        let writer = ArrowWriter::try_new(
            buffer.clone(),
            Arc::new(add_parquet_field_ids(&schema.schema_without_timestamp())),
            Some(writer_properties),
        )
        .unwrap();

        Self {
            writer: Some(writer),
            buffer,
            row_group_size_bytes,
            schema,
        }
    }

    fn suffix() -> String {
        "parquet".to_string()
    }

    fn add_batch_data(&mut self, data: &RecordBatch) {
        let writer = self.writer.as_mut().unwrap();

        // remove timestamp column
        let mut data = data.clone();
        self.schema.remove_timestamp_column(&mut data);
        writer.write(&data).unwrap();

        if writer.in_progress_size() > self.row_group_size_bytes {
            writer.flush().unwrap();
        }
    }

    fn buffered_bytes(&self) -> usize {
        self.buffer.buffer.lock().unwrap().get_ref().len()
    }

    fn split_to(&mut self, pos: usize) -> Bytes {
        let mut buf = self.buffer.buffer.lock().unwrap();
        buf.get_mut().split_to(pos).freeze()
    }

    fn get_trailing_bytes_for_checkpoint(&mut self) -> Option<Vec<u8>> {
        let writer: &mut ArrowWriter<SharedBuffer> = self.writer.as_mut().unwrap();
        writer.flush().unwrap();

        let trailing_bytes = self
            .writer
            .as_mut()
            .unwrap()
            .get_trailing_bytes(SharedBuffer::new(0))
            .unwrap()
            .into_inner();

        // TODO: this copy can likely be avoided, as the section we are copying is immutable
        // copy out the current bytes in the shared buffer, plus the trailing bytes
        let mut copied_bytes = self.buffer.buffer.lock().unwrap().get_ref().to_vec();
        copied_bytes.extend_from_slice(&trailing_bytes);
        Some(copied_bytes)
    }

    fn close(&mut self, final_batch: Option<RecordBatch>) -> Option<(Bytes, Option<FileMetadata>)> {
        let mut writer = self.writer.take().unwrap();
        if let Some(batch) = final_batch {
            writer.write(&batch).unwrap();
        }
        let metadata: FileMetadata = writer.close().unwrap().into();

        let mut buffer = SharedBuffer::new(self.row_group_size_bytes);
        mem::swap(&mut buffer, &mut self.buffer);

        Some((buffer.into_inner().freeze(), Some(metadata)))
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
    row_group_size_bytes: usize,
}

impl LocalWriter for ParquetLocalWriter {
    fn new(
        tmp_path: String,
        final_path: String,
        _table_properties: &config::FileSystemSink,
        format: Format,
        schema: ArroyoSchemaRef,
    ) -> Self {
        let Format::Parquet(parquet) = format else {
            panic!("ParquetLocalWriter configured with non-parquet format {format:?}");
        };

        let shared_buffer = SharedBuffer::new(0);
        let (writer_properties, row_group_size_bytes) = writer_properties_from_format(&parquet);
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
            row_group_size_bytes,
            schema,
        }
    }

    fn file_suffix() -> &'static str {
        "parquet"
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> anyhow::Result<usize> {
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
        let mut batch = batch.clone();
        self.schema.remove_timestamp_column(&mut batch);
        let writer = self.writer.as_mut().unwrap();

        writer.write(&batch)?;
        if writer.in_progress_size() >= self.row_group_size_bytes {
            writer.flush()?;
        }
        Ok(0)
    }

    fn sync(&mut self) -> anyhow::Result<usize> {
        let mut buffer = self.shared_buffer.buffer.try_lock().unwrap();
        self.file.write_all(buffer.get_ref())?;
        self.file.sync_all()?;
        // get size of the file
        let metadata = self.file.metadata()?;
        let size = metadata.len() as usize;
        self.stats.as_mut().unwrap().bytes_written = size;
        buffer.get_mut().clear();
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
            .get_ref()
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
