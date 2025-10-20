use super::{
    local::{CurrentFileRecovery, FilePreCommit, LocalWriter},
    BatchBufferingWriter, FsEventLogger, MultiPartWriterStats,
};
use crate::filesystem::config;
use crate::filesystem::sink::iceberg::metadata::IcebergFileMetadata;
use crate::filesystem::sink::iceberg::schema::{
    normalize_batch_to_schema, update_field_ids_to_iceberg,
};
use anyhow::Result;
use arrow::array::{Array, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::SchemaRef;
use arroyo_rpc::formats::{ParquetCompression, ParquetFormat};
use arroyo_rpc::{df::ArroyoSchemaRef, formats::Format};
use arroyo_types::from_nanos;
use bytes::{BufMut, Bytes, BytesMut};
use parquet::{
    arrow::ArrowWriter,
    basic::{GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use std::sync::Mutex;
use std::time::Duration;
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
        ParquetCompression::Lz4Raw => parquet::basic::Compression::LZ4_RAW,
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
    writer_schema: SchemaRef,
    iceberg_schema: Option<iceberg::spec::SchemaRef>,
    event_logger: FsEventLogger,
}

impl BatchBufferingWriter for ParquetBatchBufferingWriter {
    fn new(
        _config: &config::FileSystemSink,
        format: Format,
        schema: ArroyoSchemaRef,
        iceberg_schema: Option<iceberg::spec::SchemaRef>,
        event_logger: FsEventLogger,
    ) -> Self {
        let Format::Parquet(parquet) = format else {
            panic!("ParquetBatchBufferingWriter configured with non-parquet format {format:?}");
        };

        let (writer_properties, row_group_size_bytes) = writer_properties_from_format(&parquet);

        let buffer = SharedBuffer::new(row_group_size_bytes);

        let mut writer_schema = schema.schema_without_timestamp();
        if let Some(iceberg) = &iceberg_schema {
            writer_schema = update_field_ids_to_iceberg(&writer_schema, iceberg)
                .expect("failed to assign iceberg ids to schema")
        };

        let writer_schema = Arc::new(writer_schema);

        let writer = ArrowWriter::try_new(
            buffer.clone(),
            writer_schema.clone(),
            Some(writer_properties),
        )
        .unwrap();

        Self {
            writer: Some(writer),
            buffer,
            row_group_size_bytes,
            writer_schema,
            schema,
            iceberg_schema,
            event_logger,
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

        if self.iceberg_schema.is_some() {
            data = normalize_batch_to_schema(&data, &self.writer_schema)
                .expect("could not normalize batch");
        }

        let prev_size = writer.in_progress_size();
        writer.write(&data).unwrap();
        let uncompressed_bytes = writer
            .in_progress_size()
            .checked_sub(prev_size)
            .unwrap_or_default();

        let row_groups = if writer.in_progress_size() > self.row_group_size_bytes {
            writer.flush().unwrap();
            1
        } else {
            0
        };

        self.event_logger.log_fs_event(
            0,
            0,
            Duration::ZERO,
            0,
            None,
            row_groups,
            uncompressed_bytes as u64,
            data.num_rows() as u64,
        );
    }

    fn buffered_bytes(&self) -> usize {
        self.buffer.buffer.lock().unwrap().get_ref().len()
    }

    fn split_to(&mut self, pos: usize) -> Bytes {
        let mut buf = self.buffer.buffer.lock().unwrap();
        buf.get_mut().split_to(pos).freeze()
    }

    fn get_trailing_bytes_for_checkpoint(
        &mut self,
    ) -> (Option<Vec<u8>>, Option<IcebergFileMetadata>) {
        let writer: &mut ArrowWriter<SharedBuffer> = self.writer.as_mut().unwrap();
        writer.flush().unwrap();

        let (trailing_bytes, metadata) = self
            .writer
            .as_mut()
            .unwrap()
            .get_trailing_bytes(SharedBuffer::new(0))
            .unwrap();

        // TODO: this copy can likely be avoided, as the section we are copying is immutable
        // copy out the current bytes in the shared buffer, plus the trailing bytes
        let mut copied_bytes = self.buffer.buffer.lock().unwrap().get_ref().to_vec();
        copied_bytes.extend_from_slice(&trailing_bytes.into_inner());

        let metadata = self
            .iceberg_schema
            .as_ref()
            .map(|s| IcebergFileMetadata::from_parquet(metadata, s));

        (Some(copied_bytes), metadata)
    }

    fn close(
        &mut self,
        final_batch: Option<RecordBatch>,
    ) -> Option<(Bytes, Option<IcebergFileMetadata>)> {
        let mut writer = self.writer.take().unwrap();
        if let Some(batch) = final_batch {
            writer.write(&batch).unwrap();
        }

        let metadata = writer.close().unwrap();
        let metadata = self
            .iceberg_schema
            .as_ref()
            .map(|s| IcebergFileMetadata::from_parquet(metadata, s));

        let mut buffer = SharedBuffer::new(self.row_group_size_bytes);
        mem::swap(&mut buffer, &mut self.buffer);

        Some((buffer.into_inner().freeze(), metadata))
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
        let (buffer, _) = self
            .writer
            .as_mut()
            .unwrap()
            .get_trailing_bytes(SharedBuffer::new(0))?;

        let trailing_bytes = buffer.buffer.try_lock().unwrap().get_ref().to_vec();

        Ok(Some(CurrentFileRecovery {
            tmp_file: self.tmp_path.clone(),
            bytes_written,
            suffix: Some(trailing_bytes),
            destination: self.destination_path.clone(),
            metadata: None,
        }))
    }

    fn stats(&self) -> MultiPartWriterStats {
        self.stats.as_ref().unwrap().clone()
    }
}

pub(crate) fn representitive_timestamp(timestamp_column: &Arc<dyn Array>) -> Result<SystemTime> {
    let time = timestamp_column
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| anyhow::anyhow!("timestamp column is not nanosecond"))?
        .value(0);
    Ok(from_nanos(time as u128))
}
