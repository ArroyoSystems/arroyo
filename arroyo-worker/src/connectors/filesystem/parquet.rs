use std::{
    fs::File,
    io::Write,
    marker::PhantomData,
    sync::Arc,
    time::{Instant, SystemTime},
};

use arrow_array::RecordBatch;
use arroyo_types::RecordBatchBuilder;
use parquet::{
    arrow::ArrowWriter,
    basic::{GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};

use super::{
    local::{CurrentFileRecovery, FilePreCommit, LocalWriter},
    BatchBufferingWriter, BatchBuilder, FileSettings, FileSystemTable, MultiPartWriterStats,
    TableType,
};
use super::{Compression, FormatSettings};

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

pub struct FixedSizeRecordBatchBuilder<B: RecordBatchBuilder> {
    builder: B,
    batch_size: usize,
    buffered_elements: Vec<B::Data>,
}

impl<B: RecordBatchBuilder> BatchBuilder for FixedSizeRecordBatchBuilder<B> {
    type InputType = B::Data;

    type BatchData = RecordBatch;
    fn new(config: &FileSystemTable) -> Self {
        let batch_size = if let TableType::Sink {
            format_settings:
                Some(FormatSettings::Parquet {
                    compression: _,
                    row_batch_size: Some(batch_size),
                    row_group_size: _,
                }),
            ..
        } = config.table_type
        {
            batch_size as usize
        } else {
            10_000
        };
        Self {
            builder: B::default(),
            batch_size,
            buffered_elements: Vec::new(),
        }
    }

    fn insert(&mut self, value: Self::InputType) -> Option<Self::BatchData> {
        self.builder.add_data(Some(value.clone()));
        self.buffered_elements.push(value);
        if self.buffered_elements.len() == self.batch_size {
            self.buffered_elements.clear();
            Some(self.builder.flush())
        } else {
            None
        }
    }

    fn buffered_inputs(&self) -> Vec<Self::InputType> {
        self.buffered_elements.clone()
    }

    fn flush_buffer(&mut self) -> Self::BatchData {
        self.buffered_elements.clear();
        self.builder.flush()
    }
}

pub struct RecordBatchBufferingWriter<R: RecordBatchBuilder> {
    writer: Option<ArrowWriter<SharedBuffer>>,
    shared_buffer: SharedBuffer,
    target_part_size: usize,
    phantom: PhantomData<R>,
}

impl<R: RecordBatchBuilder> BatchBufferingWriter for RecordBatchBufferingWriter<R> {
    type BatchData = RecordBatch;

    fn new(config: &FileSystemTable) -> Self {
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
        let shared_buffer = SharedBuffer::new(target_part_size);
        let writer_properties = writer_properties_from_table(config);
        let writer = ArrowWriter::try_new(
            shared_buffer.clone(),
            R::default().schema(),
            Some(writer_properties),
        )
        .unwrap();

        Self {
            writer: Some(writer),
            shared_buffer,
            target_part_size,
            phantom: PhantomData,
        }
    }

    fn suffix() -> String {
        "parquet".to_string()
    }

    fn add_batch_data(&mut self, data: Self::BatchData) -> Option<Vec<u8>> {
        let writer = self.writer.as_mut().unwrap();
        writer.write(&data).unwrap();
        writer.flush().unwrap();
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

    fn close(&mut self, final_batch: Option<Self::BatchData>) -> Option<Vec<u8>> {
        let mut writer = self.writer.take().unwrap();
        if let Some(batch) = final_batch {
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();
        let buffer = self.shared_buffer.buffer.try_lock().unwrap();
        Some(buffer.to_vec())
    }
}

pub struct ParquetLocalWriter<V: RecordBatchBuilder> {
    builder: V,
    writer: Option<ArrowWriter<SharedBuffer>>,
    tmp_path: String,
    file: File,
    destination_path: String,
    shared_buffer: SharedBuffer,
    stats: Option<MultiPartWriterStats>,
}

impl<V: RecordBatchBuilder + 'static> LocalWriter<V::Data> for ParquetLocalWriter<V> {
    fn new(tmp_path: String, final_path: String, table_properties: &FileSystemTable) -> Self {
        let shared_buffer = SharedBuffer::new(0);
        let writer_properties = writer_properties_from_table(table_properties);
        let builder = V::default();
        let writer = ArrowWriter::try_new(
            shared_buffer.clone(),
            V::default().schema(),
            Some(writer_properties),
        )
        .unwrap();
        let file = File::create(tmp_path.clone()).unwrap();
        Self {
            builder,
            writer: Some(writer),
            tmp_path,
            file,
            destination_path: final_path,
            shared_buffer,
            stats: None,
        }
    }

    fn file_suffix() -> &'static str {
        "parquet"
    }

    fn write(&mut self, value: V::Data, timestamp: SystemTime) -> anyhow::Result<()> {
        if self.stats.is_none() {
            self.stats = Some(MultiPartWriterStats {
                bytes_written: 0,
                parts_written: 0,
                first_write_at: Instant::now(),
                last_write_at: Instant::now(),
                representative_timestamp: timestamp,
            });
        } else {
            self.stats.as_mut().unwrap().last_write_at = Instant::now();
        }
        self.builder.add_data(Some(value));
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
        let batch = self.builder.flush();
        let writer = self.writer.take();
        let mut writer = writer.unwrap();
        writer.write(&batch)?;
        writer.close()?;
        self.sync()?;
        Ok(FilePreCommit {
            tmp_file: self.tmp_path.clone(),
            destination: self.destination_path.clone(),
        })
    }

    fn checkpoint(&mut self) -> anyhow::Result<Option<CurrentFileRecovery>> {
        let writer = self.writer.as_mut().unwrap();
        let batch = self.builder.flush();
        writer.write(&batch)?;
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
