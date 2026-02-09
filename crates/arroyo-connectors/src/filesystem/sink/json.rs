use super::{
    BatchBufferingWriter, FsEventLogger, MultiPartWriterStats,
    local::{CurrentFileRecovery, LocalWriter},
    parquet::representitive_timestamp,
};
use crate::filesystem::config;
use crate::filesystem::sink::iceberg::metadata::IcebergFileMetadata;
use arrow::record_batch::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_rpc::{
    df::ArroyoSchemaRef,
    formats::{Format, JsonCompression},
};
use bytes::{Bytes, BytesMut};
use flate2::Compression as GzipCompression;
use flate2::write::GzEncoder;
use std::time::Duration;
use std::{fs::File, io::Write, time::Instant};
use tracing::debug;

/// Size of uncompressed chunks to compress at once.
/// Larger chunks = better compression ratio, but more memory usage.
/// 5MB provides good compression while keeping memory reasonable.
const COMPRESSION_CHUNK_SIZE: usize = 5 * 1024 * 1024; // 5MB

/// Compresses a chunk of data using the specified [`JsonCompression`].
/// Note that [`JsonCompression::Uncompressed]` can be used, the data won't transformed.
fn compress_chunk(data: Bytes, compression: JsonCompression) -> anyhow::Result<Bytes> {
    match compression {
        JsonCompression::Uncompressed => Ok(data),
        JsonCompression::Gzip => {
            let mut encoder = GzEncoder::new(Vec::new(), GzipCompression::default());
            encoder.write_all(&data)?;
            Ok(Bytes::from(encoder.finish()?))
        }
    }
}

pub struct JsonWriter {
    /// Buffer holding compressed data ready for upload
    compressed_buffer: BytesMut,
    /// Staging buffer for uncompressed JSON data (compressed in 5MB chunks)
    uncompressed_buffer: BytesMut,
    serializer: ArrowSerializer,
    compression: JsonCompression,
    event_logger: FsEventLogger,
}

impl BatchBufferingWriter for JsonWriter {
    fn new(
        _: &config::FileSystemSink,
        format: Format,
        _schema: ArroyoSchemaRef,
        _: Option<::iceberg::spec::SchemaRef>,
        event_logger: FsEventLogger,
    ) -> Self {
        let compression = if let Format::Json(ref json) = format {
            json.compression
        } else {
            panic!("JsonWriter configured with non-json format {format:?}");
        };

        Self {
            compressed_buffer: BytesMut::new(),
            uncompressed_buffer: BytesMut::new(),
            serializer: ArrowSerializer::new(format),
            compression,
            event_logger,
        }
    }

    fn suffix_for_format(format: &Format) -> &str {
        if let Format::Json(json) = format {
            match json.compression {
                JsonCompression::Uncompressed => "json",
                JsonCompression::Gzip => "json.gz",
            }
        } else {
            panic!("JsonLocalWriter configured with non-json format {format:?}");
        }
    }

    fn add_batch_data(&mut self, batch: &RecordBatch) {
        let mut size = 0;
        for k in self.serializer.serialize(batch) {
            size += k.len() + 1;
            self.uncompressed_buffer.extend(k);
            self.uncompressed_buffer.extend(b"\n");
        }

        // Compress uncompressed buffer in chunks
        self.flush_uncompressed_to_compressed();
        debug!(
            "uncompressed_buffer: {} bytes, compressed_buffer: {} bytes",
            self.uncompressed_buffer.len(),
            self.compressed_buffer.len()
        );

        self.event_logger.log_fs_event(
            0,
            0,
            Duration::ZERO,
            0,
            None,
            0,
            size as u64,
            batch.num_rows() as u64,
        );
    }

    fn unflushed_bytes(&self) -> usize {
        self.uncompressed_buffer.len()
    }

    fn buffered_bytes(&self) -> usize {
        // NOTE: Currently only retuns the compressed size, which is un under-estimate of the
        // total buffered data. The uncompressed data that have up to [`COMPRESSION_CHUNK_SIZE`]
        // bytes that are waiting to be compressed.
        self.compressed_buffer.len()
    }

    fn split_to(&mut self, pos: usize) -> Bytes {
        self.compressed_buffer.split_to(pos).freeze()
    }

    /// The bytes stored in the checkpoint are bytes that we are ready to uploaded as-is.
    /// This means that if compression is enabled, we first compress, then return the compressed buffer.
    fn get_trailing_bytes_for_checkpoint(&mut self) -> (Vec<u8>, Option<IcebergFileMetadata>) {
        self.flush_all_uncompressed();

        // Store compressed bytes in checkpoint (required because recovery
        // uploads trailing_bytes directly without re-processing)
        (self.compressed_buffer.to_vec(), None)
    }

    fn close(&mut self) -> (Bytes, Option<IcebergFileMetadata>) {
        self.flush_all_uncompressed();

        let data = self.compressed_buffer.split().freeze();
        (data, None)
    }
}

impl JsonWriter {
    /// Compresses full COMPRESSION_CHUNK_SIZE chunks from uncompressed_buffer
    /// and appends them to compressed_buffer. Leaves remainder in uncompressed_buffer.
    fn flush_uncompressed_to_compressed(&mut self) {
        while self.uncompressed_buffer.len() >= COMPRESSION_CHUNK_SIZE {
            let chunk = self
                .uncompressed_buffer
                .split_to(COMPRESSION_CHUNK_SIZE)
                .freeze();
            let compressed = compress_chunk(chunk, self.compression)
                .expect("Failed to compress JSON chunk - this should never happen with valid data");
            self.compressed_buffer.extend_from_slice(&compressed);
        }
    }

    /// Compresses ALL remaining data in uncompressed_buffer, regardless of size.
    /// Used when closing file or creating checkpoint.
    fn flush_all_uncompressed(&mut self) {
        if !self.uncompressed_buffer.is_empty() {
            let chunk = self.uncompressed_buffer.split().freeze();
            let compressed = compress_chunk(chunk, self.compression)
                .expect("Failed to compress final JSON chunk");
            self.compressed_buffer.extend_from_slice(&compressed);
        }
    }
}

pub struct JsonLocalWriter {
    tmp_path: String,
    final_path: String,
    file: File,
    serializer: ArrowSerializer,
    stats: Option<MultiPartWriterStats>,
    schema: ArroyoSchemaRef,
}

impl LocalWriter for JsonLocalWriter {
    fn new(
        tmp_path: String,
        final_path: String,
        _table_properties: &config::FileSystemSink,
        format: Format,
        schema: ArroyoSchemaRef,
    ) -> Self {
        let file = File::create(&tmp_path).unwrap();
        JsonLocalWriter {
            tmp_path,
            final_path,
            serializer: ArrowSerializer::new(format),
            file,
            stats: None,
            schema,
        }
    }

    fn file_suffix_for_format(_format: &Format) -> &str {
        // TODO: Inspect the format and properly return the extension
        // once the JsonLocalWriter supports compression.
        "json"
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> anyhow::Result<usize> {
        if let Some(stats) = &mut self.stats {
            stats.last_write_at = Instant::now();
        } else {
            self.stats = Some(MultiPartWriterStats {
                bytes_written: 0,
                parts_written: 0,
                first_write_at: Instant::now(),
                last_write_at: Instant::now(),
                representative_timestamp: representitive_timestamp(
                    batch.column(self.schema.timestamp_index),
                )?,
            });
        }

        let mut size = 0;
        for data in self.serializer.serialize(batch) {
            size += data.len() + 1;
            self.file.write_all(data.as_slice())?;
            self.file.write_all(b"\n")?;
        }
        Ok(size)
    }

    fn sync(&mut self) -> anyhow::Result<usize> {
        self.file.flush()?;
        let size = self.file.metadata()?.len() as usize;
        self.stats.as_mut().unwrap().bytes_written = size;
        Ok(size)
    }

    fn close(&mut self) -> anyhow::Result<super::local::FilePreCommit> {
        LocalWriter::sync(self)?;
        Ok(super::local::FilePreCommit {
            tmp_file: self.tmp_path.clone(),
            destination: self.final_path.clone(),
        })
    }

    fn checkpoint(&mut self) -> anyhow::Result<Option<super::local::CurrentFileRecovery>> {
        let bytes_written = LocalWriter::sync(self)?;
        if bytes_written > 0 {
            Ok(Some(CurrentFileRecovery {
                tmp_file: self.tmp_path.clone(),
                bytes_written,
                suffix: None,
                destination: self.final_path.clone(),
                metadata: None,
            }))
        } else {
            Ok(None)
        }
    }

    fn stats(&self) -> MultiPartWriterStats {
        self.stats.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::super::BatchBufferingWriter;
    use super::*;
    use arrow::array::{RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arroyo_rpc::formats::JsonFormat;
    use arroyo_types::TaskInfo;
    use bytes::Bytes;
    use flate2::read::{GzDecoder, MultiGzDecoder};
    use std::io::Read;
    use std::sync::Arc;

    #[test]
    fn test_compress_chunk_uncompressed() {
        let data = Bytes::from("Hello, World!");
        let compressed = compress_chunk(data.clone(), JsonCompression::Uncompressed).unwrap();
        assert_eq!(compressed, data);
    }

    #[test]
    fn test_compress_chunk_gzip() {
        let data = Bytes::from("Hello, World!");
        let compressed = compress_chunk(data.clone(), JsonCompression::Gzip).unwrap();

        // Verify it's actually compressed (should have gzip header)
        assert_eq!(&compressed[0..2], &[0x1f, 0x8b]); // gzip magic number

        // Verify we can decompress it
        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_suffix_for_format() {
        // Test uncompressed
        let format = Format::Json(JsonFormat {
            compression: JsonCompression::Uncompressed,
            ..Default::default()
        });
        assert_eq!(JsonWriter::suffix_for_format(&format), "json");

        // Test gzip
        let format = Format::Json(JsonFormat {
            compression: JsonCompression::Gzip,
            ..Default::default()
        });
        assert_eq!(JsonWriter::suffix_for_format(&format), "json.gz");
    }

    #[test]
    fn test_multi_member_gzip() {
        // Simulate what happens when we compress multiple parts
        let part1 = Bytes::from("{\"field\":\"value1\"}\n");
        let part2 = Bytes::from("{\"field\":\"value2\"}\n");

        let compressed1 = compress_chunk(part1.clone(), JsonCompression::Gzip).unwrap();
        let compressed2 = compress_chunk(part2.clone(), JsonCompression::Gzip).unwrap();

        // Concatenate the two gzip members (simulating multipart upload)
        let mut multi_member = Vec::new();
        multi_member.extend_from_slice(&compressed1);
        multi_member.extend_from_slice(&compressed2);

        // Verify we can decompress the entire multi-member file
        // Note: MultiGzDecoder is required to handle multiple gzip members in one file
        let mut decoder = MultiGzDecoder::new(&multi_member[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&part1);
        expected.extend_from_slice(&part2);
        assert_eq!(decompressed, expected);
    }

    #[test]
    fn test_staged_compression() {
        // Create a writer with gzip compression
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));

        let format = Format::Json(JsonFormat {
            compression: JsonCompression::Gzip,
            ..Default::default()
        });

        let mut writer = JsonWriter {
            compressed_buffer: BytesMut::new(),
            uncompressed_buffer: BytesMut::new(),
            serializer: ArrowSerializer::new(format.clone()),
            compression: JsonCompression::Gzip,
            event_logger: FsEventLogger {
                task_info: Some(Arc::new(TaskInfo {
                    job_id: "test_job".to_string(),
                    node_id: 0,
                    operator_name: "test_operator".to_string(),
                    operator_id: "test_op_id".to_string(),
                    task_index: 0,
                    parallelism: 1,
                    key_range: 0..=u64::MAX,
                })),
                connection_id: Arc::new(String::from("test")),
            },
        };

        // Create data that will exceed COMPRESSION_CHUNK_SIZE when serialized
        let data_str = "x".repeat(100_000); // 100KB per row
        let mut rows = Vec::new();
        for _ in 0..60 {
            // 60 rows * 100KB = ~6MB uncompressed
            rows.push(data_str.clone());
        }

        let array = StringArray::from(rows);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        // Add batch - should trigger compression
        writer.add_batch_data(&batch);

        // Verify compressed buffer has data
        assert!(
            !writer.compressed_buffer.is_empty(),
            "Compressed buffer should have data"
        );

        // Flush any remaining data in uncompressed buffer
        writer.flush_all_uncompressed();
        assert!(writer.uncompressed_buffer.is_empty());

        // Verify compressed data is smaller than uncompressed
        let total_uncompressed = 6_000_000; // Approximate
        assert!(
            writer.compressed_buffer.len() < total_uncompressed,
            "Compressed size should be less than uncompressed"
        );

        // Verify we can decompress it
        let compressed = writer.compressed_buffer.clone().freeze();
        let mut decoder = MultiGzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();

        // Should be valid JSON lines
        let lines: Vec<&str> = std::str::from_utf8(&decompressed)
            .unwrap()
            .lines()
            .collect();
        assert_eq!(lines.len(), 60);
        let expected_json = format!("{{\"data\":\"{}\"}}", "x".repeat(100_000));
        lines
            .into_iter()
            .for_each(|line| assert_eq!(line, expected_json));
    }

    #[test]
    fn test_checkpoint_stores_compressed_data() {
        // Create a writer with gzip compression
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));

        let format = Format::Json(JsonFormat {
            compression: JsonCompression::Gzip,
            ..Default::default()
        });

        let mut writer = JsonWriter {
            compressed_buffer: BytesMut::new(),
            uncompressed_buffer: BytesMut::new(),
            serializer: ArrowSerializer::new(format.clone()),
            compression: JsonCompression::Gzip,
            event_logger: FsEventLogger {
                task_info: Some(Arc::new(TaskInfo {
                    job_id: "test_job".to_string(),
                    node_id: 0,
                    operator_name: "test_operator".to_string(),
                    operator_id: "test_op_id".to_string(),
                    task_index: 0,
                    parallelism: 1,
                    key_range: 0..=u64::MAX,
                })),
                connection_id: Arc::new(String::from("test")),
            },
        };

        // Add some data
        let data = vec!["test1".to_string(), "test2".to_string()];
        let array = StringArray::from(data);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        writer.add_batch_data(&batch);

        // Get checkpoint data
        let (checkpoint_bytes, _) = writer.get_trailing_bytes_for_checkpoint();

        // Verify checkpoint contains compressed data (starts with gzip magic bytes)
        assert!(checkpoint_bytes.len() >= 2, "Checkpoint should have data");
        assert_eq!(
            &checkpoint_bytes[0..2],
            &[0x1f, 0x8b],
            "Should start with gzip magic bytes"
        );

        // Verify we can decompress checkpoint data
        let mut decoder = MultiGzDecoder::new(&checkpoint_bytes[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();

        // Should be valid JSON
        let text = std::str::from_utf8(&decompressed).unwrap();
        assert!(text.contains("test1"));
        assert!(text.contains("test2"));
    }
}
