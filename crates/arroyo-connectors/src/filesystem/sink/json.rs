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
    formats::{Format, JsonCompression, JsonFormat},
};
use bytes::{BufMut, Bytes, BytesMut};
use flate2::Compression as GzipCompression;
use flate2::write::GzEncoder;
use std::time::Duration;
use std::{fs::File, io::Write, time::Instant};

/// Buffer for JSON data, either uncompressed or wrapped in a gzip encoder.
///
/// Generic over `W: Write` so it can wrap both in-memory buffers (`Writer<BytesMut>`)
/// and file handles (`File`).
enum JsonBuffer<W: Write> {
    Uncompressed(W),
    /// The `Gzipped` variant uses an `Option` so that the encoder can be temporarily
    /// taken out (via `.take()`) for operations that consume it (like `finish()`).
    /// In addition, `None` is used to indicate that the writer has been closed.
    Gzipped {
        encoder: Option<GzEncoder<W>>,
        dirty: bool,
    },
}

impl<W: Write> JsonBuffer<W> {
    /// Writes data through the encoder (compressed mode) or directly to the inner writer.
    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self {
            JsonBuffer::Uncompressed(w) => w.write_all(data),
            JsonBuffer::Gzipped {
                encoder: Some(enc),
                dirty,
            } => {
                *dirty = true;
                enc.write_all(data)
            }
            JsonBuffer::Gzipped { encoder: None, .. } => panic!("write_all called after close()"),
        }
    }

    /// Returns a reference to the inner writer `W`.
    fn writer_ref(&self) -> &W {
        match self {
            JsonBuffer::Uncompressed(w) => w,
            JsonBuffer::Gzipped {
                encoder: Some(enc), ..
            } => enc.get_ref(),
            JsonBuffer::Gzipped { encoder: None, .. } => panic!("inner_ref called after close()"),
        }
    }

    /// Returns a mutable reference to the inner writer `W`.
    fn writer_ref_mut(&mut self) -> &mut W {
        match self {
            JsonBuffer::Uncompressed(w) => w,
            JsonBuffer::Gzipped {
                encoder: Some(enc), ..
            } => enc.get_mut(),
            JsonBuffer::Gzipped { encoder: None, .. } => {
                panic!("inner_ref_mut called after close()")
            }
        }
    }

    fn restore_encoder(&mut self, new_encoder: GzEncoder<W>) {
        match self {
            JsonBuffer::Gzipped {
                encoder: slot @ None,
                dirty,
            } => {
                *slot = Some(new_encoder);
                // This is a fresh gzip member; no record bytes were written yet.
                *dirty = false;
            }
            _ => panic!("restore_encoder called on non-Gzipped or non-empty buffer"),
        }
    }

    fn finish_encoder(&mut self) -> std::io::Result<W> {
        match self {
            JsonBuffer::Gzipped {
                encoder: encoder @ Some(_),
                ..
            } => encoder.take().unwrap().finish(),
            _ => panic!("finish_encoder called on non-Gzipped or already-taken buffer"),
        }
    }
}

pub struct JsonWriter {
    buffer: JsonBuffer<bytes::buf::Writer<BytesMut>>,
    serializer: ArrowSerializer,
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

        let buffer = match compression {
            JsonCompression::Uncompressed => JsonBuffer::Uncompressed(BytesMut::new().writer()),
            JsonCompression::Gzip => JsonBuffer::Gzipped {
                encoder: Some(GzEncoder::new(
                    BytesMut::new().writer(),
                    GzipCompression::default(),
                )),
                dirty: false,
            },
        };

        Self {
            buffer,
            serializer: ArrowSerializer::new(format),
            event_logger,
        }
    }

    fn suffix_for_format(format: &Format) -> &str {
        match format {
            Format::Json(JsonFormat {
                compression: JsonCompression::Gzip,
                ..
            }) => "json.gz",
            Format::Json(_) => "json",
            _ => panic!("JsonWriter configured with non-json format {format:?}"),
        }
    }

    fn add_batch_data(&mut self, batch: &RecordBatch) {
        let mut size = 0;

        for k in self.serializer.serialize(batch) {
            size += k.len() + 1;
            // Writes are infallible: the underlying Writer<BytesMut>::write() always returns `Ok`.
            // Ref: https://docs.rs/crate/bytes/1.11.1/source/src/buf/writer.rs#78-83
            self.buffer
                .write_all(&k)
                .expect("Failed to write JSON data");
            self.buffer
                .write_all(b"\n")
                .expect("Failed to write newline");
        }

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
        0
    }

    fn buffered_bytes(&self) -> usize {
        self.buffer.writer_ref().get_ref().len()
    }

    fn split_to(&mut self, pos: usize) -> Bytes {
        self.buffer
            .writer_ref_mut()
            .get_mut()
            .split_to(pos)
            .freeze()
    }

    /// The bytes stored in the checkpoint are bytes that we are ready to uploaded as-is.
    /// For compressed mode, we finish the current gzip member to ensure all data is complete.
    fn get_trailing_bytes_for_checkpoint(&mut self) -> (Vec<u8>, Option<IcebergFileMetadata>) {
        let bytes = match &self.buffer {
            JsonBuffer::Uncompressed(inner) => inner.get_ref().to_vec(),
            JsonBuffer::Gzipped { dirty, .. } => {
                if *dirty {
                    // Finish is infallible since the underlying Writer<BytesMut>::write() always returns `Ok`.
                    // Ref: https://docs.rs/crate/bytes/1.11.1/source/src/buf/writer.rs#78-83
                    let inner = self
                        .buffer
                        .finish_encoder()
                        .expect("Failed to finish gzip encoder at checkpoint");

                    let bytes = inner.get_ref().to_vec();

                    self.buffer
                        .restore_encoder(GzEncoder::new(inner, GzipCompression::default()));

                    bytes
                } else {
                    // No data written since last checkpoint — return existing buffer
                    self.buffer.writer_ref().get_ref().to_vec()
                }
            }
        };
        (bytes, None)
    }

    fn close(&mut self) -> (Bytes, Option<IcebergFileMetadata>) {
        let data = match &mut self.buffer {
            JsonBuffer::Uncompressed(inner) => inner.get_mut().split().freeze(),
            JsonBuffer::Gzipped { dirty, .. } => {
                if *dirty {
                    // Finish infallible, since the underlying Writer<BytesMut>::write() always returns `Ok`.
                    // Ref: https://docs.rs/crate/bytes/1.11.1/source/src/buf/writer.rs#78-83
                    let inner = self
                        .buffer
                        .finish_encoder()
                        .expect("Failed to finish gzip encoder at close");

                    inner.into_inner().split().freeze()
                } else {
                    // No data written since last checkpoint — return existing buffer
                    self.buffer.writer_ref_mut().get_mut().split().freeze()
                }
            }
        };

        (data, None)
    }
}

pub struct JsonLocalWriter {
    tmp_path: String,
    final_path: String,
    buffer: JsonBuffer<File>,
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
        let compression = if let Format::Json(ref json) = format {
            json.compression
        } else {
            panic!("JsonLocalWriter configured with non-json format {format:?}");
        };

        let file = File::create(&tmp_path).unwrap();

        let buffer = match compression {
            JsonCompression::Uncompressed => JsonBuffer::Uncompressed(file),
            JsonCompression::Gzip => JsonBuffer::Gzipped {
                encoder: Some(GzEncoder::new(file, GzipCompression::default())),
                dirty: false,
            },
        };

        JsonLocalWriter {
            tmp_path,
            final_path,
            serializer: ArrowSerializer::new(format),
            buffer,
            stats: None,
            schema,
        }
    }

    fn file_suffix_for_format(format: &Format) -> &str {
        match format {
            Format::Json(JsonFormat {
                compression: JsonCompression::Gzip,
                ..
            }) => "json.gz",
            Format::Json(_) => "json",
            _ => panic!("JsonLocalWriter configured with non-json format {format:?}"),
        }
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
            self.buffer.write_all(data.as_slice())?;
            self.buffer.write_all(b"\n")?;
        }
        Ok(size)
    }

    fn sync(&mut self) -> anyhow::Result<usize> {
        match &mut self.buffer {
            JsonBuffer::Gzipped {
                dirty,
                encoder: Some(enc),
            } => {
                // Only flush the encoder if it contains data.
                if *dirty {
                    enc.flush()?;
                }
            }
            JsonBuffer::Gzipped { encoder: None, .. } => panic!("sync called after close()"),
            JsonBuffer::Uncompressed(w) => w.flush()?,
        }
        let size = self.buffer.writer_ref().metadata()?.len() as usize;
        if let Some(stats) = &mut self.stats {
            stats.bytes_written = size;
        }
        Ok(size)
    }

    fn close(&mut self) -> anyhow::Result<super::local::FilePreCommit> {
        match &self.buffer {
            JsonBuffer::Gzipped { .. } => {
                // Always finalize local gzip on close so file bytes cannot change later
                // from encoder drop-time writes after we return.
                let mut file = self.buffer.finish_encoder()?;
                file.flush()?;
                if let Some(stats) = &mut self.stats {
                    stats.bytes_written = file.metadata()?.len() as usize;
                }
            }
            JsonBuffer::Uncompressed(_) => {
                self.sync()?;
            }
        }

        Ok(super::local::FilePreCommit {
            tmp_file: self.tmp_path.clone(),
            destination: self.final_path.clone(),
        })
    }

    fn checkpoint(&mut self) -> anyhow::Result<Option<super::local::CurrentFileRecovery>> {
        let bytes_written = match &self.buffer {
            JsonBuffer::Gzipped { dirty, .. } if *dirty => {
                // Finish the current gzip member (writes trailer), making the file valid
                // up to this point. We must measure the file size *before* creating a new
                // encoder, since GzEncoder writes the gzip header on the first flush/write.
                let mut file = self.buffer.finish_encoder()?;
                file.flush()?;
                let size = file.metadata()?.len() as usize;
                if let Some(stats) = &mut self.stats {
                    stats.bytes_written = size;
                }
                self.buffer
                    .restore_encoder(GzEncoder::new(file, GzipCompression::default()));
                size
            }
            _ => self.sync()?,
        };

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
    use crate::filesystem::TableFormat;

    use super::super::BatchBufferingWriter;
    use super::*;
    use arrow::array::{RecordBatch, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arroyo_rpc::formats::JsonFormat;
    use arroyo_types::TaskInfo;
    use flate2::read::MultiGzDecoder;
    use std::io::Read;
    use std::sync::Arc;

    /// Helper function to create a JsonWriter with the given schema and compression
    fn create_test_writer(schema: Arc<Schema>, compression: JsonCompression) -> JsonWriter {
        let format = Format::Json(JsonFormat {
            compression,
            ..Default::default()
        });

        let event_logger = FsEventLogger {
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
            output_format: format.name(),
            table_format: TableFormat::None.name(),
        };

        let config = config::FileSystemSink {
            path: String::new(),
            storage_options: Default::default(),
            rolling_policy: Default::default(),
            file_naming: Default::default(),
            partitioning: Default::default(),
            multipart: Default::default(),
            version: Default::default(),
        };

        let arroyo_schema = Arc::new(arroyo_rpc::df::ArroyoSchema::new(
            schema.clone(),
            0,
            None,
            None,
        ));

        JsonWriter::new(&config, format, arroyo_schema, None, event_logger)
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
        // Test that checkpoints create multi-member gzip files
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field",
            DataType::Utf8,
            false,
        )]));

        let mut writer = create_test_writer(schema.clone(), JsonCompression::Gzip);

        // Write first batch
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["value1"]))],
        )
        .unwrap();
        writer.add_batch_data(&batch1);

        // Checkpoint (finishes first gzip member)
        let (checkpoint1, _) = writer.get_trailing_bytes_for_checkpoint();

        // Write second batch
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["value2"]))],
        )
        .unwrap();
        writer.add_batch_data(&batch2);

        // Close (finishes second gzip member)
        let (final_data, _) = writer.close();

        // Combine both parts
        let mut combined = checkpoint1;
        combined.extend_from_slice(&final_data);

        // Verify we can decompress the entire multi-member file
        let mut decoder = MultiGzDecoder::new(&combined[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();

        // Verify both values are present
        let text = std::str::from_utf8(&decompressed).unwrap();
        assert!(text.contains("value1"));
        assert!(text.contains("value2"));
    }

    #[test]
    fn test_streaming_compression() {
        // Test streaming compression with large data
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));

        let mut writer = create_test_writer(schema.clone(), JsonCompression::Gzip);

        // Create large data
        let data_str = "x".repeat(100_000); // 100KB per row
        let mut rows = Vec::new();
        for _ in 0..60 {
            // 60 rows * 100KB = ~6MB uncompressed
            rows.push(data_str.clone());
        }

        let array = StringArray::from(rows);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        // Add batch - data is compressed immediately with streaming encoder
        writer.add_batch_data(&batch);

        // Verify buffer has data
        assert!(
            writer.buffered_bytes() > 0,
            "Buffer should have compressed data"
        );

        // Verify compressed data is smaller than uncompressed
        let total_uncompressed = 6_000_000; // Approximate
        assert!(
            writer.buffered_bytes() < total_uncompressed,
            "Compressed size should be less than uncompressed"
        );

        // Close and verify we can decompress it
        let (compressed, _) = writer.close();
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

        let mut writer = create_test_writer(schema.clone(), JsonCompression::Gzip);

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

    /// Schema with a timestamp column (required by JsonLocalWriter for stats tracking).
    fn local_writer_schema() -> (Arc<Schema>, ArroyoSchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("data", DataType::Utf8, false),
        ]));
        let arroyo_schema = Arc::new(arroyo_rpc::df::ArroyoSchema::new(
            schema.clone(),
            0, // timestamp_index = 0
            None,
            None,
        ));
        (schema, arroyo_schema)
    }

    /// Creates a RecordBatch with a timestamp column and string data.
    fn local_writer_batch(schema: &Arc<Schema>, values: Vec<&str>) -> RecordBatch {
        let n = values.len();
        let ts_array = TimestampNanosecondArray::from(vec![1_000_000_000i64; n]);
        let data_array = StringArray::from(values);
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ts_array), Arc::new(data_array)],
        )
        .unwrap()
    }

    /// Helper to create a JsonLocalWriter in a temp directory.
    fn create_test_local_writer(
        dir: &std::path::Path,
        compression: JsonCompression,
        arroyo_schema: ArroyoSchemaRef,
    ) -> JsonLocalWriter {
        let tmp_path = dir.join("test.tmp").to_str().unwrap().to_string();
        let final_path = dir.join("test.final").to_str().unwrap().to_string();

        let format = Format::Json(JsonFormat {
            compression,
            ..Default::default()
        });

        let config = config::FileSystemSink {
            path: String::new(),
            storage_options: Default::default(),
            rolling_policy: Default::default(),
            file_naming: Default::default(),
            partitioning: Default::default(),
            multipart: Default::default(),
            version: Default::default(),
        };

        JsonLocalWriter::new(tmp_path, final_path, &config, format, arroyo_schema)
    }

    #[test]
    fn test_local_writer_suffix_for_format() {
        let format = Format::Json(JsonFormat {
            compression: JsonCompression::Uncompressed,
            ..Default::default()
        });
        assert_eq!(JsonLocalWriter::file_suffix_for_format(&format), "json");

        let format = Format::Json(JsonFormat {
            compression: JsonCompression::Gzip,
            ..Default::default()
        });
        assert_eq!(JsonLocalWriter::file_suffix_for_format(&format), "json.gz");
    }

    #[test]
    fn test_local_writer_uncompressed_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let (schema, arroyo_schema) = local_writer_schema();

        let mut writer =
            create_test_local_writer(dir.path(), JsonCompression::Uncompressed, arroyo_schema);

        let batch = local_writer_batch(&schema, vec!["hello", "world"]);
        writer.write_batch(&batch).unwrap();

        let pre_commit = writer.close().unwrap();
        let content = std::fs::read_to_string(&pre_commit.tmp_file).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("hello"));
        assert!(lines[1].contains("world"));
    }

    #[test]
    fn test_local_writer_gzip_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let (schema, arroyo_schema) = local_writer_schema();

        let mut writer = create_test_local_writer(dir.path(), JsonCompression::Gzip, arroyo_schema);

        let batch = local_writer_batch(&schema, vec!["hello", "world"]);
        writer.write_batch(&batch).unwrap();

        let pre_commit = writer.close().unwrap();

        // Read and decompress the file
        let compressed = std::fs::read(&pre_commit.tmp_file).unwrap();
        assert_eq!(&compressed[0..2], &[0x1f, 0x8b], "Should be gzip format");

        let mut decoder = MultiGzDecoder::new(&compressed[..]);
        let mut decompressed = String::new();
        decoder.read_to_string(&mut decompressed).unwrap();

        let lines: Vec<&str> = decompressed.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("hello"));
        assert!(lines[1].contains("world"));
    }

    #[test]
    fn test_local_writer_gzip_checkpoint_multi_member() {
        let dir = tempfile::tempdir().unwrap();
        let (schema, arroyo_schema) = local_writer_schema();

        let mut writer = create_test_local_writer(dir.path(), JsonCompression::Gzip, arroyo_schema);

        // Write first batch and checkpoint
        let batch1 = local_writer_batch(&schema, vec!["value1"]);
        writer.write_batch(&batch1).unwrap();
        let recovery = writer.checkpoint().unwrap().unwrap();
        assert!(recovery.bytes_written > 0);
        assert!(recovery.suffix.is_none());

        // Write second batch and close
        let batch2 = local_writer_batch(&schema, vec!["value2"]);
        writer.write_batch(&batch2).unwrap();
        let pre_commit = writer.close().unwrap();

        // Read and decompress the entire file (multi-member gzip)
        let compressed = std::fs::read(&pre_commit.tmp_file).unwrap();
        let mut decoder = MultiGzDecoder::new(&compressed[..]);
        let mut decompressed = String::new();
        decoder.read_to_string(&mut decompressed).unwrap();

        // Both values should be present
        assert!(decompressed.contains("value1"));
        assert!(decompressed.contains("value2"));
    }

    #[test]
    fn test_local_writer_gzip_checkpoint_truncation_recovery() {
        // Verify that after a checkpoint, truncating the file to bytes_written
        // produces a valid gzip file (simulating crash recovery).
        let dir = tempfile::tempdir().unwrap();
        let (schema, arroyo_schema) = local_writer_schema();

        let mut writer = create_test_local_writer(dir.path(), JsonCompression::Gzip, arroyo_schema);

        // Write first batch and checkpoint
        let batch1 = local_writer_batch(&schema, vec!["committed_data"]);
        writer.write_batch(&batch1).unwrap();
        let recovery = writer.checkpoint().unwrap().unwrap();

        // Write second batch (this data would be lost on crash).
        // We intentionally do NOT call sync()/close() — simulating a crash.
        let batch2 = local_writer_batch(&schema, vec!["uncommitted_data"]);
        writer.write_batch(&batch2).unwrap();

        // Drop the writer to release the file handle (simulating process exit).
        drop(writer);

        // Simulate recovery: truncate to checkpoint position
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&recovery.tmp_file)
            .unwrap();
        file.set_len(recovery.bytes_written as u64).unwrap();
        drop(file);

        // Verify the truncated file is a valid gzip with only the committed data
        let compressed = std::fs::read(&recovery.tmp_file).unwrap();
        assert_eq!(
            compressed.len(),
            recovery.bytes_written,
            "File should be truncated to checkpoint size"
        );

        let mut decoder = MultiGzDecoder::new(&compressed[..]);
        let mut decompressed = String::new();
        decoder.read_to_string(&mut decompressed).unwrap();

        assert!(decompressed.contains("committed_data"));
        assert!(!decompressed.contains("uncommitted_data"));
    }

    #[test]
    fn test_local_writer_gzip_empty_checkpoint_truncation_recovery() {
        // Verify that a checkpoint with no new rows still represents a valid
        // truncation boundary for gzip recovery.
        let dir = tempfile::tempdir().unwrap();
        let (schema, arroyo_schema) = local_writer_schema();

        let mut writer = create_test_local_writer(dir.path(), JsonCompression::Gzip, arroyo_schema);

        // Write and checkpoint committed data.
        let batch1 = local_writer_batch(&schema, vec!["committed_data"]);
        writer.write_batch(&batch1).unwrap();
        let _recovery1 = writer.checkpoint().unwrap().unwrap();

        // Checkpoint again without new rows.
        let recovery2 = writer.checkpoint().unwrap().unwrap();

        // Write data after the second checkpoint (should be lost on recovery).
        let batch2 = local_writer_batch(&schema, vec!["uncommitted_data"]);
        writer.write_batch(&batch2).unwrap();
        drop(writer);

        // Simulate recovery by truncating to second checkpoint boundary.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&recovery2.tmp_file)
            .unwrap();
        file.set_len(recovery2.bytes_written as u64).unwrap();
        drop(file);

        let compressed = std::fs::read(&recovery2.tmp_file).unwrap();
        let mut decoder = MultiGzDecoder::new(&compressed[..]);
        let mut decompressed = String::new();
        decoder.read_to_string(&mut decompressed).unwrap();

        assert!(decompressed.contains("committed_data"));
        assert!(!decompressed.contains("uncommitted_data"));
    }

    #[test]
    fn test_checkpoint_no_data_no_growth() {
        // Verify that repeated checkpoints with no data written between them
        // do not cause the gzip buffer to grow (no empty gzip members emitted).
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field",
            DataType::Utf8,
            false,
        )]));

        let mut writer = create_test_writer(schema.clone(), JsonCompression::Gzip);

        // Checkpoint with no data written
        let (bytes1, _) = writer.get_trailing_bytes_for_checkpoint();

        // Checkpoint again with no data written
        let (bytes2, _) = writer.get_trailing_bytes_for_checkpoint();

        // A third time to be sure
        let (bytes3, _) = writer.get_trailing_bytes_for_checkpoint();

        // Buffer should not have grown
        assert_eq!(
            bytes1.len(),
            bytes2.len(),
            "Repeated checkpoints with no data should not grow the buffer"
        );
        assert_eq!(bytes2.len(), bytes3.len());
        assert_eq!(
            bytes1.len(),
            0,
            "Checkpoint with no data should produce empty bytes"
        );
    }

    #[test]
    fn test_local_writer_checkpoint_no_data_no_growth() {
        // Verify that repeated checkpoints on a local gzip writer with no data
        // written between them do not cause the file to grow.
        let dir = tempfile::tempdir().unwrap();
        let (schema, arroyo_schema) = local_writer_schema();

        let mut writer = create_test_local_writer(dir.path(), JsonCompression::Gzip, arroyo_schema);

        // Write initial batch so stats are initialized
        let batch = local_writer_batch(&schema, vec!["initial"]);
        writer.write_batch(&batch).unwrap();

        // First checkpoint — finalizes the gzip member
        let recovery1 = writer.checkpoint().unwrap().unwrap();
        let size_after_first = recovery1.bytes_written;

        // Second checkpoint — no new data, should not grow
        let recovery2 = writer.checkpoint().unwrap().unwrap();
        let size_after_second = recovery2.bytes_written;

        // Third checkpoint — still no new data
        let recovery3 = writer.checkpoint().unwrap().unwrap();
        let size_after_third = recovery3.bytes_written;

        assert_eq!(
            size_after_first, size_after_second,
            "File should not grow on empty checkpoint"
        );
        assert_eq!(size_after_second, size_after_third);

        // Verify the file is still valid gzip
        let compressed = std::fs::read(&recovery3.tmp_file).unwrap();
        let mut decoder = MultiGzDecoder::new(&compressed[..]);
        let mut decompressed = String::new();
        decoder.read_to_string(&mut decompressed).unwrap();
        assert!(decompressed.contains("initial"));
    }

    #[test]
    fn test_write_after_empty_checkpoint() {
        // Verify that writing data after an empty checkpoint (no data between
        // checkpoints) still produces valid compressed output.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field",
            DataType::Utf8,
            false,
        )]));

        let mut writer = create_test_writer(schema.clone(), JsonCompression::Gzip);

        // Empty checkpoint (no data written yet)
        let (empty_bytes, _) = writer.get_trailing_bytes_for_checkpoint();
        assert_eq!(empty_bytes.len(), 0, "Should be empty before any writes");

        // Now write data and checkpoint
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["after_empty"]))],
        )
        .unwrap();
        writer.add_batch_data(&batch);

        let (checkpoint_bytes, _) = writer.get_trailing_bytes_for_checkpoint();
        assert!(!checkpoint_bytes.is_empty(), "Should have data after write");

        // Verify the compressed data is valid and contains our value
        let mut decoder = MultiGzDecoder::new(&checkpoint_bytes[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();

        let text = std::str::from_utf8(&decompressed).unwrap();
        assert!(
            text.contains("after_empty"),
            "Decompressed data should contain the written value"
        );
    }

    #[test]
    fn test_close_after_clean_checkpoint() {
        // Verify that closing after a checkpoint with no new writes still
        // returns a valid suffix and preserves correct decompression semantics.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field",
            DataType::Utf8,
            false,
        )]));

        let mut writer = create_test_writer(schema.clone(), JsonCompression::Gzip);

        // Write data
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["only_data"]))],
        )
        .unwrap();
        writer.add_batch_data(&batch);

        // Checkpoint (finalizes gzip member). This returns a *copy* of the buffer.
        let (checkpoint_bytes, _) = writer.get_trailing_bytes_for_checkpoint();

        // Simulate the upload layer draining the buffer (as would happen in real use).
        let buffered = writer.buffered_bytes();
        let _ = writer.split_to(buffered);

        // Close with no new writes.
        let (close_bytes, _) = writer.close();

        // Verify combined bytes remain valid gzip and contain the value exactly once.
        let mut combined = checkpoint_bytes;
        combined.extend_from_slice(&close_bytes);

        let mut decoder = MultiGzDecoder::new(&combined[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();

        let text = std::str::from_utf8(&decompressed).unwrap();
        assert!(
            text.contains("only_data"),
            "Should contain the written data"
        );
        assert_eq!(text.lines().count(), 1);
    }
}
