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
/// The `Gzipped` variant uses an `Option` so that the encoder can be temporarily
/// taken out (via `.take()`) for operations that consume it (like `finish()`).
/// In addition, `None` is used to indicate that the `JsonWriter` has been closed.
enum JsonBuffer {
    Uncompressed(BytesMut),
    Gzipped(Option<GzEncoder<bytes::buf::Writer<BytesMut>>>),
}

impl JsonBuffer {
    /// Returns a reference to the underlying buffer.
    fn buf(&self) -> &BytesMut {
        match self {
            JsonBuffer::Uncompressed(buf) => buf,
            JsonBuffer::Gzipped(Some(encoder)) => encoder.get_ref().get_ref(),
            JsonBuffer::Gzipped(None) => panic!("encoder was not restored after take()"),
        }
    }

    /// Returns a mutable reference to the underlying buffer.
    fn buf_mut(&mut self) -> &mut BytesMut {
        match self {
            JsonBuffer::Uncompressed(buf) => buf,
            JsonBuffer::Gzipped(Some(encoder)) => encoder.get_mut().get_mut(),
            JsonBuffer::Gzipped(None) => panic!("encoder was not restored after take()"),
        }
    }

    /// Takes the encoder out of the `Gzipped` variant, leaving `None` in its place.
    /// Panics if called on `Uncompressed` or if the encoder was already taken.
    fn take_encoder(&mut self) -> GzEncoder<bytes::buf::Writer<BytesMut>> {
        match self {
            JsonBuffer::Gzipped(encoder @ Some(_)) => encoder.take().unwrap(),
            _ => panic!("take_encoder called on non-Gzipped or already-taken buffer"),
        }
    }

    /// Restores an encoder into the `Gzipped` variant after a previous `take_encoder`.
    fn restore_encoder(&mut self, new_encoder: GzEncoder<bytes::buf::Writer<BytesMut>>) {
        match self {
            JsonBuffer::Gzipped(slot @ None) => *slot = Some(new_encoder),
            _ => panic!("restore_encoder called on non-Gzipped or non-empty buffer"),
        }
    }
}

pub struct JsonWriter {
    buffer: JsonBuffer,
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
            JsonCompression::Uncompressed => JsonBuffer::Uncompressed(BytesMut::new()),
            JsonCompression::Gzip => JsonBuffer::Gzipped(Some(GzEncoder::new(
                BytesMut::new().writer(),
                GzipCompression::default(),
            ))),
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

            match &mut self.buffer {
                JsonBuffer::Gzipped(Some(encoder)) => {
                    // These writes are infallible, the Writer<BytesMut>::write() always returns `Ok`.
                    // Ref: https://docs.rs/crate/bytes/1.11.1/source/src/buf/writer.rs#78-83
                    encoder
                        .write_all(&k)
                        .expect("Failed to write to gzip encoder");
                    encoder
                        .write_all(b"\n")
                        .expect("Failed to write newline to gzip encoder");
                }
                JsonBuffer::Gzipped(None) => {
                    panic!("add_batch_data called after `JsonWriter::close()` was called");
                }
                JsonBuffer::Uncompressed(buf) => {
                    buf.extend_from_slice(&k);
                    buf.put_u8(b'\n');
                }
            }
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
        self.buffer.buf().len()
    }

    fn split_to(&mut self, pos: usize) -> Bytes {
        self.buffer.buf_mut().split_to(pos).freeze()
    }

    /// The bytes stored in the checkpoint are bytes that we are ready to uploaded as-is.
    /// For compressed mode, we finish the current gzip member to ensure all data is complete.
    fn get_trailing_bytes_for_checkpoint(&mut self) -> (Vec<u8>, Option<IcebergFileMetadata>) {
        let bytes = match &self.buffer {
            JsonBuffer::Uncompressed(buf) => buf.to_vec(),
            JsonBuffer::Gzipped(_) => {
                // finish() consumes the encoder, so we take it out temporarily.
                let encoder = self.buffer.take_encoder();
                // Finish infallible, since the underlying Writer<BytesMut>::write() always returns `Ok`.
                // Ref: https://docs.rs/crate/bytes/1.11.1/source/src/buf/writer.rs#78-83
                let inner = encoder
                    .finish()
                    .expect("Failed to finish gzip encoder at checkpoint");

                let bytes = inner.get_ref().to_vec();

                // Reuse the buffer (retaining the data) for the new gzip member.
                self.buffer
                    .restore_encoder(GzEncoder::new(inner, GzipCompression::default()));

                bytes
            }
        };

        (bytes, None)
    }

    fn close(&mut self) -> (Bytes, Option<IcebergFileMetadata>) {
        let data = match &self.buffer {
            JsonBuffer::Uncompressed(_) => self.buffer.buf_mut().split().freeze(),
            JsonBuffer::Gzipped(_) => {
                let encoder = self.buffer.take_encoder();
                // Finish infallible, since the underlying Writer<BytesMut>::write() always returns `Ok`.
                // Ref: https://docs.rs/crate/bytes/1.11.1/source/src/buf/writer.rs#78-83
                let inner = encoder
                    .finish()
                    .expect("Failed to finish gzip encoder at close");

                inner.into_inner().split().freeze()
            }
        };

        (data, None)
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
    use crate::filesystem::TableFormat;

    use super::super::BatchBufferingWriter;
    use super::*;
    use arrow::array::{RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
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
}
