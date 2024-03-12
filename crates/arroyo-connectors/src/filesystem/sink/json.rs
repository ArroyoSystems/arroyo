use std::{fs::File, io::Write, time::Instant};

use arrow::record_batch::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_rpc::{df::ArroyoSchemaRef, formats::Format};

use super::{
    local::{CurrentFileRecovery, LocalWriter},
    parquet::representitive_timestamp,
    BatchBufferingWriter, FileSettings, MultiPartWriterStats, TableType,
};

pub struct JsonWriter {
    current_buffer: Vec<u8>,
    serializer: ArrowSerializer,
    target_part_size: usize,
}

impl BatchBufferingWriter for JsonWriter {
    fn new(
        config: &super::FileSystemTable,
        format: Option<Format>,
        _schema: ArroyoSchemaRef,
    ) -> Self {
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
        Self {
            current_buffer: Vec::new(),
            serializer: ArrowSerializer::new(format.expect("should have format")),
            target_part_size,
        }
    }
    fn suffix() -> String {
        "json".to_string()
    }

    fn add_batch_data(&mut self, batch: RecordBatch) -> Option<Vec<u8>> {
        for k in self.serializer.serialize(&batch) {
            self.current_buffer.extend(k);
            self.current_buffer.extend(b"\n");
        }
        if self.buffer_length() > self.target_part_size {
            Some(self.evict_current_buffer())
        } else {
            None
        }
    }

    fn buffer_length(&self) -> usize {
        self.current_buffer.len()
    }

    fn evict_current_buffer(&mut self) -> Vec<u8> {
        let result = std::mem::take(&mut self.current_buffer);
        result
    }

    fn get_trailing_bytes_for_checkpoint(&mut self) -> Option<Vec<u8>> {
        if self.current_buffer.is_empty() {
            None
        } else {
            Some(self.current_buffer.clone())
        }
    }

    fn close(&mut self, final_batch: Option<RecordBatch>) -> Option<Vec<u8>> {
        if let Some(final_batch) = final_batch {
            if let Some(final_batch) = self.add_batch_data(final_batch) {
                return Some(final_batch);
            }
        }
        if self.current_buffer.is_empty() {
            None
        } else {
            Some(self.evict_current_buffer())
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
        _table_properties: &super::FileSystemTable,
        format: Option<Format>,
        schema: ArroyoSchemaRef,
    ) -> Self {
        let file = File::create(&tmp_path).unwrap();
        JsonLocalWriter {
            tmp_path,
            final_path,
            serializer: ArrowSerializer::new(format.expect("should have format")),
            file,
            stats: None,
            schema,
        }
    }

    fn file_suffix() -> &'static str {
        "json"
    }

    fn write_batch(&mut self, batch: RecordBatch) -> anyhow::Result<()> {
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
        for data in self.serializer.serialize(&batch) {
            self.file.write_all(data.as_slice())?;
            self.file.write_all(b"\n")?;
        }
        Ok(())
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
            }))
        } else {
            Ok(None)
        }
    }

    fn stats(&self) -> MultiPartWriterStats {
        self.stats.clone().unwrap()
    }
}
