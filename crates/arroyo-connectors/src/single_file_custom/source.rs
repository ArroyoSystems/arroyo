use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_rpc::ControlMessage;
use arroyo_types::{SignalMessage, UserError, Watermark};
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use serde_json::Value;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::select;
use tokio_stream::wrappers::LinesStream;
use tokio_stream::Stream;
use tracing::info;

use super::{Compression, FileFormat, TsFormat};

const LOG_INTERVAL: u64 = 1_000_000;

pub struct SingleFileCustomSourceFunc {
    pub path: String,
    pub file_format: FileFormat,
    pub compression: Compression,
    pub timestamp_field: String,
    pub ts_format: TsFormat,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub records_read: u64,
}

impl SingleFileCustomSourceFunc {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        path: String,
        file_format: FileFormat,
        compression: Compression,
        timestamp_field: String,
        ts_format: TsFormat,
        format: Format,
        framing: Option<Framing>,
        bad_data: Option<BadData>,
    ) -> Self {
        Self {
            path,
            file_format,
            compression,
            timestamp_field,
            ts_format,
            format,
            framing,
            bad_data,
            records_read: 0,
        }
    }

    fn parse_timestamp(&self, value: &str) -> Result<SystemTime, UserError> {
        match self.ts_format {
            TsFormat::UnixMillis => {
                let millis: i64 = value.trim().parse().map_err(|e| {
                    UserError::new(
                        "invalid timestamp",
                        format!("Failed to parse '{value}' as unix_millis: {e}"),
                    )
                })?;
                Ok(UNIX_EPOCH + Duration::from_millis(millis as u64))
            }
            TsFormat::UnixSeconds => {
                let secs: i64 = value.trim().parse().map_err(|e| {
                    UserError::new(
                        "invalid timestamp",
                        format!("Failed to parse '{value}' as unix_seconds: {e}"),
                    )
                })?;
                Ok(UNIX_EPOCH + Duration::from_secs(secs as u64))
            }
            TsFormat::Rfc3339 => {
                let dt: DateTime<Utc> = value.trim().parse().map_err(|e| {
                    UserError::new(
                        "invalid timestamp",
                        format!("Failed to parse '{value}' as RFC3339: {e}"),
                    )
                })?;
                Ok(dt.into())
            }
        }
    }

    fn extract_timestamp_from_json(&self, json: &Value) -> Result<SystemTime, UserError> {
        let ts_value = json.get(&self.timestamp_field).ok_or_else(|| {
            UserError::new(
                "missing timestamp field",
                format!("Field '{}' not found in JSON record", self.timestamp_field),
            )
        })?;

        let ts_str = match ts_value {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            _ => {
                return Err(UserError::new(
                    "invalid timestamp type",
                    format!(
                        "Timestamp field '{}' has unsupported type: {:?}",
                        self.timestamp_field, ts_value
                    ),
                ))
            }
        };

        self.parse_timestamp(&ts_str)
    }

    async fn get_line_stream(
        &self,
    ) -> Result<Box<dyn Stream<Item = Result<String, UserError>> + Unpin + Send>, UserError> {
        let file = File::open(&self.path)
            .await
            .map_err(|e| UserError::new("failed to open file", format!("{}: {e}", self.path)))?;

        let compression_reader: Box<dyn AsyncRead + Unpin + Send> = match self.compression {
            Compression::None => Box::new(BufReader::new(file)),
            Compression::Gzip => Box::new(GzipDecoder::new(BufReader::new(file))),
            Compression::Zstd => Box::new(ZstdDecoder::new(BufReader::new(file))),
        };

        let lines = LinesStream::new(BufReader::new(compression_reader).lines());
        Ok(Box::new(lines.map(|result| {
            result.map_err(|e| UserError::new("failed to read line", e.to_string()))
        })))
    }

    async fn run_json(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        let mut line_stream = self.get_line_stream().await?;

        loop {
            select! {
                line = line_stream.next() => {
                    match line.transpose()? {
                        Some(line) => {
                            if line.trim().is_empty() {
                                continue;
                            }

                            let json: Value = serde_json::from_str(&line).map_err(|e| {
                                UserError::new("invalid JSON", format!("Line: '{line}', Error: {e}"))
                            })?;

                            let event_time = self.extract_timestamp_from_json(&json)?;

                            collector.deserialize_slice(line.as_bytes(), event_time, None).await?;
                            self.records_read += 1;

                            if self.records_read % LOG_INTERVAL == 0 {
                                info!("Read {} records from {}", self.records_read, self.path);
                            }

                            if collector.should_flush() {
                                collector.flush_buffer().await?;
                            }
                        }
                        None => {
                            info!("Finished reading {} records from {}", self.records_read, self.path);
                            // Emit a far-future watermark to flush all pending windows
                            let flush_ts = SystemTime::now() + Duration::from_secs(60 * 60 * 24 * 365);
                            collector.broadcast(SignalMessage::Watermark(Watermark::EventTime(flush_ts))).await;
                            collector.flush_buffer().await?;
                            return Ok(SourceFinishType::Final);
                        }
                    }
                }
                msg = ctx.control_rx.recv() => {
                    if let Some(finish_type) = self.handle_control_message(msg, ctx, collector).await {
                        return Ok(finish_type);
                    }
                }
            }
        }
    }

    fn get_min_max_timestamp(
        &self,
        batch: &RecordBatch,
        ts_idx: usize,
    ) -> (Option<SystemTime>, Option<SystemTime>) {
        use arrow::array::AsArray;
        use arrow::datatypes::{DataType, TimeUnit};

        let ts_array = batch.column(ts_idx);
        let data_type = ts_array.data_type().clone();

        let (min_nanos, max_nanos): (Option<i64>, Option<i64>) = match &data_type {
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let arr = ts_array.as_primitive::<arrow::datatypes::TimestampMillisecondType>();
                let vals: Vec<i64> = (0..batch.num_rows())
                    .map(|i| arr.value(i) * 1_000_000)
                    .collect();
                (
                    vals.iter().copied().reduce(i64::min),
                    vals.iter().copied().reduce(i64::max),
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let arr = ts_array.as_primitive::<arrow::datatypes::TimestampNanosecondType>();
                let vals: Vec<i64> = (0..batch.num_rows()).map(|i| arr.value(i)).collect();
                (
                    vals.iter().copied().reduce(i64::min),
                    vals.iter().copied().reduce(i64::max),
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let arr = ts_array.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
                let vals: Vec<i64> = (0..batch.num_rows())
                    .map(|i| arr.value(i) * 1_000)
                    .collect();
                (
                    vals.iter().copied().reduce(i64::min),
                    vals.iter().copied().reduce(i64::max),
                )
            }
            DataType::Int64 => {
                let arr = ts_array.as_primitive::<arrow::datatypes::Int64Type>();
                let vals: Vec<i64> = match self.ts_format {
                    TsFormat::UnixMillis => (0..batch.num_rows())
                        .map(|i| arr.value(i) * 1_000_000)
                        .collect(),
                    TsFormat::UnixSeconds => (0..batch.num_rows())
                        .map(|i| arr.value(i) * 1_000_000_000)
                        .collect(),
                    _ => vec![],
                };
                (
                    vals.iter().copied().reduce(i64::min),
                    vals.iter().copied().reduce(i64::max),
                )
            }
            _ => (None, None),
        };

        let to_systime = |nanos: i64| UNIX_EPOCH + Duration::from_nanos(nanos as u64);
        (min_nanos.map(to_systime), max_nanos.map(to_systime))
    }

    async fn run_parquet(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        let file = tokio::fs::File::open(&self.path)
            .await
            .map_err(|e| UserError::new("failed to open file", format!("{}: {e}", self.path)))?;

        let file_meta = file
            .metadata()
            .await
            .map_err(|e| UserError::new("failed to get file metadata", e.to_string()))?;

        let file_size = file_meta.len();

        // Create a local file system object store
        let local_fs = Arc::new(object_store::local::LocalFileSystem::new());

        // Create object meta for the parquet reader
        let object_meta = object_store::ObjectMeta {
            location: object_store::path::Path::from(self.path.as_str()),
            last_modified: file_meta
                .modified()
                .ok()
                .map(|t| t.into())
                .unwrap_or_else(chrono::Utc::now),
            size: file_size as usize,
            e_tag: None,
            version: None,
        };

        let reader = ParquetObjectReader::new(local_fs, object_meta);

        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| UserError::new("failed to create parquet reader", e.to_string()))?
            .with_batch_size(8192);

        // Get the timestamp column index
        let parquet_schema = builder.schema();
        let ts_idx =
            parquet_schema
                .fields()
                .iter()
                .position(|f| f.name() == &self.timestamp_field)
                .ok_or_else(|| {
                    UserError::new(
                        "missing timestamp field",
                        format!(
                        "Timestamp field '{}' not found in parquet schema. Available fields: {:?}",
                        self.timestamp_field,
                        parquet_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                    ),
                    )
                })?;

        let mut stream = builder
            .build()
            .map_err(|e| UserError::new("failed to build parquet stream", e.to_string()))?;

        loop {
            select! {
                batch = stream.next() => {
                    match batch {
                        Some(Ok(batch)) => {
                            // Broadcast min timestamp BEFORE collecting so windows aren't dropped as late
                            let (min_ts, max_ts) = self.get_min_max_timestamp(&batch, ts_idx);
                            if let Some(ts) = min_ts {
                                collector.broadcast(SignalMessage::Watermark(Watermark::EventTime(ts))).await;
                            }

                            // Extract timestamps from the batch and add _timestamp column
                            let out_batch = self.add_timestamp_to_batch(&batch, ts_idx)?;
                            collector.collect(out_batch).await;

                            // Advance watermark to max timestamp AFTER collecting
                            if let Some(ts) = max_ts {
                                collector.broadcast(SignalMessage::Watermark(Watermark::EventTime(ts))).await;
                            }

                            self.records_read += batch.num_rows() as u64;

                            if self.records_read % LOG_INTERVAL == 0 {
                                info!("Read {} records from {}", self.records_read, self.path);
                            }
                        }
                        Some(Err(e)) => {
                            return Err(UserError::new("failed to read parquet batch", e.to_string()));
                        }
                        None => {
                            info!("Finished reading {} records from {}", self.records_read, self.path);
                            return Ok(SourceFinishType::Final);
                        }
                    }
                }
                msg = ctx.control_rx.recv() => {
                    if let Some(finish_type) = self.handle_control_message(msg, ctx, collector).await {
                        return Ok(finish_type);
                    }
                }
            }
        }
    }

    fn add_timestamp_to_batch(
        &self,
        batch: &RecordBatch,
        ts_idx: usize,
    ) -> Result<RecordBatch, UserError> {
        let ts_array = batch.column(ts_idx);

        // Convert timestamp column values to nanoseconds for _timestamp column
        let mut timestamps = Vec::with_capacity(batch.num_rows());

        use arrow::array::AsArray;
        use arrow::datatypes::{DataType, TimeUnit};

        let data_type = ts_array.data_type().clone();

        for i in 0..batch.num_rows() {
            let ts_nanos: i64 = match &data_type {
                // Arrow Timestamp types - use the type's semantics directly
                DataType::Timestamp(TimeUnit::Nanosecond, _) => ts_array
                    .as_primitive::<arrow::datatypes::TimestampNanosecondType>()
                    .value(i),
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    ts_array
                        .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                        .value(i)
                        * 1_000
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    ts_array
                        .as_primitive::<arrow::datatypes::TimestampMillisecondType>()
                        .value(i)
                        * 1_000_000
                }
                DataType::Timestamp(TimeUnit::Second, _) => {
                    ts_array
                        .as_primitive::<arrow::datatypes::TimestampSecondType>()
                        .value(i)
                        * 1_000_000_000
                }

                // Integer types - interpret based on ts_format
                DataType::Int64 => {
                    let value = ts_array
                        .as_primitive::<arrow::datatypes::Int64Type>()
                        .value(i);
                    match self.ts_format {
                        TsFormat::UnixMillis => value * 1_000_000,
                        TsFormat::UnixSeconds => value * 1_000_000_000,
                        TsFormat::Rfc3339 => {
                            return Err(UserError::new(
                                "invalid ts_format",
                                "ts_format 'rfc3339' cannot be used with integer columns",
                            ))
                        }
                    }
                }

                _ => {
                    return Err(UserError::new(
                        "unsupported timestamp type",
                        format!(
                        "Timestamp field has type {data_type:?}. Supported: Int64, Timestamp types"
                    ),
                    ))
                }
            };
            timestamps.push(ts_nanos);
        }

        // Build the timestamp column
        let timestamp_array = arrow::array::TimestampNanosecondArray::from(timestamps);

        // Build output schema from batch schema + _timestamp column
        let mut fields: Vec<arrow::datatypes::FieldRef> =
            batch.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(arrow::datatypes::Field::new(
            "_timestamp",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        )));
        let output_schema = Arc::new(arrow::datatypes::Schema::new(fields));

        // Create output batch with all original columns plus _timestamp
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(timestamp_array));

        RecordBatch::try_new(output_schema, columns).map_err(|e| {
            UserError::new(
                "failed to create output batch",
                format!("Schema mismatch: {e}"),
            )
        })
    }

    async fn handle_control_message(
        &mut self,
        msg: Option<ControlMessage>,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Option<SourceFinishType> {
        match msg {
            Some(ControlMessage::Checkpoint(c)) => {
                collector.flush_buffer().await.ok();
                if self.start_checkpoint(c, ctx, collector).await {
                    return Some(SourceFinishType::Immediate);
                }
            }
            Some(ControlMessage::Stop { mode }) => {
                info!("Stopping single_file_custom source {:?}", mode);
                match mode {
                    StopMode::Graceful => return Some(SourceFinishType::Graceful),
                    StopMode::Immediate => return Some(SourceFinishType::Immediate),
                }
            }
            Some(ControlMessage::NoOp) => {}
            _ => {}
        }
        None
    }
}

#[async_trait]
impl SourceOperator for SingleFileCustomSourceFunc {
    fn name(&self) -> String {
        "SingleFileCustomSource".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        // No checkpointing state - starts over on restart
        HashMap::new()
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        // Enforce parallelism = 1
        if ctx.task_info.task_index != 0 {
            return SourceFinishType::Final;
        }

        // Initialize deserializer for JSON format
        if matches!(self.file_format, FileFormat::Json) {
            collector.initialize_deserializer(
                self.format.clone(),
                self.framing.clone(),
                self.bad_data.clone(),
                &[],
            );
        }

        let result = match self.file_format {
            FileFormat::Json => self.run_json(ctx, collector).await,
            FileFormat::Parquet => self.run_parquet(ctx, collector).await,
        };

        match result {
            Ok(finish_type) => finish_type,
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;
                panic!("{}: {}", e.name, e.details);
            }
        }
    }
}
