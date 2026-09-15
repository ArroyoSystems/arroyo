use anyhow::{Context, anyhow};
use arrow::array::TimestampNanosecondArray;
use arrow::json::writer::JsonArray;
use arrow::json::{Writer, WriterBuilder};
use arroyo_connectors::preview::{PreviewFilePath, PreviewMetadata};
use arroyo_formats::json::encoders::ArroyoEncoderFactory;
use arroyo_rpc::api_types::pipelines::OutputData;
use arroyo_rpc::formats::TimestampFormat;
use arroyo_storage::StorageProvider;
use arroyo_types::{from_nanos, to_micros};
use bytes::Bytes;
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;

use crate::rest_utils::{ErrorResp, bad_request, log_and_map};

pub(crate) struct PreviewOutputReader {
    storage: StorageProvider,
    job_id: String,
    include_subdirectories: bool,
    operator_id: Option<String>,
}

impl PreviewOutputReader {
    pub(crate) async fn new(
        base_path: &str,
        job_id: &str,
        operator_id: Option<&str>,
    ) -> Result<Self, ErrorResp> {
        if base_path.is_empty() {
            return Err(bad_request("preview output path must not be empty"));
        }
        let output_path =
            PreviewFilePath::directory(base_path, job_id, operator_id.map(|id| (id, 0)));

        let storage = StorageProvider::for_url(&output_path)
            .await
            .map_err(|error| {
                log_and_map(anyhow!(
                    "failed to access preview output path '{output_path}': {error}"
                ))
            })?;

        Ok(Self {
            storage,
            job_id: job_id.to_string(),
            include_subdirectories: operator_id.is_none(),
            operator_id: operator_id.map(ToOwned::to_owned),
        })
    }

    pub(crate) async fn read(&self, offset: u64) -> Result<Vec<OutputData>, ErrorResp> {
        let mut files = self
            .storage
            .list(self.include_subdirectories)
            .await
            .map_err(|error| {
                log_and_map(anyhow!("failed to list preview output files: {error}"))
            })?;
        let mut available = Vec::new();

        // Capture the set of files before reading any of them. Files completed after this
        // listing are intentionally left for the client's next request.
        while let Some(file) = files.next().await {
            let file = file.map_err(|error| {
                log_and_map(anyhow!("failed to list a preview output file: {error}"))
            })?;
            let operator = self
                .operator_id
                .as_deref()
                .map(|operator_id| (operator_id, 0));
            let Some(path) = PreviewFilePath::from_listing(file.as_ref(), &self.job_id, operator)
            else {
                continue;
            };
            if path.start_row >= offset {
                available.push((path, file));
            }
        }
        drop(files);

        available.sort_by(|(a, _), (b, _)| a.cmp(b));
        let object_store = self.storage.get_backing_store();
        let mut output = Vec::with_capacity(available.len());

        for (_, file) in available {
            let bytes = object_store
                .get(&file)
                .await
                .map_err(|error| {
                    log_and_map(anyhow!(
                        "failed to read preview output file '{file}': {error}"
                    ))
                })?
                .bytes()
                .await
                .map_err(|error| {
                    log_and_map(anyhow!(
                        "failed to read preview output file '{file}': {error}"
                    ))
                })?;
            let data = decode_preview_file(bytes).map_err(|error| {
                log_and_map(error.context(format!("failed to decode preview output file '{file}'")))
            })?;

            if self
                .operator_id
                .as_ref()
                .is_some_and(|operator_id| operator_id != &data.operator_id)
            {
                return Err(log_and_map(anyhow!(
                    "preview output file '{file}' belongs to operator '{}', not the requested operator",
                    data.operator_id
                )));
            }

            output.push(data);
        }

        Ok(output)
    }
}

fn decode_preview_file(bytes: Bytes) -> anyhow::Result<OutputData> {
    let mut json = Vec::with_capacity(bytes.len());
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let metadata: PreviewMetadata = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned()
        .unwrap_or_default()
        .try_into()?;
    let timestamp_idx = usize::try_from(metadata.timestamp_idx)
        .context("preview timestamp column index does not fit in usize")?;
    let reader = builder.build()?;
    let mut timestamps = Vec::new();

    {
        let mut writer: Writer<_, JsonArray> = WriterBuilder::new()
            .with_explicit_nulls(true)
            .with_encoder_factory(Arc::new(ArroyoEncoderFactory {
                timestamp_format: TimestampFormat::RFC3339,
                decimal_encoding: Default::default(),
            }))
            .build(&mut json);

        for batch in reader {
            let mut batch = batch?;
            let timestamp_column = batch
                .columns()
                .get(timestamp_idx)
                .context("preview timestamp column index is out of bounds")?
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .context("preview timestamp column is not a nanosecond timestamp")?;

            timestamps.extend(
                timestamp_column
                    .iter()
                    .map(|timestamp| to_micros(from_nanos(timestamp.unwrap_or(0).max(0) as u128))),
            );
            batch.remove_column(timestamp_idx);
            writer.write(&batch)?;
        }

        writer.finish()?;
    }

    Ok(OutputData {
        operator_id: metadata.operator_id,
        subtask_idx: metadata
            .subtask_idx
            .try_into()
            .context("preview subtask index does not fit in u32")?,
        timestamps,
        start_id: metadata.start_row,
        batch: serde_json::from_slice(&json).context("failed to parse preview JSON output")?,
    })
}

pub(crate) fn flatten_preview_output(
    chunks: Vec<OutputData>,
) -> Result<Vec<serde_json::Value>, ErrorResp> {
    let mut output = Vec::new();
    for chunk in chunks {
        let rows: Vec<serde_json::Value> =
            serde_json::from_value(chunk.batch).map_err(|error| {
                log_and_map(anyhow!(
                    "failed to decode rows in preview output for operator '{}': {error}",
                    chunk.operator_id
                ))
            })?;
        output.extend(rows);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    async fn write_preview_file(
        storage: &StorageProvider,
        path: &str,
        start_row: u64,
        rows: &[(i64, i64)],
    ) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
                Arc::new(TimestampNanosecondArray::from_iter_values(
                    rows.iter().map(|(_, timestamp)| *timestamp),
                )),
            ],
        )
        .unwrap();
        let metadata = PreviewMetadata {
            start_row,
            operator_id: "preview_1".to_string(),
            subtask_idx: 0,
            timestamp_idx: 1,
        };
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(metadata.into()))
            .build();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        storage.put(path, bytes).await.unwrap();
    }

    #[tokio::test]
    async fn reads_parquet_files_at_and_after_offset() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("arroyo-preview-output-{unique}"));
        let path_string = path.to_string_lossy().to_string();
        let storage = StorageProvider::for_url(&path_string).await.unwrap();

        write_preview_file(
            &storage,
            "job_1/preview_1/0/00000000000000000000.parquet",
            0,
            &[(1, 1_000_000), (2, 2_000_000)],
        )
        .await;
        write_preview_file(
            &storage,
            "job_1/preview_1/0/00000000000000000002.parquet",
            2,
            &[(3, 3_000_000)],
        )
        .await;

        let reader = PreviewOutputReader::new(&path_string, "job_1", Some("preview_1"))
            .await
            .unwrap();
        let output = reader.read(0).await.unwrap();
        assert_eq!(
            output
                .iter()
                .map(|chunk| chunk.start_id)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );

        let output = reader.read(2).await.unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].start_id, 2);
        assert_eq!(output[0].timestamps, vec![3_000]);
        assert_eq!(output[0].batch, json!([{"id": 3}]));
        assert!(reader.read(3).await.unwrap().is_empty());

        std::fs::remove_dir_all(path).unwrap();
    }
}
