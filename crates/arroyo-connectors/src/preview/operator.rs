use super::{PreviewFilePath, PreviewMetadata, PreviewTable};
use arrow::array::RecordBatch;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::connector_err;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_state::global_table_config;
use arroyo_storage::StorageProvider;
use arroyo_types::{CheckpointBarrier, SignalMessage, TaskInfo};
use itertools::Itertools;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct PreviewSink {
    config: PreviewTable,
    storage: Option<StorageProvider>,
    row: usize,
    pending_start_id: Option<u64>,
    pending_batches: Vec<RecordBatch>,
    pending_bytes: usize,
    last_flush: Instant,
}

impl PreviewSink {
    pub fn new(config: PreviewTable) -> Self {
        Self {
            config,
            storage: None,
            row: 0,
            pending_start_id: None,
            pending_batches: vec![],
            pending_bytes: 0,
            last_flush: Instant::now(),
        }
    }

    async fn flush(
        &mut self,
        input: &ArroyoSchema,
        task_info: Arc<TaskInfo>,
    ) -> DataflowResult<()> {
        if self.pending_batches.is_empty() {
            return Ok(());
        }

        let Some(start_id) = self.pending_start_id.take() else {
            return Ok(());
        };

        let batches = self.pending_batches.drain(..).collect_vec();
        self.pending_bytes = 0;

        let path = PreviewFilePath::new(
            &task_info.job_id,
            &task_info.operator_id,
            task_info.task_index,
            start_id,
        );

        let mut file = self
            .storage
            .as_mut()
            .expect("storage must have been initialized")
            .buf_writer(path.to_string());

        let metadata = PreviewMetadata {
            start_row: start_id,
            operator_id: task_info.operator_id.clone(),
            subtask_idx: task_info.task_index as u64,
            timestamp_idx: input.timestamp_index as u64,
        };

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                ZstdLevel::try_new(3).unwrap(),
            ))
            .set_key_value_metadata(Some(metadata.into()))
            .build();

        let mut writer = AsyncArrowWriter::try_new(&mut file, batches.first().unwrap().schema().clone(), Some(props))
            .map_err(|e| connector_err!(Internal, NoRetry, source: e.into(), "failed to construct parquet writer"))?;

        for batch in batches {
            writer.write(&batch).await
                .map_err(|e| connector_err!(Internal, NoRetry, source: e.into(), "failed to write preview output"))?;
        }

        writer.close().await
            .map_err(|e| connector_err!(Internal, NoRetry, source: e.into(), "failed to close parquet writer"))?;

        self.last_flush = Instant::now();
        Ok(())
    }
}

impl Default for PreviewSink {
    fn default() -> Self {
        Self::new(PreviewTable::default())
    }
}

#[async_trait::async_trait]
impl ArrowOperator for PreviewSink {
    fn name(&self) -> String {
        "Preview".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config(
            "s",
            "Number of rows of output produced by this preview sink",
        )
    }

    fn tick_interval(&self) -> Option<Duration> {
        self.config.flush_interval_millis.map(Duration::from_millis)
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        let table = ctx.table_manager.get_global_keyed_state("s").await?;
        self.row = *table.get(&ctx.task_info.task_index).unwrap_or(&0);
        self.storage = Some(
            StorageProvider::for_url(&self.config.path)
                .await
                .map_err(|e| {
                    connector_err!(
                        Internal,
                        NoRetry,
                        source: e.into(),
                        "invalid path for preview: {}",
                        self.config.path
                    )
                })?,
        );

        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let batch_bytes = batch.get_array_memory_size();
        if !self.pending_batches.is_empty()
            && self.pending_bytes.saturating_add(batch_bytes) > self.config.max_buffer_bytes
        {
            self.flush(&ctx.in_schemas[0], ctx.task_info.clone())
                .await?;
        }

        self.pending_start_id.get_or_insert(self.row as u64);
        self.row += batch.num_rows();
        self.pending_bytes = self.pending_bytes.saturating_add(batch_bytes);
        self.pending_batches.push(batch);

        if self.pending_bytes >= self.config.max_buffer_bytes {
            self.flush(&ctx.in_schemas[0], ctx.task_info.clone())
                .await?;
        }

        Ok(())
    }

    async fn handle_tick(
        &mut self,
        _: u64,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if self
            .config
            .flush_interval_millis
            .is_some_and(|millis| self.last_flush.elapsed() >= Duration::from_millis(millis))
        {
            self.flush(&ctx.in_schemas[0], ctx.task_info.clone())
                .await?;
            self.last_flush = Instant::now();
        }
        Ok(())
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.flush(&ctx.in_schemas[0], ctx.task_info.clone())
            .await?;

        let table = ctx
            .table_manager
            .get_global_keyed_state::<u32, usize>("s")
            .await
            .unwrap();
        table.insert(ctx.task_info.task_index, self.row).await;
        Ok(())
    }

    async fn on_close(
        &mut self,
        _: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.flush(&ctx.in_schemas[0], ctx.task_info.clone()).await
    }
}
