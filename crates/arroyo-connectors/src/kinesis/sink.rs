use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use arrow::array::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::retry;
use arroyo_types::CheckpointBarrier;
use async_trait::async_trait;
use aws_config::{from_env, Region};
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::PutRecordsRequestEntry;
use aws_sdk_kinesis::Client as KinesisClient;
use tracing::warn;
use uuid::Uuid;

pub struct KinesisSinkFunc {
    pub client: Option<Arc<KinesisClient>>,
    pub aws_region: Option<String>,
    pub in_progress_batch: Option<BatchRecordPreparer>,
    pub flush_config: FlushConfig,
    pub serializer: ArrowSerializer,
    pub name: String,
}

#[async_trait]
impl ArrowOperator for KinesisSinkFunc {
    fn name(&self) -> String {
        format!("kinesis-producer-{}", self.name)
    }

    async fn on_start(&mut self, _ctx: &mut ArrowContext) {
        let mut loader = from_env();
        if let Some(region) = &self.aws_region {
            loader = loader.region(Region::new(region.clone()));
        }

        let client = Arc::new(KinesisClient::new(&loader.load().await));
        self.client = Some(client.clone());

        self.in_progress_batch = Some(BatchRecordPreparer::new(client, self.name.clone()));
    }

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext) {
        for v in self.serializer.serialize(&batch) {
            self.in_progress_batch
                .as_mut()
                .unwrap()
                .add_record(Uuid::new_v4().to_string(), v);
            self.maybe_flush_with_retries(ctx)
                .await
                .expect("failed to flush batch during processing");
        }
    }

    async fn handle_checkpoint(&mut self, _: CheckpointBarrier, _: &mut ArrowContext) {
        retry!(
            self.in_progress_batch.as_mut().unwrap().flush().await,
            30,
            Duration::from_millis(100),
            Duration::from_secs(10),
            |e| warn!("{}", e)
        )
        .expect("could not flush to Kinesis during checkpointing");
    }

    async fn handle_tick(&mut self, _: u64, ctx: &mut ArrowContext) {
        self.maybe_flush_with_retries(ctx)
            .await
            .expect("failed to flush batch during tick");
    }
}

impl KinesisSinkFunc {
    async fn maybe_flush_with_retries(&mut self, ctx: &mut ArrowContext) -> Result<()> {
        if !self
            .flush_config
            .should_flush(self.in_progress_batch.as_ref().unwrap())
        {
            return Ok(());
        }

        retry!(
            self.in_progress_batch.as_mut().unwrap().flush().await,
            20,
            Duration::from_millis(100),
            Duration::from_secs(2),
            |e| ctx
                .report_error("failed to write to Kinesis", format!("{:?}", e))
                .await
        )?;

        Ok(())
    }
}

pub struct BatchRecordPreparer {
    client: Arc<KinesisClient>,
    stream: String,
    buffered_records: Vec<(String, Vec<u8>)>,
    data_size: usize,
    last_flush_time: Instant,
}

pub struct FlushConfig {
    max_record_count: usize,
    max_data_size: usize,
    max_age: Duration,
}

impl FlushConfig {
    pub fn new(
        flush_interval_millis: Option<i64>,
        max_buffer_size: Option<i64>,
        records_per_batch: Option<i64>,
    ) -> Self {
        Self {
            max_record_count: records_per_batch.unwrap_or(500) as usize,
            max_data_size: max_buffer_size.unwrap_or(4_000_000) as usize,
            max_age: Duration::from_millis(flush_interval_millis.unwrap_or(1000) as u64),
        }
    }

    fn should_flush(&self, batch_preparer: &BatchRecordPreparer) -> bool {
        batch_preparer.buffered_records.len() >= self.max_record_count
            || batch_preparer.data_size >= self.max_data_size
            || batch_preparer.last_flush_time.elapsed() >= self.max_age
    }
}

impl BatchRecordPreparer {
    fn new(client: Arc<KinesisClient>, name: String) -> Self {
        Self {
            client,
            stream: name,
            buffered_records: Vec::new(),
            data_size: 0,
            last_flush_time: Instant::now(),
        }
    }
    fn add_record(&mut self, key: String, value: Vec<u8>) {
        self.data_size += value.len() + key.len();
        self.buffered_records.push((key, value));
    }

    async fn flush(&mut self) -> Result<()> {
        if self.buffered_records.is_empty() {
            return Ok(());
        }

        let response = self
            .client
            .put_records()
            .stream_name(self.stream.clone())
            .set_records(Some(
                self.buffered_records
                    .iter()
                    .map(|(k, v)| {
                        PutRecordsRequestEntry::builder()
                            .partition_key(k)
                            .data(Blob::new(v.clone()))
                            .build()
                            // cannot occur when partition key and data are supplied
                            .unwrap()
                    })
                    .collect(),
            ))
            .send()
            .await?;

        self.last_flush_time = Instant::now();

        let failed_record_count = response.failed_record_count().unwrap_or(0);
        if failed_record_count == 0 {
            self.buffered_records.clear();
            self.data_size = 0;
            return Ok(());
        }

        let records_to_retry: HashSet<_> = response
            .records()
            .iter()
            .enumerate()
            .filter_map(|(i, record)| {
                if record.error_code().is_some() {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        self.buffered_records = self
            .buffered_records
            .drain(..)
            .enumerate()
            .filter(|(i, _)| records_to_retry.contains(i))
            .map(|(_, v)| v)
            .collect();

        self.data_size = self
            .buffered_records
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum();

        bail!(
            "batch write had {} failed responses out of {}",
            failed_record_count,
            self.buffered_records.len()
        )
    }
}
