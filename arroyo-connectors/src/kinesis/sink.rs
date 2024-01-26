use std::time::{Duration, SystemTime};

use anyhow::Result;
use arrow::array::RecordBatch;
use arroyo_formats::serialize::ArrowSerializer;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::ArrowOperator;
use arroyo_types::CheckpointBarrier;
use async_trait::async_trait;
use aws_config::from_env;
use aws_sdk_kinesis::{
    client::fluent_builders::PutRecords, model::PutRecordsRequestEntry, types::Blob,
    Client as KinesisClient, Region,
};
use tracing::warn;
use uuid::Uuid;

pub struct KinesisSinkFunc {
    pub client: Option<KinesisClient>,
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
        self.client = Some(KinesisClient::new(&loader.load().await));
    }

    async fn process_batch(&mut self, batch: RecordBatch, _ctx: &mut ArrowContext) {
        let mut batch_preparer = match self.in_progress_batch.take() {
            None => BatchRecordPreparer::new(
                self.client
                    .as_ref()
                    .unwrap()
                    .put_records()
                    .stream_name(self.name.clone()),
            ),
            Some(batch_preparer) => batch_preparer,
        };

        for v in self.serializer.serialize(&batch) {
            batch_preparer.add_record(Uuid::new_v4().to_string(), v);
        }

        if self.flush_config.should_flush(&batch_preparer) {
            self.flush_with_retries(batch_preparer)
                .await
                .expect("failed to flush batch during processing");
        } else {
            self.in_progress_batch = Some(batch_preparer);
        }
    }

    async fn handle_checkpoint(&mut self, _: CheckpointBarrier, _: &mut ArrowContext) {
        if let Some(batch_preparer) = self.in_progress_batch.take() {
            batch_preparer
                .flush()
                .await
                .expect("failed to flush batch during checkpoint");
        }
    }

    async fn handle_tick(&mut self, _: u64, _ctx: &mut ArrowContext) {
        let Some(batch_preparer) = &self.in_progress_batch else {
            return;
        };

        if !self.flush_config.should_flush(batch_preparer) {
            return;
        }
        let in_progress_batch = self.in_progress_batch.take().unwrap();

        self.flush_with_retries(in_progress_batch)
            .await
            .expect("failed to flush batch during tick");
    }
}

impl KinesisSinkFunc {
    async fn flush_with_retries(
        &mut self,
        mut record_batch_preparer: BatchRecordPreparer,
    ) -> Result<()> {
        let mut retries = 0;
        loop {
            let vectors_to_retry = record_batch_preparer.flush().await?;
            if vectors_to_retry.is_empty() {
                return Ok(());
            } else {
                retries += 1;
                warn!("failed to flush batch, retry attempt: {}", retries);
                tokio::time::sleep(std::time::Duration::from_millis(2000.min(100 << retries)))
                    .await;
                record_batch_preparer = self.take_or_create_batch_preparer().await;
                for (k, v) in vectors_to_retry {
                    record_batch_preparer.add_record(k, v);
                }
            }
        }
    }

    async fn take_or_create_batch_preparer(&mut self) -> BatchRecordPreparer {
        match self.in_progress_batch.take() {
            None => BatchRecordPreparer::new(
                self.client
                    .as_ref()
                    .unwrap()
                    .put_records()
                    .stream_name(self.name.clone()),
            ),
            Some(batch_preparer) => batch_preparer,
        }
    }
}

pub struct BatchRecordPreparer {
    // TODO: figure out how to not need an option
    put_records_call: Option<PutRecords>,
    buffered_records: Vec<(String, Vec<u8>)>,
    record_count: usize,
    data_size: usize,
    creation_time: SystemTime,
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
            max_data_size: max_buffer_size.unwrap_or(4_500_000) as usize,
            max_age: Duration::from_millis(flush_interval_millis.unwrap_or(1000) as u64),
        }
    }

    fn should_flush(&self, batch_preparer: &BatchRecordPreparer) -> bool {
        batch_preparer.record_count >= self.max_record_count
            || batch_preparer.data_size >= self.max_data_size
            || batch_preparer.creation_time.elapsed().unwrap_or_default() >= self.max_age
    }
}

impl BatchRecordPreparer {
    fn new(put_records_call: PutRecords) -> Self {
        Self {
            put_records_call: Some(put_records_call),
            buffered_records: Vec::new(),
            record_count: 0,
            data_size: 0,
            creation_time: SystemTime::now(),
        }
    }
    fn add_record(&mut self, key: String, value: Vec<u8>) {
        self.buffered_records.push((key.clone(), value.clone()));
        let blob = Blob::new(value);
        self.data_size += blob.as_ref().len();
        let put_record_request = PutRecordsRequestEntry::builder()
            .data(blob)
            .partition_key(key)
            .build();
        self.put_records_call = self
            .put_records_call
            .take()
            .map(|call| call.records(put_record_request));
        self.record_count += 1;
    }

    async fn flush(mut self) -> Result<Vec<(String, Vec<u8>)>> {
        if self.record_count == 0 {
            return Ok(Vec::new());
        }
        let response = self.put_records_call.take().unwrap().send().await?;
        let failed_record_count = response.failed_record_count().unwrap_or(0);
        if failed_record_count > 0 {
            warn!(
                "batch write had {} failed responses out of {}",
                failed_record_count, self.record_count
            );
            let records_to_retry = response
                .records()
                .unwrap()
                .iter()
                .enumerate()
                .filter_map(|(i, record)| {
                    if record.error_code().is_some() {
                        Some(self.buffered_records[i].clone())
                    } else {
                        None
                    }
                })
                .collect();
            Ok(records_to_retry)
        } else {
            Ok(Vec::new())
        }
    }
}
