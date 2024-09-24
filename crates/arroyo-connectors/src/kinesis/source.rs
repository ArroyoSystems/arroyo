use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fmt::Debug,
    hash::{Hash, Hasher},
    pin::Pin,
    time::SystemTime,
};

use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::{grpc::rpc::StopMode, ControlMessage};
use arroyo_state::global_table_config;
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::{from_nanos, UserError};
use async_trait::async_trait;
use aws_config::{from_env, Region};
use aws_sdk_kinesis::error::SdkError;
use aws_sdk_kinesis::operation::get_records::GetRecordsOutput;
use aws_sdk_kinesis::operation::get_shard_iterator::builders::GetShardIteratorFluentBuilder;
use aws_sdk_kinesis::types::{Shard, ShardIteratorType};
use aws_sdk_kinesis::Client as KinesisClient;
use bincode::{Decode, Encode};
use futures::{stream::FuturesUnordered, Future, StreamExt};
use tokio::{
    select,
    time::{Duration, MissedTickBehavior},
};
use tracing::{debug, info, warn};

use super::SourceOffset;

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub enum KinesisOffset {
    Earliest,
    Latest,
    SequenceNumber(String),
    Timestamp(SystemTime),
}

pub struct KinesisSourceFunc {
    pub stream_name: String,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub kinesis_client: Option<KinesisClient>,
    pub aws_region: Option<String>,
    pub shards: HashMap<String, ShardState>,
    pub offset: SourceOffset,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct ShardState {
    stream_name: String,
    shard_id: String,
    offset: KinesisOffset,
    closed: bool,
}

impl TryFrom<(String, Shard, SourceOffset)> for ShardState {
    type Error = anyhow::Error;
    fn try_from(
        (stream_name, shard, source_offset): (String, Shard, SourceOffset),
    ) -> Result<Self> {
        let shard_id = shard.shard_id().to_string();
        let offset = match source_offset {
            SourceOffset::Earliest => KinesisOffset::Earliest,
            SourceOffset::Latest => KinesisOffset::Latest,
        };
        Ok(Self {
            stream_name,
            shard_id,
            offset,
            closed: false,
        })
    }
}
type BoxedFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl ShardState {
    fn new(stream_name: String, shard: Shard, source_offset: SourceOffset) -> Self {
        Self {
            stream_name,
            shard_id: shard.shard_id().to_string(),
            offset: match source_offset {
                SourceOffset::Earliest => KinesisOffset::Earliest,
                SourceOffset::Latest => KinesisOffset::Latest,
            },
            closed: false,
        }
    }
    fn get_update_shard_iterator_future(
        &self,
        kinesis_client: &KinesisClient,
    ) -> BoxedFuture<AsyncNamedResult<AsyncResult>> {
        let shard_iterator_call: GetShardIteratorFluentBuilder = kinesis_client
            .get_shard_iterator()
            .stream_name(&self.stream_name)
            .set_shard_id(Some(self.shard_id.clone()));

        let shard_iterator_call = match &self.offset {
            KinesisOffset::Earliest => {
                shard_iterator_call.shard_iterator_type(ShardIteratorType::TrimHorizon)
            }
            KinesisOffset::Latest => {
                shard_iterator_call.shard_iterator_type(ShardIteratorType::Latest)
            }
            KinesisOffset::SequenceNumber(sequence_number) => shard_iterator_call
                .shard_iterator_type(ShardIteratorType::AtSequenceNumber)
                .starting_sequence_number(sequence_number.clone()),
            KinesisOffset::Timestamp(timestamp) => shard_iterator_call
                .shard_iterator_type(ShardIteratorType::AtTimestamp)
                .timestamp((*timestamp).into()),
        };
        let shard_id = self.shard_id.clone();
        Box::pin(AsyncNamedResult::wrap_future(shard_id, async move {
            Ok(AsyncResult::ShardIteratorIdUpdate(
                shard_iterator_call
                    .send()
                    .await
                    .context("failed to get shard iterator")?
                    .shard_iterator()
                    .map(|s| s.to_string()),
            ))
        }))
    }
}

struct AsyncNamedResult<T: Debug> {
    name: String,
    result: Result<T>,
}

impl<T: Debug> AsyncNamedResult<T> {
    async fn wrap_future(
        name: String,
        future: impl Future<Output = Result<T>> + Send + 'static,
    ) -> Self {
        Self {
            name,
            result: future.await,
        }
    }
}

#[derive(Debug)]
enum AsyncResult {
    // returns the new shard iterator id. Should always initialize a read after receiving this, if it is not None.
    ShardIteratorIdUpdate(Option<String>),
    GetRecords(GetRecordsOutput),
    NeedNewIterator,
}

#[async_trait]
impl SourceOperator for KinesisSourceFunc {
    fn name(&self) -> String {
        format!("kinesis-{}", self.stream_name)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("k", "kinesis source state")
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        ctx.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
        );
    }

    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType {
        match self.run_int(ctx).await {
            Ok(r) => r,
            Err(UserError { name, details, .. }) => {
                ctx.report_error(name.clone(), details.clone()).await;
                panic!("{}: {}", name, details);
            }
        }
    }
}

impl KinesisSourceFunc {
    /// Initializes the shards for the operator. First shards are read out of state,
    /// then `sync_shards()` is called to find any new shards.
    /// It returns a future for each shard to fetch the next shard iterator id.
    async fn init_shards(
        &mut self,
        ctx: &mut ArrowContext,
    ) -> anyhow::Result<Vec<BoxedFuture<AsyncNamedResult<AsyncResult>>>> {
        let mut futures = Vec::new();
        let s: &mut GlobalKeyedView<String, ShardState> = ctx
            .table_manager
            .get_global_keyed_state("k")
            .await
            .expect("failed to get state for kinesis source");
        for (shard_id, shard_state) in s
            .get_all()
            .values()
            .map(|shard_state| (shard_state.shard_id.clone(), shard_state.clone()))
            .filter(|(shard_id, _shard_state)| {
                let mut hasher = DefaultHasher::new();
                shard_id.hash(&mut hasher);
                let shard_hash = hasher.finish() as usize;
                shard_hash % ctx.task_info.parallelism == ctx.task_info.task_index
            })
        {
            futures.push(
                shard_state.get_update_shard_iterator_future(self.kinesis_client.as_ref().unwrap()),
            );
            self.shards.insert(shard_id, shard_state);
        }
        let new_futures = self.sync_shards(ctx).await?;
        futures.extend(new_futures.into_iter());

        Ok(futures)
    }

    async fn handle_async_result_split(
        &mut self,
        shard_id: String,
        async_result: AsyncResult,
        ctx: &mut ArrowContext,
    ) -> Result<Option<BoxedFuture<AsyncNamedResult<AsyncResult>>>, UserError> {
        match async_result {
            AsyncResult::ShardIteratorIdUpdate(new_shard_iterator) => {
                self.handle_shard_iterator_id_update(shard_id, new_shard_iterator)
                    .await
            }
            AsyncResult::GetRecords(get_records) => {
                self.handle_get_records(shard_id, get_records, ctx).await
            }
            AsyncResult::NeedNewIterator => self.handle_need_new_iterator(shard_id).await,
        }
    }

    async fn handle_shard_iterator_id_update(
        &mut self,
        shard_id: String,
        shard_iterator_id: Option<String>,
    ) -> Result<Option<BoxedFuture<AsyncNamedResult<AsyncResult>>>, UserError> {
        let shard_state = self.shards.get_mut(&shard_id).unwrap();
        match shard_iterator_id {
            Some(shard_iterator) => Ok(Some(self.next_read_future(shard_id, shard_iterator))),
            None => {
                shard_state.closed = true;
                Ok(None)
            }
        }
    }

    fn next_read_future(
        &mut self,
        shard_id: String,
        shard_iterator_id: String,
    ) -> BoxedFuture<AsyncNamedResult<AsyncResult>> {
        Box::pin(AsyncNamedResult::wrap_future(
            shard_id,
            Self::read_data_from_shard_iterator(
                self.kinesis_client.as_ref().unwrap().clone(),
                shard_iterator_id,
            ),
        ))
    }

    async fn read_data_from_shard_iterator(
        kinesis_client: KinesisClient,
        shard_iterator: String,
    ) -> Result<AsyncResult> {
        let mut retries = 0;
        loop {
            let get_records_call = kinesis_client.get_records().shard_iterator(&shard_iterator);
            match get_records_call.send().await {
                Ok(result) => return Ok(AsyncResult::GetRecords(result)),
                Err(error) => match &error {
                    SdkError::ServiceError(e) => {
                        if e.err().is_expired_iterator_exception() {
                            warn!("Expired iterator exception, requesting new iterator");
                            return Ok(AsyncResult::NeedNewIterator);
                        }
                        if e.err().is_kms_throttling_exception()
                            || e.err().is_provisioned_throughput_exceeded_exception()
                        {
                            // TODO: make retry behavior configurable
                            if retries == 5 {
                                bail!("failed after {} retries", retries);
                            }
                            retries += 1;
                            tokio::time::sleep(Duration::from_millis(200 * (1 << retries))).await;
                        } else {
                            return Err(anyhow!(error));
                        }
                    }
                    _ => return Err(anyhow!(error)),
                },
            }
        }
    }

    async fn handle_get_records(
        &mut self,
        shard_id: String,
        get_records: GetRecordsOutput,
        ctx: &mut ArrowContext,
    ) -> Result<Option<BoxedFuture<AsyncNamedResult<AsyncResult>>>, UserError> {
        let last_sequence_number = get_records
            .records()
            .last()
            .map(|record| record.sequence_number().to_owned());

        let next_shard_iterator = self.process_records(get_records, ctx).await?;
        let shard_state = self.shards.get_mut(&shard_id).unwrap();

        if let Some(last_sequence_number) = last_sequence_number {
            shard_state.offset = KinesisOffset::SequenceNumber(last_sequence_number.to_string());
        }

        match next_shard_iterator {
            Some(shard_iterator_id) => Ok(Some(self.next_read_future(shard_id, shard_iterator_id))),
            None => {
                shard_state.closed = true;
                Ok(None)
            }
        }
    }
    async fn handle_need_new_iterator(
        &mut self,
        shard_id: String,
    ) -> Result<Option<BoxedFuture<AsyncNamedResult<AsyncResult>>>, UserError> {
        let shard_state = self.shards.get_mut(&shard_id).unwrap();
        Ok(Some(shard_state.get_update_shard_iterator_future(
            self.kinesis_client.as_ref().unwrap(),
        )))
    }

    async fn init_client(&mut self) {
        let mut loader = from_env();
        if let Some(region) = &self.aws_region {
            loader = loader.region(Region::new(region.clone()));
        }
        self.kinesis_client = Some(KinesisClient::new(&loader.load().await));
    }

    /// Runs the Kinesis source, handling incoming records and control messages.
    ///
    /// This method initializes the Kinesis client, initializes the shards, and enters a loop to handle incoming
    /// records and control messages. There are three prongs to the tokio select loop:
    /// * A `FuturesUnordered` tha contains futures for reading off of shards.
    /// * An interval that periodically polls for new shards, initializing their futures.
    /// * Polling off of the control queue, to perform checkpointing and stop the operator.
    async fn run_int(&mut self, ctx: &mut ArrowContext) -> Result<SourceFinishType, UserError> {
        self.init_client().await;
        let starting_futures = self
            .init_shards(ctx)
            .await
            .map_err(|e| UserError::new("failed to initialize shards.", e.to_string()))?;
        let mut futures = FuturesUnordered::new();
        futures.extend(starting_futures.into_iter());

        let mut shard_poll_interval = tokio::time::interval(Duration::from_secs(1));
        shard_poll_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            select! {
                result = futures.select_next_some() => {
                    let shard_id = result.name;
                    if let Some(future) = self.handle_async_result_split(shard_id,
                        result.result.map_err(|e| UserError::new("Fatal Kinesis error", e.to_string()))?, ctx).await? {
                            futures.push(future);
                        }
                },
                _ = shard_poll_interval.tick() => {
                    if ctx.should_flush() {
                        ctx.flush_buffer().await?;
                    }
                    match self.sync_shards(ctx).await {
                        Err(err) => {
                            warn!("failed to sync shards: {}", err);
                            ctx.report_error("failed to sync shards".to_string(), err.to_string()).await;
                        },
                        Ok(new_futures) => {
                            futures.extend(new_futures.into_iter());
                        }
                     }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("starting checkpointing {}", ctx.task_info.task_index);
                            let s = ctx.table_manager.get_global_keyed_state("k").await.unwrap();
                            for (shard_id, shard_state) in &self.shards {
                                s.insert(shard_id.clone(), shard_state.clone()).await;
                            }
                            if self.start_checkpoint(c, ctx).await {
                                return Ok(SourceFinishType::Immediate);
                            }
                        },
                        Some(ControlMessage::Stop { mode }) => {
                            info!("Stopping kinesis source: {:?}", mode);

                            match mode {
                                StopMode::Graceful => {
                                    return Ok(SourceFinishType::Graceful);
                                }
                                StopMode::Immediate => {
                                    return Ok(SourceFinishType::Immediate);
                                }
                            }
                        }
                        Some(ControlMessage::Commit { .. }) => {
                            unreachable!("sources shouldn't receive commit messages");
                        }
                        Some(ControlMessage::LoadCompacted { compacted }) => {
                            ctx.load_compacted(compacted).await;
                        },
                        Some(ControlMessage::NoOp ) => {}
                        None => {
                        }
                    }
                }
            }
        }
    }

    async fn process_records(
        &mut self,
        get_records_output: GetRecordsOutput,
        ctx: &mut ArrowContext,
    ) -> Result<Option<String>, UserError> {
        let records = get_records_output.records;
        for record in records {
            let data = record.data.into_inner();
            let timestamp = record.approximate_arrival_timestamp.unwrap();

            ctx.deserialize_slice(&data, from_nanos(timestamp.as_nanos() as u128))
                .await?;

            if ctx.should_flush() {
                ctx.flush_buffer().await?
            }
        }
        Ok(get_records_output.next_shard_iterator)
    }

    async fn sync_shards(
        &mut self,
        ctx: &mut ArrowContext,
    ) -> Result<Vec<BoxedFuture<AsyncNamedResult<AsyncResult>>>> {
        let mut futures = Vec::new();
        for shard in self.get_splits().await? {
            // check hash
            let shard_id = shard.shard_id().to_string();
            let mut hasher = DefaultHasher::new();
            shard_id.hash(&mut hasher);
            let shard_hash = hasher.finish() as usize;

            if self.shards.contains_key(&shard_id)
                || shard_hash % ctx.task_info.parallelism != ctx.task_info.task_index
            {
                continue;
            }
            let shard_state = ShardState::new(self.stream_name.clone(), shard, self.offset);

            futures.push(
                shard_state.get_update_shard_iterator_future(self.kinesis_client.as_ref().unwrap()),
            );
            self.shards.insert(shard_id, shard_state);
        }
        Ok(futures)
    }

    async fn get_splits(&mut self) -> Result<Vec<Shard>> {
        let mut shard_collect: Vec<Shard> = Vec::new();

        let mut list_shard_output = self
            .kinesis_client
            .as_ref()
            .unwrap()
            .list_shards()
            .stream_name(&self.stream_name)
            .send()
            .await?;
        shard_collect.extend(list_shard_output.shards().iter().cloned());

        while let Some(next_token) = list_shard_output.next_token() {
            list_shard_output = self
                .kinesis_client
                .as_ref()
                .unwrap()
                .list_shards()
                .set_next_token(Some(next_token.to_string()))
                .send()
                .await?;
            shard_collect.extend(list_shard_output.shards().iter().cloned());
        }
        Ok(shard_collect)
    }
}
