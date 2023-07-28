use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fmt::Debug,
    hash::{Hash, Hasher},
    marker::PhantomData,
    pin::Pin,
    time::SystemTime,
};

use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use arroyo_macro::{source_fn, StreamNode};
use arroyo_rpc::{
    grpc::{StopMode, TableDescriptor},
    ControlMessage, ControlResp,
};
use arroyo_state::tables::GlobalKeyedState;
use arroyo_types::{from_nanos, Data, Record};
use aws_config::load_from_env;
use aws_sdk_kinesis::{
    client::fluent_builders::GetShardIterator,
    model::{Shard, ShardIteratorType},
    output::GetRecordsOutput,
    types::SdkError,
    Client as KinesisClient,
};
use bincode::{Decode, Encode};
use futures::stream::StreamExt;
use futures::{stream::FuturesUnordered, Future};
use serde::de::DeserializeOwned;
use tokio::{
    select,
    time::{sleep, Duration},
};
use tracing::{debug, info, warn};

use crate::{
    connectors::{OperatorConfig, OperatorConfigSerializationMode},
    engine::Context,
    operators::{SerializationMode, UserError},
    SourceFinishType,
};

use super::{KinesisTable, SourceOffset, TableType};

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub enum KinesisOffset {
    Earliest,
    Latest,
    SequenceNumber(String),
    Timestamp(SystemTime),
}
#[derive(StreamNode)]
pub struct KinesisSourceFunc<K: Data, T: Data + DeserializeOwned> {
    stream_name: String,
    serialization_mode: SerializationMode,
    read_mode: SourceOffset,
    _phantom: PhantomData<(K, T)>,
}

struct ShardReader {
    shards: HashMap<String, ShardState>,
    kinesis_client: KinesisClient,
}

impl ShardReader {
    fn update_shard(&mut self, shard_state: ShardState) {
        let current_state = self.shards.get_mut(&shard_state.shard_id);
        match current_state {
            Some(_current_state) => {}
            None => {
                self.shards
                    .insert(shard_state.shard_id.clone(), shard_state);
            }
        }
    }
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
struct ShardState {
    stream_name: String,
    shard_id: String,
    offset: KinesisOffset,
    next_shard_iterator_id: Option<String>,
    closed: bool,
}

impl TryFrom<(String, Shard, SourceOffset)> for ShardState {
    type Error = anyhow::Error;
    fn try_from(
        (stream_name, shard, source_offset): (String, Shard, SourceOffset),
    ) -> Result<Self> {
        let shard_id = shard
            .shard_id()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("missing_shard_id"))?;
        let offset = match source_offset {
            SourceOffset::Earliest => KinesisOffset::Earliest,
            SourceOffset::Latest => KinesisOffset::Latest,
        };
        Ok(Self {
            stream_name,
            shard_id,
            offset,
            next_shard_iterator_id: None,
            closed: false,
        })
    }
}
type BoxedFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl ShardState {
    fn new_shard_iterator_call(&self, kinesis_client: &KinesisClient) -> GetShardIterator {
        let shard_iterator_call = kinesis_client
            .get_shard_iterator()
            .stream_name(&self.stream_name)
            .set_shard_id(Some(self.shard_id.clone()));
        match &self.offset {
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
        }
    }

    fn update_shard_iterator(
        &self,
        get_shard_iterator_call: GetShardIterator,
    ) -> BoxedFuture<AsyncNamedResult<AsyncResult>> {
        let shard_id = self.shard_id.clone();
        Box::pin(AsyncNamedResult::wrap_future(shard_id, async move {
            Ok(AsyncResult::ShardIteratorIdUpdate(
                get_shard_iterator_call
                    .send()
                    .await
                    .context("failed to get shard iterator")?
                    .shard_iterator()
                    .map(|s| s.to_string()),
            ))
        }))
    }

    fn next_read_future(
        &mut self,
        kinesis_client: &KinesisClient,
    ) -> BoxedFuture<AsyncNamedResult<AsyncResult>> {
        let shard = self.shard_id.clone();
        let client = kinesis_client.clone();
        Box::pin(AsyncNamedResult::wrap_future(
            shard,
            ShardState::read_data_from_shard_iterator(
                client,
                self.next_shard_iterator_id.clone().unwrap().clone(),
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
                    SdkError::ServiceError { err, raw: _ } => {
                        if err.is_expired_iterator_exception() {
                            warn!("Expired iterator exception, requesting new iterator");
                            return Ok(AsyncResult::NeedNewIterator);
                        }
                        if err.is_kms_throttling_exception()
                            || err.is_provisioned_throughput_exceeded_exception()
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

impl ShardReader {
    async fn new(shards: HashMap<String, ShardState>) -> Self {
        let config = load_from_env().await;
        let client = KinesisClient::new(&config);
        Self {
            shards,
            kinesis_client: client,
        }
    }
}

#[source_fn(out_k = (), out_t = T)]
impl<K: Data, T: Data + DeserializeOwned> KinesisSourceFunc<K, T> {
    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for KafkaSink");
        let table: KinesisTable =
            serde_json::from_value(config.table).expect("Invalid table config for KafkaSource");
        let TableType::Source{offset: read_mode} = table.type_ else {
            panic!("found non-sink kafka config in sink operator");
        };

        Self {
            stream_name: table.stream_name,
            serialization_mode: match config.serialization_mode.unwrap() {
                OperatorConfigSerializationMode::Json => SerializationMode::Json,
                OperatorConfigSerializationMode::JsonSchemaRegistry => {
                    SerializationMode::JsonSchemaRegistry
                }
                OperatorConfigSerializationMode::RawJson => SerializationMode::RawJson,
                OperatorConfigSerializationMode::DebeziumJson => SerializationMode::Json,
                OperatorConfigSerializationMode::Parquet => {
                    unimplemented!("parquet out of kafka source doesn't make sense")
                }
            },
            read_mode,
            _phantom: PhantomData,
        }
    }

    fn name(&self) -> String {
        format!("kinesis-{}", self.stream_name)
    }

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
        match self.run_int(ctx).await {
            Ok(r) => r,
            Err(e) => {
                ctx.control_tx
                    .send(ControlResp::Error {
                        operator_id: ctx.task_info.operator_id.clone(),
                        task_index: ctx.task_info.task_index,
                        message: e.name.clone(),
                        details: e.details.clone(),
                    })
                    .await
                    .unwrap();

                panic!("{}: {}", e.name, e.details);
            }
        }
    }

    async fn get_reader(&mut self, ctx: &mut Context<(), T>) -> anyhow::Result<ShardReader> {
        let mut s: GlobalKeyedState<String, ShardState, _> =
            ctx.state.get_global_keyed_state('k').await;
        let state: HashMap<String, ShardState> = s
            .get_all()
            .into_iter()
            .map(|shard_state| (shard_state.shard_id.clone(), shard_state.clone()))
            .filter(|(shard_id, _shard_state)| {
                let mut hasher = DefaultHasher::new();
                shard_id.hash(&mut hasher);
                let shard_hash = hasher.finish() as usize;
                shard_hash % ctx.task_info.parallelism == ctx.task_info.task_index
            })
            .collect();
        let mut shard_reader = ShardReader::new(state).await;
        self.sync_shards(ctx, &mut shard_reader).await?;
        // print a comma separated list of subscribed shards
        let subscribed_shards = shard_reader
            .shards
            .keys()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(",");
        info!(
            "subtask {} subscribed to shards: {}",
            ctx.task_info.task_index, subscribed_shards
        );

        Ok(shard_reader)
    }
    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("k", "kinesis source state")]
    }

    async fn handle_async_result(
        &mut self,
        shard_state: &mut ShardState,
        kinesis_client: &KinesisClient,
        async_result: AsyncResult,
        ctx: &mut Context<(), T>,
    ) -> Result<Option<BoxedFuture<AsyncNamedResult<AsyncResult>>>, UserError> {
        match async_result {
            // TODO: factor out the new shard iterator code
            AsyncResult::ShardIteratorIdUpdate(new_shard_iterator) => match new_shard_iterator {
                Some(shard_iterator) => {
                    shard_state.next_shard_iterator_id = Some(shard_iterator);
                    Ok(Some(shard_state.next_read_future(kinesis_client)))
                }
                None => {
                    shard_state.closed = true;
                    Ok(None)
                }
            },
            AsyncResult::GetRecords(get_records) => {
                // TODO: Wait a little bit if a shard doesn't have any new records coming through.
                // Right now we just peg the server with read requests until we get throughput exceeded exceptions.
                if let Some(last_sequence_number) = get_records
                    .records()
                    .and_then(|records| records.last().map(|record| record.sequence_number()))
                    .flatten()
                {
                    shard_state.offset =
                        KinesisOffset::SequenceNumber(last_sequence_number.to_string());
                }
                let next_shard_iterator = self.process_records(get_records, ctx).await?;
                match next_shard_iterator {
                    Some(shard_iterator) => {
                        shard_state.next_shard_iterator_id = Some(shard_iterator);
                        Ok(Some(shard_state.next_read_future(kinesis_client)))
                    }
                    None => Ok(None),
                }
            }
            AsyncResult::NeedNewIterator => {
                let get_shard_iterator_call = shard_state.new_shard_iterator_call(&kinesis_client);
                Ok(Some(
                    shard_state.update_shard_iterator(get_shard_iterator_call),
                ))
            }
        }
    }

    async fn run_int(&mut self, ctx: &mut Context<(), T>) -> Result<SourceFinishType, UserError> {
        let mut reader = self
            .get_reader(ctx)
            .await
            .map_err(|e| UserError::new("Could not create Kinesis reader", format!("{:?}", e)))?;
        let mut futures = FuturesUnordered::new();
        for shard in reader.shards.values_mut() {
            if shard.closed {
                continue;
            }
            let get_shard_iterator_call = shard.new_shard_iterator_call(&reader.kinesis_client);
            futures.push(shard.update_shard_iterator(get_shard_iterator_call));
        }

        loop {
            select! {
                result = futures.select_next_some() => {
                    let shard_id = result.name;
                    let shard_state = reader.shards.get_mut(&shard_id).unwrap();
                    match self.handle_async_result(shard_state,
                        &reader.kinesis_client,
                        result.result.map_err(|e| UserError::new("Fatal Kinesis error", e.to_string()))?, ctx).await? {
                        Some(future) => {
                            futures.push(future);
                        },
                        None => {}
                    }
                },
                _ = sleep(Duration::from_secs(60)) => {
                    if self.sync_shards(ctx, &mut reader).await.is_err() {
                        warn!("failed to sync shards");
                    }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("starting checkpointing {}", ctx.task_info.task_index);
                            let mut s = ctx.state.get_global_keyed_state('k').await;
                            for (shard_id, shard_state) in &reader.shards {
                                s.insert(shard_id.clone(), shard_state.clone()).await;
                            }
                            if self.checkpoint(c, ctx).await {
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
                        Some(ControlMessage::Commit { epoch: _ }) => {
                            unreachable!("sources shouldn't receive commit messages");
                        }
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
        ctx: &mut Context<(), T>,
    ) -> Result<Option<String>, UserError> {
        let records = get_records_output.records.unwrap_or_default();
        for record in records {
            let data = record.data.unwrap().into_inner();
            let value = self.serialization_mode.deserialize_slice(&data)?;
            let timestamp = record.approximate_arrival_timestamp.unwrap();
            let output_record = Record {
                timestamp: from_nanos(timestamp.as_nanos() as u128),
                key: Some(()),
                value,
            };
            ctx.collect(output_record).await;
        }
        Ok(get_records_output.next_shard_iterator)
    }

    async fn sync_shards(
        &mut self,
        ctx: &mut Context<(), T>,
        shard_reader: &mut ShardReader,
    ) -> Result<()> {
        for shard in self.get_splits(ctx, &shard_reader.kinesis_client).await? {
            let shard_state: ShardState =
                (self.stream_name.clone(), shard, self.read_mode).try_into()?;
            // check hash
            let mut hasher = DefaultHasher::new();
            shard_state.shard_id.hash(&mut hasher);
            let shard_hash = hasher.finish() as usize;
            if shard_hash % ctx.task_info.parallelism != ctx.task_info.task_index {
                continue;
            }
            shard_reader.update_shard(shard_state)
        }
        Ok(())
    }

    async fn get_splits(
        &mut self,
        _ctx: &mut Context<(), T>,
        client: &KinesisClient,
    ) -> Result<Vec<Shard>> {
        let mut shard_collect: Vec<Shard> = Vec::new();

        let mut list_shard_output = client
            .list_shards()
            .stream_name(&self.stream_name)
            .send()
            .await?;
        match list_shard_output.shards() {
            Some(shards) => {
                shard_collect.extend(shards.iter().cloned());
            }
            None => {
                bail!("no shards for stream {}", self.stream_name);
            }
        }
        while let Some(next_token) = list_shard_output.next_token() {
            list_shard_output = client
                .list_shards()
                .set_next_token(Some(next_token.to_string()))
                .send()
                .await?;
            match list_shard_output.shards() {
                Some(shards) => {
                    shard_collect.extend(shards.iter().cloned());
                }
                None => {
                    bail!(
                        "received a list_shard_output with no new shards for stream {}",
                        self.stream_name
                    );
                }
            }
        }
        Ok(shard_collect)
    }
}
