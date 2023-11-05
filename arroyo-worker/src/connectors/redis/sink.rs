use crate::connectors::redis::{RedisConfig, RedisConfigConnection, RedisTable, TableType, Target};
use crate::engine::{Context, ErrorReporter, StreamNode};
use crate::formats::DataSerializer;
use crate::SchemaData;
use arroyo_macro::process_fn;
use arroyo_rpc::OperatorConfig;
use arroyo_types::{CheckpointBarrier, Key, Record};
use redis::aio::{ConnectionLike, ConnectionManager};
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{Client, Cmd, Pipeline, RedisFuture};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::info;

use super::ListOperation;

const FLUSH_TIMEOUT: Duration = Duration::from_millis(100);
const FLUSH_BYTES: usize = 10 * 1024 * 1024;

#[derive(StreamNode)]
pub struct RedisSinkFunc<K, T>
where
    K: Key,
    T: Serialize + SchemaData,
{
    serializer: DataSerializer<T>,
    table: RedisTable,
    client: Clients,
    cmd_q: Option<(Sender<u32>, Receiver<RedisCmd>)>,

    rx: Receiver<u32>,
    tx: Sender<RedisCmd>,
    _t: PhantomData<K>,
}

#[derive(Copy, Clone, Debug)]
enum RedisBehavior {
    Set { ttl: Option<usize> },
    Push { append: bool, max: Option<usize> },
    Hash,
}

enum RedisCmd {
    Data {
        key: String,
        value: Vec<u8>,
    },

    HData {
        key: String,
        field: String,
        value: Vec<u8>,
    },

    Flush(u32),
}

enum Clients {
    Standard(Client),
    Clustered(ClusterClient),
}

impl Clients {
    async fn get_connection(&self) -> Result<GeneralConnection, redis::RedisError> {
        Ok(match self {
            Clients::Standard(c) => {
                GeneralConnection::Standard(ConnectionManager::new(c.clone()).await?)
            }
            Clients::Clustered(c) => GeneralConnection::Clustered(c.get_async_connection().await?),
        })
    }
}

pub enum GeneralConnection {
    Standard(ConnectionManager),
    Clustered(ClusterConnection),
}

impl ConnectionLike for GeneralConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, redis::Value> {
        match self {
            GeneralConnection::Standard(c) => c.req_packed_command(cmd),
            GeneralConnection::Clustered(c) => c.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<redis::Value>> {
        match self {
            GeneralConnection::Standard(c) => c.req_packed_commands(cmd, offset, count),
            GeneralConnection::Clustered(c) => c.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            GeneralConnection::Standard(c) => c.get_db(),
            GeneralConnection::Clustered(c) => c.get_db(),
        }
    }
}

struct RedisWriter {
    rx: Receiver<RedisCmd>,
    tx: Sender<u32>,
    max_push_keys: HashSet<String>,
    behavior: RedisBehavior,
    connection: GeneralConnection,
    pipeline: Pipeline,
    size_estimate: usize,
    last_flushed: Instant,
    error_reporter: ErrorReporter,
}

impl RedisWriter {
    fn start(mut self) {
        tokio::spawn(async move {
            loop {
                let flush_duration = FLUSH_TIMEOUT.checked_sub(self.last_flushed.elapsed());

                if self.size_estimate > FLUSH_BYTES || flush_duration.is_none() {
                    self.flush().await;
                    continue;
                }

                let flush_timeout = tokio::time::sleep(flush_duration.unwrap());

                select! {
                    cmd = self.rx.recv() => {
                        match cmd {
                            None => {
                                info!("closing Redis writer");
                                return;
                            }
                            Some(RedisCmd::Data { key, value }) => {
                                self.size_estimate += key.len() + value.len();

                                match self.behavior {
                                    RedisBehavior::Set {ttl } => {
                                        // TODO: resolve duplicates before sending
                                        if let Some(ttl) = ttl {
                                            self.pipeline.set_ex(key, value,  ttl);
                                        } else {
                                            self.pipeline.set(key, value);
                                        }
                                    }
                                    RedisBehavior::Push { append, max } => {
                                        if max.is_some() {
                                            if !self.max_push_keys.contains(&key) {
                                                self.max_push_keys.insert(key.clone());
                                            }
                                        }

                                        if append {
                                            self.pipeline.rpush(key, value);
                                        } else {
                                            self.pipeline.lpush(key, value);
                                        }
                                    }
                                    RedisBehavior::Hash => {
                                        unreachable!();
                                    }
                                }
                            }
                            Some(RedisCmd::HData { key, field, value }) => {
                                self.size_estimate += key.len() + field.len() + value.len();

                                self.pipeline.hset(key, field, value);
                            }
                            Some(RedisCmd::Flush(i)) => {
                                self.flush().await;
                                if self.tx.send(i).await.is_err() {
                                    info!("receiver hung up, closing Redis writer");
                                    return;
                                }
                            }
                        }
                    }
                    _ = flush_timeout => {
                        self.flush().await;
                    }
                }
            }
        });
    }

    async fn flush(&mut self) {
        let mut attempts = 0;

        match self.behavior {
            RedisBehavior::Push {
                max: Some(max),
                append,
            } => {
                for k in self.max_push_keys.drain().into_iter() {
                    if append {
                        self.pipeline.ltrim(k, -(max as isize), -1);
                    } else {
                        self.pipeline.ltrim(k, 0, max as isize - 1);
                    }
                }
            }
            _ => {}
        }

        while attempts < 20 {
            match self
                .pipeline
                .query_async::<_, ()>(&mut self.connection)
                .await
            {
                Ok(_) => {
                    self.pipeline.clear();
                    self.size_estimate = 0;
                    self.last_flushed = Instant::now();
                    return;
                }
                Err(e) => {
                    self.error_reporter
                        .report_error(
                            "Redis error",
                            format!("Failed to write data to redis: {:?}", e),
                        )
                        .await;
                }
            }

            tokio::time::sleep(Duration::from_millis((50 * (1 << attempts)).min(5_000))).await;
            attempts -= 1;
        }

        panic!("Exhausted retries writing to Redis");
    }
}

#[process_fn(in_k = K, in_t = T)]
impl<K, T> RedisSinkFunc<K, T>
where
    K: Key,
    T: Serialize + SchemaData,
{
    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for RedisSink");
        let profile: RedisConfig = serde_json::from_value(config.connection)
            .expect("Invalid connection profile for RedisSink");
        let table: RedisTable =
            serde_json::from_value(config.table).expect("Invalid table config for Redis");

        let client = match profile.connection {
            RedisConfigConnection::Address(address) => {
                Clients::Standard(Client::open(address.0).expect("invalid address"))
            }
            RedisConfigConnection::Addresses(addresses) => Clients::Clustered(
                ClusterClient::new(addresses.into_iter().map(|e| e.0).collect())
                    .expect("failed to construct cluster client"),
            ),
        };

        let (tx, cmd_rx) = tokio::sync::mpsc::channel(128);
        let (cmd_tx, rx) = tokio::sync::mpsc::channel(128);

        Self {
            serializer: DataSerializer::new(config.format.expect("redis table must have a format")),
            table,
            client,
            cmd_q: Some((cmd_tx, cmd_rx)),
            tx,
            rx,
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        "RedisSink".to_string()
    }

    async fn on_start(&mut self, ctx: &mut Context<(), ()>) {
        let mut attempts = 0;
        while attempts < 20 {
            match self.client.get_connection().await {
                Ok(connection) => {
                    let (tx, rx) = self.cmd_q.take().expect("on_start called multiple times!");
                    RedisWriter {
                        connection,
                        error_reporter: ctx.error_reporter.clone(),
                        tx,
                        rx,
                        pipeline: redis::pipe(),
                        size_estimate: 0,
                        last_flushed: Instant::now(),
                        max_push_keys: HashSet::new(),
                        behavior: match self.table.connector_type {
                            TableType::Target(Target::StringTable { ttl_secs, .. }) => {
                                RedisBehavior::Set {
                                    ttl: ttl_secs.map(|t| t.get() as usize),
                                }
                            }
                            TableType::Target(Target::ListTable {
                                max_length,
                                operation,
                                ..
                            }) => {
                                let max = max_length.map(|x| x.get() as usize);
                                match operation {
                                    ListOperation::Append => {
                                        RedisBehavior::Push { append: true, max }
                                    }
                                    ListOperation::Prepend => {
                                        RedisBehavior::Push { append: false, max }
                                    }
                                }
                            }
                            TableType::Target(Target::HashTable { .. }) => RedisBehavior::Hash,
                        },
                    }
                    .start();
                    return;
                }
                Err(e) => {
                    ctx.report_error("Failed to connect", e.to_string()).await;
                }
            }

            tokio::time::sleep(Duration::from_millis((50 * (1 << attempts)).min(5_000))).await;
            attempts -= 1;
        }

        panic!("Failed to establish connection to redis after 20 retries");
    }

    fn make_key(prefix: &String, column: &Option<String>, v: &Value) -> String {
        let mut key = prefix.to_string();

        if let Some(key_field) = &column {
            key.push_str(
                &v.get(key_field)
                    .expect("key field not found in data")
                    .as_str()
                    .expect("key field is not a string"),
            );
        };

        key
    }

    async fn process_element(&mut self, record: &Record<K, T>, _ctx: &mut Context<(), ()>) {
        let value = serde_json::to_value(&record.value).unwrap();
        let data = self.serializer.to_vec(&record.value).unwrap();
        match &self.table.connector_type {
            TableType::Target(target) => match &target {
                Target::StringTable {
                    key_column,
                    key_prefix,
                    ..
                } => {
                    let key = Self::make_key(key_prefix, key_column, &value);
                    self.tx
                        .send(RedisCmd::Data { key, value: data })
                        .await
                        .expect("Redis writer panicked");
                }
                Target::ListTable {
                    list_key_column,
                    list_prefix,
                    ..
                } => {
                    let key = Self::make_key(list_prefix, list_key_column, &value);

                    self.tx
                        .send(RedisCmd::Data { key, value: data })
                        .await
                        .expect("Redis writer panicked");
                }
                Target::HashTable {
                    hash_field_column,
                    hash_key_column,
                    hash_key_prefix,
                } => {
                    let key = Self::make_key(hash_key_prefix, hash_key_column, &value);
                    let field = value
                        .get(hash_field_column)
                        .expect("hash field column not found in data")
                        .as_str()
                        .expect("hash field is not a string")
                        .to_string();

                    self.tx
                        .send(RedisCmd::HData {
                            key,
                            field,
                            value: data,
                        })
                        .await
                        .expect("Redis writer panicked");
                }
            },
        };
    }

    async fn handle_checkpoint(
        &mut self,
        checkpoint: &CheckpointBarrier,
        _ctx: &mut Context<(), ()>,
    ) {
        self.tx
            .send(RedisCmd::Flush(checkpoint.epoch))
            .await
            .expect("writer has panicked");

        loop {
            match tokio::time::timeout(Duration::from_secs(30), self.rx.recv()).await {
                Ok(Some(epoch)) => {
                    if checkpoint.epoch == epoch {
                        return;
                    }
                }
                Ok(None) => {
                    panic!("writer has closed tx queue");
                }
                Err(_) => {
                    panic!("Timed out waiting for writer to confirm checkpoint flush");
                }
            }
        }
    }
}
