use crate::redis::{ListOperation, RedisClient, RedisTable, TableType, Target};
use arrow::array::{AsArray, RecordBatch};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::{ArrowContext, ErrorReporter};
use arroyo_operator::operator::ArrowOperator;
use arroyo_types::CheckpointBarrier;
use async_trait::async_trait;
use redis::aio::{ConnectionLike, ConnectionManager};
use redis::cluster_async::ClusterConnection;
use redis::{Cmd, Pipeline, RedisFuture};
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::info;

const FLUSH_TIMEOUT: Duration = Duration::from_millis(100);
const FLUSH_BYTES: usize = 10 * 1024 * 1024;

pub struct RedisSinkFunc {
    pub serializer: ArrowSerializer,
    pub table: RedisTable,
    pub client: RedisClient,
    pub cmd_q: Option<(Sender<u32>, Receiver<RedisCmd>)>,

    pub rx: Receiver<u32>,
    pub tx: Sender<RedisCmd>,

    pub key_index: Option<usize>,
    pub hash_index: Option<usize>,
}

impl RedisSinkFunc {
    fn make_key(&self, prefix: &String, batch: &RecordBatch, idx: usize) -> String {
        let mut key = prefix.to_string();

        if let Some(key_index) = self.key_index {
            key.push_str(batch.column(key_index).as_string::<i32>().value(idx));
        };

        key
    }
}

#[derive(Copy, Clone, Debug)]
enum RedisBehavior {
    Set { ttl: Option<usize> },
    Push { append: bool, max: Option<usize> },
    Hash,
}

pub enum RedisCmd {
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
                                            self.pipeline.set_ex(key, value, ttl as u64);
                                        } else {
                                            self.pipeline.set(key, value);
                                        }
                                    }
                                    RedisBehavior::Push { append, max } => {
                                        if max.is_some() && !self.max_push_keys.contains(&key) {
                                            self.max_push_keys.insert(key.clone());
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

        if let RedisBehavior::Push {
            max: Some(max),
            append,
        } = self.behavior
        {
            for k in self.max_push_keys.drain() {
                if append {
                    self.pipeline.ltrim(k, -(max as isize), -1);
                } else {
                    self.pipeline.ltrim(k, 0, max as isize - 1);
                }
            }
        }

        while attempts < 20 {
            match self.pipeline.query_async::<()>(&mut self.connection).await {
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
            attempts += 1;
        }

        panic!("Exhausted retries writing to Redis");
    }
}

#[async_trait]
impl ArrowOperator for RedisSinkFunc {
    fn name(&self) -> String {
        "RedisSink".to_string()
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        match &self.table.connector_type {
            TableType::Target(Target::ListTable {
                list_key_column: Some(key),
                ..
            })
            | TableType::Target(Target::StringTable {
                key_column: Some(key),
                ..
            })
            | TableType::Target(Target::HashTable {
                hash_key_column: Some(key),
                ..
            }) => {
                self.key_index = Some(
                    ctx.in_schemas
                        .first()
                        .expect("no in-schema for redis sink!")
                        .schema
                        .index_of(key)
                        .unwrap_or_else(|_| {
                            panic!(
                                "key column ({key}) does not exist in input schema for redis sink"
                            )
                        }),
                );
            }
            _ => {}
        }

        if let TableType::Target(Target::HashTable {
            hash_field_column, ..
        }) = &self.table.connector_type
        {
            self.hash_index = Some(ctx.in_schemas.first().expect("no in-schema for redis sink!")
                .schema
                .index_of(hash_field_column)
                .unwrap_or_else(|_| panic!("hash field column ({hash_field_column}) does not exist in input schema for redis sink")));
        }

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
            attempts += 1;
        }

        panic!("Failed to establish connection to redis after 20 retries");
    }

    async fn process_batch(&mut self, batch: RecordBatch, _: &mut ArrowContext) {
        for (i, value) in self.serializer.serialize(&batch).enumerate() {
            match &self.table.connector_type {
                TableType::Target(target) => match &target {
                    Target::StringTable { key_prefix, .. } => {
                        let key = self.make_key(key_prefix, &batch, i);
                        self.tx
                            .send(RedisCmd::Data { key, value })
                            .await
                            .expect("Redis writer panicked");
                    }
                    Target::ListTable { list_prefix, .. } => {
                        let key = self.make_key(list_prefix, &batch, i);

                        self.tx
                            .send(RedisCmd::Data { key, value })
                            .await
                            .expect("Redis writer panicked");
                    }
                    Target::HashTable {
                        hash_key_prefix, ..
                    } => {
                        let key = self.make_key(hash_key_prefix, &batch, i);
                        let field = batch
                            .column(self.hash_index.expect("no hash index"))
                            .as_string::<i32>()
                            .value(i)
                            .to_string();

                        self.tx
                            .send(RedisCmd::HData { key, field, value })
                            .await
                            .expect("Redis writer panicked");
                    }
                },
            };
        }
    }

    async fn handle_checkpoint(&mut self, checkpoint: CheckpointBarrier, _ctx: &mut ArrowContext) {
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
