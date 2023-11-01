use std::marker::PhantomData;
use std::time::Duration;
use fluvio::dataplane::Encoder;
use redis::{AsyncCommands, Client, RedisResult};
use serde::Serialize;
use serde_json_path::JsonPathExt;
use tokio_tungstenite::tungstenite::connect;
use arroyo_macro::process_fn;
use arroyo_rpc::OperatorConfig;
use arroyo_types::{Key, Record};
use crate::connectors::redis::{Address, RedisConfig, RedisConfigConnection, RedisTable, TableType, Target, ValueMode};
use crate::engine::{Context, StreamNode};
use crate::formats::DataSerializer;
use crate::SchemaData;

#[derive(StreamNode)]
pub struct RedisSinkFunc<K, T>
    where
        K: Key,
        T: Serialize + SchemaData,
{
    serializer: DataSerializer<T>,
    profile: RedisConfig,
    table: RedisTable,
    client: Client,
    connection: Option<redis::aio::Connection>,
    _t: PhantomData<K>,
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
        let profile: RedisConfig = serde_json::from_value(config.connection).expect("Invalid connection profile for RedisSink");
        let table: RedisTable = serde_json::from_value(config.table).expect("Invalid table config for Redis");

        let client = match &profile.connection {
            RedisConfigConnection::Address(address) => {
                Client::open(address.0.to_string()).expect("invalid address")
            }
            RedisConfigConnection::Addresses(_) => {
                todo!("cluster support");
            }
        };


        Self {
            serializer: DataSerializer::new(config.format.expect("redis table must have a format")),
            profile,
            table,
            client,
            connection: None,
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        "RedisSink".to_string()
    }

    async fn on_start(&mut self, ctx: &mut Context<(), ()>) {
        self.connect(ctx).await;
    }

    async fn connect(&mut self, ctx: &mut Context<(), ()>) {
        let mut attempts = 0;
        while attempts < 20 {
            match self.client.get_tokio_connection().await {
                Ok(connection) => {
                    self.connection = Some(connection);
                    return;
                }
                Err(e) => {
                    ctx.report_error("Failed to connect", e.to_string()).await;
                }
            }

            tokio::time::sleep(Duration::from_millis((50 * (1 << attempts)).min(5_000)))
                .await;
            attempts -= 1;
        }

        panic!("Failed to establish connection to redis after 20 retries");
    }


    async fn process_element(&mut self, record: &Record<K, T>, ctx: &mut Context<(), ()>) {
        let connection = self.connection.as_mut().unwrap();

        let mut key = String::new();
        match &self.table.connector_type {
            TableType::Target(target) => {
                match &target {
                    Target::StringTable { key_field, prefix, value_type } => {
                        if let Some(prefix) = &prefix {
                            key.push_str(prefix);
                        }

                        if let Some(key_field) = &key_field {
                            key.push_str(&serde_json::to_value(&record.value).unwrap()
                                .get(key_field).unwrap().to_string());
                        }

                        match value_type {
                            ValueMode::ValuePerField => {
                                todo!()
                            }
                            ValueMode::SingleValue => {
                                let value = self.serializer.to_vec(&record.value).unwrap();
                                let _: () = connection.set(key, value)
                                    .await.unwrap();
                            }
                            ValueMode::SingleField => {
                                todo!()
                            }
                        }
                    }
                    Target::Stream { .. } => {
                        todo!();
                    }
                }
            }
        };
    }
}
