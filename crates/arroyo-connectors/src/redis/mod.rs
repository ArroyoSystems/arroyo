mod operator;

use anyhow::{anyhow, bail};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::OperatorNode;
use arroyo_rpc::var_str::VarStr;
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::{Client, ConnectionInfo, IntoConnectionInfo};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::oneshot::Receiver;
use typify::import_types;

use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, FieldType, PrimitiveType,
    TestSourceMessage,
};
use arroyo_rpc::OperatorConfig;

use crate::redis::operator::sink::{GeneralConnection, RedisSinkFunc};
use crate::{pull_opt, pull_option_to_u64};

pub struct RedisConnector {}

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./redis.svg");

import_types!(
    schema = "src/redis/profile.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

import_types!(schema = "src/redis/table.json");

enum RedisClient {
    Standard(Client),
    Clustered(ClusterClient),
}

impl RedisClient {
    pub fn new(config: &RedisConfig) -> anyhow::Result<Self> {
        Ok(match &config.connection {
            RedisConfigConnection::Address(address) => {
                let info = from_address(config, &address.0)?;

                RedisClient::Standard(Client::open(info).map_err(|e| {
                    anyhow!(
                        "Failed to construct Redis client for {}: {:?}",
                        address.0,
                        e
                    )
                })?)
            }
            RedisConfigConnection::Addresses(addresses) => {
                let infos: anyhow::Result<Vec<ConnectionInfo>> = addresses
                    .iter()
                    .map(|address| from_address(config, address))
                    .collect();

                RedisClient::Clustered(
                    ClusterClient::new(infos?).map_err(|e| {
                        anyhow!("Failed to construct Redis Cluster client: {:?}", e)
                    })?,
                )
            }
        })
    }

    async fn get_connection(&self) -> Result<GeneralConnection, redis::RedisError> {
        Ok(match self {
            RedisClient::Standard(c) => {
                GeneralConnection::Standard(ConnectionManager::new(c.clone()).await?)
            }
            RedisClient::Clustered(c) => {
                GeneralConnection::Clustered(c.get_async_connection().await?)
            }
        })
    }
}

fn from_address(config: &RedisConfig, address: &str) -> anyhow::Result<ConnectionInfo> {
    let mut info: ConnectionInfo = address
        .to_string()
        .into_connection_info()
        .map_err(|e| anyhow!("invalid redis address: {:?}", e))?;

    if let Some(username) = &config.username {
        info.redis.username = Some(username.sub_env_vars().map_err(|e| anyhow!("{}", e))?);
    }

    if let Some(password) = &config.password {
        info.redis.password = Some(password.sub_env_vars().map_err(|e| anyhow!("{}", e))?);
    }

    Ok(info)
}

#[allow(dependency_on_unit_never_type_fallback)]
async fn test_inner(
    c: RedisConfig,
    tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
) -> anyhow::Result<String> {
    tx.send(TestSourceMessage::info("Connecting to Redis"))
        .await
        .unwrap();

    let client = RedisClient::new(&c)?;

    match &client {
        RedisClient::Standard(client) => {
            let mut connection = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| anyhow!("Failed to connect to to Redis Cluster: {:?}", e))?;
            tx.send(TestSourceMessage::info(
                "Connected successfully, sending PING",
            ))
            .await
            .unwrap();

            redis::cmd("PING")
                .query_async::<()>(&mut connection)
                .await
                .map_err(|e| anyhow!("Received error sending PING command: {:?}", e))?;
        }
        RedisClient::Clustered(client) => {
            let mut connection = client
                .get_async_connection()
                .await
                .map_err(|e| anyhow!("Failed to connect to to Redis Cluster: {:?}", e))?;
            tx.send(TestSourceMessage::info(
                "Connected successfully, sending PING",
            ))
            .await
            .unwrap();

            redis::cmd("PING")
                .query_async::<()>(&mut connection)
                .await
                .map_err(|e| anyhow!("Received error sending PING command: {:?}", e))?;
        }
    }

    Ok("Received PING response successfully".to_string())
}

impl Connector for RedisConnector {
    type ProfileT = RedisConfig;
    type TableT = RedisTable;

    fn name(&self) -> &'static str {
        "redis"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "redis".to_string(),
            name: "Redis".to_string(),
            icon: ICON.to_string(),
            description: "Write results to Redis".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: false,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Source
    }

    fn get_schema(
        &self,
        _: Self::ProfileT,
        _: Self::TableT,
        s: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        s.cloned()
    }

    fn test_profile(&self, profile: Self::ProfileT) -> Option<Receiver<TestSourceMessage>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let (itx, _rx) = tokio::sync::mpsc::channel(8);
            let message = match test_inner(profile, itx).await {
                Ok(_) => TestSourceMessage::done("Successfully connected to Redis"),
                Err(e) => TestSourceMessage::fail(format!("Failed to connect to Redis: {:?}", e)),
            };

            tx.send(message).unwrap();
        });

        Some(rx)
    }

    fn test(
        &self,
        _: &str,
        c: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let resp = match test_inner(c, tx.clone()).await {
                Ok(c) => TestSourceMessage::done(c),
                Err(e) => TestSourceMessage::fail(e.to_string()),
            };

            tx.send(resp).await.unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        s: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let connection_config = match profile {
            Some(connection_profile) => {
                serde_json::from_value(connection_profile.config.clone())
                    .map_err(|e| anyhow!("Failed to parse connection config: {:?}", e))?
            }
            None => {
                let address = options.remove("address");

                let cluster_addresses = options.remove("cluster.addresses");
                let connection = match (address, cluster_addresses) {
                    (Some(address), None) => {
                        RedisConfigConnection::Address(Address(address.to_string()))
                    }
                    (None, Some(cluster_addresses)) => RedisConfigConnection::Addresses(
                        cluster_addresses
                            .split(',')
                            .map(|s| Address(s.to_string()))
                            .collect(),
                    ),
                    (Some(_), Some(_)) => {
                        bail!("only one of `address` or `cluster.addresses` may be set");
                    }
                    (None, None) => {
                        bail!("one of `address` or `cluster.addresses` must be set");
                    }
                };

                let username = options.remove("username").map(VarStr::new);
                let password = options.remove("password").map(VarStr::new);

                RedisConfig {
                    connection,
                    username,
                    password,
                }
            }
        };

        let typ = pull_opt("type", options)?;

        let schema = s
            .as_ref()
            .ok_or_else(|| anyhow!("No schema defined for Redis connection"))?;

        fn validate_column(
            schema: &ConnectionSchema,
            column: String,
            sql: &str,
        ) -> anyhow::Result<String> {
            if !schema.fields.iter().any(|f| {
                f.field_name == column
                    && f.field_type.r#type == FieldType::Primitive(PrimitiveType::String)
                    && !f.nullable
            }) {
                bail!("invalid value '{}' for {}, must be the name of a non-nullable TEXT column on the table", column, sql);
            };

            Ok(column)
        }

        let sink = match typ.as_str() {
            "sink" => TableType::Target(match pull_opt("target", options)?.as_str() {
                "string" => Target::StringTable {
                    key_prefix: pull_opt("target.key_prefix", options)?,
                    key_column: options
                        .remove("target.key_column")
                        .map(|name| validate_column(schema, name, "target.key_column"))
                        .transpose()?,
                    ttl_secs: pull_option_to_u64("target.ttl_secs", options)?
                        .map(|t| t.try_into())
                        .transpose()
                        .map_err(|_| anyhow!("target.ttl_secs must be greater than 0"))?,
                },
                "list" => Target::ListTable {
                    list_prefix: pull_opt("target.key_prefix", options)?,
                    list_key_column: options
                        .remove("target.key_column")
                        .map(|name| validate_column(schema, name, "target.key_column"))
                        .transpose()?,
                    max_length: pull_option_to_u64("target.max_length", options)?
                        .map(|t| t.try_into())
                        .transpose()
                        .map_err(|_| anyhow!("target.max_length must be greater than 0"))?,
                    operation: match options.remove("target.operation").as_deref() {
                        Some("append") | None => ListOperation::Append,
                        Some("prepend") => ListOperation::Prepend,
                        Some(op) => {
                            bail!("'{}' is not a valid value for target.operation; must be one of 'append' or 'prepend'", op);
                        }
                    },
                },
                "hash" => Target::HashTable {
                    hash_field_column: validate_column(
                        schema,
                        pull_opt("target.field_column", options)?,
                        "targets.field_column",
                    )?,
                    hash_key_column: options
                        .remove("target.key_column")
                        .map(|name| validate_column(schema, name, "target.key_column"))
                        .transpose()?,
                    hash_key_prefix: pull_opt("target.key_prefix", options)?,
                },
                s => {
                    bail!("'{}' is not a valid redis target", s);
                }
            }),
            s => {
                bail!("'{}' is not a valid type; must be `sink`", s);
            }
        };

        self.from_config(
            None,
            name,
            connection_config,
            RedisTable {
                connector_type: sink,
            },
            s,
        )
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("No schema defined for Redis connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Redis connection"))?;

        let _ = RedisClient::new(&config)?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
        };

        Ok(Connection {
            id,
            connector: self.name(),
            name: name.to_string(),
            connection_type: ConnectionType::Sink,
            schema,
            config: serde_json::to_string(&config).unwrap(),
            description: "RedisSink".to_string(),
        })
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<OperatorNode> {
        let client = RedisClient::new(&profile)?;

        let (tx, cmd_rx) = tokio::sync::mpsc::channel(128);
        let (cmd_tx, rx) = tokio::sync::mpsc::channel(128);

        Ok(OperatorNode::from_operator(Box::new(RedisSinkFunc {
            serializer: ArrowSerializer::new(
                config.format.expect("redis table must have a format"),
            ),
            table,
            client,
            cmd_q: Some((cmd_tx, cmd_rx)),
            tx,
            rx,
            key_index: None,
            hash_index: None,
        })))
    }
}
