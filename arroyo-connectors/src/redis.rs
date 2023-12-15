use anyhow::{anyhow, bail};
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, FieldType, PrimitiveType,
    TestSourceMessage,
};
use arroyo_rpc::OperatorConfig;
use axum::response::sse::Event;
use redis::cluster::ClusterClient;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use tokio::sync::oneshot::Receiver;
use typify::import_types;

use crate::{pull_opt, pull_option_to_u64, Connection, Connector};

pub struct RedisConnector {}

const CONFIG_SCHEMA: &str = include_str!("../../connector-schemas/redis/connection.json");
const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/redis/table.json");
const ICON: &str = include_str!("../resources/redis.svg");

import_types!(schema = "../connector-schemas/redis/connection.json",);
import_types!(schema = "../connector-schemas/redis/table.json");

fn validate_column(
    schema: &ConnectionSchema,
    column: String,
    path: &str,
) -> anyhow::Result<String> {
    if schema
        .fields
        .iter()
        .find(|f| {
            f.field_name == column
                && f.field_type.r#type == FieldType::Primitive(PrimitiveType::String)
        })
        .is_none()
    {
        bail!(
            "invalid value '{}' for {}, must be the name of a TEXT column on the table",
            column,
            path
        );
    }

    Ok(column)
}

async fn test_inner(
    c: RedisConfig,
    tx: tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
) -> anyhow::Result<String> {
    tx.send(Ok(Event::default()
        .json_data(TestSourceMessage::info("Connecting to Redis"))
        .unwrap()))
        .await
        .unwrap();

    match c.connection {
        RedisConfigConnection::Address(address) => {
            let client = redis::Client::open(address.0.clone()).map_err(|e| {
                anyhow!(
                    "Failed to construct Redis client for {}: {:?}",
                    address.0,
                    e
                )
            })?;

            let mut connection = client
                .get_async_connection()
                .await
                .map_err(|e| anyhow!("Failed to connect to Redis at {}: {:?}", address.0, e))?;

            tx.send(Ok(Event::default()
                .json_data(TestSourceMessage::info(
                    "Connected successfully, sending PING",
                ))
                .unwrap()))
                .await
                .unwrap();

            redis::cmd("PING")
                .query_async(&mut connection)
                .await
                .map_err(|e| anyhow!("Received error sending PING command: {:?}", e))?;
        }
        RedisConfigConnection::Addresses(addresses) => {
            let client = ClusterClient::new(addresses.into_iter().map(|a| a.0).collect())
                .map_err(|e| anyhow!("Failed to construct Redis Cluster client: {:?}", e))?;

            let mut connection = client
                .get_async_connection()
                .await
                .map_err(|e| anyhow!("Failed to connect to to Redis Cluster: {:?}", e))?;

            tx.send(Ok(Event::default()
                .json_data(TestSourceMessage::info(
                    "Connected successfully, sending PING",
                ))
                .unwrap()))
                .await
                .unwrap();

            redis::cmd("PING")
                .query_async(&mut connection)
                .await
                .map_err(|e| anyhow!("Received error sending PING command: {:?}", e))?;
        }
    };

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
        return ConnectionType::Source;
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
            let (itx, _) = tokio::sync::mpsc::channel(8);
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
        tx: tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
    ) {
        tokio::task::spawn(async move {
            let resp = match test_inner(c, tx.clone()).await {
                Ok(c) => TestSourceMessage::done(c),
                Err(e) => TestSourceMessage::fail(e.to_string()),
            };

            tx.send(Ok(Event::default().json_data(resp).unwrap()))
                .await
                .unwrap();
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
                let config: RedisConfigConnection =
                    serde_json::from_value(connection_profile.config.clone())
                        .map_err(|e| anyhow!("Failed to parse connection config: {:?}", e))?;
                RedisConfig { connection: config }
            }
            None => {
                let address = options.remove("address");

                let cluster_addresses = options.remove("cluster.addresses");
                let connection_config = match (address, cluster_addresses) {
                    (Some(address), None) => {
                        RedisConfigConnection::Address(RedisConfigConnectionAddress(address))
                    }
                    (None, Some(cluster_addresses)) => RedisConfigConnection::Addresses(
                        cluster_addresses
                            .split(",")
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
                RedisConfig {
                    connection: connection_config,
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
            if schema
                .fields
                .iter()
                .find(|f| {
                    f.field_name == column
                        && f.field_type.r#type == FieldType::Primitive(PrimitiveType::String)
                        && !f.nullable
                })
                .is_none()
            {
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
                    operation: match options
                        .remove("target.operation")
                        .as_ref()
                        .map(|s| s.as_str())
                    {
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

        match &config.connection {
            RedisConfigConnection::Address(address) => {
                let _ = redis::Client::open(address.0.clone()).map_err(|e| {
                    anyhow!(
                        "Failed to construct Redis client for {}: {:?}",
                        address.0,
                        e
                    )
                })?;
            }
            RedisConfigConnection::Addresses(addresses) => {
                let _ = ClusterClient::new(addresses.into_iter().map(|a| a.0.clone()).collect())
                    .map_err(|e| anyhow!("Failed to construct Redis Cluster client: {:?}", e))?;
            }
        }

        match &table.connector_type {
            TableType::Target(Target::StringTable { key_column, .. }) => {
                if let Some(key_column) = key_column {
                    validate_column(&schema, key_column.clone(), "connector_type.key_column")?;
                }
            }
            TableType::Target(Target::ListTable {
                list_key_column, ..
            }) => {
                if let Some(n) = list_key_column {
                    validate_column(&schema, n.clone(), "connector_type.list_key_column")?;
                }
            }
            TableType::Target(Target::HashTable {
                hash_key_column,
                hash_field_column,
                ..
            }) => {
                if let Some(n) = hash_key_column {
                    validate_column(&schema, n.clone(), "connector_type.hash_key_column")?;
                }

                validate_column(
                    &schema,
                    hash_field_column.clone(),
                    "connector_type.hash_field_column",
                )?;
            }
        };

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
            name: name.to_string(),
            connection_type: ConnectionType::Sink,
            schema,
            operator: "connectors::redis::sink::RedisSinkFunc::<#in_k, #in_t>".to_string(),
            config: serde_json::to_string(&config).unwrap(),
            description: "RedisSink".to_string(),
        })
    }
}
