use arroyo_rpc::api_types::connections::{ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::OperatorConfig;
use axum::response::sse::Event;
use std::convert::Infallible;
use std::str::FromStr;
use anyhow::{anyhow, bail};
use typify::import_types;
use serde::{Deserialize, Serialize};

use crate::{Connection, Connector, EmptyConfig, pull_opt};

pub struct RedisConnector {}

const CONFIG_SCHEMA: &str = include_str!("../../connector-schemas/redis/connection.json");
const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/redis/table.json");
const ICON: &str = include_str!("../resources/kafka.svg");

import_types!(schema = "../connector-schemas/redis/connection.json",);
import_types!(schema = "../connector-schemas/redis/table.json");


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

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(Ok(Event::default().json_data(message).unwrap()))
                .await
                .unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        opts: &mut std::collections::HashMap<String, String>,
        s: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let address = opts.remove("address");

        let cluster_addresses = opts.remove("cluster.addresses");

        let connection_config = match (address, cluster_addresses) {
            (Some(address), None) => {
                RedisConfigConnection::Address(RedisConfigConnectionAddress(address))
            }
            (None, Some(cluster_addresses)) => {
                RedisConfigConnection::Addresses(cluster_addresses
                    .split(",")
                    .map(|s| Address(s.to_string()))
                    .collect())
            }
            (Some(_), Some(_)) => {
                bail!("only one of `address` or `cluster.addresses` may be set");
            }
            (None, None) => {
                bail!("one of `address` or `cluster.addresses` must be set");
            }
        };

        let typ = pull_opt("type", opts)?;

        let sink = match typ.as_str()  {
            "sink" => {
                TableType::Target(match pull_opt("target", opts)?.as_str() {
                    "string_table" => {
                        Target::StringTable {
                            key_field: opts.remove("target.key_field"),
                            prefix: opts.remove("target.prefix"),
                            value_type: ValueMode::from_str(&pull_opt("target.value_type", opts)?)
                                .map_err(|e| anyhow!("invalid value for 'target.value_type: {}'", e))?
                        }
                    }
                    s => {
                        bail!("'{}' is not a valid redis target", s);
                    }
                })
            }
            s => {
                bail!("'{}' is not a valid type; must be `sink`", s);
            }
        };

        self.from_config(
            None,
            name,
            RedisConfig {
                connection: connection_config,
            },
            RedisTable {
                connector_type: sink
            },
            s
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
            .ok_or_else(|| anyhow!("No schema defined for Kafka connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Kafka connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
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
