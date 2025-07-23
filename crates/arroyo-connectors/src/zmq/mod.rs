use anyhow::{anyhow, bail};
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{ConnectionType, TestSourceMessage};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Receiver;
use typify::import_types;
use zmq;

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./zmq.svg");

mod sink;
mod source;

use sink::ZmqSinkFunc;
use source::ZmqSourceFunc;

import_types!(
    schema = "src/zmq/profile.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

import_types!(schema = "src/zmq/table.json");

pub struct ZmqConnector {}

impl ZmqConnector {
    pub fn connection_from_options(options: &mut ConnectorOptions) -> anyhow::Result<ZmqConfig> {
        let connection_pattern = options.pull_str("connection_pattern")?;
        let url = VarStr::new(options.pull_str("url")?);

        let connection_pattern = match connection_pattern.as_str() {
            "bind" => ConnectionPattern::Bind,
            "connect" => ConnectionPattern::Connect,
            _ => {
                bail!("invalid connection pattern: {}", connection_pattern)
            }
        };

        Ok(ZmqConfig {
            connection_pattern,
            url,
        })
    }

    pub fn table_from_options(options: &mut ConnectorOptions) -> anyhow::Result<ZmqTable> {
        let socket_type = options.pull_str("socket_type")?;

        let socket_type = match socket_type.as_str() {
            "push" => SocketType::Push,
            "pull" => SocketType::Pull,
            _ => {
                bail!("invalid socket type: {}", socket_type)
            }
        };

        Ok(ZmqTable { socket_type })
    }
}

impl Connector for ZmqConnector {
    type ProfileT = ZmqConfig;
    type TableT = ZmqTable;

    fn name(&self) -> &'static str {
        "zmq"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "zmq".to_owned(),
            name: "ZeroMQ".to_owned(),
            icon: ICON.to_owned(),
            description: "High-performance brokerless messaging library for distributed systems with automatic connection handling and multiple messaging patterns.".to_owned(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_owned()),
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn config_description(&self, config: Self::ProfileT) -> String {
        let url = config.url.sub_env_vars().unwrap();
        match config.connection_pattern {
            ConnectionPattern::Bind => {
                format!("Bind to {url}")
            }
            ConnectionPattern::Connect => {
                format!("Connect to {url}")
            }
        }
    }

    fn table_type(&self, _config: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.socket_type {
            SocketType::Push => ConnectionType::Sink,
            SocketType::Pull => ConnectionType::Source,
        }
    }

    fn test(
        &self,
        _name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        _schema: Option<&arroyo_rpc::api_types::connections::ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<arroyo_rpc::api_types::connections::TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let resp = match test_inner(config, Some(table), tx.clone()).await {
                Ok(c) => TestSourceMessage::done(c),
                Err(e) => TestSourceMessage::fail(e.to_string()),
            };

            tx.send(resp).await.unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut arroyo_rpc::ConnectorOptions,
        schema: Option<&arroyo_rpc::api_types::connections::ConnectionSchema>,
        profile: Option<&arroyo_rpc::api_types::connections::ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let connection = profile
            .map(|p| {
                serde_json::from_value(p.config.clone())
                    .map_err(|e| anyhow!("invalid config for profile '{}' in database: {e}", p.id))
            })
            .unwrap_or_else(|| Self::connection_from_options(options))?;

        let table = Self::table_from_options(options)?;

        Self::from_config(self, None, name, connection, table, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&arroyo_rpc::api_types::connections::ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let url = config.url.sub_env_vars().unwrap();

        let (typ, desc) = match table.socket_type {
            SocketType::Push => (ConnectionType::Sink, format!("ZmqSink<{url}>")),
            SocketType::Pull => (ConnectionType::Source, format!("ZmqSource<{url}>")),
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("No schema defined for Zmq connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Zmq connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        let connection = Connection::new(
            id,
            self.name(),
            name.to_string(),
            typ,
            schema,
            &config,
            desc,
        );

        Ok(connection)
    }

    fn test_profile(&self, profile: Self::ProfileT) -> Option<Receiver<TestSourceMessage>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let (itx, _rx) = tokio::sync::mpsc::channel(8);
            let message = match test_inner(profile, None, itx).await {
                Ok(_) => TestSourceMessage::done("Successfully connected to Zmq"),
                Err(e) => TestSourceMessage::fail(format!("Failed to connect to Zmq: {e:?}")),
            };
            tx.send(message).unwrap();
        });

        Some(rx)
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: arroyo_rpc::OperatorConfig,
    ) -> anyhow::Result<arroyo_operator::operator::ConstructedOperator> {
        let operator = match table.socket_type {
            SocketType::Pull => ConstructedOperator::from_source(Box::new(ZmqSourceFunc::new(
                profile,
                config
                    .format
                    .ok_or_else(|| anyhow!("format is required for zmq source"))?,
                config.framing,
                config.bad_data,
                config
                    .rate_limit
                    .map(|l| l.messages_per_second)
                    .unwrap_or(u32::MAX),
                config.metadata_fields,
            ))),
            SocketType::Push => ConstructedOperator::from_operator(Box::new(ZmqSinkFunc::new(
                profile,
                config
                    .format
                    .ok_or_else(|| anyhow!("format is required for zmq sink"))?,
            ))),
        };

        Ok(operator)
    }
}

async fn test_inner(
    config: ZmqConfig,
    table: Option<ZmqTable>,
    tx: Sender<TestSourceMessage>,
) -> anyhow::Result<String> {
    tx.send(TestSourceMessage::info("Connecting to ZMQ"))
        .await
        .unwrap();

    let context = zmq::Context::new();

    let socket_type = match table {
        Some(t) => match t.socket_type {
            SocketType::Push => zmq::SocketType::PUSH,
            SocketType::Pull => zmq::SocketType::PULL,
        },
        None => zmq::SocketType::PUSH,
    };

    let socket = context.socket(socket_type)?;

    // Set a reasonable timeout for testing
    socket.set_rcvtimeo(1000)?;
    socket.set_sndtimeo(1000)?;

    let url = config.url.sub_env_vars().map_err(|e| anyhow!("{e}"))?;
    let operation = match config.connection_pattern {
        ConnectionPattern::Bind => {
            tx.send(TestSourceMessage::info(format!("Binding to {url}")))
                .await
                .unwrap();
            socket.bind(&url)?;
            "bound to"
        }
        ConnectionPattern::Connect => {
            tx.send(TestSourceMessage::info(format!("Connecting to {url}")))
                .await
                .unwrap();
            socket.connect(&url)?;
            "connected to"
        }
    };

    Ok(format!("Successfully {operation} {url}"))
}
