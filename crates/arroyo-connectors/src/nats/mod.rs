use crate::nats::sink::NatsSinkFunc;
use crate::nats::source::NatsSourceFunc;
use crate::pull_opt;
use anyhow::anyhow;
use anyhow::bail;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::OperatorNode;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::OperatorConfig;
use async_nats::ServerAddr;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroU32;
use tokio::sync::mpsc::Sender;
use typify::import_types;

pub mod sink;
pub mod source;

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./nats.svg");

import_types!(
    schema = "src/nats/profile.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

import_types!(schema = "src/nats/table.json");

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct NatsState {
    stream_name: String,
    stream_sequence_number: u64,
}

pub struct NatsConnector {}

impl NatsConnector {
    pub fn connection_from_options(
        options: &mut HashMap<String, String>,
    ) -> anyhow::Result<NatsConfig> {
        let nats_servers = VarStr::new(pull_opt("servers", options)?);
        let nats_auth = options.remove("auth.type");
        let nats_auth: NatsConfigAuthentication = match nats_auth.as_deref() {
            Some("none") | None => NatsConfigAuthentication::None {},
            Some("credentials") => NatsConfigAuthentication::Credentials {
                username: VarStr::new(pull_opt("auth.username", options)?),
                password: VarStr::new(pull_opt("auth.password", options)?),
            },
            Some(other) => bail!("Unknown auth type '{}'", other),
        };

        Ok(NatsConfig {
            authentication: nats_auth,
            servers: nats_servers,
        })
    }

    pub fn table_from_options(options: &mut HashMap<String, String>) -> anyhow::Result<NatsTable> {
        let conn_type = pull_opt("type", options)?;
        let nats_table_type = match conn_type.as_str() {
            "source" => {
                let source_type = match (
                    pull_opt("stream", options).ok(),
                    pull_opt("subject", options).ok(),
                ) {
                    (Some(stream), None) => Some(SourceType::Jetstream {
                        stream,
                        description: options.remove("consumer.description"),
                        ack_policy: options
                            .remove("consumer.ack_policy")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(AcknowledgmentPolicy::Explicit),
                        replay_policy: options
                            .remove("consumer.replay_policy")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(ReplayPolicy::Instant),
                        ack_wait: options
                            .remove("consumer.ack_wait")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(30),
                        filter_subjects: options
                            .remove("consumer.filter_subjects")
                            .map_or_else(Vec::new, |s| s.split(',').map(String::from).collect()),
                        sample_frequency: options
                            .remove("consumer.sample_frequency")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(0),
                        num_replicas: options
                            .remove("consumer.num_replicas")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(1),
                        inactive_threshold: options
                            .remove("consumer.inactive_threshold")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(600),
                        rate_limit: options
                            .remove("consumer.rate_limit")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(-1),
                        max_ack_pending: options
                            .remove("consumer.max_ack_pending")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(-1),
                        max_deliver: options
                            .remove("consumer.max_deliver")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(-1),
                        max_waiting: options
                            .remove("consumer.max_waiting")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(1000000),
                        max_batch: options
                            .remove("consumer.max_batch")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(10000),
                        max_bytes: options
                            .remove("consumer.max_bytes")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(104857600),
                        max_expires: options
                            .remove("consumer.max_expires")
                            .unwrap_or_default()
                            .parse()
                            .unwrap_or(300000),
                    }),
                    (None, Some(subject)) => Some(SourceType::Core { subject }),
                    (Some(_), Some(_)) => bail!("Exactly one of `stream` or `subject` must be set"),
                    (None, None) => bail!("One of `stream` or `subject` must be set"),
                };

                ConnectorType::Source { source_type }
            }
            "sink" => {
                let sink_type = match pull_opt("subject", options).ok() {
                    Some(subject) => Some(SinkType::Subject(subject)),
                    None => bail!("`subject` must be set for sink"),
                };
                ConnectorType::Sink { sink_type }
            }
            _ => bail!("Type must be one of 'source' or 'sink'"),
        };

        // TODO: Use parameters with `nats.` prefix for the NATS connection configuration
        Ok(NatsTable {
            connector_type: nats_table_type,
        })
    }
}

impl Connector for NatsConnector {
    type ProfileT = NatsConfig;
    type TableT = NatsTable;

    fn name(&self) -> &'static str {
        "nats"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "nats".to_string(),
            name: "Nats".to_string(),
            icon: ICON.to_string(),
            description: "Read and write from a NATS cluster".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn config_description(&self, config: Self::ProfileT) -> String {
        (*config
            .servers
            .sub_env_vars()
            .map_err(|e| e.context("servers"))
            .unwrap())
        .to_string()
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match &table.connector_type {
            ConnectorType::Source { .. } => ConnectionType::Source,
            ConnectorType::Sink { .. } => ConnectionType::Sink,
        }
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
        tx: Sender<TestSourceMessage>,
    ) {
        // TODO: Implement a full-fledge `NatsTester` struct for testing the client connection,
        // the stream or subject existence and access permissions, the deserialization of messages
        // for the specified format, the authentication to the schema registry (if any), etc.
        tokio::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(message).await.unwrap();
        });
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: NatsConfig,
        table: NatsTable,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let stream_or_subject = match &table.connector_type {
            ConnectorType::Source { source_type, .. } => {
                match source_type
                    .as_ref()
                    .ok_or_else(|| anyhow!("sourceType is required"))?
                {
                    SourceType::Jetstream { stream, .. } => stream,
                    SourceType::Core { subject, .. } => subject,
                }
            }
            ConnectorType::Sink { sink_type, .. } => {
                match sink_type
                    .as_ref()
                    .ok_or_else(|| anyhow!("sinkType is required"))?
                {
                    SinkType::Subject(s) => s,
                }
            }
        };

        let (connection_type, desc) = match &table.connector_type {
            ConnectorType::Source { .. } => (
                ConnectionType::Source,
                format!("NatsSource<{:?}>", stream_or_subject),
            ),
            ConnectorType::Sink { .. } => (
                ConnectionType::Sink,
                format!("NatsSink<{:?}>", stream_or_subject),
            ),
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("No schema defined for NATS connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for NATS connection"))?;

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
            connection_type,
            schema,
            config: serde_json::to_string(&config).unwrap(),
            description: desc,
        })
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let connection = profile
            .map(|p| {
                serde_json::from_value(p.config.clone()).map_err(|e| {
                    anyhow!("Invalid config for profile '{}' in database: {}", p.id, e)
                })
            })
            .unwrap_or_else(|| Self::connection_from_options(options))?;

        let table = Self::table_from_options(options)?;

        Self::from_config(self, None, name, connection, table, schema)
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<OperatorNode> {
        Ok(match table.connector_type {
            ConnectorType::Source { ref source_type } => {
                OperatorNode::from_source(Box::new(NatsSourceFunc {
                    source_type: source_type
                        .clone()
                        .ok_or_else(|| anyhow!("`sourceType` is required"))?,
                    servers: profile
                        .servers
                        .sub_env_vars()
                        .map_err(|e| e.context("servers"))?
                        .clone(),
                    connection: profile.clone(),
                    table: table.clone(),
                    framing: config.framing,
                    format: config.format.expect("Format must be set for NATS source"),
                    bad_data: config.bad_data,
                    messages_per_second: NonZeroU32::new(
                        config
                            .rate_limit
                            .map(|l| l.messages_per_second)
                            .unwrap_or(u32::MAX),
                    )
                    .unwrap(),
                }))
            }
            ConnectorType::Sink { ref sink_type } => {
                OperatorNode::from_operator(Box::new(NatsSinkFunc {
                    sink_type: sink_type
                        .clone()
                        .ok_or_else(|| anyhow!("`sinkType` is required"))?,
                    servers: profile
                        .servers
                        .sub_env_vars()
                        .map_err(|e| e.context("servers"))?
                        .clone(),
                    connection: profile.clone(),
                    table: table.clone(),
                    publisher: None,
                    serializer: ArrowSerializer::new(
                        config.format.expect("Format must be set for NATS source"),
                    ),
                }))
            }
        })
    }
}

async fn get_nats_client(connection: &NatsConfig) -> anyhow::Result<async_nats::Client> {
    let mut opts = async_nats::ConnectOptions::new();

    let servers_str = connection
        .servers
        .sub_env_vars()
        .map_err(|e| e.context("servers"))?
        .clone();
    let servers_vec: Vec<ServerAddr> = servers_str
        .split(',')
        .map(|s| {
            s.parse::<ServerAddr>()
                .expect("Something went wrong while parsing servers string")
        })
        .collect();

    match &connection.authentication {
        NatsConfigAuthentication::None {} => {}
        NatsConfigAuthentication::Credentials { username, password } => {
            opts = opts.user_and_password(
                username.sub_env_vars().map_err(|e| e.context("username"))?,
                password.sub_env_vars().map_err(|e| e.context("password"))?,
            )
        }
    };
    let client = opts
        .connect(servers_vec)
        .await
        .map_err(|e| anyhow!("Failed to connect to nats: {:?}", e))?;
    Ok(client)
}
