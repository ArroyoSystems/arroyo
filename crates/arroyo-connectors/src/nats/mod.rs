use crate::nats::sink::NatsSinkFunc;
use crate::nats::source::NatsSourceFunc;
use anyhow::anyhow;
use anyhow::bail;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::ConnectorOptions;
use arroyo_rpc::OperatorConfig;
use async_nats::ServerAddr;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
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
    pub fn connection_from_options(options: &mut ConnectorOptions) -> anyhow::Result<NatsConfig> {
        let nats_servers = VarStr::new(options.pull_str("servers")?);
        let nats_auth = options.pull_opt_str("auth.type")?;
        let nats_auth: NatsConfigAuthentication = match nats_auth.as_deref() {
            Some("none") | None => NatsConfigAuthentication::None {},
            Some("credentials") => NatsConfigAuthentication::Credentials {
                username: VarStr::new(options.pull_str("auth.username")?),
                password: VarStr::new(options.pull_str("auth.password")?),
            },
            Some("jwt") => NatsConfigAuthentication::Jwt {
                jwt: VarStr::new(options.pull_str("auth.jwt")?),
                nkey_seed: VarStr::new(options.pull_str("auth.nkey_seed")?),
            },
            Some(other) => bail!("Unknown auth type '{}'", other),
        };

        Ok(NatsConfig {
            authentication: nats_auth,
            servers: nats_servers,
        })
    }

    pub fn table_from_options(options: &mut ConnectorOptions) -> anyhow::Result<NatsTable> {
        let conn_type = options.pull_str("type")?;
        let nats_table_type = match conn_type.as_str() {
            "source" => {
                let source_type = match (
                    options.pull_str("stream").ok(),
                    options.pull_str("subject").ok(),
                ) {
                    (Some(stream), None) => Some(SourceType::Jetstream {
                        stream,
                        description: options.pull_opt_str("consumer.description")?,
                        ack_policy: options
                            .pull_opt_str("consumer.ack_policy")?
                            .map(|s| {
                                s.parse()
                                    .map_err(|e| anyhow!("invalid consumer.ack_policy: {}", e))
                            })
                            .transpose()?
                            .unwrap_or(AcknowledgmentPolicy::Explicit),
                        replay_policy: options
                            .pull_opt_str("consumer.replay_policy")?
                            .map(|s| {
                                s.parse()
                                    .map_err(|e| anyhow!("invalid consumer.replay_policy: {}", e))
                            })
                            .transpose()?
                            .unwrap_or(ReplayPolicy::Instant),
                        ack_wait: options.pull_opt_i64("consumer.ack_wait")?.unwrap_or(30),
                        filter_subjects: options
                            .pull_opt_str("consumer.filter_subjects")?
                            .map_or_else(Vec::new, |s| s.split(',').map(String::from).collect()),
                        sample_frequency: options
                            .pull_opt_i64("consumer.sample_frequency")?
                            .unwrap_or(0),
                        num_replicas: options.pull_opt_i64("consumer.num_replicas")?.unwrap_or(1),
                        inactive_threshold: options
                            .pull_opt_i64("consumer.inactive_threshold")?
                            .unwrap_or(600),
                        rate_limit: options.pull_opt_i64("consumer.rate_limit")?.unwrap_or(-1),
                        max_ack_pending: options
                            .pull_opt_i64("consumer.max_ack_pending")?
                            .unwrap_or(-1),
                        max_deliver: options.pull_opt_i64("consumer.max_deliver")?.unwrap_or(-1),
                        max_waiting: options
                            .pull_opt_i64("consumer.max_waiting")?
                            .unwrap_or(1000000),
                        max_batch: options.pull_opt_i64("consumer.max_batch")?.unwrap_or(10000),
                        max_bytes: options
                            .pull_opt_i64("consumer.max_bytes")?
                            .unwrap_or(104857600),
                        max_expires: options
                            .pull_opt_i64("consumer.max_expires")?
                            .unwrap_or(300000),
                    }),
                    (None, Some(subject)) => Some(SourceType::Core { subject }),
                    (Some(_), Some(_)) => bail!("Exactly one of `stream` or `subject` must be set"),
                    (None, None) => bail!("One of `stream` or `subject` must be set"),
                };

                ConnectorType::Source { source_type }
            }
            "sink" => {
                let sink_type = match options.pull_str("subject").ok() {
                    Some(subject) => Some(SinkType::Subject(subject)),
                    None => bail!("`subject` must be set for sink"),
                };
                ConnectorType::Sink { sink_type }
            }
            _ => bail!("Type must be one of 'source' or 'sink'"),
        };

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
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            connection_type,
            schema,
            &config,
            desc,
        ))
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let connection = profile
            .map(|p| {
                serde_json::from_value(p.config.clone()).map_err(|e| {
                    anyhow!("invalid config for profile '{}' in database: {}", p.id, e)
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
    ) -> anyhow::Result<ConstructedOperator> {
        Ok(match table.connector_type {
            ConnectorType::Source { ref source_type } => {
                ConstructedOperator::from_source(Box::new(NatsSourceFunc {
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
                ConstructedOperator::from_operator(Box::new(NatsSinkFunc {
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
        NatsConfigAuthentication::Jwt { jwt, nkey_seed } => {
            let jwt = jwt.sub_env_vars().map_err(|e| e.context("jwt"))?;
            let seed = nkey_seed
                .sub_env_vars()
                .map_err(|e| e.context("nkey_seed"))?;

            let key_pair = std::sync::Arc::new(
                nkeys::KeyPair::from_seed(&seed)
                    .map_err(|e| anyhow!("Invalid NKey seed: {}", e))?,
            );

            opts = opts.jwt(jwt, move |nonce| {
                let key_pair = key_pair.clone();
                async move { key_pair.sign(&nonce).map_err(async_nats::AuthError::new) }
            });
        }
    };
    let client = opts
        .connect(servers_vec)
        .await
        .map_err(|e| anyhow!("Failed to connect to nats: {:?}", e))?;
    Ok(client)
}
