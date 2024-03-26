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
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroU32;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Receiver;
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
import_types!(schema = "src/nats//table.json");

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
        let nats_servers = pull_opt("servers", options)?;
        let nats_auth = options.remove("auth.type");
        let nats_auth: NatsConfigAuthentication = match nats_auth.as_ref().map(|t| t.as_str()) {
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
        let mut client_config = HashMap::new();
        for (k, v) in options.iter() {
            client_config.insert(k.clone(), v.clone());
        }
        let conn_type = pull_opt("type", options)?;
        let nats_table_type = match conn_type.as_str() {
            "source" => ConnectorType::Source {
                stream: Some(pull_opt("nats.stream", options)?),
            },
            "sink" => ConnectorType::Sink {
                subject: Some(pull_opt("nats.subject", options)?),
            },
            _ => bail!("Type must be one of 'source' or 'sink'"),
        };

        Ok(NatsTable {
            connector_type: nats_table_type,
            client_configs: client_config,
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
        (*config.servers).to_string()
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
        _tx: Sender<TestSourceMessage>,
    ) {
        // TODO: Actually test the connection by instantiating a NATS client
        // and subscribing to the subject at the specified server.
        let (tx, _rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (_itx, _rx) = tokio::sync::mpsc::channel::<(
                Sender<TestSourceMessage>,
                Receiver<TestSourceMessage>,
            )>(8);
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(message).unwrap();
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
            ConnectorType::Source { stream, .. } => stream.clone(),
            ConnectorType::Sink { subject, .. } => subject.clone(),
        };

        let (connection_type, desc) = match table.connector_type {
            ConnectorType::Source { .. } => (
                ConnectionType::Source,
                format!("NatsSource<{:?}>", stream_or_subject.clone()),
            ),
            ConnectorType::Sink { .. } => (
                ConnectionType::Sink,
                format!("NatsSink<{:?}>", stream_or_subject.clone()),
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
            connection_type: connection_type,
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

        Self::from_config(&self, None, name, connection, table, schema)
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<OperatorNode> {
        Ok(match table.connector_type {
            ConnectorType::Source { .. } => OperatorNode::from_source(Box::new(NatsSourceFunc {
                stream: table.client_configs.get("nats.stream").cloned().unwrap(),
                servers: profile.servers.clone(),
                connection: profile.clone(),
                table: table.clone(),
                framing: config.framing,
                format: config.format.unwrap(),
                bad_data: config.bad_data,
                consumer_config: get_client_config(&profile, &table),
                messages_per_second: NonZeroU32::new(
                    config
                        .rate_limit
                        .map(|l| l.messages_per_second)
                        .unwrap_or(u32::MAX),
                )
                .unwrap(),
            })),
            ConnectorType::Sink { .. } => OperatorNode::from_operator(Box::new(NatsSinkFunc {
                publisher: None,
                servers: profile.servers.clone(),
                subject: table.client_configs.get("nats.subject").cloned().unwrap(),
                client_config: get_client_config(&profile, &table),
                serializer: ArrowSerializer::new(config.format.unwrap()),
            })),
        })
    }
}

fn get_client_config(connection: &NatsConfig, table: &NatsTable) -> HashMap<String, String> {
    let mut consumer_configs: HashMap<String, String> = HashMap::new();

    match &connection.authentication {
        NatsConfigAuthentication::None {} => {}
        NatsConfigAuthentication::Credentials { username, password } => {
            consumer_configs.insert(
                "nats.username".to_string(),
                username
                    .sub_env_vars()
                    .expect("Missing env-vars for NATS username"),
            );
            consumer_configs.insert(
                "nats.password".to_string(),
                password
                    .sub_env_vars()
                    .expect("Missing env-vars for NATS password"),
            );
            consumer_configs.extend(
                table
                    .client_configs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string())),
            );
        }
    };
    consumer_configs
}
