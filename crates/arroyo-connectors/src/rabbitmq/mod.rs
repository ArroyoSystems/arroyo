use anyhow::{anyhow, bail};
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::{api_types::connections::TestSourceMessage, OperatorConfig};
use rabbitmq_stream_client::types::OffsetSpecification;
use rabbitmq_stream_client::{Environment, TlsConfiguration};
use serde::{Deserialize, Serialize};
use typify::import_types;

use crate::rabbitmq::source::RabbitmqStreamSourceFunc;
use crate::{pull_opt, ConnectionType};

mod source;

pub struct RabbitmqConnector {}

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./rabbitmq.svg");

import_types!(schema = "src/rabbitmq/profile.json");
import_types!(schema = "src/rabbitmq/table.json");

impl Connector for RabbitmqConnector {
    type ProfileT = RabbitmqStreamConfig;
    type TableT = RabbitmqStreamTable;

    fn name(&self) -> &'static str {
        "rabbitmq"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: self.name().to_string(),
            name: "RabbitMQ Stream".to_string(),
            icon: ICON.to_string(),
            description: "RabbitMQ stream source".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: false,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn table_type(
        &self,
        _: Self::ProfileT,
        _: Self::TableT,
    ) -> arroyo_rpc::api_types::connections::ConnectionType {
        ConnectionType::Source
    }

    fn test(
        &self,
        _: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        _schema: Option<&arroyo_rpc::api_types::connections::ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<arroyo_rpc::api_types::connections::TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let message = match config.get_environment().await {
                Ok(environment) => {
                    if let Err(e) = environment.consumer().build(&table.stream).await {
                        TestSourceMessage {
                            error: true,
                            done: true,
                            message: e.to_string(),
                        }
                    } else {
                        TestSourceMessage {
                            error: false,
                            done: true,
                            message: "Successfully validated connection".to_string(),
                        }
                    }
                }
                Err(e) => TestSourceMessage {
                    error: true,
                    done: true,
                    message: e.to_string(),
                },
            };
            tx.send(message).await.unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut std::collections::HashMap<String, String>,
        schema: Option<&arroyo_rpc::api_types::connections::ConnectionSchema>,
        profile: Option<&arroyo_rpc::api_types::connections::ConnectionProfile>,
    ) -> anyhow::Result<arroyo_operator::connector::Connection> {
        let connection_config = match profile {
            Some(connection_profile) => {
                serde_json::from_value(connection_profile.config.clone())
                    .map_err(|e| anyhow!("Failed to parse connection config: {:?}", e))?
            }
            None => {
                let host = options.remove("host");
                let username = options.remove("username");
                let password = options.remove("password");
                let virtual_host = options.remove("virtual_host");
                let port = match options.remove("port") {
                    Some(v) => Some(v.parse::<u16>()?),
                    None => None,
                };

                let tls_config = options
                    .remove("tls_config.enabled")
                    .map(|enabled| TlsConfig {
                        enabled: Some(enabled == "true"),
                        trust_certificates: options
                            .remove("tls_config.trust_certificates")
                            .map(|trust_certificates| trust_certificates == "true"),
                        root_certificates_path: options.remove("tls_config.root_certificates_path"),
                        client_certificates_path: options
                            .remove("tls_config.client_certificates_path"),
                        client_keys_path: options.remove("tls_config.client_keys_path"),
                    });

                let load_balancer_mode = options.remove("load_balancer_mode").map(|t| t == "true");

                RabbitmqStreamConfig {
                    host,
                    username,
                    password,
                    virtual_host,
                    port,
                    load_balancer_mode,
                    tls_config,
                }
            }
        };

        let stream = pull_opt("stream", options)?;
        let table_type = pull_opt("type", options)?;

        let table_type = match table_type.as_str() {
            "source" => {
                let offset = options.remove("source.offset");
                TableType::Source {
                    offset: match offset.as_deref() {
                        Some("first") => SourceOffset::First,
                        Some("next") => SourceOffset::Next,
                        None | Some("last") => SourceOffset::Last,
                        Some(other) => bail!("invalid value for source.offset '{}'", other),
                    },
                }
            }
            _ => {
                bail!("type must 'source'");
            }
        };

        let table = RabbitmqStreamTable {
            stream,
            type_: table_type,
        };

        Self::from_config(self, None, name, connection_config, table, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&arroyo_rpc::api_types::connections::ConnectionSchema>,
    ) -> anyhow::Result<arroyo_operator::connector::Connection> {
        let (typ, desc) = match table.type_ {
            TableType::Source { .. } => (
                ConnectionType::Source,
                format!("RabbitmqStreamSource<{}>", table.stream),
            ),
            TableType::Sink { .. } => (
                ConnectionType::Sink,
                format!("RabbitmqStreamSink<{}>", table.stream),
            ),
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for RabbitMQ Stream connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for RabbitMQ Stream connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection {
            id,
            connector: self.name(),
            name: name.to_string(),
            connection_type: typ,
            schema,
            config: serde_json::to_string(&config).unwrap(),
            description: desc,
        })
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: arroyo_rpc::OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        match table.type_ {
            TableType::Source { offset } => Ok(ConstructedOperator::from_source(Box::new(
                RabbitmqStreamSourceFunc {
                    config: profile,
                    stream: table.stream,
                    offset_mode: offset,
                    format: config
                        .format
                        .ok_or_else(|| anyhow!("format required for rabbitmq stream source"))?,
                    framing: config.framing,
                    bad_data: config.bad_data,
                },
            ))),
            TableType::Sink { .. } => {
                todo!()
            }
        }
    }
}

impl RabbitmqStreamConfig {
    async fn get_environment(&self) -> anyhow::Result<Environment> {
        let builder = Environment::builder()
            .host(&self.host.clone().unwrap_or("localhost".to_owned()))
            .username(&self.username.clone().unwrap_or("guest".to_owned()))
            .password(&self.password.clone().unwrap_or("guest".to_owned()))
            .virtual_host(&self.virtual_host.clone().unwrap_or("/".to_owned()))
            .port(self.port.unwrap_or(5552));

        let builder = if let Some(tls_config) = self.tls_config.clone() {
            builder.tls(tls_config.into())
        } else {
            builder
        };
        let builder = if let Some(load_balancer_mode) = self.load_balancer_mode {
            builder.load_balancer_mode(load_balancer_mode)
        } else {
            builder
        };

        let environment = builder.build().await?;
        Ok(environment)
    }
}

impl SourceOffset {
    pub fn offset(&self) -> OffsetSpecification {
        match self {
            SourceOffset::First => OffsetSpecification::First,
            SourceOffset::Last => OffsetSpecification::Last,
            SourceOffset::Next => OffsetSpecification::Next,
        }
    }
}

impl From<TlsConfig> for TlsConfiguration {
    fn from(val: TlsConfig) -> Self {
        let mut config = TlsConfiguration::default();
        config.enable(val.enabled.unwrap_or(false));
        config.trust_certificates(val.trust_certificates.unwrap_or(false));
        config.add_root_certificates_path(val.root_certificates_path.unwrap_or(String::from("")));
        config.add_client_certificates_keys(
            val.client_certificates_path.unwrap_or(String::from("")),
            val.client_keys_path.clone().unwrap_or(String::from("")),
        );
        config
    }
}
