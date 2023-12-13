use anyhow::{anyhow, bail};
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, TestSourceMessage};
use arroyo_rpc::{var_str::VarStr, OperatorConfig};
use axum::response::sse::Event;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    message::BorrowedMessage,
    ClientConfig, Offset, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Receiver;
use tonic::Status;
use tracing::{error, info, warn};
use typify::import_types;

use crate::{pull_opt, Connection, ConnectionType, send};

use super::Connector;

const CONFIG_SCHEMA: &str = include_str!("../../connector-schemas/kafka/connection.json");
const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/kafka/table.json");
const ICON: &str = include_str!("../resources/kafka.svg");

import_types!(
    schema = "../connector-schemas/kafka/connection.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);
import_types!(schema = "../connector-schemas/kafka/table.json");

pub struct KafkaConnector {}

impl KafkaConnector {
    pub fn connection_from_options(
        options: &mut HashMap<String, String>,
    ) -> anyhow::Result<KafkaConfig> {
        let auth = options.remove("auth.type");
        let auth = match auth.as_ref().map(|t| t.as_str()) {
            Some("none") | None => KafkaConfigAuthentication::None {},
            Some("sasl") => KafkaConfigAuthentication::Sasl {
                mechanism: pull_opt("auth.mechanism", options)?,
                protocol: pull_opt("auth.protocol", options)?,
                username: VarStr::new(pull_opt("auth.username", options)?),
                password: VarStr::new(pull_opt("auth.password", options)?),
            },
            Some(other) => bail!("unknown auth type '{}'", other),
        };

        let schema_registry = options.remove("schema_registry.endpoint").map(|endpoint| {
            let api_key = options.remove("schema_registry.api_key").map(VarStr::new);
            let api_secret = options
                .remove("schema_registry.api_secret")
                .map(VarStr::new);
            SchemaRegistry::ConfluentSchemaRegistry {
                endpoint,
                api_key,
                api_secret,
            }
        });
        Ok(KafkaConfig {
            authentication: auth,
            bootstrap_servers: BootstrapServers(pull_opt("bootstrap_servers", options)?),
            schema_registry_enum: schema_registry,
        })
    }

    pub fn table_from_options(options: &mut HashMap<String, String>) -> anyhow::Result<KafkaTable> {
        let typ = pull_opt("type", options)?;
        let table_type = match typ.as_str() {
            "source" => {
                let offset = options.remove("source.offset");
                TableType::Source {
                    offset: match offset.as_ref().map(|f| f.as_str()) {
                        Some("earliest") => SourceOffset::Earliest,
                        None | Some("latest") => SourceOffset::Latest,
                        Some(other) => bail!("invalid value for source.offset '{}'", other),
                    },
                    read_mode: match options
                        .remove("source.read_mode")
                        .as_ref()
                        .map(|f| f.as_str())
                    {
                        Some("read_committed") => Some(ReadMode::ReadCommitted),
                        Some("read_uncommitted") | None => Some(ReadMode::ReadUncommitted),
                        Some(other) => bail!("invalid value for source.read_mode '{}'", other),
                    },
                    group_id: options.remove("source.group_id"),
                }
            }
            "sink" => {
                let commit_mode = options.remove("sink.commit_mode");
                TableType::Sink {
                    commit_mode: match commit_mode.as_ref().map(|f| f.as_str()) {
                        Some("at_least_once") | None => SinkCommitMode::AtLeastOnce,
                        Some("exactly_once") => SinkCommitMode::ExactlyOnce,
                        Some(other) => bail!("invalid value for commit_mode '{}'", other),
                    },
                }
            }
            _ => {
                bail!("type must be one of 'source' or 'sink")
            }
        };

        Ok(KafkaTable {
            topic: pull_opt("topic", options)?,
            type_: table_type,
            client_configs: HashMap::new(),
        })
    }
}

impl Connector for KafkaConnector {
    type ProfileT = KafkaConfig;
    type TableT = KafkaTable;

    fn name(&self) -> &'static str {
        "kafka"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "kafka".to_string(),
            name: "Kafka".to_string(),
            icon: ICON.to_string(),
            description: "Read and write from a Kafka cluster".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn config_description(&self, config: Self::ProfileT) -> String {
        (*config.bootstrap_servers).clone()
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: KafkaConfig,
        table: KafkaTable,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let (typ, operator, desc) = match table.type_ {
            TableType::Source { .. } => (
                ConnectionType::Source,
                "connectors::kafka::source::KafkaSourceFunc",
                format!("KafkaSource<{}>", table.topic),
            ),
            TableType::Sink { .. } => (
                ConnectionType::Sink,
                "connectors::kafka::sink::KafkaSinkFunc::<#in_k, #in_t>",
                format!("KafkaSink<{}>", table.topic),
            ),
        };

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
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: typ,
            schema,
            operator: operator.to_string(),
            config: serde_json::to_string(&config).unwrap(),
            description: desc,
        })
    }

    fn test_profile(&self, profile: Self::ProfileT) -> Option<Receiver<TestSourceMessage>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let tester = KafkaTester {
                connection: profile,
            };

            let mut message = tester.test_connection().await;
            message.done = true;
            tx.send(message).unwrap();
        });

        Some(rx)
    }

    fn test(
        &self,
        _: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<Result<Event, Infallible>>,
    ) {
        let tester = KafkaTester {
            connection: config,
        };

        tester.start(table, tx);
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.type_ {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink { .. } => ConnectionType::Sink,
        }
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
                    anyhow!("invalid config for profile '{}' in database: {}", p.id, e)
                })
            })
            .unwrap_or_else(|| Self::connection_from_options(options))?;

        let table = Self::table_from_options(options)?;

        Self::from_config(&self, None, name, connection, table, schema)
    }
}

pub struct KafkaTester {
    pub connection: KafkaConfig,
}

pub struct TopicMetadata {
    pub partitions: usize,
}

impl KafkaTester {
    async fn connect(&self) -> Result<BaseConsumer, String> {
        let mut client_config = ClientConfig::new();
        client_config
            .set(
                "bootstrap.servers",
                &self.connection.bootstrap_servers.to_string(),
            )
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("group.id", "arroyo-kafka-source-tester");

        match &self.connection.authentication {
            KafkaConfigAuthentication::None {} => {}
            KafkaConfigAuthentication::Sasl {
                mechanism,
                password,
                protocol,
                username,
            } => {
                client_config.set("sasl.mechanism", mechanism);
                client_config.set("security.protocol", protocol);
                client_config.set(
                    "sasl.username",
                    username.sub_env_vars().map_err(|e| e.to_string())?,
                );
                client_config.set(
                    "sasl.password",
                    password.sub_env_vars().map_err(|e| e.to_string())?,
                );
            }
        };

        let client: BaseConsumer = client_config
            .create()
            .map_err(|e| format!("Failed to connect: {:?}", e))?;

        client
            .fetch_metadata(None, Duration::from_secs(10))
            .map_err(|e| format!("Failed to connect to Kafka: {:?}", e))?;

        Ok(client)
    }

    #[allow(unused)]
    pub async fn topic_metadata(&self, topic: &str) -> Result<TopicMetadata, Status> {
        let client = self.connect().await.map_err(Status::failed_precondition)?;
        let metadata = client
            .fetch_metadata(Some(&topic), Duration::from_secs(5))
            .map_err(|e| {
                Status::failed_precondition(format!(
                    "Failed to read topic metadata from Kafka: {:?}",
                    e
                ))
            })?;

        let topic_metadata = metadata.topics().iter().next().ok_or_else(|| {
            Status::failed_precondition("Metadata response from broker did not include topic")
        })?;

        if let Some(e) = topic_metadata.error() {
            let err = match e {
                rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED => {
                    "Not authorized to access topic".to_string()
                }
                rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART => {
                    "Topic does not exist".to_string()
                }
                _ => format!("Error while fetching topic metadata: {:?}", e),
            };
            return Err(Status::failed_precondition(err));
        }

        Ok(TopicMetadata {
            partitions: topic_metadata.partitions().len(),
        })
    }

    async fn test(&self, table: KafkaTable, mut tx: Sender<Result<Event, Infallible>>) -> Result<(), String> {
        let client = self.connect().await?;

        self.info(&mut tx, "Connected to Kafka").await;

        let topic = table.topic.clone();

        let metadata = client
            .fetch_metadata(Some(&topic), Duration::from_secs(10))
            .map_err(|e| format!("Failed to fetch metadata: {:?}", e))?;

        self.info(&mut tx, "Fetched topic metadata").await;

        {
            let topic_metadata = metadata.topics().get(0).ok_or_else(|| {
                format!(
                    "Returned metadata was empty; unable to subscribe to topic '{}'",
                    topic
                )
            })?;

            if let Some(err) = topic_metadata.error() {
                match err {
                    rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION
                    | rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC
                    | rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART => {
                        return Err(format!(
                            "Topic '{}' does not exist in the configured Kafka cluster",
                            topic
                        ));
                    }
                    e => {
                        error!("Unhandled Kafka error while fetching metadata: {:?}", e);
                        return Err(format!(
                            "Something went wrong while fetching topic metadata: {:?}",
                            e
                        ));
                    }
                }
            }

            let map = topic_metadata
                .partitions()
                .iter()
                .map(|p| ((topic.clone(), p.id()), Offset::Beginning))
                .collect();

            client
                .assign(&TopicPartitionList::from_topic_map(&map).unwrap())
                .map_err(|e| format!("Failed to subscribe to topic '{}': {:?}", topic, e))?;
        }

        if let TableType::Source { .. } = table.type_ {
            self.info(&mut tx, "Waiting for messages").await;

            let start = Instant::now();
            let timeout = Duration::from_secs(30);
            while start.elapsed() < timeout {
                match client.poll(Duration::ZERO) {
                    Some(Ok(message)) => {
                        self.info(&mut tx, "Received message from Kafka").await;
                        self.test_schema(message)?;
                        return Ok(());
                    }
                    Some(Err(e)) => {
                        warn!("Error while reading from kafka in test: {:?}", e);
                        return Err(format!("Error while reading messages from Kafka: {}", e));
                    }
                    None => {
                        // wait
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }

            return Err(format!(
                "No messages received from Kafka within {} seconds",
                timeout.as_secs()
            ));
        }

        Ok(())
    }

    fn test_schema(&self, _: BorrowedMessage) -> Result<(), String> {
        // TODO: test the schema against the message
        Ok(())
    }

    async fn info(&self, tx: &mut Sender<Result<Event, Infallible>>, s: impl Into<String>) {
        send(tx, TestSourceMessage {
            error: false,
            done: false,
            message: s.into(),
        })
        .await;
    }

    #[allow(unused)]
    pub async fn test_connection(&self) -> TestSourceMessage {
        match self.connect().await {
            Ok(_) => TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully connected to Kafka".to_string(),
            },
            Err(e) => TestSourceMessage {
                error: true,
                done: true,
                message: e,
            },
        }
    }

    pub fn start(self, table: KafkaTable, mut tx: Sender<Result<Event, Infallible>>) {
        tokio::spawn(async move {
            info!("Started kafka tester");
            if let Err(e) = self.test(table, tx.clone()).await {
                send(&mut tx, TestSourceMessage {
                    error: true,
                    done: true,
                    message: e,
                })
                .await;
            } else {
                send(&mut tx, TestSourceMessage {
                    error: false,
                    done: true,
                    message: "Connection is valid".to_string(),
                })
                .await;
            }
        });
    }
}
