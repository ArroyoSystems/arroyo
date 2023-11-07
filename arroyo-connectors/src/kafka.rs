use anyhow::{anyhow, bail};
use arroyo_rpc::OperatorConfig;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use typify::import_types;

use axum::response::sse::Event;
use std::time::{Duration, Instant};

use arroyo_rpc::api_types::connections::{ConnectionSchema, TestSourceMessage};
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    message::BorrowedMessage,
    ClientConfig, Offset, TopicPartitionList,
};
use tokio::sync::mpsc::Sender;
use tonic::Status;
use tracing::{error, info, warn};

use crate::{pull_opt, Connection, ConnectionType};

use super::Connector;

const CONFIG_SCHEMA: &str = include_str!("../../connector-schemas/kafka/connection.json");
const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/kafka/table.json");
const ICON: &str = include_str!("../resources/kafka.svg");

import_types!(schema = "../connector-schemas/kafka/connection.json",);
import_types!(schema = "../connector-schemas/kafka/table.json");

pub struct KafkaConnector {}

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
            description: "Confluent Cloud, Amazon MSK, or self-hosted".to_string(),
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
            table,
            tx,
        };

        tester.start();
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
        opts: &mut std::collections::HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let auth = opts.remove("auth.type");
        let auth = match auth.as_ref().map(|t| t.as_str()) {
            Some("none") | None => KafkaConfigAuthentication::None {},
            Some("sasl") => KafkaConfigAuthentication::Sasl {
                mechanism: pull_opt("auth.mechanism", opts)?,
                protocol: pull_opt("auth.protocol", opts)?,
                username: pull_opt("auth.username", opts)?,
                password: pull_opt("auth.password", opts)?,
            },
            Some(other) => bail!("unknown auth type '{}'", other),
        };

        let schema_registry = opts
            .remove("schema_registry.endpoint")
            .map(|endpoint| SchemaRegistry { endpoint });

        let connection = KafkaConfig {
            authentication: auth,
            bootstrap_servers: BootstrapServers(pull_opt("bootstrap_servers", opts)?),
            schema_registry,
        };

        let typ = pull_opt("type", opts)?;
        let table_type = match typ.as_str() {
            "source" => {
                let offset = opts.remove("source.offset");
                TableType::Source {
                    offset: match offset.as_ref().map(|f| f.as_str()) {
                        Some("earliest") => SourceOffset::Earliest,
                        None | Some("latest") => SourceOffset::Latest,
                        Some(other) => bail!("invalid value for source.offset '{}'", other),
                    },
                    read_mode: match opts.remove("source.read_mode").as_ref().map(|f| f.as_str()) {
                        Some("read_committed") => Some(ReadMode::ReadCommitted),
                        Some("read_uncommitted") | None => Some(ReadMode::ReadUncommitted),
                        Some(other) => bail!("invalid value for source.read_mode '{}'", other),
                    },
                    group_id: opts.remove("source.group_id"),
                }
            }
            "sink" => {
                let commit_mode = opts.remove("sink.commit_mode");
                TableType::Sink {
                    commit_mode: match commit_mode.as_ref().map(|f| f.as_str()) {
                        Some("at_least_once") | None => Some(SinkCommitMode::AtLeastOnce),
                        Some("exactly_once") => Some(SinkCommitMode::ExactlyOnce),
                        Some(other) => bail!("invalid value for commit_mode '{}'", other),
                    },
                }
            }
            _ => {
                bail!("type must be one of 'source' or 'sink")
            }
        };

        let table = KafkaTable {
            topic: pull_opt("topic", opts)?,
            type_: table_type,
        };

        Self::from_config(&self, None, name, connection, table, schema)
    }
}

struct KafkaTester {
    connection: KafkaConfig,
    table: KafkaTable,
    tx: Sender<Result<Event, Infallible>>,
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
                client_config.set("sasl.username", username);
                client_config.set("sasl.password", password);
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
    pub async fn topic_metadata(&self) -> Result<TopicMetadata, Status> {
        let client = self.connect().await.map_err(Status::failed_precondition)?;
        let metadata = client
            .fetch_metadata(Some(&self.table.topic), Duration::from_secs(5))
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

    async fn test(&self) -> Result<(), String> {
        let client = self.connect().await?;

        self.info("Connected to Kafka").await;

        let topic = self.table.topic.clone();

        let metadata = client
            .fetch_metadata(Some(&topic), Duration::from_secs(10))
            .map_err(|e| format!("Failed to fetch metadata: {:?}", e))?;

        self.info("Fetched topic metadata").await;

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

        if let TableType::Source { .. } = self.table.type_ {
            self.info("Waiting for messages").await;

            let start = Instant::now();
            let timeout = Duration::from_secs(30);
            while start.elapsed() < timeout {
                match client.poll(Duration::ZERO) {
                    Some(Ok(message)) => {
                        self.info("Received message from Kafka").await;
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

    async fn info(&self, s: impl Into<String>) {
        self.send(TestSourceMessage {
            error: false,
            done: false,
            message: s.into(),
        })
        .await;
    }

    async fn send(&self, msg: TestSourceMessage) {
        if self
            .tx
            .send(Ok(Event::default().json_data(msg).unwrap()))
            .await
            .is_err()
        {
            warn!("Test API rx closed while sending message");
        }
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

    pub fn start(self) {
        tokio::spawn(async move {
            info!("Started kafka tester");
            if let Err(e) = self.test().await {
                self.send(TestSourceMessage {
                    error: true,
                    done: true,
                    message: e,
                })
                .await;
            } else {
                self.send(TestSourceMessage {
                    error: false,
                    done: true,
                    message: "Connection is valid".to_string(),
                })
                .await;
            }
        });
    }
}
