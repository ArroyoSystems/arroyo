use arroyo_sql::{ArroyoSchemaProvider, ConnectorType};
use serde::{Deserialize, Serialize};
use typify::import_types;

use std::{
    time::{Duration, Instant},
};

use arroyo_rpc::grpc::{
    self,
    api::{ConnectionSchema, TestSourceMessage},
};
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    message::BorrowedMessage,
    ClientConfig, Offset, TopicPartitionList,
};
use tokio::sync::mpsc::Sender;
use tonic::Status;
use tracing::{error, info, warn};

use super::{schema_defs, schema_type, serialization_mode, Connector, OperatorConfig};

const CONFIG_SCHEMA: &str = include_str!("../../../connector-schemas/kafka/connection.json");
const TABLE_SCHEMA: &str = include_str!("../../../connector-schemas/kafka/table.json");

import_types!(schema = "../connector-schemas/kafka/connection.json",);
import_types!(schema = "../connector-schemas/kafka/table.json");

pub struct KafkaConnector {}

impl Connector for KafkaConnector {
    type ConfigT = KafkaConfig;
    type TableT = KafkaTable;

    fn name(&self) -> &'static str {
        "kafka"
    }

    fn metadata(&self) -> grpc::api::Connector {
        grpc::api::Connector {
            id: "kafka".to_string(),
            name: "Kafka".to_string(),
            icon: "SiApachekafka".to_string(),
            description: "Confluent Cloud, Amazon MSK, or self-hosted".to_string(),
            enabled: true,
            source: true,
            sink: true,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn register(
        &self,
        name: &str,
        config: KafkaConfig,
        table: KafkaTable,
        schema: Option<&ConnectionSchema>,
        provider: &mut ArroyoSchemaProvider,
    ) {
        let (typ, operator, desc) = match table.type_ {
            TableType::Source { .. } => (
                ConnectorType::Source,
                "connectors::kafka::source::KafkaSourceFunc",
                format!("KafkaSource<{}>", table.topic),
            ),
            TableType::Sink { .. } => (
                ConnectorType::Sink,
                "connectors::kafka::sink::KafkaSinkFunc",
                format!("KafkaSink<{}>", table.topic),
            ),
        };

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            serialization_mode: serialization_mode(schema.as_ref().unwrap()),
        };

        if let Some(defs) = schema_defs(name, schema.as_ref().unwrap()) {
            provider.add_defs(name, defs);
        }

        provider.add_connector_table(
            name.to_string(),
            typ,
            schema
                .as_ref()
                .unwrap()
                .fields
                .iter()
                .map(|t| t.clone().into())
                .collect(),
            schema_type(name, schema.as_ref().unwrap()),
            operator.to_string(),
            serde_json::to_string(&config).unwrap(),
            desc,
            serialization_mode(schema.as_ref().unwrap()).into(),
        );
    }

    fn test(
        &self,
        _: &str,
        config: Self::ConfigT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<Result<TestSourceMessage, Status>>,
    ) {
        let tester = KafkaTester {
            connection: config,
            table,
            tx,
        };

        tester.start();
    }

    fn table_type(&self, config: Self::ConfigT, table: Self::TableT) -> grpc::api::TableType {
        match table.type_ {
            TableType::Source { .. } => grpc::api::TableType::Source,
            TableType::Sink { .. } => grpc::api::TableType::Sink,
        }
    }
}


struct KafkaTester {
    connection: KafkaConfig,
    table: KafkaTable,
    tx: Sender<Result<TestSourceMessage, Status>>,
}

pub struct TopicMetadata {
    pub partitions: usize,
}

impl KafkaTester {
    async fn connect(&self) -> Result<BaseConsumer, String> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.connection.bootstrap_servers.to_string())
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("group.id", "arroyo-kafka-source-tester");

        match &self.connection.authentication {
            KafkaConfigAuthentication::None { } => {},
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
            },
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
            .fetch_metadata(
                Some(&self.table.topic),
                Duration::from_secs(5),
            )
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

        self.info("Waiting for messsages").await;

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
                    warn!("Error while reading from kafka in source test: {:?}", e);
                    return Err(format!("Error while reading messages from Kafka: {}", e));
                }
                None => {
                    // wait
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Err(format!(
            "No messages received from Kafka within {} seconds",
            timeout.as_secs()
        ))
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
        if self.tx.send(Ok(msg)).await.is_err() {
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
                    message: "Source and schema are valid".to_string(),
                })
                .await;
            }
        });
    }
}
