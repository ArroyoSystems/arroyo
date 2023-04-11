use std::time::{Duration, Instant};

use arroyo_rpc::grpc::api::{source_schema::Schema, KafkaConnection, TestSourceMessage};
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    message::BorrowedMessage,
    ClientConfig, Offset, TopicPartitionList,
};
use tokio::sync::mpsc::Sender;
use tonic::Status;
use tracing::{error, info, warn};

use crate::{required_field, sources::auth_config_to_hashmap};

pub struct KafkaTester {
    connection: KafkaConnection,
    topic: Option<String>,
    #[allow(unused)]
    schema: Option<Schema>,
    tx: Sender<Result<TestSourceMessage, Status>>,
}

pub struct TopicMetadata {
    pub partitions: usize,
}

impl KafkaTester {
    pub fn new(
        connection: KafkaConnection,
        topic: Option<String>,
        schema: Option<Schema>,
        tx: Sender<Result<TestSourceMessage, Status>>,
    ) -> Self {
        Self {
            connection,
            topic,
            schema,
            tx,
        }
    }

    async fn connect(&self) -> Result<BaseConsumer, String> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.connection.bootstrap_servers)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("group.id", "arroyo-kafka-source-tester");
        for (key, value) in &auth_config_to_hashmap(self.connection.auth_config.clone()) {
            client_config.set(key, value);
        }
        let client: BaseConsumer = client_config
            .create()
            .map_err(|e| format!("Failed to connect: {:?}", e))?;

        client
            .fetch_metadata(None, Duration::from_secs(10))
            .map_err(|e| format!("Failed to connect to Kafka: {:?}", e))?;

        Ok(client)
    }

    pub async fn topic_metadata(&self) -> Result<TopicMetadata, Status> {
        let client = self.connect().await.map_err(Status::failed_precondition)?;
        let metadata = client
            .fetch_metadata(
                Some(
                    self.topic
                        .as_ref()
                        .ok_or_else(|| required_field("type_oneof.kafka.topic"))?,
                ),
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

        let Some(topic) = &self.topic else {
            return Ok(());
        };

        let metadata = client
            .fetch_metadata(Some(topic), Duration::from_secs(10))
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
