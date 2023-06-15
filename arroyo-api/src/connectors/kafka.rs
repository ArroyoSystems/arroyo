use arroyo_sql::{ArroyoSchemaProvider, ConnectorType};
use serde::{Deserialize, Serialize};
use typify::import_types;

use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use arroyo_rpc::grpc::{api::{source_schema::Schema, SourceSchema, TestSourceMessage}, self};
use arroyo_types::string_to_map;
use http::{HeaderMap, HeaderName, HeaderValue};
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    message::BorrowedMessage,
    ClientConfig, Offset, TopicPartitionList,
};
use tokio::sync::mpsc::Sender;
use tonic::Status;
use tracing::{error, info, warn};

use crate::required_field;

use super::{
    schema_defs, schema_fields, schema_type, serialization_mode, Connector, OperatorConfig,
};

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
        schema: Option<SourceSchema>,
        provider: &mut ArroyoSchemaProvider,
    ) {
        let (typ, operator, desc) = match table.type_ {
            TableType::Source(_) => (
                ConnectorType::Source,
                "connectors::kafka::source::KafkaSourceFunc",
                format!("KafkaSource<{}>", table.topic),
            ),
            TableType::Sink(_) => (
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
            schema_fields(name, schema.as_ref().unwrap()).unwrap(),
            schema_type(name, schema.as_ref().unwrap()),
            operator.to_string(),
            serde_json::to_string(&config).unwrap(),
            desc,
            serialization_mode(schema.as_ref().unwrap()).into(),
        );
    }

}

// pub fn auth_config_to_hashmap(config: Option<KafkaAuthConfig>) -> HashMap<String, String> {
//     match config.and_then(|config| config.auth_type) {
//         None | Some(AuthType::NoAuth(_)) => HashMap::default(),
//         Some(AuthType::SaslAuth(sasl_auth)) => vec![
//             ("security.protocol".to_owned(), sasl_auth.protocol),
//             ("sasl.mechanism".to_owned(), sasl_auth.mechanism),
//             ("sasl.username".to_owned(), sasl_auth.username),
//             ("sasl.password".to_owned(), sasl_auth.password),
//         ]
//         .into_iter()
//         .collect(),
//     }
// }

/*
pub struct KafkaSource {
    topic: String,
    bootstrap_servers: Vec<String>,
    offset_mode: OffsetMode,
    messages_per_second: u32,
}

impl KafkaSource {
    pub fn new(topic: &str, bootstrap_servers: Vec<&str>, offset_mode: OffsetMode) -> Self {
        Self {
            topic: topic.to_string(),
            bootstrap_servers: bootstrap_servers.iter().map(|s| s.to_string()).collect(),
            offset_mode,
            messages_per_second: 10_000,
        }
    }

    pub fn new_with_schema_registry(
        topic: &str,
        bootstrap_servers: Vec<&str>,
        offset_mode: OffsetMode,
    ) -> Self {
        Self {
            topic: topic.to_string(),
            bootstrap_servers: bootstrap_servers.iter().map(|s| s.to_string()).collect(),
            offset_mode,
            messages_per_second: 10_000,
        }
    }
}

impl Source<Vec<u8>> for KafkaSource {
    fn as_operator(&self) -> Operator {
        Operator::KafkaSource {
            topic: self.topic.clone(),
            bootstrap_servers: self.bootstrap_servers.clone(),
            offset_mode: self.offset_mode,
            kafka_input_format: SerializationMode::Json,
            messages_per_second: self.messages_per_second,
            client_configs: HashMap::default(),
        }
    }
}

pub struct KeyedKafkaSink {
    topic: String,
    bootstrap_servers: Vec<String>,
}

impl KeyedKafkaSink {
    pub fn new(topic: String, bootstrap_servers: Vec<String>) -> KeyedKafkaSink {
        KeyedKafkaSink {
            topic,
            bootstrap_servers,
        }
    }
}
impl KeyedSink<Vec<u8>, Vec<u8>> for KeyedKafkaSink {
    fn as_operator(&self) -> Operator {
        Operator::KafkaSink {
            topic: self.topic.clone(),
            bootstrap_servers: self.bootstrap_servers.clone(),
            client_configs: HashMap::default(),
        }
    }
}


 */

// pub struct KafkaTester {
//     connection: KafkaConnection,
//     topic: Option<String>,
//     #[allow(unused)]
//     schema: Option<Schema>,
//     tx: Sender<Result<TestSourceMessage, Status>>,
// }

// pub struct TopicMetadata {
//     pub partitions: usize,
// }

// impl KafkaTester {
//     pub fn new(
//         connection: KafkaConnection,
//         topic: Option<String>,
//         schema: Option<Schema>,
//         tx: Sender<Result<TestSourceMessage, Status>>,
//     ) -> Self {
//         Self {
//             connection,
//             topic,
//             schema,
//             tx,
//         }
//     }

//     async fn connect(&self) -> Result<BaseConsumer, String> {
//         let mut client_config = ClientConfig::new();
//         client_config
//             .set("bootstrap.servers", &self.connection.bootstrap_servers)
//             .set("enable.auto.commit", "false")
//             .set("auto.offset.reset", "earliest")
//             .set("group.id", "arroyo-kafka-source-tester");
//         for (key, value) in &auth_config_to_hashmap(self.connection.auth_config.clone()) {
//             client_config.set(key, value);
//         }
//         let client: BaseConsumer = client_config
//             .create()
//             .map_err(|e| format!("Failed to connect: {:?}", e))?;

//         client
//             .fetch_metadata(None, Duration::from_secs(10))
//             .map_err(|e| format!("Failed to connect to Kafka: {:?}", e))?;

//         Ok(client)
//     }

//     pub async fn topic_metadata(&self) -> Result<TopicMetadata, Status> {
//         let client = self.connect().await.map_err(Status::failed_precondition)?;
//         let metadata = client
//             .fetch_metadata(
//                 Some(
//                     self.topic
//                         .as_ref()
//                         .ok_or_else(|| required_field("type_oneof.kafka.topic"))?,
//                 ),
//                 Duration::from_secs(5),
//             )
//             .map_err(|e| {
//                 Status::failed_precondition(format!(
//                     "Failed to read topic metadata from Kafka: {:?}",
//                     e
//                 ))
//             })?;

//         let topic_metadata = metadata.topics().iter().next().ok_or_else(|| {
//             Status::failed_precondition("Metadata response from broker did not include topic")
//         })?;

//         if let Some(e) = topic_metadata.error() {
//             let err = match e {
//                 rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED => {
//                     "Not authorized to access topic".to_string()
//                 }
//                 rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART => {
//                     "Topic does not exist".to_string()
//                 }
//                 _ => format!("Error while fetching topic metadata: {:?}", e),
//             };
//             return Err(Status::failed_precondition(err));
//         }

//         Ok(TopicMetadata {
//             partitions: topic_metadata.partitions().len(),
//         })
//     }

//     async fn test(&self) -> Result<(), String> {
//         let client = self.connect().await?;

//         self.info("Connected to Kafka").await;

//         let Some(topic) = &self.topic else {
//             return Ok(());
//         };

//         let metadata = client
//             .fetch_metadata(Some(topic), Duration::from_secs(10))
//             .map_err(|e| format!("Failed to fetch metadata: {:?}", e))?;

//         self.info("Fetched topic metadata").await;

//         {
//             let topic_metadata = metadata.topics().get(0).ok_or_else(|| {
//                 format!(
//                     "Returned metadata was empty; unable to subscribe to topic '{}'",
//                     topic
//                 )
//             })?;

//             if let Some(err) = topic_metadata.error() {
//                 match err {
//                     rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION
//                     | rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC
//                     | rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART => {
//                         return Err(format!(
//                             "Topic '{}' does not exist in the configured Kafka cluster",
//                             topic
//                         ));
//                     }
//                     e => {
//                         error!("Unhandled Kafka error while fetching metadata: {:?}", e);
//                         return Err(format!(
//                             "Something went wrong while fetching topic metadata: {:?}",
//                             e
//                         ));
//                     }
//                 }
//             }

//             let map = topic_metadata
//                 .partitions()
//                 .iter()
//                 .map(|p| ((topic.clone(), p.id()), Offset::Beginning))
//                 .collect();

//             client
//                 .assign(&TopicPartitionList::from_topic_map(&map).unwrap())
//                 .map_err(|e| format!("Failed to subscribe to topic '{}': {:?}", topic, e))?;
//         }

//         self.info("Waiting for messsages").await;

//         let start = Instant::now();
//         let timeout = Duration::from_secs(30);
//         while start.elapsed() < timeout {
//             match client.poll(Duration::ZERO) {
//                 Some(Ok(message)) => {
//                     self.info("Received message from Kafka").await;
//                     self.test_schema(message)?;
//                     return Ok(());
//                 }
//                 Some(Err(e)) => {
//                     warn!("Error while reading from kafka in source test: {:?}", e);
//                     return Err(format!("Error while reading messages from Kafka: {}", e));
//                 }
//                 None => {
//                     // wait
//                 }
//             }

//             tokio::time::sleep(Duration::from_millis(500)).await;
//         }

//         Err(format!(
//             "No messages received from Kafka within {} seconds",
//             timeout.as_secs()
//         ))
//     }

//     fn test_schema(&self, _: BorrowedMessage) -> Result<(), String> {
//         // TODO: test the schema against the message
//         Ok(())
//     }

//     async fn info(&self, s: impl Into<String>) {
//         self.send(TestSourceMessage {
//             error: false,
//             done: false,
//             message: s.into(),
//         })
//         .await;
//     }

//     async fn send(&self, msg: TestSourceMessage) {
//         if self.tx.send(Ok(msg)).await.is_err() {
//             warn!("Test API rx closed while sending message");
//         }
//     }

//     pub async fn test_connection(&self) -> TestSourceMessage {
//         match self.connect().await {
//             Ok(_) => TestSourceMessage {
//                 error: false,
//                 done: true,
//                 message: "Successfully connected to Kafka".to_string(),
//             },
//             Err(e) => TestSourceMessage {
//                 error: true,
//                 done: true,
//                 message: e,
//             },
//         }
//     }

//     pub fn start(self) {
//         tokio::spawn(async move {
//             info!("Started kafka tester");
//             if let Err(e) = self.test().await {
//                 self.send(TestSourceMessage {
//                     error: true,
//                     done: true,
//                     message: e,
//                 })
//                 .await;
//             } else {
//                 self.send(TestSourceMessage {
//                     error: false,
//                     done: true,
//                     message: "Source and schema are valid".to_string(),
//                 })
//                 .await;
//             }
//         });
//     }
// }

// fn get_kafka_tester(
//     config: &KafkaSourceConfig,
//     schema: Option<Schema>,
//     connections: Vec<Connection>,
//     tx: Sender<Result<TestSourceMessage, Status>>,
// ) -> Result<KafkaTester, Status> {
//     let connection = connections
//         .into_iter()
//         .find(|c| c.name == config.connection)
//         .ok_or_else(|| {
//             Status::invalid_argument(format!(
//                 "Invalid kafka definition: no connection with name {}",
//                 config.connection
//             ))
//         })?;

//     if let Some(ConnectionType::Kafka(conn)) = connection.connection_type {
//         Ok(KafkaTester::new(
//             conn,
//             Some(config.topic.clone()),
//             schema,
//             tx,
//         ))
//     } else {
//         Err(Status::invalid_argument(format!(
//             "Configured connection '{}' is not a Kafka connection",
//             config.connection
//         )))
//     }
// }
