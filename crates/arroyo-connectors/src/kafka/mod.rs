use anyhow::{anyhow, bail};
use arroyo_formats::de::ArrowDeserializer;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, TestSourceMessage};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::formats::{BadData, Format, JsonFormat};
use arroyo_rpc::schema_resolver::{
    ConfluentSchemaRegistry, ConfluentSchemaRegistryClient, SchemaResolver,
};
use arroyo_rpc::{schema_resolver, var_str::VarStr, OperatorConfig};
use arroyo_types::string_to_map;
use futures::TryFutureExt;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;
use tonic::Status;
use tracing::{error, info, warn};
use typify::import_types;

use crate::{pull_opt, send, ConnectionType};

use crate::kafka::sink::KafkaSinkFunc;
use crate::kafka::source::KafkaSourceFunc;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::OperatorNode;

mod sink;
mod source;

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./kafka.svg");

import_types!(
    schema = "src/kafka/profile.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

import_types!(schema = "src/kafka/table.json");

impl KafkaTable {
    pub fn subject(&self) -> Cow<str> {
        match &self.value_subject {
            None => Cow::Owned(format!("{}-value", self.topic)),
            Some(s) => Cow::Borrowed(s),
        }
    }
}

pub struct KafkaConnector {}

impl KafkaConnector {
    pub fn connection_from_options(
        options: &mut HashMap<String, String>,
    ) -> anyhow::Result<KafkaConfig> {
        let auth = options.remove("auth.type");
        let auth = match auth.as_deref() {
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
                    offset: match offset.as_deref() {
                        Some("earliest") => SourceOffset::Earliest,
                        Some("group") => SourceOffset::Group,
                        None | Some("latest") => SourceOffset::Latest,
                        Some(other) => bail!("invalid value for source.offset '{}'", other),
                    },
                    read_mode: match options.remove("source.read_mode").as_deref() {
                        Some("read_committed") => Some(ReadMode::ReadCommitted),
                        Some("read_uncommitted") | None => Some(ReadMode::ReadUncommitted),
                        Some(other) => bail!("invalid value for source.read_mode '{}'", other),
                    },
                    group_id: options.remove("source.group_id"),
                    group_id_prefix: options.remove("source.group_id_prefix"),
                }
            }
            "sink" => {
                let commit_mode = options.remove("sink.commit_mode");
                TableType::Sink {
                    commit_mode: match commit_mode.as_deref() {
                        Some("at_least_once") | None => SinkCommitMode::AtLeastOnce,
                        Some("exactly_once") => SinkCommitMode::ExactlyOnce,
                        Some(other) => bail!("invalid value for commit_mode '{}'", other),
                    },
                    timestamp_field: options.remove("sink.timestamp_field"),
                    key_field: options.remove("sink.key_field"),
                }
            }
            _ => {
                bail!("type must be one of 'source' or 'sink")
            }
        };

        Ok(KafkaTable {
            topic: pull_opt("topic", options)?,
            type_: table_type,
            client_configs: options
                .remove("client_configs")
                .map(|c| {
                    string_to_map(&c, '=').ok_or_else(|| {
                        anyhow!("invalid client_config: expected comma and equals-separated pairs")
                    })
                })
                .transpose()?
                .unwrap_or_else(HashMap::new),
            value_subject: options.remove("value.subject"),
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
        let (typ, desc) = match table.type_ {
            TableType::Source { .. } => (
                ConnectionType::Source,
                format!("KafkaSource<{}>", table.topic),
            ),
            TableType::Sink { .. } => (ConnectionType::Sink, format!("KafkaSink<{}>", table.topic)),
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
            connector: self.name(),
            name: name.to_string(),
            connection_type: typ,
            schema,
            config: serde_json::to_string(&config).unwrap(),
            description: desc,
        })
    }

    fn get_autocomplete(
        &self,
        profile: Self::ProfileT,
    ) -> oneshot::Receiver<anyhow::Result<HashMap<String, Vec<String>>>> {
        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            let kafka = KafkaTester {
                connection: profile,
            };

            tx.send(
                kafka
                    .fetch_topics()
                    .await
                    .map_err(|e| anyhow!("Failed to fetch topics from Kafka: {:?}", e))
                    .map(|topics| {
                        let mut map = HashMap::new();
                        map.insert(
                            "topic".to_string(),
                            topics
                                .into_iter()
                                .map(|(name, _)| name)
                                .filter(|name| !name.starts_with('_'))
                                .collect(),
                        );
                        map
                    }),
            )
            .unwrap();
        });

        rx
    }

    fn test_profile(&self, profile: Self::ProfileT) -> Option<Receiver<TestSourceMessage>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let tester = KafkaTester {
                connection: profile,
            };

            let mut message = tester.test_connection().await;

            if !message.error {
                if let Err(e) = tester.test_schema_registry().await {
                    message.error = true;
                    message.message = format!("Failed to connect to schema registry: {:?}", e);
                }
            }

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
        schema: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        let tester = KafkaTester { connection: config };

        tester.start(table, schema.cloned(), tx);
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

        Self::from_config(self, None, name, connection, table, schema)
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<OperatorNode> {
        match &table.type_ {
            TableType::Source {
                group_id,
                offset,
                read_mode,
                group_id_prefix,
            } => {
                let mut client_configs = client_configs(&profile, &table);
                if let Some(ReadMode::ReadCommitted) = read_mode {
                    client_configs
                        .insert("isolation.level".to_string(), "read_committed".to_string());
                }

                let schema_resolver: Option<Arc<dyn SchemaResolver + Sync>> =
                    if let Some(SchemaRegistry::ConfluentSchemaRegistry {
                        endpoint,
                        api_key,
                        api_secret,
                    }) = &profile.schema_registry_enum
                    {
                        Some(Arc::new(
                            ConfluentSchemaRegistry::new(
                                endpoint,
                                &table.subject(),
                                api_key.clone(),
                                api_secret.clone(),
                            )
                            .expect("failed to construct confluent schema resolver"),
                        ))
                    } else {
                        None
                    };

                Ok(OperatorNode::from_source(Box::new(KafkaSourceFunc {
                    topic: table.topic,
                    bootstrap_servers: profile.bootstrap_servers.to_string(),
                    group_id: group_id.clone(),
                    group_id_prefix: group_id_prefix.clone(),
                    offset_mode: *offset,
                    format: config.format.expect("Format must be set for Kafka source"),
                    framing: config.framing,
                    schema_resolver,
                    bad_data: config.bad_data,
                    client_configs,
                    messages_per_second: NonZeroU32::new(
                        config
                            .rate_limit
                            .map(|l| l.messages_per_second)
                            .unwrap_or(u32::MAX),
                    )
                    .unwrap(),
                })))
            }
            TableType::Sink {
                commit_mode,
                key_field,
                timestamp_field,
            } => Ok(OperatorNode::from_operator(Box::new(KafkaSinkFunc {
                bootstrap_servers: profile.bootstrap_servers.to_string(),
                producer: None,
                consistency_mode: (*commit_mode).into(),
                timestamp_field: timestamp_field.clone(),
                timestamp_col: None,
                key_field: key_field.clone(),
                key_col: None,
                write_futures: vec![],
                client_config: client_configs(&profile, &table),
                topic: table.topic,
                serializer: ArrowSerializer::new(
                    config.format.expect("Format must be defined for KafkaSink"),
                ),
            }))),
        }
    }
}

pub struct KafkaTester {
    pub connection: KafkaConfig,
}

pub struct TopicMetadata {
    pub partitions: usize,
}

impl KafkaTester {
    async fn connect(&self, table: Option<KafkaTable>) -> Result<BaseConsumer, String> {
        let mut client_config = ClientConfig::new();
        client_config
            .set(
                "bootstrap.servers",
                self.connection.bootstrap_servers.to_string(),
            )
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set(
                "group.id",
                match &table {
                    Some(KafkaTable {
                        type_:
                            TableType::Source {
                                group_id_prefix: Some(prefix),
                                ..
                            },
                        ..
                    }) => {
                        format!("{}-arroyo-kafka-tester", prefix)
                    }
                    _ => "arroyo-kafka-tester".to_string(),
                },
            );

        // TODO: merge this with client_configs()
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

        if let Some(table) = table {
            for (k, v) in table.client_configs {
                client_config.set(k, v);
            }
        }

        let client: BaseConsumer = client_config
            .create()
            .map_err(|e| format!("invalid kafka config: {:?}", e))?;

        tokio::task::spawn_blocking(move || {
            client
                .fetch_metadata(None, Duration::from_secs(10))
                .map_err(|e| format!("Failed to connect to Kafka: {:?}", e))?;

            Ok(client)
        })
        .await
        .map_err(|_| "unexpected error while connecting to kafka")?
    }

    #[allow(unused)]
    pub async fn topic_metadata(&self, topic: &str) -> Result<TopicMetadata, Status> {
        let client = self
            .connect(None)
            .await
            .map_err(Status::failed_precondition)?;

        let topic = topic.to_string();
        tokio::task::spawn_blocking(move || {
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
        }).map_err(|_| Status::internal("unexpected error while fetching topic metadata")).await?
    }

    async fn fetch_topics(&self) -> anyhow::Result<Vec<(String, usize)>> {
        let client = self.connect(None).await.map_err(|e| anyhow!("{}", e))?;

        tokio::task::spawn_blocking(move || {
            let metadata = client
                .fetch_metadata(None, Duration::from_secs(5))
                .map_err(|e| anyhow!("Failed to read topic metadata from Kafka: {:?}", e))?;

            Ok(metadata
                .topics()
                .iter()
                .map(|t| (t.name().to_string(), t.partitions().len()))
                .collect())
        })
        .await
        .map_err(|_| anyhow!("unexpected error while fetching topic metadata"))?
    }

    pub async fn test_schema_registry(&self) -> anyhow::Result<()> {
        if let Some(SchemaRegistry::ConfluentSchemaRegistry {
            api_key,
            api_secret,
            endpoint,
        }) = &self.connection.schema_registry_enum
        {
            let client =
                ConfluentSchemaRegistryClient::new(endpoint, api_key.clone(), api_secret.clone())?;

            client.test().await?;
        }

        Ok(())
    }

    pub async fn validate_schema(
        &self,
        table: &KafkaTable,
        schema: &ConnectionSchema,
        format: &Format,
        msg: Vec<u8>,
    ) -> anyhow::Result<()> {
        match format {
            Format::Json(JsonFormat {
                confluent_schema_registry,
                ..
            }) => {
                if *confluent_schema_registry {
                    if msg[0] != 0 {
                        bail!("Message appears to be encoded as normal JSON, rather than SR-JSON, but the schema registry is enabled. Ensure that the format and schema type are correct.");
                    }
                    serde_json::from_slice::<Value>(&msg[5..]).map_err(|e|
                        anyhow!("Failed to parse message as schema-registry JSON (SR-JSON): {:?}. Ensure that the format and schema type are correct.", e))?;
                } else if msg[0] == 0 {
                    bail!("Message is not valid JSON. It may be encoded as SR-JSON, but the schema registry is not enabled. Ensure that the format and schema type are correct.");
                } else {
                    serde_json::from_slice::<Value>(&msg).map_err(|e|
                        anyhow!("Failed to parse message as JSON: {:?}. Ensure that the format and schema type are correct.", e))?;
                }
            }
            Format::Avro(avro) => {
                if avro.confluent_schema_registry {
                    let schema_resolver = match &self.connection.schema_registry_enum {
                        Some(SchemaRegistry::ConfluentSchemaRegistry {
                            endpoint,
                            api_key,
                            api_secret,
                        }) => schema_resolver::ConfluentSchemaRegistry::new(
                            endpoint,
                            &table.subject(),
                            api_key.clone(),
                            api_secret.clone(),
                        ),
                        _ => {
                            bail!(
                                "schema registry is enabled, but no schema registry is configured"
                            );
                        }
                    }
                    .map_err(|e| anyhow!("Failed to construct schema registry: {:?}", e))?;

                    if msg[0] != 0 {
                        bail!("Message appears to be encoded as normal Avro, rather than SR-Avro, but the schema registry is enabled. Ensure that the format and schema type are correct.");
                    }

                    let aschema: ArroyoSchema = schema.clone().into();
                    let mut deserializer = ArrowDeserializer::with_schema_resolver(
                        format.clone(),
                        None,
                        aschema.clone(),
                        BadData::Fail {},
                        Arc::new(schema_resolver),
                    );
                    let mut builders = aschema.builders();

                    let mut error = deserializer
                        .deserialize_slice(&mut builders, &msg, SystemTime::now())
                        .await
                        .into_iter()
                        .next();
                    if let Some(Err(e)) = deserializer.flush_buffer() {
                        error.replace(e);
                    }

                    if let Some(error) = error {
                        bail!("Failed to parse message as schema-registry Avro (SR-Avro): {:?}. Ensure that the format and schema type are correct.", error.details());
                    }
                } else {
                    let aschema: ArroyoSchema = schema.clone().into();
                    let mut deserializer = ArrowDeserializer::new(
                        format.clone(),
                        aschema.clone(),
                        None,
                        BadData::Fail {},
                    );
                    let mut builders = aschema.builders();

                    let mut error = deserializer
                        .deserialize_slice(&mut builders, &msg, SystemTime::now())
                        .await
                        .into_iter()
                        .next();
                    if let Some(Err(e)) = deserializer.flush_buffer() {
                        error.replace(e);
                    }

                    if let Some(error) = error {
                        if msg[0] == 0 {
                            bail!("Failed to parse message as regular Avro. It may be encoded as SR-Avro, but the schema registry is not enabled. Ensure that the format and schema type are correct.");
                        } else {
                            bail!("Failed to parse message as Avro: {:?}. Ensure that the format and schema type are correct.", error.details());
                        };
                    }
                }
            }
            Format::Parquet(_) => {
                unreachable!()
            }
            Format::RawString(_) => {
                String::from_utf8(msg).map_err(|e|
                    anyhow!("Failed to parse message as UTF-8: {:?}. Ensure that the format and schema type are correct.", e))?;
            }
            Format::RawBytes(_) => {
                // all bytes are valid
            }
            Format::Protobuf(_) => {
                let aschema: ArroyoSchema = schema.clone().into();
                let mut deserializer =
                    ArrowDeserializer::new(format.clone(), aschema.clone(), None, BadData::Fail {});
                let mut builders = aschema.builders();

                let mut error = deserializer
                    .deserialize_slice(&mut builders, &msg, SystemTime::now())
                    .await
                    .into_iter()
                    .next();
                if let Some(Err(e)) = deserializer.flush_buffer() {
                    error.replace(e);
                }

                if let Some(error) = error {
                    bail!(
                        "Failed to parse message according to the provided Protobuf schema: {}",
                        error.details()
                    );
                }
            }
        };

        Ok(())
    }

    async fn test(
        &self,
        table: KafkaTable,
        schema: Option<ConnectionSchema>,
        mut tx: Sender<TestSourceMessage>,
    ) -> anyhow::Result<()> {
        let format = schema
            .as_ref()
            .and_then(|s| s.format.clone())
            .ok_or_else(|| anyhow!("No format defined for Kafka connection"))?;

        let client = self
            .connect(Some(table.clone()))
            .await
            .map_err(|e| anyhow!("{}", e))?;

        self.info(&mut tx, "Connected to Kafka").await;

        let topic = table.topic.clone();

        let metadata = client
            .fetch_metadata(Some(&topic), Duration::from_secs(10))
            .map_err(|e| anyhow!("Failed to fetch metadata: {:?}", e))?;

        self.info(&mut tx, "Fetched topic metadata").await;

        {
            let topic_metadata = metadata.topics().first().ok_or_else(|| {
                anyhow!(
                    "Returned metadata was empty; unable to subscribe to topic '{}'",
                    topic
                )
            })?;

            if let Some(err) = topic_metadata.error() {
                match err {
                    rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION
                    | rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC
                    | rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART => {
                        bail!(
                            "Topic '{}' does not exist in the configured Kafka cluster",
                            topic
                        );
                    }
                    e => {
                        error!("Unhandled Kafka error while fetching metadata: {:?}", e);
                        bail!(
                            "Something went wrong while fetching topic metadata: {:?}",
                            e
                        );
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
                .map_err(|e| anyhow!("Failed to subscribe to topic '{}': {:?}", topic, e))?;
        }

        if let TableType::Source { .. } = table.type_ {
            self.info(&mut tx, "Waiting for messages").await;

            let start = Instant::now();
            let timeout = Duration::from_secs(30);
            while start.elapsed() < timeout {
                match client.poll(Duration::ZERO) {
                    Some(Ok(message)) => {
                        self.info(&mut tx, "Received message from Kafka").await;
                        self.validate_schema(
                            &table,
                            schema.as_ref().unwrap(),
                            &format,
                            message
                                .detach()
                                .payload()
                                .ok_or_else(|| anyhow!("received message with empty payload"))?
                                .to_vec(),
                        )
                        .await?;

                        self.info(&mut tx, "Successfully validated message schema")
                            .await;
                        return Ok(());
                    }
                    Some(Err(e)) => {
                        warn!("Error while reading from kafka in test: {:?}", e);
                        return Err(anyhow!("Error while reading messages from Kafka: {}", e));
                    }
                    None => {
                        // wait
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }

            return Err(anyhow!(
                "No messages received from Kafka within {} seconds",
                timeout.as_secs()
            ));
        }

        Ok(())
    }

    async fn info(&self, tx: &mut Sender<TestSourceMessage>, s: impl Into<String>) {
        send(
            tx,
            TestSourceMessage {
                error: false,
                done: false,
                message: s.into(),
            },
        )
        .await;
    }

    #[allow(unused)]
    pub async fn test_connection(&self) -> TestSourceMessage {
        match self.connect(None).await {
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

    pub fn start(
        self,
        table: KafkaTable,
        schema: Option<ConnectionSchema>,
        mut tx: Sender<TestSourceMessage>,
    ) {
        tokio::spawn(async move {
            info!("Started kafka tester");
            if let Err(e) = self.test(table, schema, tx.clone()).await {
                send(
                    &mut tx,
                    TestSourceMessage {
                        error: true,
                        done: true,
                        message: e.to_string(),
                    },
                )
                .await;
            } else {
                send(
                    &mut tx,
                    TestSourceMessage {
                        error: false,
                        done: true,
                        message: "Connection is valid".to_string(),
                    },
                )
                .await;
            }
        });
    }
}

impl SourceOffset {
    fn get_offset(&self) -> Offset {
        match self {
            SourceOffset::Earliest => Offset::Beginning,
            SourceOffset::Latest => Offset::End,
            SourceOffset::Group => Offset::Stored,
        }
    }
}

pub fn client_configs(connection: &KafkaConfig, table: &KafkaTable) -> HashMap<String, String> {
    let mut client_configs: HashMap<String, String> = HashMap::new();

    match &connection.authentication {
        KafkaConfigAuthentication::None {} => {}
        KafkaConfigAuthentication::Sasl {
            mechanism,
            password,
            protocol,
            username,
        } => {
            client_configs.insert("sasl.mechanism".to_string(), mechanism.to_string());
            client_configs.insert("security.protocol".to_string(), protocol.to_string());
            client_configs.insert(
                "sasl.username".to_string(),
                username
                    .sub_env_vars()
                    .expect("Missing env-vars for Kafka username"),
            );
            client_configs.insert(
                "sasl.password".to_string(),
                password
                    .sub_env_vars()
                    .expect("Missing env-vars for Kafka password"),
            );
        }
    };

    client_configs.extend(
        table
            .client_configs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string())),
    );

    client_configs
}
