use anyhow::{anyhow, bail};
use arrow::datatypes::DataType;
use arroyo_formats::de::ArrowDeserializer;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::{Connection, MetadataDef};
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, TestSourceMessage};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::formats::{BadData, Format, JsonFormat};
use arroyo_rpc::schema_resolver::{
    ConfluentSchemaRegistry, ConfluentSchemaRegistryClient, SchemaResolver,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig, schema_resolver, var_str::VarStr};
use arroyo_types::string_to_map;
use aws_config::Region;
use aws_msk_iam_sasl_signer::generate_auth_token;
use futures::TryFutureExt;
use rdkafka::{
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList,
    client::OAuthToken,
    consumer::{Consumer, ConsumerContext},
    producer::ProducerContext,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;
use tokio::time::timeout;
use tonic::Status;
use tracing::{error, info, warn};
use typify::import_types;

use crate::{ConnectionType, send};

use crate::kafka::sink::KafkaSinkFunc;
use crate::kafka::source::KafkaSourceFunc;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;

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
    pub fn subject(&self) -> Cow<'_, str> {
        match &self.value_subject {
            None => {
                // For single topic, use the standard subject naming convention
                // For patterns, the value_subject should be explicitly set
                let topic = self.topic.as_ref()
                    .map(|t| format!("{}-value", t))
                    .unwrap_or_else(|| "unknown-value".to_string());
                Cow::Owned(topic)
            }
            Some(s) => Cow::Borrowed(s),
        }
    }

    /// Returns the topic name or pattern for display purposes
    pub fn topic_display(&self) -> String {
        self.topic.clone().unwrap_or_else(|| {
            self.topic_pattern
                .clone()
                .map(|p| format!("pattern:{}", p))
                .unwrap_or_else(|| "unknown".to_string())
        })
    }
}

pub struct KafkaConnector {}

impl KafkaConnector {
    pub fn connection_from_options(options: &mut ConnectorOptions) -> anyhow::Result<KafkaConfig> {
        let auth = options.pull_opt_str("auth.type")?;
        let auth = match auth.as_deref() {
            Some("none") | None => KafkaConfigAuthentication::None {},
            Some("sasl") => KafkaConfigAuthentication::Sasl {
                mechanism: options.pull_str("auth.mechanism")?,
                protocol: options.pull_str("auth.protocol")?,
                username: VarStr::new(options.pull_str("auth.username")?),
                password: VarStr::new(options.pull_str("auth.password")?),
            },
            Some("aws_msk_iam") => KafkaConfigAuthentication::AwsMskIam {
                region: options.pull_str("auth.region")?,
            },
            Some(other) => bail!("unknown auth type '{}'", other),
        };

        let schema_registry = options
            .pull_opt_str("schema_registry.endpoint")?
            .map(|endpoint| {
                let api_key = options
                    .pull_opt_str("schema_registry.api_key")?
                    .map(VarStr::new);
                let api_secret = options
                    .pull_opt_str("schema_registry.api_secret")?
                    .map(VarStr::new);
                datafusion::common::Result::<_>::Ok(SchemaRegistry::ConfluentSchemaRegistry {
                    endpoint,
                    api_key,
                    api_secret,
                })
            })
            .transpose()?;

        Ok(KafkaConfig {
            authentication: auth,
            bootstrap_servers: BootstrapServers(options.pull_str("bootstrap_servers")?),
            schema_registry_enum: schema_registry,
            connection_properties: HashMap::new(),
        })
    }

    pub fn table_from_options(options: &mut ConnectorOptions) -> anyhow::Result<KafkaTable> {
        let typ = options.pull_str("type")?;
        let table_type = match typ.as_str() {
            "source" => {
                let offset = options.pull_opt_str("source.offset")?;
                TableType::Source {
                    offset: match offset.as_deref() {
                        Some("earliest") => SourceOffset::Earliest,
                        Some("group") => SourceOffset::Group,
                        None | Some("latest") => SourceOffset::Latest,
                        Some(other) => bail!("invalid value for source.offset '{}'", other),
                    },
                    read_mode: match options.pull_opt_str("source.read_mode")?.as_deref() {
                        Some("read_committed") => Some(ReadMode::ReadCommitted),
                        Some("read_uncommitted") | None => Some(ReadMode::ReadUncommitted),
                        Some(other) => bail!("invalid value for source.read_mode '{}'", other),
                    },
                    group_id: options.pull_opt_str("source.group_id")?,
                    group_id_prefix: options.pull_opt_str("source.group_id_prefix")?,
                }
            }
            "sink" => {
                let commit_mode = options.pull_opt_str("sink.commit_mode")?;
                TableType::Sink {
                    commit_mode: match commit_mode.as_deref() {
                        Some("at_least_once") | None => SinkCommitMode::AtLeastOnce,
                        Some("exactly_once") => SinkCommitMode::ExactlyOnce,
                        Some(other) => bail!("invalid value for commit_mode '{}'", other),
                    },
                    timestamp_field: options.pull_opt_str("sink.timestamp_field")?,
                    key_field: options.pull_opt_str("sink.key_field")?,
                }
            }
            _ => {
                bail!("type must be one of 'source' or 'sink")
            }
        };

        let topic = options.pull_opt_str("topic")?;
        let topic_pattern = options.pull_opt_str("topic_pattern")?;

        // Validate: exactly one of topic or topic_pattern must be set
        match (&topic, &topic_pattern) {
            (None, None) => bail!("Either 'topic' or 'topic_pattern' must be specified"),
            (Some(_), Some(_)) => bail!("Cannot specify both 'topic' and 'topic_pattern'"),
            _ => {}
        }

        // Validate: topic_pattern can only be used with source tables
        if topic_pattern.is_some() {
            match &table_type {
                TableType::Source { .. } => {}
                TableType::Sink { .. } => {
                    bail!("'topic_pattern' can only be used with source tables, not sinks")
                }
            }
        }

        Ok(KafkaTable {
            topic,
            topic_pattern,
            type_: table_type,
            client_configs: options
                .pull_opt_str("client_configs")?
                .map(|c| {
                    string_to_map(&c, '=').ok_or_else(|| {
                        anyhow!("invalid client_config: expected comma and equals-separated pairs")
                    })
                })
                .transpose()?
                .unwrap_or_else(HashMap::new),
            value_subject: options.pull_opt_str("value.subject")?,
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
                format!("KafkaSource<{}>", table.topic_display()),
            ),
            TableType::Sink { .. } => (
                ConnectionType::Sink,
                format!("KafkaSink<{}>", table.topic_display()),
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
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            typ,
            schema,
            &config,
            desc,
        ))
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

            if !message.error
                && let Err(e) = tester.test_schema_registry().await
            {
                message.error = true;
                message.message = format!("Failed to connect to schema registry: {e:?}");
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

    fn metadata_defs(&self) -> &'static [MetadataDef] {
        &[
            MetadataDef {
                name: "offset_id",
                data_type: DataType::Int64,
            },
            MetadataDef {
                name: "partition",
                data_type: DataType::Int32,
            },
            MetadataDef {
                name: "topic",
                data_type: DataType::Utf8,
            },
            MetadataDef {
                name: "timestamp",
                data_type: DataType::Int64,
            },
            MetadataDef {
                name: "key",
                data_type: DataType::Binary,
            },
        ]
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
        match &table.type_ {
            TableType::Source {
                group_id,
                offset,
                read_mode,
                group_id_prefix,
            } => {
                let mut client_configs = client_configs(&profile, Some(table.clone()))?;
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

                Ok(ConstructedOperator::from_source(Box::new(
                    KafkaSourceFunc {
                        topic: table.topic.clone(),
                        topic_pattern: table.topic_pattern.clone(),
                        bootstrap_servers: profile.bootstrap_servers.to_string(),
                        group_id: group_id.clone(),
                        group_id_prefix: group_id_prefix.clone(),
                        offset_mode: *offset,
                        format: config.format.expect("Format must be set for Kafka source"),
                        framing: config.framing,
                        schema_resolver,
                        bad_data: config.bad_data,
                        client_configs,
                        context: Context::new(Some(profile.clone())),
                        messages_per_second: NonZeroU32::new(
                            config
                                .rate_limit
                                .map(|l| l.messages_per_second)
                                .unwrap_or(u32::MAX),
                        )
                        .unwrap(),
                        metadata_fields: config.metadata_fields,
                    },
                )))
            }
            TableType::Sink {
                commit_mode,
                key_field,
                timestamp_field,
            } => Ok(ConstructedOperator::from_operator(Box::new(
                KafkaSinkFunc {
                    bootstrap_servers: profile.bootstrap_servers.to_string(),
                    producer: None,
                    consistency_mode: (*commit_mode).into(),
                    timestamp_field: timestamp_field.clone(),
                    timestamp_col: None,
                    key_field: key_field.clone(),
                    key_col: None,
                    write_futures: vec![],
                    client_config: client_configs(&profile, Some(table.clone()))?,
                    context: Context::new(Some(profile.clone())),
                    // Safe to unwrap: validation ensures sink tables have a topic
                    topic: table.topic.clone().expect("Sink tables must have a topic"),
                    serializer: ArrowSerializer::new(
                        config.format.expect("Format must be defined for KafkaSink"),
                    ),
                },
            ))),
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
                        format!("{prefix}-arroyo-kafka-tester")
                    }
                    _ => "arroyo-kafka-tester".to_string(),
                },
            );

        for (k, v) in client_configs(&self.connection, table)
            .map_err(|e| e.to_string())?
            .into_iter()
        {
            client_config.set(k, v);
        }

        let context = Context::new(Some(self.connection.clone()));
        let client: BaseConsumer = client_config
            .create_with_context(context)
            .map_err(|e| format!("invalid kafka config: {e:?}"))?;

        // NOTE: this is required to trigger an oauth token refresh (when using
        // OAUTHBEARER auth).
        if client.poll(Duration::from_secs(0)).is_some() {
            return Err("unexpected poll event from new consumer".to_string());
        }

        tokio::task::spawn_blocking(move || {
            client
                .fetch_metadata(None, Duration::from_secs(10))
                .map_err(|e| format!("Failed to connect to Kafka: {e:?}"))?;

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
                        "Failed to read topic metadata from Kafka: {e:?}"
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
                    _ => format!("Error while fetching topic metadata: {e:?}"),
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
                        bail!(
                            "Message appears to be encoded as normal JSON, rather than SR-JSON, but the schema registry is enabled. Ensure that the format and schema type are correct."
                        );
                    }
                    serde_json::from_slice::<Value>(&msg[5..]).map_err(|e|
                        anyhow!("Failed to parse message as schema-registry JSON (SR-JSON): {:?}. Ensure that the format and schema type are correct.", e))?;
                } else if msg[0] == 0 {
                    bail!(
                        "Message is not valid JSON. It may be encoded as SR-JSON, but the schema registry is not enabled. Ensure that the format and schema type are correct."
                    );
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
                        bail!(
                            "Message appears to be encoded as normal Avro, rather than SR-Avro, but the schema registry is enabled. Ensure that the format and schema type are correct."
                        );
                    }

                    let aschema: ArroyoSchema = schema.clone().into();
                    let mut deserializer = ArrowDeserializer::with_schema_resolver(
                        format.clone(),
                        None,
                        Arc::new(aschema),
                        &schema.metadata_fields(),
                        BadData::Fail {},
                        Arc::new(schema_resolver),
                    );

                    let mut error = deserializer
                        .deserialize_slice(&msg, SystemTime::now(), None)
                        .await
                        .into_iter()
                        .next();
                    if let Some(e) = deserializer.flush_buffer().1.pop() {
                        error.replace(e);
                    }

                    if let Some(error) = error {
                        bail!(
                            "Failed to parse message as schema-registry Avro (SR-Avro): {}. Ensure that the format and schema type are correct.",
                            error
                        );
                    }
                } else {
                    let aschema: ArroyoSchema = schema.clone().into();
                    let mut deserializer = ArrowDeserializer::new(
                        format.clone(),
                        Arc::new(aschema),
                        &schema.metadata_fields(),
                        None,
                        BadData::Fail {},
                    );

                    let mut error = deserializer
                        .deserialize_slice(&msg, SystemTime::now(), None)
                        .await
                        .into_iter()
                        .next();
                    if let Some(e) = deserializer.flush_buffer().1.pop() {
                        error.replace(e);
                    }

                    if let Some(error) = error {
                        if msg[0] == 0 {
                            bail!(
                                "Failed to parse message as regular Avro. It may be encoded as SR-Avro, but the schema registry is not enabled. Ensure that the format and schema type are correct."
                            );
                        } else {
                            bail!(
                                "Failed to parse message as Avro: {}. Ensure that the format and schema type are correct.",
                                error
                            );
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
                let mut deserializer = ArrowDeserializer::new(
                    format.clone(),
                    Arc::new(aschema),
                    &schema.metadata_fields(),
                    None,
                    BadData::Fail {},
                );

                let mut error = deserializer
                    .deserialize_slice(&msg, SystemTime::now(), None)
                    .await
                    .into_iter()
                    .next();
                if let Some(e) = deserializer.flush_buffer().1.pop() {
                    error.replace(e);
                }

                if let Some(error) = error {
                    bail!(
                        "Failed to parse message according to the provided Protobuf schema: {}",
                        error
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

pub fn client_configs(
    connection: &KafkaConfig,
    table: Option<KafkaTable>,
) -> anyhow::Result<HashMap<String, String>> {
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
            client_configs.insert("sasl.username".to_string(), username.sub_env_vars()?);
            client_configs.insert("sasl.password".to_string(), password.sub_env_vars()?);
        }
        KafkaConfigAuthentication::AwsMskIam { region: _ } => {
            client_configs.insert("sasl.mechanism".to_string(), "OAUTHBEARER".to_string());
            client_configs.insert("security.protocol".to_string(), "SASL_SSL".to_string());
        }
    };

    for (k, v) in connection.connection_properties.iter() {
        client_configs.insert(k.to_string(), v.to_string());
    }

    if let Some(table) = table {
        for (k, v) in table.client_configs.iter() {
            if connection.connection_properties.contains_key(k) {
                warn!(
                    "rdkafka config key {:?} defined in both connection and table config",
                    k
                );
            }

            client_configs.insert(k.to_string(), v.to_string());
        }
    }

    Ok(client_configs)
}

type BaseConsumer = rdkafka::consumer::BaseConsumer<Context>;
type FutureProducer = rdkafka::producer::FutureProducer<Context>;
type StreamConsumer = rdkafka::consumer::StreamConsumer<Context>;

#[derive(Clone)]
pub struct Context {
    config: Option<KafkaConfig>,
}

impl Context {
    pub fn new(config: Option<KafkaConfig>) -> Self {
        Self { config }
    }
}

impl ConsumerContext for Context {}

impl ProducerContext for Context {
    type DeliveryOpaque = ();
    fn delivery(
        &self,
        _delivery_result: &rdkafka::message::DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
    }
}

impl ClientContext for Context {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        if let Some(KafkaConfigAuthentication::AwsMskIam { region }) =
            self.config.as_ref().map(|c| &c.authentication)
        {
            let region = Region::new(region.clone());

            let (token, expiration_time_ms) = thread::spawn(move || {
                tokio::runtime::Runtime::new()?
                    .block_on(async {
                        timeout(Duration::from_secs(10), generate_auth_token(region.clone())).await
                    })
                    .map_err(|e| anyhow!("timed out generating MSK oauth token {:?}", e))?
                    .map_err(|e| anyhow!("signing error {:?}", e))
            })
            .join()
            .map_err(|e| anyhow!("failed to join thread {:?}", e))??;

            Ok(OAuthToken {
                token,
                principal_name: "".to_string(),
                lifetime_ms: expiration_time_ms,
            })
        } else {
            Err(anyhow!("only AWS_MSK_IAM is supported for sasl oauth").into())
        }
    }
}
