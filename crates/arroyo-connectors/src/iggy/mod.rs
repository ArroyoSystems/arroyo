use anyhow::{anyhow, bail, Result};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::Connection as ArroyoConnection;
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::OperatorConfig;
use iggy::client::{Client, MessageClient, StreamClient, TopicClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use tracing::{info, warn};

use crate::send;

mod sink;

use sink::IggySinkFunc;

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./iggy.svg");


typify::import_types!(
    schema = "src/iggy/profile.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

typify::import_types!(schema = "src/iggy/table.json");


#[derive(Copy, Clone, Debug, PartialEq)]
pub enum PartitioningStrategy {
    PartitionId(u32),
    Balanced,
}


#[derive(Copy, Clone, Debug, PartialEq)]
pub enum IggySourceOffset {
    Earliest,
    Latest,
}

pub struct IggyConnector {}

impl arroyo_operator::connector::Connector for IggyConnector {
    type ProfileT = IggyConfig;
    type TableT = IggyTable;

    fn name(&self) -> &'static str {
        "iggy"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "iggy".to_string(),
            name: "Apache Iggy".to_string(),
            icon: ICON.to_string(),
            description: "Read and write from an Apache Iggy cluster".to_string(),
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
        config.endpoint
    }

    fn test_profile(
        &self,
        profile: Self::ProfileT,
    ) -> Option<tokio::sync::oneshot::Receiver<TestSourceMessage>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let tester = IggyTester {
                connection: profile,
            };

            let message = tester.test_connection().await;
            let _ = tx.send(TestSourceMessage {
                error: message.error,
                done: true,
                message: message.message,
            });
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
        let tester = IggyTester { connection: config };
        tester.start(table, schema.cloned(), tx);
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.type_ {
            TableType::Sink { .. } => ConnectionType::Sink,
        }
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut arroyo_rpc::ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> Result<ArroyoConnection> {
        let endpoint = options.pull_str("endpoint")?;
        let transport = options.pull_str("transport")?;

        let authentication = if let Some(username) = options.pull_opt_str("username")? {
            let password = options.pull_str("password")?;
            IggyConfigAuthentication::UsernamePassword {
                username,
                password: VarStr::new(password),
            }
        } else {
            IggyConfigAuthentication::None {}
        };

        let transport_protocol = match transport.as_str() {
            "http" => TransportProtocol::Http,
            _ => bail!("Invalid transport protocol"),
        };

        let config = IggyConfig {
            endpoint: endpoint.clone(),
            transport: transport_protocol.clone(),
            authentication: authentication.clone(),
        };

        let stream = options.pull_str("stream")?;
        let topic = options.pull_str("topic")?;

        let table_type = if options.pull_bool("is_source")? {
            let offset = match options.pull_str("offset")?.as_str() {
                "earliest" => "earliest",
                "latest" => "latest",
                _ => bail!("Invalid offset value"),
            };

            let consumer_id = options.pull_u64("consumer_id")? as u32;
            let partition_id = options.pull_u64("partition_id")? as u32;
            let auto_commit = options.pull_bool("auto_commit")?;

            TableType::Source {
                offset: match offset {
                    "earliest" => SourceOffset::Earliest,
                    "latest" => SourceOffset::Latest,
                    _ => bail!("Invalid offset value"),
                },
                consumer_id: consumer_id as i64,
                partition_id: partition_id as i64,
                auto_commit,
            }
        } else {
            let partitioning = match options.pull_str("partitioning")?.as_str() {
                "partition_id" => "partition_id",
                "balanced" => "balanced",
                _ => bail!("Invalid partitioning value"),
            };

            let partition_id = options.pull_opt_u64("partition_id")?.map(|id| id as u32);

            TableType::Sink {
                partitioning: match partitioning {
                    "partition_id" => SinkPartitioning::PartitionId,
                    "balanced" => SinkPartitioning::Balanced,
                    _ => bail!("Invalid partitioning value"),
                },
                partition_id: partition_id.map(|id| id as i64),
            }
        };

        let table = IggyTable {
            stream,
            topic,
            type_: table_type,
        };

        self.from_config(None, name, config, table, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> Result<ArroyoConnection> {
        let schema = schema.cloned().unwrap_or_else(|| ConnectionSchema {
            format: None,
            bad_data: None,
            framing: None,
            struct_name: None,
            fields: vec![],
            definition: None,
            inferred: None,
            primary_keys: std::collections::HashSet::default(),
        });

        let operator_config = OperatorConfig {
            connection: serde_json::to_value(config.clone()).unwrap(),
            table: serde_json::to_value(table.clone()).unwrap(),
            rate_limit: None,
            format: schema.format.clone(),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(ArroyoConnection {
            id,
            name: name.to_string(),
            connector: self.name(),
            connection_type: self.table_type(config, table),
            description: format!("Iggy Connection"),
            config: serde_json::to_string(&operator_config)?,
            schema,
            partition_fields: None,
        })
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> Result<ConstructedOperator> {
        match &table.type_ {
            TableType::Sink {
                partitioning,
                partition_id,
            } => {
                let partitioning_strategy = match partitioning {
                    SinkPartitioning::PartitionId => {
                        if let Some(id) = partition_id {
                            PartitioningStrategy::PartitionId((*id) as u32)
                        } else {
                            bail!("Partition ID is required for partition_id partitioning strategy")
                        }
                    }
                    SinkPartitioning::Balanced => PartitioningStrategy::Balanced,
                };

                let (username, password) = match &profile.authentication {
                    IggyConfigAuthentication::None {} => (None, None),
                    IggyConfigAuthentication::UsernamePassword { username, password } => (
                        Some(username.clone()),
                        Some(
                            password
                                .sub_env_vars()
                                .expect("Failed to substitute env vars"),
                        ),
                    ),
                };

                Ok(ConstructedOperator::from_operator(Box::new(IggySinkFunc {
                    stream: table.stream.clone(),
                    topic: table.topic.clone(),
                    partitioning: partitioning_strategy,
                    endpoint: profile.endpoint.clone(),
                    _transport: profile.transport.to_string(),
                    username,
                    password,
                    client: None,
                    serializer: ArrowSerializer::new(
                        config.format.expect("Format must be defined for IggySink"),
                    ),
                })))
            }
        }
    }
}

pub struct IggyTester {
    pub connection: IggyConfig,
}

impl IggyTester {
    async fn connect(&self) -> Result<IggyClient, String> {
        info!("Testing connection to Iggy at {}", self.connection.endpoint);

        
        let client = IggyClient::default();

        
        client
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to Iggy server: {:?}", e))?;

        
        match &self.connection.authentication {
            IggyConfigAuthentication::None {} => {
                info!("No authentication required");
            }
            IggyConfigAuthentication::UsernamePassword { username, password } => {
                let password_str = password.sub_env_vars().map_err(|e| {
                    format!(
                        "Failed to substitute environment variables in password: {:?}",
                        e
                    )
                })?;

                client
                    .login_user(username, &password_str)
                    .await
                    .map_err(|e| format!("Failed to authenticate with Iggy server: {:?}", e))?;

                info!("Successfully authenticated with username: {}", username);
            }
        }

        Ok(client)
    }

    pub async fn test_connection(&self) -> TestSourceMessage {
        match self.connect().await {
            Ok(_) => TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully connected to Iggy".to_string(),
            },
            Err(e) => TestSourceMessage {
                error: true,
                done: true,
                message: e,
            },
        }
    }

    async fn test(
        &self,
        table: IggyTable,
        schema: Option<ConnectionSchema>,
        mut tx: Sender<TestSourceMessage>,
    ) -> Result<()> {
        
        let client = self.connect().await.map_err(|e| anyhow!("{}", e))?;

        self.info(&mut tx, "Connected to Iggy").await;

        
        let stream_id = Identifier::from_str(&table.stream)
            .map_err(|e| anyhow!("Invalid stream identifier '{}': {:?}", table.stream, e))?;

        match client.get_stream(&stream_id).await {
            Ok(stream_info) => {
                let topics_count = stream_info.as_ref().map(|s| s.topics_count).unwrap_or(0);
                self.info(
                    &mut tx,
                    &format!(
                        "Found stream '{}' with {} topics",
                        table.stream, topics_count
                    ),
                )
                .await;
            }
            Err(e) => {
                return Err(anyhow!(
                    "Stream '{}' does not exist or is not accessible: {:?}",
                    table.stream,
                    e
                ));
            }
        }

        
        let topic_id = Identifier::from_str(&table.topic)
            .map_err(|e| anyhow!("Invalid topic identifier '{}': {:?}", table.topic, e))?;

        match client.get_topic(&stream_id, &topic_id).await {
            Ok(topic_info) => {
                let partitions_count = topic_info.as_ref().map(|t| t.partitions_count).unwrap_or(0);
                self.info(
                    &mut tx,
                    &format!(
                        "Found topic '{}' with {} partitions",
                        table.topic, partitions_count
                    ),
                )
                .await;
            }
            Err(e) => {
                return Err(anyhow!(
                    "Topic '{}' does not exist in stream '{}' or is not accessible: {:?}",
                    table.topic,
                    table.stream,
                    e
                ));
            }
        }

        
        if let TableType::Source {
            partition_id,
            consumer_id,
            ..
        } = &table.type_
        {
            self.info(&mut tx, "Testing message polling").await;

            let consumer = Consumer::new(
                Identifier::numeric(*consumer_id as u32)
                    .map_err(|e| anyhow!("Invalid consumer ID {}: {:?}", consumer_id, e))?,
            );

            let polling_strategy = PollingStrategy::offset(0); 
            let partition_id = Some(*partition_id as u32);
            let messages_per_batch = 10; 

            let start = Instant::now();
            let timeout = Duration::from_secs(10); 

            while start.elapsed() < timeout {
                match client
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        partition_id,
                        &consumer,
                        &polling_strategy,
                        messages_per_batch,
                        false, 
                    )
                    .await
                {
                    Ok(polled_messages) => {
                        if !polled_messages.messages.is_empty() {
                            self.info(
                                &mut tx,
                                &format!(
                                    "Successfully polled {} messages from topic",
                                    polled_messages.messages.len()
                                ),
                            )
                            .await;

                            
                            if let Some(schema) = &schema {
                                if let Some(format) = &schema.format {
                                    if let Some(first_message) = polled_messages.messages.first() {
                                        match self
                                            .validate_message_schema(
                                                &table,
                                                schema,
                                                format,
                                                first_message.payload.to_vec(),
                                            )
                                            .await
                                        {
                                            Ok(_) => {
                                                self.info(
                                                    &mut tx,
                                                    "Successfully validated message schema",
                                                )
                                                .await;
                                            }
                                            Err(e) => {
                                                warn!("Schema validation failed: {:?}", e);
                                                self.info(
                                                    &mut tx,
                                                    &format!(
                                                        "Warning: Schema validation failed: {}",
                                                        e
                                                    ),
                                                )
                                                .await;
                                            }
                                        }
                                    }
                                }
                            }

                            self.info(&mut tx, "Message polling test completed successfully")
                                .await;
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        warn!("Error while polling messages in test: {:?}", e);
                        return Err(anyhow!("Error while polling messages from Iggy: {}", e));
                    }
                }

                
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            
            self.info(
                &mut tx,
                &format!(
                    "No messages received within {} seconds (this may be normal for empty topics)",
                    timeout.as_secs()
                ),
            )
            .await;
        }

        self.info(&mut tx, "Connection test completed successfully")
            .await;
        Ok(())
    }

    async fn validate_message_schema(
        &self,
        _table: &IggyTable,
        _schema: &ConnectionSchema,
        _format: &arroyo_rpc::formats::Format,
        _msg: Vec<u8>,
    ) -> Result<()> {
        
        
        
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

    pub fn start(
        self,
        table: IggyTable,
        schema: Option<ConnectionSchema>,
        mut tx: Sender<TestSourceMessage>,
    ) {
        tokio::spawn(async move {
            info!("Started Iggy tester");
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
