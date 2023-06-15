use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use arroyo_datastream::auth_config_to_hashmap;
use arroyo_datastream::Operator;
use arroyo_datastream::SerializationMode;
use arroyo_datastream::SinkConfig;
use arroyo_datastream::SourceConfig;
use arroyo_datastream::{ImpulseSpec, OffsetMode};
use arroyo_rpc::grpc::api::connection::ConnectionType;
use arroyo_rpc::grpc::api::Connection;
use arroyo_types::string_to_map;

use crate::types::StructDef;
use crate::SqlConfig;

#[derive(Clone, Debug)]
pub struct SqlSource {
    pub id: Option<i64>,
    pub struct_def: StructDef,
    pub source_config: SourceConfig,
    pub serialization_mode: SerializationMode,
}

impl SqlSource {
    pub(crate) fn get_operator(&self, sql_config: &SqlConfig) -> Operator {
        match self.source_config.clone() {
            SourceConfig::Kafka {
                bootstrap_servers,
                topic,
                client_configs,
            } => Operator::KafkaSource {
                topic,
                bootstrap_servers: vec![bootstrap_servers],
                offset_mode: OffsetMode::Latest,
                kafka_input_format: self.serialization_mode,
                messages_per_second: sql_config.kafka_qps.unwrap_or(10_000),
                client_configs,
            },
            SourceConfig::Impulse {
                interval,
                events_per_second,
                total_events,
            } => Operator::ImpulseSource {
                start_time: SystemTime::now(),
                spec: interval
                    .map(ImpulseSpec::Delay)
                    .unwrap_or(ImpulseSpec::EventsPerSecond(events_per_second)),
                total_events,
            },
            SourceConfig::FileSource {
                directory: _,
                interval: _,
            } => unimplemented!("file source not exposed in SQL"),
            SourceConfig::NexmarkSource {
                event_rate,
                runtime,
            } => Operator::NexmarkSource {
                first_event_rate: event_rate,
                num_events: runtime.map(|runtime| event_rate * runtime.as_secs()),
            },
            SourceConfig::EventSourceSource {
                url,
                headers,
                events,
            } => Operator::EventSourceSource {
                url,
                headers,
                events,
                serialization_mode: self.serialization_mode,
            },
        }
    }

    pub fn try_new(
        id: Option<i64>,
        struct_def: StructDef,
        connection: Connection,
        connection_config: &HashMap<String, String>,
    ) -> Result<Self> {
        let serialization_mode = SerializationMode::from_config_value(
            connection_config
                .get("serialization_mode")
                .map(|x| x.as_str()),
        );

        match connection.connection_type.unwrap() {
            ConnectionType::Kafka(kafka) => {
                let topic = connection_config
                    .get("topic")
                    .cloned()
                    .ok_or_else(|| anyhow!("Missing topic"))?;
                Ok(SqlSource {
                    id,
                    struct_def,
                    source_config: SourceConfig::Kafka {
                        topic,
                        bootstrap_servers: kafka.bootstrap_servers,
                        client_configs: auth_config_to_hashmap(kafka.auth_config),
                    },
                    serialization_mode,
                })
            }
            ConnectionType::Kinesis(_) => {
                bail!("Kinesis connections are not yet supported")
            }
            ConnectionType::Http(http) => {
                let mut path = connection_config.get("path").cloned().unwrap_or_default();

                if !path.is_empty() && !path.starts_with('/') {
                    path = format!("/{}", path);
                }

                let events = connection_config
                    .get("events")
                    .map(|e| e.split(',').map(|t| t.to_string()).collect())
                    .unwrap_or_default();

                Ok(SqlSource {
                    id,
                    struct_def,
                    source_config: SourceConfig::EventSourceSource {
                        url: http.url + &path,
                        headers: string_to_map(&http.headers)
                            .ok_or_else(|| anyhow!("Headers are invalid, expected a comma-delimited set of
                                header/value pairs, like `Content-Type: applicaiton/json,User-Agent:arroyo`"))?,
                        events,
                    },
                    serialization_mode
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqlSink {
    pub id: Option<i64>,
    pub struct_def: StructDef,
    pub sink_config: SinkConfig,
    pub updating_type: SinkUpdateType,
}

#[derive(Clone, Debug)]
pub enum SinkUpdateType {
    Allow,
    Disallow,
    Force,
}

impl SqlSink {
    pub fn new_from_sink_config(struct_def: StructDef, sink_config: SinkConfig) -> Self {
        let updating_type = match &sink_config {
            SinkConfig::Kafka {
                bootstrap_servers: _,
                topic: _,
                client_configs,
            } => {
                if SerializationMode::from_config_value(
                    client_configs.get("serialization_mode").map(|x| x.as_str()),
                )
                .is_updating()
                {
                    SinkUpdateType::Force
                } else {
                    SinkUpdateType::Disallow
                }
            }
            SinkConfig::Console
            | SinkConfig::File { directory: _ }
            | SinkConfig::Grpc
            | SinkConfig::Null => SinkUpdateType::Allow,
        };
        Self {
            id: None,
            struct_def,
            sink_config,
            updating_type,
        }
    }
    pub fn try_new_from_connection(
        id: Option<i64>,
        struct_def: StructDef,
        connection: Connection,
        connection_config: HashMap<String, String>,
    ) -> Result<Self> {
        let Some(ConnectionType::Kafka(kafka_config)) = connection.connection_type else {
            bail!("Only Kafka sinks are supported")
        };
        let serialization_mode = SerializationMode::from_config_value(
            connection_config
                .get("serialization_mode")
                .map(|x| x.as_str()),
        );
        let updating_type = if serialization_mode.is_updating() {
            SinkUpdateType::Force
        } else {
            SinkUpdateType::Disallow
        };

        let topic = Arc::new(connection_config)
            .get("topic")
            .cloned()
            .ok_or_else(|| anyhow!("Missing topic"))?;
        Ok(SqlSink {
            id,
            struct_def,
            sink_config: SinkConfig::Kafka {
                topic,
                bootstrap_servers: kafka_config.bootstrap_servers,
                client_configs: auth_config_to_hashmap(kafka_config.auth_config),
            },
            updating_type,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SqlTable {
    pub struct_def: StructDef,
    pub name: String,
}
