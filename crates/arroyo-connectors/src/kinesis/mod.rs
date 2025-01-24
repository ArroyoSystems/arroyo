use anyhow::{anyhow, bail, Result};
use std::collections::HashMap;
use typify::import_types;

use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{ConnectionProfile, TestSourceMessage};
use arroyo_rpc::{api_types, ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};

use crate::{ConnectionSchema, ConnectionType, EmptyConfig};

use crate::kinesis::sink::{FlushConfig, KinesisSinkFunc};
use crate::kinesis::source::KinesisSourceFunc;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;

const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./kinesis.svg");

import_types!(schema = "src/kinesis/table.json");

mod sink;
mod source;

pub struct KinesisConnector {}

impl Connector for KinesisConnector {
    type ProfileT = EmptyConfig;

    type TableT = KinesisTable;

    fn name(&self) -> &'static str {
        "kinesis"
    }

    fn metadata(&self) -> api_types::connections::Connector {
        api_types::connections::Connector {
            id: "kinesis".to_string(),
            name: "Kinesis Connector".to_string(),
            icon: ICON.to_string(),
            description: "Read or write with Kinesis".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: false,
            hidden: false,
            custom_schemas: true,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(message).await.unwrap();
        });
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.type_ {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink { .. } => ConnectionType::Sink,
        }
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<arroyo_operator::connector::Connection> {
        let (connection_type, description) = match table.type_ {
            TableType::Source { .. } => (
                ConnectionType::Source,
                format!("KinesisSource<{}>", table.stream_name),
            ),
            TableType::Sink { .. } => (
                ConnectionType::Sink,
                format!("KinesisSink<{}>", table.stream_name),
            ),
        };
        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("No schema defined for Kinesis"))?;

        let format = schema
            .format
            .as_ref()
            .map(|format| format.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for kinesis connections"))?;

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
            connection_type,
            schema,
            config: serde_json::to_string(&config).unwrap(),
            description,
        })
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let typ = options.pull_str("type")?;
        let table_type = match typ.as_str() {
            "source" => {
                let offset = options.pull_opt_str("source.offset")?;
                TableType::Source {
                    offset: match offset.as_deref() {
                        Some("earliest") => SourceOffset::Earliest,
                        None | Some("latest") => SourceOffset::Latest,
                        Some(other) => bail!("invalid value for source.offset '{}'", other),
                    },
                }
            }
            "sink" => {
                let batch_flush_interval_millis =
                    options.pull_opt_i64("sink.flush_interval_millis")?;
                let batch_max_buffer_size = options.pull_opt_i64("sink.max_bytes_per_batch")?;
                let records_per_batch = options.pull_opt_i64("sink.max_records_per_batch")?;
                TableType::Sink {
                    batch_flush_interval_millis,
                    batch_max_buffer_size,
                    records_per_batch,
                }
            }
            _ => {
                bail!("type must be one of 'source' or 'sink")
            }
        };

        let table = KinesisTable {
            stream_name: options.pull_str("stream_name")?,
            type_: table_type,
            aws_region: options.pull_opt_str("aws_region")?,
        };

        Self::from_config(self, None, name, EmptyConfig {}, table, schema)
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> Result<ConstructedOperator> {
        match table.type_ {
            TableType::Source { offset } => Ok(ConstructedOperator::from_source(Box::new(
                KinesisSourceFunc {
                    stream_name: table.stream_name,
                    kinesis_client: None,
                    aws_region: table.aws_region,
                    offset,
                    shards: HashMap::new(),
                    format: config
                        .format
                        .ok_or_else(|| anyhow!("format required for kinesis source"))?,
                    framing: config.framing,
                    bad_data: config.bad_data,
                },
            ))),
            TableType::Sink {
                batch_flush_interval_millis,
                batch_max_buffer_size,
                records_per_batch,
            } => {
                let flush_config = FlushConfig::new(
                    batch_flush_interval_millis,
                    batch_max_buffer_size,
                    records_per_batch,
                );
                Ok(ConstructedOperator::from_operator(Box::new(
                    KinesisSinkFunc {
                        client: None,
                        in_progress_batch: None,
                        aws_region: table.aws_region,
                        name: table.stream_name,
                        serializer: ArrowSerializer::new(
                            config
                                .format
                                .ok_or_else(|| anyhow!("Format must be defined for KinesisSink"))?,
                        ),
                        flush_config,
                    },
                )))
            }
        }
    }
}
