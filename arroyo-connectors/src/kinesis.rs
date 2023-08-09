use anyhow::{anyhow, bail, Result};
use axum::response::sse::Event;
use std::convert::Infallible;
use typify::import_types;

use arroyo_rpc::types::TestSourceMessage;
use arroyo_rpc::{types, OperatorConfig};
use serde::{Deserialize, Serialize};

use crate::{
    pull_opt, pull_option_to_i64, Connection, ConnectionSchema, ConnectionType, EmptyConfig,
};

use super::Connector;

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/kinesis/table.json");
const ICON: &str = include_str!("../resources/kinesis.svg");

import_types!(schema = "../connector-schemas/kinesis/table.json");

pub struct KinesisConnector {}

impl Connector for KinesisConnector {
    type ProfileT = EmptyConfig;

    type TableT = KinesisTable;

    fn name(&self) -> &'static str {
        "kinesis"
    }

    fn metadata(&self) -> types::Connector {
        types::Connector {
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
        tx: tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(Ok(Event::default().json_data(message).unwrap()))
                .await
                .unwrap();
        });
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        return match table.type_ {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink { .. } => ConnectionType::Sink,
        };
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<crate::Connection> {
        let (connection_type, operator, description) = match table.type_ {
            TableType::Source { .. } => (
                ConnectionType::Source,
                "connectors::kinesis::source::KinesisSourceFunc",
                format!("KinesisSource<{}>", table.stream_name),
            ),
            TableType::Sink { .. } => (
                ConnectionType::Sink,
                "connectors::kinesis::sink::KinesisSinkFunc::<#in_k, #in_t>",
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
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type,
            schema: schema,
            operator: operator.to_string(),
            config: serde_json::to_string(&config).unwrap(),
            description,
        })
    }

    fn from_options(
        &self,
        name: &str,
        opts: &mut std::collections::HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<crate::Connection> {
        let typ = pull_opt("type", opts)?;
        let table_type = match typ.as_str() {
            "source" => {
                let offset: Option<String> = opts.remove("source.offset");
                TableType::Source {
                    offset: match offset.as_ref().map(|f| f.as_str()) {
                        Some("earliest") => SourceOffset::Earliest,
                        None | Some("latest") => SourceOffset::Latest,
                        Some(other) => bail!("invalid value for source.offset '{}'", other),
                    },
                }
            }
            "sink" => {
                let batch_flush_interval_millis =
                    pull_option_to_i64("sink.flush_interval_millis", opts)?;
                let batch_max_buffer_size = pull_option_to_i64("sink.max_bytes_per_batch", opts)?;
                let records_per_batch = pull_option_to_i64("sink.max_records_per_batch", opts)?;
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
            stream_name: pull_opt("stream_name", opts)?,
            type_: table_type,
        };

        Self::from_config(&self, None, name, EmptyConfig {}, table, schema)
    }
}
