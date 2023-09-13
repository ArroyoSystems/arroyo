use anyhow::{anyhow, bail};
use arroyo_rpc::types::{ConnectionSchema, TestSourceMessage};
use arroyo_rpc::OperatorConfig;
use axum::response::sse::Event;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use typify::import_types;

use crate::{pull_opt, Connection, ConnectionType, Connector, EmptyConfig};

pub struct FluvioConnector {}

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/fluvio/table.json");
const ICON: &str = include_str!("../resources/fluvio.svg");

import_types!(schema = "../connector-schemas/fluvio/table.json");

impl Connector for FluvioConnector {
    type ProfileT = EmptyConfig;
    type TableT = FluvioTable;

    fn name(&self) -> &'static str {
        "fluvio"
    }

    fn metadata(&self) -> arroyo_rpc::types::Connector {
        arroyo_rpc::types::Connector {
            id: "fluvio".to_string(),
            name: "Fluvio".to_string(),
            icon: ICON.to_string(),
            description: "Read or write from Fluvio streams".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: false,
            hidden: false,
            custom_schemas: true,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, t: Self::TableT) -> ConnectionType {
        match t.type_ {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink {} => ConnectionType::Sink,
        }
    }

    fn get_schema(
        &self,
        _: Self::ProfileT,
        _: Self::TableT,
        s: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        s.cloned()
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

    fn from_options(
        &self,
        name: &str,
        options: &mut std::collections::HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let endpoint = options.remove("endpoint");
        let topic = pull_opt("topic", options)?;
        let table_type = pull_opt("type", options)?;

        let table_type = match table_type.as_str() {
            "source" => {
                let offset = options.remove("source.offset");
                TableType::Source {
                    offset: match offset.as_ref().map(|f| f.as_str()) {
                        Some("earliest") => SourceOffset::Earliest,
                        None | Some("latest") => SourceOffset::Latest,
                        Some(other) => bail!("invalid value for source.offset '{}'", other),
                    },
                }
            }
            "sink" => TableType::Sink {},
            _ => {
                bail!("type must be one of 'source' or 'sink");
            }
        };

        let table = FluvioTable {
            endpoint,
            topic,
            type_: table_type,
        };

        Self::from_config(&self, None, name, EmptyConfig {}, table, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: EmptyConfig,
        table: FluvioTable,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let (typ, operator, desc) = match table.type_ {
            TableType::Source { .. } => (
                ConnectionType::Source,
                "connectors::fluvio::source::FluvioSourceFunc",
                format!("FluvioSource<{}>", table.topic),
            ),
            TableType::Sink { .. } => (
                ConnectionType::Sink,
                "connectors::fluvio::sink::FluvioSinkFunc::<#in_k, #in_t>",
                format!("FluvioSink<{}>", table.topic),
            ),
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for Fluvio connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Fluvio connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
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
}
