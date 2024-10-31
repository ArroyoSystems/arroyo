use anyhow::{anyhow, bail};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::OperatorNode;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, TestSourceMessage};
use arroyo_rpc::OperatorConfig;
use fluvio::Offset;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use typify::import_types;

use crate::fluvio::sink::FluvioSinkFunc;
use crate::fluvio::source::FluvioSourceFunc;
use crate::{pull_opt, ConnectionType, EmptyConfig};

mod sink;
mod source;

pub struct FluvioConnector {}

const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./fluvio.svg");

import_types!(schema = "src/fluvio/table.json");

impl Connector for FluvioConnector {
    type ProfileT = EmptyConfig;
    type TableT = FluvioTable;

    fn name(&self) -> &'static str {
        "fluvio"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
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

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let endpoint = options.remove("endpoint");
        let topic = pull_opt("topic", options)?;
        let table_type = pull_opt("type", options)?;

        let table_type = match table_type.as_str() {
            "source" => {
                let offset = options.remove("source.offset");
                TableType::Source {
                    offset: match offset.as_deref() {
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

        Self::from_config(self, None, name, EmptyConfig {}, table, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: EmptyConfig,
        table: FluvioTable,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let (typ, desc) = match table.type_ {
            TableType::Source { .. } => (
                ConnectionType::Source,
                format!("FluvioSource<{}>", table.topic),
            ),
            TableType::Sink { .. } => {
                (ConnectionType::Sink, format!("FluvioSink<{}>", table.topic))
            }
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
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
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

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<OperatorNode> {
        match table.type_ {
            TableType::Source { offset } => {
                Ok(OperatorNode::from_source(Box::new(FluvioSourceFunc {
                    topic: table.topic,
                    endpoint: table.endpoint.clone(),
                    offset_mode: offset,
                    format: config
                        .format
                        .ok_or_else(|| anyhow!("format required for fluvio source"))?,
                    framing: config.framing,
                    bad_data: config.bad_data,
                })))
            }
            TableType::Sink { .. } => Ok(OperatorNode::from_operator(Box::new(FluvioSinkFunc {
                topic: table.topic,
                endpoint: table.endpoint,
                producer: None,
                serializer: ArrowSerializer::new(
                    config
                        .format
                        .ok_or_else(|| anyhow!("format required for fluvio sink"))?,
                ),
            }))),
        }
    }
}

impl SourceOffset {
    pub fn offset(&self) -> Offset {
        match self {
            SourceOffset::Earliest => Offset::beginning(),
            SourceOffset::Latest => Offset::end(),
        }
    }
}
