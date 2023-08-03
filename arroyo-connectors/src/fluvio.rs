use anyhow::{anyhow, bail};
use arroyo_rpc::grpc::{
    self,
    api::{ConnectionSchema, TestSourceMessage},
};
use arroyo_types::OperatorConfig;
use serde::{Deserialize, Serialize};
use typify::import_types;

use crate::{
    pull_opt, format, Connection, ConnectionType, Connector, EmptyConfig,
};

pub struct FluvioConnector {}

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/kafka/table.json");
const ICON: &str = include_str!("../resources/fluvio.svg");

import_types!(schema = "../connector-schemas/fluvio/table.json");

impl Connector for FluvioConnector {
    type ConfigT = EmptyConfig;
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

    fn table_type(&self, _: Self::ConfigT, t: Self::TableT) -> arroyo_rpc::grpc::api::TableType {
        match t.type_ {
            TableType::Source { .. } => grpc::api::TableType::Source,
            TableType::Sink {} => grpc::api::TableType::Sink,
        }
    }

    fn get_schema(
        &self,
        _: Self::ConfigT,
        _: Self::TableT,
        s: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        s.cloned()
    }

    fn test(
        &self,
        _: &str,
        _: Self::ConfigT,
        _: Self::TableT,
        _: Option<&arroyo_rpc::grpc::api::ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<
            Result<arroyo_rpc::grpc::api::TestSourceMessage, tonic::Status>,
        >,
    ) {
        tokio::task::spawn(async move {
            tx.send(Ok(TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            }))
            .await
            .unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut std::collections::HashMap<String, String>,
        schema: Option<&arroyo_rpc::grpc::api::ConnectionSchema>,
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

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format(schema.as_ref().unwrap())),
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: typ,
            schema: schema
                .map(|s| s.to_owned())
                .ok_or_else(|| anyhow!("No schema defined for Fluvio connection"))?,
            operator: operator.to_string(),
            config: serde_json::to_string(&config).unwrap(),
            description: desc,
        })
    }
}
