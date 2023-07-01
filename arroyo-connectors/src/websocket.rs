use anyhow::{anyhow};
use arroyo_rpc::grpc::{
    self,
    api::{ConnectionSchema, TestSourceMessage},
};
use tokio::sync::mpsc::Sender;
use tonic::Status;
use typify::import_types;

use serde::{Deserialize, Serialize};

use crate::{
    pull_opt, serialization_mode, Connection, ConnectionType, EmptyConfig, OperatorConfig,
};

use super::Connector;

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/websocket/table.json");

import_types!(schema = "../connector-schemas/websocket/table.json");
const ICON: &str = include_str!("../resources/sse.svg");

pub struct WebsocketConnector {}

impl Connector for WebsocketConnector {
    type ConfigT = EmptyConfig;

    type TableT = WebsocketTable;

    fn name(&self) -> &'static str {
        "websocket"
    }

    fn metadata(&self) -> grpc::api::Connector {
        grpc::api::Connector {
            id: "websocket".to_string(),
            name: "Websocket".to_string(),
            icon: ICON.to_string(),
            description: "Connect to a Websocket server".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: true,
            custom_schemas: true,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn test(
        &self,
        _: &str,
        _: Self::ConfigT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<Result<TestSourceMessage, Status>>,
    ) {
        tokio::task::spawn(async move {
            tx.send(Ok(TestSourceMessage { error: false, done: true, message: "Yay".to_string() })).await.unwrap();
        });
    }

    fn table_type(&self, _: Self::ConfigT, _: Self::TableT) -> grpc::api::TableType {
        return grpc::api::TableType::Source;
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ConfigT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<crate::Connection> {
        let description = format!("WebsocketSource<{}>", table.endpoint);

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            serialization_mode: Some(serialization_mode(schema.as_ref().unwrap())),
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: ConnectionType::Source,
            schema: schema
                .map(|s| s.to_owned())
                .ok_or_else(|| anyhow!("No schema defined for Websocket source"))?,
            operator: "connectors::websocket::WebsocketSourceFunc".to_string(),
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
        let endpoint = pull_opt("endpoint", opts)?;
        let subscription_message = opts.remove("subscription_message");

        self.from_config(
            None,
            name,
            EmptyConfig {},
            WebsocketTable {
                endpoint,
                subscription_message
            },
            schema,
        )
    }
}
