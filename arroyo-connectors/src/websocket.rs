use std::time::Duration;

use anyhow::anyhow;
use arroyo_rpc::grpc::{
    self,
    api::{ConnectionSchema, TestSourceMessage},
};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::{connect_async, tungstenite};
use tonic::Status;
use typify::import_types;

use serde::{Deserialize, Serialize};

use crate::{
    pull_opt, serialization_mode, Connection, ConnectionType, EmptyConfig, OperatorConfig,
};

use super::Connector;

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/websocket/table.json");

import_types!(schema = "../connector-schemas/websocket/table.json");
const ICON: &str = include_str!("../resources/websocket.svg");

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
            hidden: false,
            custom_schemas: true,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn test(
        &self,
        _: &str,
        _: Self::ConfigT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<Result<TestSourceMessage, Status>>,
    ) {
        tokio::task::spawn(async move {
            let send = |error: bool, done: bool, message: String| {
                let tx = tx.clone();
                async move {
                    tx.send(Ok(TestSourceMessage {
                        error,
                        done,
                        message,
                    }))
                    .await
                    .unwrap();
                }
            };

            let ws_stream = match connect_async(&table.endpoint).await {
                Ok((ws_stream, _)) => ws_stream,
                Err(e) => {
                    send(
                        true,
                        true,
                        format!("Failed to connect to websocket server: {:?}", e),
                    )
                    .await;
                    return;
                }
            };

            send(
                false,
                false,
                "Successfully connected to websocket server".to_string(),
            )
            .await;

            let (mut tx, mut rx) = ws_stream.split();

            if let Some(msg) = table.subscription_message {
                match tx
                    .send(tungstenite::Message::Text(msg.clone().into()))
                    .await
                {
                    Ok(_) => {
                        send(false, false, "Sent subscription message".to_string()).await;
                    }
                    Err(e) => {
                        send(
                            true,
                            true,
                            format!("Failed to send subscription message: {:?}", e),
                        )
                        .await;
                        return;
                    }
                }
            }

            tokio::select! {
                message = rx.next() => {
                    match message {
                        Some(Ok(_)) => {
                            send(false, false, "Received message from websocket".to_string()).await;
                        },
                        Some(Err(e)) => {
                            send(true, true, format!("Received error from websocket: {:?}", e)).await;
                            return;
                        }
                        None => {
                            send(true, true, "Websocket disconnected before sending message".to_string()).await;
                            return;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    send(true, true, "Did not receive any messages after 30 seconds".to_string()).await;
                    return;
                }
            }

            send(
                false,
                true,
                "Successfully validated websocket connection".to_string(),
            )
            .await;
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
                subscription_message: subscription_message.map(SubscriptionMessage),
            },
            schema,
        )
    }
}
