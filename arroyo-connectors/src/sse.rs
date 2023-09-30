use std::convert::Infallible;
use std::time::Duration;

use anyhow::{anyhow, bail};
use arroyo_rpc::OperatorConfig;
use arroyo_types::string_to_map;
use axum::response::sse::Event;
use eventsource_client::Client;
use futures::StreamExt;
use tokio::sync::mpsc::Sender;
use typify::import_types;

use arroyo_rpc::types::{ConnectionSchema, ConnectionType, TestSourceMessage};
use serde::{Deserialize, Serialize};

use crate::{pull_opt, Connection, EmptyConfig};

use super::Connector;

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/sse/table.json");

import_types!(schema = "../connector-schemas/sse/table.json");
const ICON: &str = include_str!("../resources/sse.svg");

pub struct SSEConnector {}

impl Connector for SSEConnector {
    type ProfileT = EmptyConfig;

    type TableT = SseTable;

    fn name(&self) -> &'static str {
        "sse"
    }

    fn metadata(&self) -> arroyo_rpc::types::Connector {
        arroyo_rpc::types::Connector {
            id: "sse".to_string(),
            name: "Server-Sent Events".to_string(),
            icon: ICON.to_string(),
            description: "Connect to a SSE/EventSource server".to_string(),
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
        _: Self::ProfileT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<Result<Event, Infallible>>,
    ) {
        SseTester { config: table, tx }.start();
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        return ConnectionType::Source;
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<crate::Connection> {
        let description = format!("SSESource<{}>", table.endpoint);

        if let Some(headers) = &table.headers {
            string_to_map(headers).ok_or_else(|| {
                anyhow!(
                    "Invalid format for headers; should be a \
                    comma-separated list of colon-separated key value pairs"
                )
            })?;
        }

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for SSE connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for SSE connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            framing: schema.framing.clone(),
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: ConnectionType::Source,
            schema,
            operator: "connectors::sse::SSESourceFunc".to_string(),
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
        let headers = opts.remove("headers");
        let events = opts.remove("events");

        self.from_config(
            None,
            name,
            EmptyConfig {},
            SseTable {
                endpoint,
                events,
                headers: headers.map(Headers),
            },
            schema,
        )
    }
}

struct SseTester {
    config: SseTable,
    tx: Sender<Result<Event, Infallible>>,
}

impl SseTester {
    pub fn start(self) {
        tokio::task::spawn(async move {
            self.tx
                .send(Ok(Event::default()
                    .json_data(match self.test_internal().await {
                        Ok(_) => TestSourceMessage {
                            error: false,
                            done: true,
                            message: "Successfully validated SSE connection".to_string(),
                        },
                        Err(e) => TestSourceMessage {
                            error: true,
                            done: true,
                            message: e.to_string(),
                        },
                    })
                    .unwrap()))
                .await
                .unwrap();
        });
    }

    async fn test_internal(&self) -> anyhow::Result<()> {
        let mut client = eventsource_client::ClientBuilder::for_url(&self.config.endpoint)
            .map_err(|_| anyhow!("Endpoint URL is invalid"))?;

        let headers = string_to_map(
            self.config
                .headers
                .as_ref()
                .map(|t| t.0.as_str())
                .unwrap_or(""),
        )
        .ok_or_else(|| anyhow!("Headers are invalid; should be comma-separated pairs"))?;

        for (k, v) in headers {
            client = client
                .header(&k, &v)
                .map_err(|_| anyhow!("Invalid header '{}: {}'", k, v))?;
        }

        let mut stream = client.build().stream();

        let timeout = Duration::from_secs(30);

        let message = TestSourceMessage {
            error: false,
            done: false,
            message: "Constructed SSE client".to_string(),
        };
        self.tx
            .send(Ok(Event::default().json_data(message).unwrap()))
            .await
            .unwrap();

        tokio::select! {
            val = stream.next() => {
                // TODO: validate schema
                match val {
                    Some(Ok(_)) => {
                        let message = TestSourceMessage {
                            error: false,
                            done: false,
                            message: "Received message from SSE server".to_string()
                        };
                        self.tx.send(Ok(Event::default().json_data(message).unwrap())).await.unwrap();
                    }
                    Some(Err(e)) => {
                        bail!("Received error from server: {:?}", e);
                    }
                    None => {
                        bail!("Server closed connection");
                    }
                }
            }
            _ = tokio::time::sleep(timeout) => {
                bail!("Did not receive any messages after 30 seconds");
            }
        };

        Ok(())
    }
}
