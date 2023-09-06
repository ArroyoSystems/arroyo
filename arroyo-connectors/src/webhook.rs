use std::{convert::Infallible, time::Duration};

use anyhow::{anyhow, bail};
use arroyo_rpc::OperatorConfig;
use arroyo_types::string_to_map;
use axum::response::sse::Event;
use reqwest::{header::HeaderMap, header::HeaderName, header::HeaderValue, Client, Request};
use serde_json::json;
use tokio::sync::mpsc::Sender;
use typify::import_types;

use arroyo_rpc::types::{ConnectionSchema, ConnectionType, TestSourceMessage};
use serde::{Deserialize, Serialize};

use crate::{pull_opt, Connection, EmptyConfig};

use super::Connector;

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/webhook/table.json");

import_types!(schema = "../connector-schemas/webhook/table.json");
const ICON: &str = include_str!("../resources/webhook.svg");

pub struct WebhookConnector {}

impl WebhookConnector {
    fn construct_test_request(config: &WebhookTable) -> anyhow::Result<(Client, Request)> {
        if let Err(e) = reqwest::Url::parse(&config.endpoint) {
            bail!("invalid endpoint '{}': {:?}", config.endpoint, e)
        };

        let headers: anyhow::Result<HeaderMap> =
            string_to_map(config.headers.as_ref().map(|t| t.0.as_str()).unwrap_or(""))
                .expect("Invalid header map")
                .into_iter()
                .map(|(k, v)| {
                    Ok((
                        TryInto::<HeaderName>::try_into(&k)
                            .map_err(|_| anyhow!("invalid header name {}", k))?,
                        TryInto::<HeaderValue>::try_into(&v)
                            .map_err(|_| anyhow!("invalid header value {}", v))?,
                    ))
                })
                .collect();

        let client = reqwest::ClientBuilder::new()
            .default_headers(headers?)
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| anyhow!("could not construct HTTP client: {:?}", e))?;

        let req = client
            .post(&config.endpoint)
            // TODO: use the schema to construct a correctly-formatted message
            .body(
                serde_json::to_string(&json! {{
                    "message": "this is a test message from the Arroyo Webhook Sink"
                }})
                .unwrap(),
            )
            .build()
            .map_err(|e| anyhow!("invalid URL for websink: {}", e.to_string()))?;

        Ok((client, req))
    }

    async fn test_int(
        config: &WebhookTable,
        tx: Sender<Result<Event, Infallible>>,
    ) -> anyhow::Result<()> {
        let (client, req) = Self::construct_test_request(config)?;

        tx.send(Ok(Event::default()
            .json_data(TestSourceMessage {
                error: false,
                done: false,
                message: "Sending websink message".to_string(),
            })
            .unwrap()))
            .await
            .unwrap();

        client
            .execute(req)
            .await
            .map_err(|e| anyhow!("HTTP request failed: {}", e))?;

        Ok(())
    }
}

impl Connector for WebhookConnector {
    type ProfileT = EmptyConfig;

    type TableT = WebhookTable;

    fn name(&self) -> &'static str {
        "webhook"
    }

    fn metadata(&self) -> arroyo_rpc::types::Connector {
        arroyo_rpc::types::Connector {
            id: "webhook".to_string(),
            name: "Webhook".to_string(),
            icon: ICON.to_string(),
            description: "Sink results via Webhooks".to_string(),
            enabled: true,
            source: false,
            sink: true,
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
        tokio::task::spawn(async move {
            let message = match Self::test_int(&table, tx.clone()).await {
                Ok(_) => TestSourceMessage {
                    error: false,
                    done: true,
                    message: "Successfully validated webhook".to_string(),
                },
                Err(err) => TestSourceMessage {
                    error: true,
                    done: true,
                    message: format!("{:?}", err),
                },
            };

            tx.send(Ok(Event::default().json_data(message).unwrap()))
                .await
                .unwrap();
        });
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
        let description = format!("WebhookSource<{}>", table.endpoint);

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for webhook connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for webhook connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: ConnectionType::Sink,
            schema,
            operator: "connectors::webhook::WebhookSinkFunc::<#in_k, #in_t>".to_string(),
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

        let headers = opts
            .remove("headers")
            .map(|s| s.try_into())
            .transpose()
            .map_err(|e| anyhow!("invalid value for 'headers' config: {:?}", e))?;

        let table = WebhookTable { endpoint, headers };
        let _ = Self::construct_test_request(&table)?;

        self.from_config(None, name, EmptyConfig {}, table, schema)
    }
}
