use std::convert::Infallible;

use anyhow::anyhow;
use arroyo_rpc::OperatorConfig;
use arroyo_types::string_to_map;
use axum::response::sse::Event;
use reqwest::{Client, Request};
use tokio::sync::mpsc::Sender;
use typify::import_types;

use arroyo_rpc::types::{ConnectionSchema, ConnectionType, TestSourceMessage};
use serde::{Deserialize, Serialize};

use crate::{construct_http_client, pull_opt, pull_option_to_i64, Connection, EmptyConfig};

use super::Connector;

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/polling_http/table.json");

import_types!(schema = "../connector-schemas/polling_http/table.json");
const ICON: &str = include_str!("../resources/http.svg");

pub struct PollingHTTPConnector {}

impl PollingHTTPConnector {
    fn construct_test_request(
        client: &Client,
        config: &PollingHttpTable,
    ) -> anyhow::Result<Request> {
        let mut req = client.request(
            match config.method {
                None | Some(Method::Get) => reqwest::Method::GET,
                Some(Method::Post) => reqwest::Method::POST,
                Some(Method::Put) => reqwest::Method::PUT,
                Some(Method::Patch) => reqwest::Method::PATCH,
            },
            &config.endpoint,
        );

        if let Some(body) = &config.body {
            req = req.body(body.clone());
        }

        let req = req
            .build()
            .map_err(|e| anyhow!("invalid request: {}", e.to_string()))?;

        Ok(req)
    }

    async fn test_int(
        config: &PollingHttpTable,
        tx: Sender<Result<Event, Infallible>>,
    ) -> anyhow::Result<()> {
        let client =
            construct_http_client(&config.endpoint, config.headers.as_ref().map(|t| &t.0))?;
        let req = Self::construct_test_request(&client, config)?;

        tx.send(Ok(Event::default()
            .json_data(TestSourceMessage {
                error: false,
                done: false,
                message: "Requesting data".to_string(),
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

impl Connector for PollingHTTPConnector {
    type ProfileT = EmptyConfig;

    type TableT = PollingHttpTable;

    fn name(&self) -> &'static str {
        "polling_http"
    }

    fn metadata(&self) -> arroyo_rpc::types::Connector {
        arroyo_rpc::types::Connector {
            id: "polling_http".to_string(),
            name: "Polling HTTP".to_string(),
            icon: ICON.to_string(),
            description: "Poll an HTTP server to produce events".to_string(),
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

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        return ConnectionType::Source;
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
                    message: "Successfully validated source".to_string(),
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

    fn from_options(
        &self,
        name: &str,
        opts: &mut std::collections::HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<crate::Connection> {
        let endpoint = pull_opt("endpoint", opts)?;
        let headers = opts.remove("headers");
        let method: Option<Method> = opts
            .remove("method")
            .map(|s| s.try_into())
            .transpose()
            .map_err(|_| anyhow!("invalid value for 'method'"))?;

        let body = opts.remove("body");

        let interval = pull_option_to_i64("poll_interval_ms", opts)?;
        let emit_behavior: Option<EmitBehavior> = opts
            .remove("emit_behavior")
            .map(|s| s.try_into())
            .transpose()
            .map_err(|_| anyhow!("invalid value for 'emit_behavior'"))?;

        self.from_config(
            None,
            name,
            EmptyConfig {},
            PollingHttpTable {
                endpoint,
                headers: headers.map(Headers),
                method,
                body,
                poll_interval_ms: interval,
                emit_behavior,
            },
            schema,
        )
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<crate::Connection> {
        let description = format!("PollingHTTPSource<{}>", table.endpoint);

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
            .ok_or_else(|| anyhow!("no schema defined for polling HTTP connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for polling HTTP connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: ConnectionType::Source,
            schema,
            operator: "connectors::polling_http::PollingHttpSourceFunc".to_string(),
            config: serde_json::to_string(&config).unwrap(),
            description,
        })
    }
}
