mod operator;

use std::str::FromStr;
use std::time::Duration;

use anyhow::anyhow;
use arroyo_rpc::{ConnectorOptions, OperatorConfig, var_str::VarStr};
use arroyo_types::string_to_map;
use reqwest::{Client, Request};
use tokio::sync::mpsc::Sender;
use typify::import_types;

use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use serde::{Deserialize, Serialize};

use crate::{EmptyConfig, construct_http_client};

use crate::polling_http::operator::{PollingHttpSourceFunc, PollingHttpSourceState};
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;

const TABLE_SCHEMA: &str = include_str!("./table.json");
const DEFAULT_POLLING_INTERVAL: Duration = Duration::from_secs(1);

import_types!(
    schema = "src/polling_http/table.json",
    convert = { {type = "string", format = "var-str"} = VarStr }
);
const ICON: &str = include_str!("./http.svg");

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
        tx: Sender<TestSourceMessage>,
    ) -> anyhow::Result<()> {
        let headers = config
            .headers
            .as_ref()
            .map(|s| s.sub_env_vars())
            .transpose()?;

        let client = construct_http_client(&config.endpoint, headers)?;
        let req = Self::construct_test_request(&client, config)?;

        tx.send(TestSourceMessage {
            error: false,
            done: false,
            message: "Requesting data".to_string(),
        })
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

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
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
        ConnectionType::Source
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
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
                    message: format!("{}", err.root_cause()),
                },
            };

            tx.send(message).await.unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let endpoint = options.pull_str("endpoint")?;
        let headers = options.pull_opt_str("headers")?;
        let method: Option<Method> = options
            .pull_opt_str("method")?
            .map(|s| s.try_into())
            .transpose()
            .map_err(|_| anyhow!("invalid value for 'method'"))?;

        let body = options.pull_opt_str("body")?;

        let interval = options.pull_opt_i64("poll_interval_ms")?;
        let emit_behavior: Option<EmitBehavior> = options
            .pull_opt_str("emit_behavior")?
            .map(|s| s.try_into())
            .transpose()
            .map_err(|_| anyhow!("invalid value for 'emit_behavior'"))?;

        self.from_config(
            None,
            name,
            EmptyConfig {},
            PollingHttpTable {
                endpoint,
                headers: headers.map(VarStr::new),
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
    ) -> anyhow::Result<arroyo_operator::connector::Connection> {
        let description = format!("PollingHTTPSource<{}>", table.endpoint);

        if let Some(headers) = &table.headers {
            string_to_map(&headers.sub_env_vars()?, ':').ok_or_else(|| {
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
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: vec![],
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            ConnectionType::Source,
            schema,
            &config,
            description,
        ))
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        let headers = string_to_map(
            &table
                .headers
                .as_ref()
                .map(|t| t.sub_env_vars().expect("Failed to substitute env vars"))
                .unwrap_or("".to_string()),
            ':',
        )
        .expect("Invalid header map")
        .into_iter()
        .map(|(k, v)| {
            (
                (&k).try_into()
                    .unwrap_or_else(|_| panic!("invalid header name {k}")),
                (&v).try_into()
                    .unwrap_or_else(|_| panic!("invalid header value {v}")),
            )
        })
        .collect();

        Ok(ConstructedOperator::from_source(Box::new(
            PollingHttpSourceFunc {
                state: PollingHttpSourceState::default(),
                client: reqwest::ClientBuilder::new()
                    .default_headers(headers)
                    .timeout(Duration::from_secs(5))
                    .build()
                    .expect("could not construct http client"),
                endpoint: url::Url::from_str(&table.endpoint).expect("invalid endpoint"),
                method: match table.method {
                    None | Some(Method::Get) => reqwest::Method::GET,
                    Some(Method::Post) => reqwest::Method::POST,
                    Some(Method::Put) => reqwest::Method::PUT,
                    Some(Method::Patch) => reqwest::Method::PATCH,
                },
                body: table.body.map(|b| b.into()),
                polling_interval: table
                    .poll_interval_ms
                    .map(|d| Duration::from_millis(d as u64))
                    .unwrap_or(DEFAULT_POLLING_INTERVAL),
                emit_behavior: table.emit_behavior.unwrap_or(EmitBehavior::All),
                format: config
                    .format
                    .expect("PollingHTTP source must have a format"),
                framing: config.framing,
                bad_data: config.bad_data,
            },
        )))
    }
}
