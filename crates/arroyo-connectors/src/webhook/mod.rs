mod operator;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::anyhow;
use arroyo_rpc::OperatorConfig;

use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::var_str::VarStr;
use reqwest::{Client, Request};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, Semaphore};
use typify::import_types;

use crate::{construct_http_client, pull_opt, EmptyConfig};

use crate::webhook::operator::WebhookSinkFunc;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::OperatorNode;

const TABLE_SCHEMA: &str = include_str!("./table.json");

import_types!(schema = "src/webhook/table.json", convert = { {type = "string", format = "var-str"} = VarStr });
const ICON: &str = include_str!("./webhook.svg");

const MAX_INFLIGHT: u32 = 50;

pub struct WebhookConnector {}

impl WebhookConnector {
    fn construct_test_request(client: &Client, config: &WebhookTable) -> anyhow::Result<Request> {
        let req = client
            .post(config.endpoint.sub_env_vars()?)
            // TODO: use the schema to construct a correctly-formatted message
            .body(
                serde_json::to_string(&json! {{
                    "message": "this is a test message from the Arroyo Webhook Sink"
                }})
                .unwrap(),
            )
            .build()
            .map_err(|e| anyhow!("invalid URL for websink: {}", e.to_string()))?;

        Ok(req)
    }

    async fn test_int(config: &WebhookTable, tx: Sender<TestSourceMessage>) -> anyhow::Result<()> {
        let headers = config
            .headers
            .as_ref()
            .map(|s| s.sub_env_vars())
            .transpose()?;

        let client = construct_http_client(&config.endpoint.sub_env_vars()?, headers)?;
        let req = Self::construct_test_request(&client, config)?;

        tx.send(TestSourceMessage {
            error: false,
            done: false,
            message: "Sending websink message".to_string(),
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

impl Connector for WebhookConnector {
    type ProfileT = EmptyConfig;

    type TableT = WebhookTable;

    fn name(&self) -> &'static str {
        "webhook"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
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
        tx: Sender<TestSourceMessage>,
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

            tx.send(message).await.unwrap();
        });
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Sink
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        _enable_metadata: Option<bool>,
        _metadata_fields: Option<HashMap<String, String>>,
    ) -> anyhow::Result<arroyo_operator::connector::Connection> {
        let description = format!("WebhookSink<{}>", table.endpoint.sub_env_vars()?);

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
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            enable_metadata: None,
            metadata_fields: None,
        };

        Ok(Connection {
            id,
            connector: self.name(),
            name: name.to_string(),
            connection_type: ConnectionType::Sink,
            schema,
            config: serde_json::to_string(&config).unwrap(),
            description,
        })
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
        _enable_metadata: Option<bool>,
        _metadata_fields: Option<HashMap<String, String>>,
    ) -> anyhow::Result<Connection> {
        let endpoint = pull_opt("endpoint", options)?;

        let headers = options.remove("headers").map(VarStr::new);

        let table = WebhookTable {
            endpoint: VarStr::new(endpoint),
            headers,
        };

        let client = construct_http_client(
            &table.endpoint.sub_env_vars()?,
            table
                .headers
                .as_ref()
                .map(|s| s.sub_env_vars())
                .transpose()?,
        )?;
        let _ = Self::construct_test_request(&client, &table)?;

        self.from_config(None, name, EmptyConfig {}, table, schema, None, None)
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<OperatorNode> {
        let url = table.endpoint.sub_env_vars()?;
        Ok(OperatorNode::from_operator(Box::new(WebhookSinkFunc {
            url: Arc::new(url.clone()),
            client: construct_http_client(
                &url,
                table
                    .headers
                    .as_ref()
                    .map(|s| s.sub_env_vars())
                    .transpose()?,
            )?,
            semaphore: Arc::new(Semaphore::new(MAX_INFLIGHT as usize)),
            serializer: ArrowSerializer::new(
                config
                    .format
                    .expect("No format configured for webhook sink"),
            ),
            last_reported_error_at: Arc::new(Mutex::new(SystemTime::UNIX_EPOCH)),
        })))
    }
}
