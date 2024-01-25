mod operator;

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, bail};
use arroyo_rpc::{var_str::VarStr, OperatorConfig};
use arroyo_types::string_to_map;
use eventsource_client::Client;
use futures::StreamExt;
use tokio::sync::mpsc::Sender;
use typify::import_types;

use arroyo_operator::connector::Connection;
use arroyo_operator::operator::OperatorNode;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use serde::{Deserialize, Serialize};

use crate::sse::operator::SSESourceFunc;
use crate::{pull_opt, EmptyConfig};

use arroyo_operator::connector::Connector;

const TABLE_SCHEMA: &str = include_str!("./table.json");

import_types!(schema = "src/sse/table.json", convert = { {type = "string", format = "var-str"} = VarStr });
const ICON: &str = include_str!("./sse.svg");

pub struct SSEConnector {}

impl Connector for SSEConnector {
    type ProfileT = EmptyConfig;

    type TableT = SseTable;

    fn name(&self) -> &'static str {
        "sse"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
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
        tx: Sender<TestSourceMessage>,
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
    ) -> anyhow::Result<arroyo_operator::connector::Connection> {
        let description = format!("SSESource<{}>", table.endpoint);

        if let Some(headers) = &table.headers {
            string_to_map(&headers.sub_env_vars()?).ok_or_else(|| {
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
            bad_data: schema.bad_data.clone(),
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
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let endpoint = pull_opt("endpoint", options)?;
        let headers = options.remove("headers");
        let events = options.remove("events");

        self.from_config(
            None,
            name,
            EmptyConfig {},
            SseTable {
                endpoint,
                events,
                headers: headers.map(|s| VarStr::new(s)),
            },
            schema,
        )
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<OperatorNode> {
        SSESourceFunc::new(table, config)
    }
}

struct SseTester {
    config: SseTable,
    tx: Sender<TestSourceMessage>,
}

impl SseTester {
    pub fn start(self) {
        tokio::task::spawn(async move {
            self.tx
                .send(match self.test_internal().await {
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
                .await
                .unwrap();
        });
    }

    async fn test_internal(&self) -> anyhow::Result<()> {
        let mut client = eventsource_client::ClientBuilder::for_url(&self.config.endpoint)
            .map_err(|_| anyhow!("Endpoint URL is invalid"))?;

        let headers = string_to_map(
            &self
                .config
                .headers
                .as_ref()
                .map(|s| s.sub_env_vars())
                .transpose()?
                .unwrap_or("".to_string()),
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
        self.tx.send(message).await.unwrap();

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
                        self.tx.send(message).await.unwrap();
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
