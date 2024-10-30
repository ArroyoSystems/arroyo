use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use anyhow::anyhow;
use arrow::datatypes::DataType;
use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::OperatorConfig;
use arroyo_types::string_to_map;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::{connect_async, tungstenite};
use tungstenite::http::Request;
use typify::import_types;

use crate::{header_map, pull_opt, EmptyConfig};

use crate::websocket::operator::{WebsocketSourceFunc, WebsocketSourceState};
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::OperatorNode;

mod operator;

const TABLE_SCHEMA: &str = include_str!("./table.json");

import_types!(schema = "src/websocket/table.json", convert = { {type = "string", format = "var-str"} = VarStr });
const ICON: &str = include_str!("./websocket.svg");

pub struct WebsocketConnector {}

impl Connector for WebsocketConnector {
    type ProfileT = EmptyConfig;

    type TableT = WebsocketTable;

    fn name(&self) -> &'static str {
        "websocket"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
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
        _: Self::ProfileT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let send = |error: bool, done: bool, message: String| {
                let tx = tx.clone();
                async move {
                    let msg = TestSourceMessage {
                        error,
                        done,
                        message,
                    };
                    tx.send(msg).await.unwrap();
                }
            };

            let headers_str = match table.headers.as_ref().map(|s| s.sub_env_vars()).transpose() {
                Ok(headers) => headers,
                Err(e) => {
                    send(true, true, format!("{}", e.root_cause())).await;
                    return;
                }
            };

            let headers = match string_to_map(&headers_str.unwrap_or("".to_string()), ':')
                .ok_or_else(|| anyhow!("Headers are invalid; should be comma-separated pairs"))
            {
                Ok(headers) => headers,
                Err(e) => {
                    send(true, true, format!("Failed to parse headers: {:?}", e)).await;
                    return;
                }
            };

            let uri = match Uri::from_str(&table.endpoint.to_string()) {
                Ok(uri) => uri,
                Err(e) => {
                    send(true, true, format!("Failed to parse endpoint: {:?}", e)).await;
                    return;
                }
            };

            let host = match uri.host() {
                Some(host) => host,
                None => {
                    send(true, true, "Endpoint must have a host".to_string()).await;
                    return;
                }
            };

            let mut request_builder = Request::builder().uri(&table.endpoint);

            for (k, v) in headers {
                request_builder = request_builder.header(k, v);
            }

            let request = match request_builder
                .header("Host", host)
                .header("Sec-WebSocket-Key", generate_key())
                .header("Sec-WebSocket-Version", "13")
                .header("Connection", "Upgrade")
                .header("Upgrade", "websocket")
                .body(())
            {
                Ok(request) => request,
                Err(e) => {
                    send(true, true, format!("Failed to build request: {:?}", e)).await;
                    return;
                }
            };

            let ws_stream = match connect_async(request).await {
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

            for msg in table.subscription_messages {
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

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Source
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        _metadata_fields: Option<HashMap<String, (String, DataType)>>,
    ) -> anyhow::Result<arroyo_operator::connector::Connection> {
        let description = format!("WebsocketSource<{}>", table.endpoint);

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
            .ok_or_else(|| anyhow!("no schema defined for WebSocket connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for WebSocket connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            additional_fields: None,
        };

        Ok(Connection {
            id,
            connector: self.name(),
            name: name.to_string(),
            connection_type: ConnectionType::Source,
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
        _metadata_fields: Option<HashMap<String, (String, DataType)>>,
    ) -> anyhow::Result<Connection> {
        let endpoint = pull_opt("endpoint", options)?;
        let headers = options.remove("headers");
        let mut subscription_messages = vec![];

        // add the single subscription message if it exists
        if let Some(message) = options.remove("subscription_message") {
            subscription_messages.push(SubscriptionMessage(message));

            if options.contains_key("subscription_messages.0") {
                return Err(anyhow!(
                    "Cannot specify both 'subscription_message' and 'subscription_messages.0'"
                ));
            }
        }

        // add the indexed subscription messages if they exist
        let mut message_index = 0;
        while let Some(message) =
            options.remove(&format!("subscription_messages.{}", message_index))
        {
            subscription_messages.push(SubscriptionMessage(message));
            message_index += 1;
        }

        self.from_config(
            None,
            name,
            EmptyConfig {},
            WebsocketTable {
                endpoint,
                headers: headers.map(VarStr::new),
                subscription_message: None,
                subscription_messages,
            },
            schema,
            None,
        )
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<OperatorNode> {
        // Include subscription_message for backwards compatibility
        let mut subscription_messages = vec![];
        if let Some(message) = table.subscription_message {
            subscription_messages.push(message.to_string());
        };
        subscription_messages.extend(
            table
                .subscription_messages
                .into_iter()
                .map(|m| m.to_string()),
        );

        let headers = header_map(table.headers)
            .into_iter()
            .map(|(k, v)| ((&k).into(), (&v).into()))
            .collect();

        Ok(OperatorNode::from_source(Box::new(WebsocketSourceFunc {
            url: table.endpoint,
            headers,
            subscription_messages,
            format: config
                .format
                .ok_or_else(|| anyhow!("format required for websocket source"))?,
            framing: config.framing,
            bad_data: config.bad_data,
            state: WebsocketSourceState::default(),
        })))
    }
}
