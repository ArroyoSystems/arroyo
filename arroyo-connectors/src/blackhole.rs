use arroyo_rpc::types::{ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::OperatorConfig;
use axum::response::sse::Event;
use std::convert::Infallible;

use crate::{Connection, Connector, EmptyConfig};

pub struct BlackholeConnector {}

const ICON: &str = include_str!("../resources/blackhole.svg");

impl Connector for BlackholeConnector {
    type ProfileT = EmptyConfig;
    type TableT = EmptyConfig;

    fn name(&self) -> &'static str {
        "null"
    }

    fn metadata(&self) -> arroyo_rpc::types::Connector {
        arroyo_rpc::types::Connector {
            id: "blackhole".to_string(),
            name: "Blackhole".to_string(),
            icon: ICON.to_string(),
            description: "No-op sink that swallows all data".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: false,
            hidden: false,
            custom_schemas: true,
            connection_config: None,
            table_config: "{\"type\": \"object\", \"title\": \"BlackholeTable\"}".to_string(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        return ConnectionType::Source;
    }

    fn get_schema(
        &self,
        _: Self::ProfileT,
        _: Self::TableT,
        s: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        s.cloned()
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(Ok(Event::default().json_data(message).unwrap()))
                .await
                .unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        _: &mut std::collections::HashMap<String, String>,
        s: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        self.from_config(None, name, EmptyConfig {}, EmptyConfig {}, s)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        s: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let description = "Blackhole".to_string();

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: None,
            framing: None,
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: ConnectionType::Sink,
            schema: s.cloned().unwrap_or_else(|| ConnectionSchema {
                format: None,
                framing: None,
                struct_name: None,
                fields: vec![],
                definition: None,
            }),
            operator: "connectors::blackhole::BlackholeSinkFunc::<#in_k, #in_t>".to_string(),
            config: serde_json::to_string(&config).unwrap(),
            description,
        })
    }
}
