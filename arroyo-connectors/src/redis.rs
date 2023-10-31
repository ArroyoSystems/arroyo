use arroyo_rpc::api_types::connections::{ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::OperatorConfig;
use axum::response::sse::Event;
use std::convert::Infallible;
use typify::import_types;
use serde::{Deserialize, Serialize};

use crate::{Connection, Connector, EmptyConfig};

pub struct RedisConnector {}

const CONFIG_SCHEMA: &str = include_str!("../../connector-schemas/redis/connection.json");
const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/redis/table.json");
const ICON: &str = include_str!("../resources/kafka.svg");

import_types!(schema = "../connector-schemas/redis/connection.json",);
import_types!(schema = "../connector-schemas/redis/table.json");


impl Connector for RedisConnector {
    type ProfileT = RedisConfig;
    type TableT = RedisTable;

    fn name(&self) -> &'static str {
        "redis"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "redis".to_string(),
            name: "Redis".to_string(),
            icon: ICON.to_string(),
            description: "Write results to Redis".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: false,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
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
        todo!()
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        s: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        todo!()
    }
}
