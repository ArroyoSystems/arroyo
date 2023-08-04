use anyhow::{anyhow, bail};
use arroyo_rpc::grpc::{
    self,
    api::{source_field_type, ConnectionSchema, TestSourceMessage},
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use typify::import_types;

use crate::{
    pull_opt, source_field, Connection, ConnectionType, Connector, EmptyConfig, OperatorConfig,
};

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/impulse/table.json");

import_types!(schema = "../connector-schemas/impulse/table.json");
const ICON: &str = include_str!("../resources/impulse.svg");

pub fn impulse_schema() -> ConnectionSchema {
    use grpc::api::PrimitiveType::*;
    use source_field_type::Type::Primitive;
    ConnectionSchema {
        format: None,
        struct_name: Some("arroyo_types::ImpulseEvent".to_string()),
        fields: vec![
            source_field("counter", Primitive(UInt64 as i32)),
            source_field("subtask_index", Primitive(UInt64 as i32)),
        ],
        format_options: None,
        definition: None,
    }
}

pub struct ImpulseConnector {}

impl Connector for ImpulseConnector {
    type ConfigT = EmptyConfig;
    type TableT = ImpulseTable;

    fn name(&self) -> &'static str {
        "impulse"
    }

    fn metadata(&self) -> arroyo_rpc::types::Connector {
        arroyo_rpc::types::Connector {
            id: "impulse".to_string(),
            name: "Impulse".to_string(),
            icon: ICON.to_string(),
            description: "Periodic demo source".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: false,
            hidden: false,
            custom_schemas: false,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn table_type(&self, _: Self::ConfigT, _: Self::TableT) -> arroyo_rpc::grpc::api::TableType {
        return grpc::api::TableType::Source;
    }

    fn get_schema(
        &self,
        _: Self::ConfigT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        Some(impulse_schema())
    }

    fn test(
        &self,
        _: &str,
        _: Self::ConfigT,
        _: Self::TableT,
        _: Option<&arroyo_rpc::grpc::api::ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<
            Result<arroyo_rpc::grpc::api::TestSourceMessage, tonic::Status>,
        >,
    ) {
        tokio::task::spawn(async move {
            tx.send(Ok(TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            }))
            .await
            .unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut std::collections::HashMap<String, String>,
        s: Option<&arroyo_rpc::grpc::api::ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let event_rate = f64::from_str(&pull_opt("event_rate", options)?)
            .map_err(|_| anyhow!("invalid value for event_rate; expected float"))?;

        let event_time_interval: Option<i64> = options
            .remove("event_time_interval")
            .map(|t| i64::from_str(&t))
            .transpose()
            .map_err(|_| anyhow!("invalid value for event_time_interval; expected float"))?;

        let message_count: Option<i64> = options
            .remove("message_count")
            .map(|t| i64::from_str(&t))
            .transpose()
            .map_err(|_| anyhow!("invalid value for event_time_interval; expected float"))?;

        // validate the schema
        if let Some(s) = s {
            if s.fields != impulse_schema().fields {
                bail!("invalid schema for impulse source");
            }
        }

        self.from_config(
            None,
            name,
            EmptyConfig {},
            ImpulseTable {
                event_rate,
                event_time_interval,
                message_count,
            },
            None,
        )
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ConfigT,
        table: Self::TableT,
        _: Option<&arroyo_rpc::grpc::api::ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let description = format!(
            "{}Impulse<{} eps{}>",
            if table.message_count.is_some() {
                "Bounded"
            } else {
                ""
            },
            table.event_rate,
            table
                .event_time_interval
                .map(|t| format!(", {} micros", t))
                .unwrap_or("".to_string())
        );

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            serialization_mode: None,
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: ConnectionType::Source,
            schema: impulse_schema(),
            operator: "connectors::impulse::ImpulseSourceFunc".to_string(),
            config: serde_json::to_string(&config).unwrap(),
            description,
        })
    }

    fn parse_config(&self, s: &str) -> Result<Self::ConfigT, serde_json::Error> {
        serde_json::from_str(if s.is_empty() { "{}" } else { s })
    }

    fn parse_table(&self, s: &str) -> Result<Self::TableT, serde_json::Error> {
        serde_json::from_str(s)
    }
}
