use anyhow::bail;
use arroyo_rpc::grpc::{
    self,
    api::{source_field_type, StructType, TestSourceMessage},
};
use arroyo_rpc::OperatorConfig;
use serde::{Deserialize, Serialize};
use typify::import_types;

use crate::{
    nullable_field, source_field, Connection, ConnectionSchema, ConnectionType, Connector,
    EmptyConfig,
};

const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/nexmark/table.json");
const ICON: &str = include_str!("../resources/nexmark.svg");

import_types!(schema = "../connector-schemas/nexmark/table.json");

pub fn nexmark_schema() -> ConnectionSchema {
    use grpc::api::PrimitiveType::*;
    use source_field_type::Type::Primitive;
    ConnectionSchema {
        format: None,
        struct_name: Some("arroyo_types::nexmark::Event".to_string()),
        fields: vec![
            nullable_field(
                "person",
                source_field_type::Type::Struct(StructType {
                    name: Some("arroyo_types::nexmark::Person".to_string()),
                    fields: vec![
                        source_field("id", Primitive(Int64 as i32)),
                        source_field("name", Primitive(String as i32)),
                        source_field("email_address", Primitive(String as i32)),
                        source_field("credit_card", Primitive(String as i32)),
                        source_field("city", Primitive(String as i32)),
                        source_field("state", Primitive(String as i32)),
                        source_field("datetime", Primitive(UnixMillis as i32)),
                        source_field("extra", Primitive(String as i32)),
                    ],
                }),
            ),
            nullable_field(
                "bid",
                source_field_type::Type::Struct(StructType {
                    name: Some("arroyo_types::nexmark::Bid".to_string()),
                    fields: vec![
                        source_field("auction", Primitive(Int64 as i32)),
                        source_field("bidder", Primitive(Int64 as i32)),
                        source_field("price", Primitive(Int64 as i32)),
                        source_field("channel", Primitive(String as i32)),
                        source_field("url", Primitive(String as i32)),
                        source_field("datetime", Primitive(UnixMillis as i32)),
                        source_field("extra", Primitive(String as i32)),
                    ],
                }),
            ),
            nullable_field(
                "auction",
                source_field_type::Type::Struct(StructType {
                    name: Some("arroyo_types::nexmark::Auction".to_string()),
                    fields: vec![
                        source_field("id", Primitive(Int64 as i32)),
                        source_field("description", Primitive(String as i32)),
                        source_field("item_name", Primitive(String as i32)),
                        source_field("initial_bid", Primitive(Int64 as i32)),
                        source_field("reserve", Primitive(Int64 as i32)),
                        source_field("datetime", Primitive(UnixMillis as i32)),
                        source_field("expires", Primitive(UnixMillis as i32)),
                        source_field("seller", Primitive(Int64 as i32)),
                        source_field("category", Primitive(Int64 as i32)),
                        source_field("extra", Primitive(String as i32)),
                    ],
                }),
            ),
        ],
        definition: None,
    }
}

pub struct NexmarkConnector {}

impl Connector for NexmarkConnector {
    type ConfigT = EmptyConfig;
    type TableT = NexmarkTable;

    fn name(&self) -> &'static str {
        "nexmark"
    }

    fn metadata(&self) -> arroyo_rpc::types::Connector {
        arroyo_rpc::types::Connector {
            id: "nexmark".to_string(),
            name: "Nexmark".to_string(),
            icon: ICON.to_string(),
            description: "Demo source for a simulated auction website".to_string(),
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
        Some(nexmark_schema())
    }

    fn test(
        &self,
        _: &str,
        _: Self::ConfigT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
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
        _: &str,
        _: &mut std::collections::HashMap<String, String>,
        _: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        // let event_rate =
        //     f64::from_str(&pull_opt("event_rate", options)?)
        //     .map_err(|_| anyhow!("invalid value for event_rate; expected float"))?;

        // let runtime = options.remove("runtime")
        //     .map(|t| f64::from_str(&t))
        //     .transpose()
        //     .map_err(|_| anyhow!("invalid value for runtime; expected float"))?;

        // self.from_config(None, name, EmptyConfig {}, NexmarkTable {
        //     event_rate,
        //     runtime,
        // }, None)
        bail!("Nexmark sources cannot currently be created in SQL; create using the web ui instead")
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ConfigT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let description = format!(
            "{}Nexmark<{} eps>",
            if table.runtime.is_some() {
                "Bounded"
            } else {
                ""
            },
            table.event_rate
        );

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: None,
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: ConnectionType::Source,
            schema: nexmark_schema(),
            operator: "connectors::nexmark::NexmarkSourceFunc".to_string(),
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
