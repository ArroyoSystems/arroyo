use arroyo_rpc::grpc::{
    self,
    api::{ConnectionSchema, Format, FormatOptions, TestSourceMessage},
};

use crate::{Connection, ConnectionType, Connector, EmptyConfig, OperatorConfig};

pub struct BlackholeConnector {}

const ICON: &str = include_str!("../resources/blackhole.svg");

impl Connector for BlackholeConnector {
    type ConfigT = EmptyConfig;
    type TableT = EmptyConfig;

    fn name(&self) -> &'static str {
        "null"
    }

    fn metadata(&self) -> grpc::api::Connector {
        grpc::api::Connector {
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

    fn table_type(&self, _: Self::ConfigT, _: Self::TableT) -> arroyo_rpc::grpc::api::TableType {
        return grpc::api::TableType::Source;
    }

    fn get_schema(
        &self,
        _: Self::ConfigT,
        _: Self::TableT,
        s: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        s.cloned()
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
        _: &mut std::collections::HashMap<String, String>,
        s: Option<&arroyo_rpc::grpc::api::ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        self.from_config(None, name, EmptyConfig {}, EmptyConfig {}, s)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ConfigT,
        table: Self::TableT,
        s: Option<&arroyo_rpc::grpc::api::ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let description = "Blackhole".to_string();

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            serialization_mode: None,
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: ConnectionType::Sink,
            schema: s.cloned().unwrap_or_else(|| ConnectionSchema {
                format: Some(Format::JsonFormat as i32),
                format_options: Some(FormatOptions::default()),
                struct_name: None,
                fields: vec![],
                definition: None,
            }),
            operator: "connectors::blackhole::BlackholeSinkFunc::<#in_k, #in_t>".to_string(),
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
