use crate::blackhole::operator::BlackholeSinkFunc;
use anyhow::anyhow;
use arrow::datatypes::DataType;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::OperatorNode;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::OperatorConfig;
use std::collections::HashMap;

use crate::EmptyConfig;

mod operator;

pub struct BlackholeConnector {}

const ICON: &str = include_str!("./blackhole.svg");

impl Connector for BlackholeConnector {
    type ProfileT = EmptyConfig;
    type TableT = EmptyConfig;

    fn name(&self) -> &'static str {
        "blackhole"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: self.name().to_string(),
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
        ConnectionType::Sink
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
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(message).await.unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        _options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
        _metadata_fields: Option<HashMap<String, (String, DataType)>>,
    ) -> anyhow::Result<Connection> {
        self.from_config(None, name, EmptyConfig {}, EmptyConfig {}, schema, None)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        s: Option<&ConnectionSchema>,
        _metadata_fields: Option<HashMap<String, (String, DataType)>>,
    ) -> anyhow::Result<Connection> {
        let description = "Blackhole".to_string();

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: None,
            bad_data: None,
            framing: None,
            additional_fields: None,
        };

        Ok(Connection {
            id,
            connector: self.name(),
            name: name.to_string(),
            connection_type: ConnectionType::Sink,
            schema: s
                .cloned()
                .ok_or_else(|| anyhow!("no schema for blackhole sink"))?,
            config: serde_json::to_string(&config).unwrap(),
            description,
        })
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        _: Self::TableT,
        _: OperatorConfig,
    ) -> anyhow::Result<OperatorNode> {
        Ok(OperatorNode::from_operator(Box::new(
            BlackholeSinkFunc::new(),
        )))
    }
}
