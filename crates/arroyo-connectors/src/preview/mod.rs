mod operator;

use std::collections::HashMap;

use anyhow::{anyhow, bail};
use arroyo_rpc::OperatorConfig;

use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use tokio::sync::mpsc::Sender;

use crate::EmptyConfig;

use crate::preview::operator::PreviewSink;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::OperatorNode;

pub struct PreviewConnector {}

impl Connector for PreviewConnector {
    type ProfileT = EmptyConfig;
    type TableT = EmptyConfig;
    fn name(&self) -> &'static str {
        "preview"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "preview".to_string(),
            name: "Preview".to_string(),
            icon: "".to_string(),
            description: "Preview outputs in the console".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: false,
            hidden: true,
            custom_schemas: false,
            connection_config: None,
            table_config: "{}".to_string(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Sink
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
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
        _: &str,
        _: &mut HashMap<String, String>,
        _: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        bail!("Preview connector cannot be created in SQL");
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let description = "PreviewSink".to_string();

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for preview connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: None,
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
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

    fn make_operator(
        &self,
        _: Self::ProfileT,
        _: Self::TableT,
        _: OperatorConfig,
    ) -> anyhow::Result<OperatorNode> {
        Ok(OperatorNode::from_operator(Box::<PreviewSink>::default()))
    }
}
