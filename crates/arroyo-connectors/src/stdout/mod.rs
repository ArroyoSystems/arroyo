mod operator;

use std::collections::HashMap;

use anyhow::anyhow;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use tokio::io::BufWriter;

use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use tokio::sync::mpsc::Sender;

use crate::EmptyConfig;

use crate::stdout::operator::StdoutSink;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::formats::{Format, JsonFormat};

pub struct StdoutConnector {}

const ICON: &str = include_str!("./stdout.svg");

impl Connector for StdoutConnector {
    type ProfileT = EmptyConfig;
    type TableT = EmptyConfig;
    fn name(&self) -> &'static str {
        "stdout"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "stdout".to_string(),
            name: "StdOut".to_string(),
            icon: ICON.to_string(),
            description: "Write outputs to stdout".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: false,
            hidden: false,
            custom_schemas: true,
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
        name: &str,
        _options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        self.from_config(None, name, EmptyConfig {}, EmptyConfig {}, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let description = "StdoutSink".to_string();

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for stdout sink"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for stdout sink"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: vec![],
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
        c: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        let format = c
            .format
            .unwrap_or_else(|| Format::Json(JsonFormat::default()));
        Ok(ConstructedOperator::from_operator(Box::new(StdoutSink {
            stdout: BufWriter::new(tokio::io::stdout()),
            serializer: ArrowSerializer::new(format),
        })))
    }
}
