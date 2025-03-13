use anyhow::Result;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};

use crate::EmptyConfig;
mod operator;

use crate::amqp::operator::AmqpSourceFunc;
pub struct AmqpConnector {}

static ICON: &str = &"placeholdersvg";

impl Connector for AmqpConnector {
    type ProfileT = EmptyConfig;
    type TableT = EmptyConfig;

    fn name(&self) -> &'static str {
        "amqp"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: self.name().to_string(),
            name: "AMQP".to_string(),
            icon: ICON.to_string(),
            description: "Source of AMQP messages".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: false,
            hidden: false,
            custom_schemas: true,
            connection_config: None,
            table_config: "{\"type\": \"object\", \"title\": \"Amqp\"}".to_string(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Source
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
        s: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let description = "Amqp".to_string();

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: None,
            bad_data: None,
            framing: None,
            metadata_fields: vec![],
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            ConnectionType::Source,
            s.cloned()
                .ok_or_else(|| anyhow!("no schema for amqp source"))?,
            &config,
            description,
        ))
    }

    // todo I need a table json actually for this and import it in the mod.rs
    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> Result<ConstructedOperator> {
        match table.table_type {
            TableType::Source => {
                panic!("todo implement now")
                Ok(ConstructedOperator::from_source(Box::new(AmqpSourceFunc {
                    address: table,
                    topic: config.bad_data,
                    format: config.format,
                    framing: todo!(),
                    bad_data: config.bad_data?,
                    metadata_fields: todo!(),
                })))
            }

            TableType::Sink => unreachable!("not yet attempted"),
        }
    }
}
