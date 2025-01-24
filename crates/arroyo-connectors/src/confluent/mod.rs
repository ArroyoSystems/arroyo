use crate::kafka;
use crate::kafka::{
    KafkaConfig, KafkaConfigAuthentication, KafkaConnector, KafkaTable, KafkaTester, TableType,
};
use anyhow::anyhow;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Receiver;
use typify::import_types;

const CLIENT_ID: &str = "cwc|0014U00003Df8ZvQAJ";

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("../kafka/table.json");
const ICON: &str = include_str!("./confluent.svg");

import_types!(
    schema = "src/confluent/profile.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

pub struct ConfluentConnector {}

impl ConfluentConnector {
    pub fn connection_from_options(
        opts: &mut ConnectorOptions,
    ) -> anyhow::Result<ConfluentProfile> {
        let schema_registry: Option<anyhow::Result<_>> = opts
            .pull_opt_str("schema_registry.endpoint")?
            .map(|endpoint| {
                let api_key = VarStr::new(opts.pull_str("schema_registry.api_key")?);
                let api_secret = VarStr::new(opts.pull_str("schema_registry.api_secret")?);
                Ok(ConfluentSchemaRegistry {
                    endpoint: Some(endpoint),
                    api_key: Some(api_key),
                    api_secret: Some(api_secret),
                })
            });

        Ok(ConfluentProfile {
            bootstrap_servers: BootstrapServers(opts.pull_str("bootstrap_servers")?),
            key: VarStr::new(opts.pull_str("key")?),
            secret: VarStr::new(opts.pull_str("secret")?),
            schema_registry: schema_registry.transpose()?,
        })
    }
}

impl From<Option<ConfluentSchemaRegistry>> for kafka::SchemaRegistry {
    fn from(value: Option<ConfluentSchemaRegistry>) -> Self {
        let Some(value) = value else {
            return kafka::SchemaRegistry::None {};
        };

        let Some(endpoint) = value.endpoint else {
            return kafka::SchemaRegistry::None {};
        };

        kafka::SchemaRegistry::ConfluentSchemaRegistry {
            api_key: value.api_key,
            api_secret: value.api_secret,
            endpoint,
        }
    }
}

impl From<ConfluentProfile> for KafkaConfig {
    fn from(c: ConfluentProfile) -> Self {
        Self {
            bootstrap_servers: c.bootstrap_servers.0.try_into().unwrap(),
            authentication: KafkaConfigAuthentication::Sasl {
                protocol: "SASL_SSL".to_string(),
                mechanism: "PLAIN".to_string(),
                username: c.key,
                password: c.secret,
            },
            schema_registry_enum: Some(c.schema_registry.into()),
            connection_properties: HashMap::new(),
        }
    }
}

impl Connector for ConfluentConnector {
    type ProfileT = ConfluentProfile;
    type TableT = KafkaTable;

    fn name(&self) -> &'static str {
        "confluent"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "confluent".to_string(),
            name: "Confluent Cloud".to_string(),
            icon: ICON.to_string(),
            description: "Connect to a Kafka cluster hosted in Confluent Cloud".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn config_description(&self, config: Self::ProfileT) -> String {
        (*config.bootstrap_servers).clone()
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.type_ {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink { .. } => ConnectionType::Sink,
        }
    }

    fn get_autocomplete(
        &self,
        profile: Self::ProfileT,
    ) -> Receiver<anyhow::Result<HashMap<String, Vec<String>>>> {
        let profile = profile.into();
        KafkaConnector {}.get_autocomplete(profile)
    }

    fn test(
        &self,
        _: &str,
        config: Self::ProfileT,
        mut table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        table
            .client_configs
            .insert("client.id".to_string(), CLIENT_ID.to_string());
        let tester = KafkaTester {
            connection: config.into(),
        };

        tester.start(table, schema.cloned(), tx);
    }

    fn test_profile(&self, profile: Self::ProfileT) -> Option<Receiver<TestSourceMessage>> {
        let profile = profile.into();
        KafkaConnector {}.test_profile(profile)
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let connection = profile
            .map(|p| {
                serde_json::from_value(p.config.clone()).map_err(|e| {
                    anyhow!("invalid config for profile '{}' in database: {}", p.id, e)
                })
            })
            .unwrap_or_else(|| Self::connection_from_options(options))?;

        let table = KafkaConnector::table_from_options(options)?;

        self.from_config(None, name, connection, table, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        mut table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        table
            .client_configs
            .insert("client.id".to_string(), CLIENT_ID.to_string());
        KafkaConnector {}.from_config(id, name, config.into(), table, schema)
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        KafkaConnector {}.make_operator(profile.into(), table, config)
    }
}
