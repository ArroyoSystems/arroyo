use crate::kafka::{
    KafkaConfig, KafkaConfigAuthentication, KafkaConnector, KafkaTable, KafkaTester, TableType,
};
use crate::{kafka, pull_opt, Connection, Connector};
use anyhow::anyhow;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, ConnectionType};
use arroyo_rpc::var_str::VarStr;
use axum::response::sse::Event;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use tokio::sync::mpsc::Sender;
use typify::import_types;

const CLIENT_ID: &str = "cwc|0014U00003Df8ZvQAJ";

const CONFIG_SCHEMA: &str = include_str!("../../connector-schemas/confluent/connection.json");
const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/kafka/table.json");
const ICON: &str = include_str!("../resources/confluent.svg");

import_types!(
    schema = "../connector-schemas/confluent/connection.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

pub struct ConfluentConnector {}

impl ConfluentConnector {
    pub fn connection_from_options(
        opts: &mut HashMap<String, String>,
    ) -> anyhow::Result<ConfluentProfile> {
        let schema_registry: Option<anyhow::Result<_>> =
            opts.remove("schema_registry.endpoint").map(|endpoint| {
                let api_key = VarStr::new(pull_opt("schema_registry.api_key", opts)?);
                let api_secret = VarStr::new(pull_opt("schema_registry.api_secret", opts)?);
                Ok(ConfluentSchemaRegistry {
                    endpoint: Some(endpoint),
                    api_key: Some(api_key),
                    api_secret: Some(api_secret),
                })
            });

        Ok(ConfluentProfile {
            bootstrap_servers: BootstrapServers(pull_opt("bootstrap_servers", opts)?),
            key: VarStr::new(pull_opt("key", opts)?),
            secret: VarStr::new(pull_opt("secret", opts)?),
            schema_registry: schema_registry.transpose()?,
        })
    }
}

impl TryFrom<Option<ConfluentSchemaRegistry>> for kafka::SchemaRegistry {
    type Error = anyhow::Error;

    fn try_from(value: Option<ConfluentSchemaRegistry>) -> anyhow::Result<Self> {
        let Some(value) = value else {
            return Ok(kafka::SchemaRegistry::None {});
        };

        let Some(endpoint) = value.endpoint else {
            return Ok(kafka::SchemaRegistry::None {});
        };

        Ok(kafka::SchemaRegistry::ConfluentSchemaRegistry {
            api_key: Some(
                value
                    .api_key
                    .ok_or_else(|| anyhow!("schema registry api_key is required"))?,
            ),
            api_secret: Some(
                value
                    .api_secret
                    .ok_or_else(|| anyhow!("schema registry api_secret is required"))?,
            ),
            endpoint,
        })
    }
}

impl TryFrom<ConfluentProfile> for KafkaConfig {
    type Error = anyhow::Error;

    fn try_from(c: ConfluentProfile) -> anyhow::Result<Self> {
        Ok(Self {
            bootstrap_servers: c.bootstrap_servers.0.try_into().unwrap(),
            authentication: KafkaConfigAuthentication::Sasl {
                protocol: "SASL_SSL".to_string(),
                mechanism: "PLAIN".to_string(),
                username: c.key,
                password: c.secret,
            },
            schema_registry_enum: Some(c.schema_registry.try_into()?),
        })
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

    fn test(
        &self,
        _: &str,
        config: Self::ProfileT,
        mut table: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<Result<Event, Infallible>>,
    ) {
        table
            .client_configs
            .insert("client.id".to_string(), CLIENT_ID.to_string());
        let tester = KafkaTester {
            connection: config.try_into().unwrap(),
        };

        tester.start(table, tx);
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
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
        KafkaConnector {}.from_config(id, name, config.try_into()?, table, schema)
    }
}
