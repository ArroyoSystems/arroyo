use std::{collections::HashMap};

use arroyo_datastream::SerializationMode;
use arroyo_rpc::grpc::{
    self,
    api::{ConnectionSchema, TestSourceMessage, TableType},
};
use arroyo_sql::ArroyoSchemaProvider;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tonic::Status;
use typify::import_types;

use crate::{connectors::http::SSEConnector, json_schema};

use self::kafka::KafkaConnector;

pub mod http;
pub mod kafka;

import_types!(schema = "../connector-schemas/common.json",);

pub trait Connector: Send {
    type ConfigT: DeserializeOwned + Serialize;
    type TableT: DeserializeOwned + Serialize;

    fn name(&self) -> &'static str;

    fn parse_config(&self, s: &str) -> Result<Self::ConfigT, serde_json::Error> {
        serde_json::from_str(s)
    }

    fn parse_table(&self, s: &str) -> Result<Self::TableT, serde_json::Error> {
        serde_json::from_str(s)
    }

    fn metadata(&self) -> grpc::api::Connector;

    fn table_type(&self, config: Self::ConfigT, table: Self::TableT) -> TableType;

    fn test(
        &self,
        name: &str,
        config: Self::ConfigT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        tx: Sender<Result<TestSourceMessage, Status>>,
    );

    fn register(
        &self,
        id: i64,
        name: &str,
        config: Self::ConfigT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        schema_provider: &mut ArroyoSchemaProvider,
    );
}

pub trait ErasedConnector: Send {
    fn name(&self) -> &'static str;

    fn metadata(&self) -> grpc::api::Connector;

    fn validate_config(&self, s: &str) -> Result<(), serde_json::Error>;

    fn validate_table(&self, s: &str) -> Result<(), serde_json::Error>;

    fn table_type(&self, config: &str, table: &str) -> Result<TableType, serde_json::Error>;

    fn test(
        &self,
        name: &str,
        config: &str,
        table: &str,
        schema: Option<&ConnectionSchema>,
        tx: Sender<Result<TestSourceMessage, Status>>,
    ) -> Result<(), serde_json::Error>;

    fn register(
        &self,
        id: i64,
        name: &str,
        config: &str,
        table: &str,
        schema: Option<&ConnectionSchema>,
        schema_provider: &mut ArroyoSchemaProvider,
    ) -> Result<(), serde_json::Error>;
}

impl<C: Connector> ErasedConnector for C {
    fn name(&self) -> &'static str {
        self.name()
    }

    fn metadata(&self) -> grpc::api::Connector {
        self.metadata()
    }

    fn validate_config(&self, s: &str) -> Result<(), serde_json::Error> {
        self.parse_config(s).map(|_| ())
    }

    fn validate_table(&self, s: &str) -> Result<(), serde_json::Error> {
        self.parse_table(s).map(|_| ())
    }

    fn table_type(&self, config: &str, table: &str) -> Result<TableType, serde_json::Error> {
        Ok(self.table_type(self.parse_config(config)?, self.parse_table(table)?))
    }

    fn test(
        &self,
        name: &str,
        config: &str,
        table: &str,
        schema: Option<&ConnectionSchema>,
        tx: Sender<Result<TestSourceMessage, Status>>,
    ) -> Result<(), serde_json::Error> {
        self.test(
            name,
            self.parse_config(config)?,
            self.parse_table(table)?,
            schema,
            tx,
        );

        Ok(())
    }

    fn register(
        &self,
        id: i64,
        name: &str,
        config: &str,
        table: &str,
        schema: Option<&ConnectionSchema>,
        schema_provider: &mut ArroyoSchemaProvider,
    ) -> Result<(), serde_json::Error> {
        self.register(
            id,
            name,
            self.parse_config(config)?,
            self.parse_table(table)?,
            schema,
            schema_provider,
        );

        Ok(())
    }
}

pub fn connector_for_type(t: &str) -> Option<Box<dyn ErasedConnector>> {
    connectors().remove(t)
}

pub fn connectors() -> HashMap<&'static str, Box<dyn ErasedConnector>> {
    let mut m: HashMap<&'static str, Box<dyn ErasedConnector>> = HashMap::new();
    m.insert("kafka", Box::new(KafkaConnector {}));
    m.insert("sse", Box::new(SSEConnector {}));

    m
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RawConfig {
    charset: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum JsonConfig {
    JsonSchema { schema: String },
    JsonFields {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SchemaType {
    Raw(RawConfig),
    JSON(JsonConfig),
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct SchemaOptions {
    confluent_schema_registry: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    schema_type: SchemaType,
    options: SchemaOptions,
}

pub fn serialization_mode(schema: &ConnectionSchema) -> OperatorConfigSerializationMode {
    let confluent = schema
        .format_options
        .as_ref()
        .filter(|t| t.confluent_schema_registry)
        .is_some();
    match &schema.format() {
        grpc::api::Format::JsonFormat => {
            if confluent {
                OperatorConfigSerializationMode::JsonSchemaRegistry
            } else {
                OperatorConfigSerializationMode::Json
            }
        }
        grpc::api::Format::ProtobufFormat => todo!(),
        grpc::api::Format::AvroFormat => todo!(),
        grpc::api::Format::RawStringFormat => {
            if confluent {
                OperatorConfigSerializationMode::JsonSchemaRegistry
            } else {
                OperatorConfigSerializationMode::Json
            }
        }
    }
}

impl From<OperatorConfigSerializationMode> for SerializationMode {
    fn from(value: OperatorConfigSerializationMode) -> Self {
        match value {
            OperatorConfigSerializationMode::Json => SerializationMode::Json,
            OperatorConfigSerializationMode::JsonSchemaRegistry => {
                SerializationMode::JsonSchemaRegistry
            }
            OperatorConfigSerializationMode::RawJson => SerializationMode::RawJson,
        }
    }
}

pub fn schema_type(name: &str, schema: &ConnectionSchema) -> Option<String> {
    let def = schema.definition.as_ref()?;
    match def {
        grpc::api::connection_schema::Definition::JsonSchema(_) => {
            Some(format!("{}::{}", name, json_schema::ROOT_NAME))
        }
        grpc::api::connection_schema::Definition::ProtobufSchema(_) => todo!(),
        grpc::api::connection_schema::Definition::AvroSchema(_) => todo!(),
        grpc::api::connection_schema::Definition::RawSchema(_) => {
            Some("arroyo_types::RawJson".to_string())
        },
    }
}

pub fn schema_defs(name: &str, schema: &ConnectionSchema) -> Option<String> {
    let def = schema.definition.as_ref()?;

    match def {
        grpc::api::connection_schema::Definition::JsonSchema(s) => {
            Some(json_schema::get_defs(&name, &s).unwrap())
        }
        grpc::api::connection_schema::Definition::ProtobufSchema(_) => todo!(),
        grpc::api::connection_schema::Definition::AvroSchema(_) => todo!(),
        grpc::api::connection_schema::Definition::RawSchema(_) => {
            None
        },
    }
}
