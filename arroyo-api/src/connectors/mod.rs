use arroyo_datastream::SerializationMode;
use arroyo_rpc::grpc::api::SourceSchema;
use arroyo_sql::{types::StructField, ArroyoSchemaProvider};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use typify::import_types;

use crate::{
    json_schema,
    sources::{raw_schema, SchemaField},
};

use self::kafka::KafkaConnector;

pub mod http;
pub mod kafka;

import_types!(schema = "../connector-schemas/common.json",);

pub trait Connector {
    type ConfigT: DeserializeOwned + Serialize;
    type TableT: DeserializeOwned + Serialize;

    fn name(&self) -> &'static str;

    fn parse_schema(&self, s: &str) -> Result<Self::ConfigT, serde_json::Error> {
        serde_json::from_str(s)
    }

    fn parse_table(&self, s: &str) -> Result<Self::TableT, serde_json::Error> {
        serde_json::from_str(s)
    }

    fn register(
        &self,
        name: &str,
        config: Self::ConfigT,
        table: Self::TableT,
        schema: Option<SourceSchema>,
        schema_provider: &mut ArroyoSchemaProvider,
    );
}

pub trait ErasedConnector {
    fn name(&self) -> &'static str;

    fn validate_schema(&self, s: &str) -> Result<(), serde_json::Error>;

    fn validate_table(&self, s: &str) -> Result<(), serde_json::Error>;

    fn register(
        &self,
        name: &str,
        config: &str,
        table: &str,
        schema: Option<SourceSchema>,
        schema_provider: &mut ArroyoSchemaProvider,
    ) -> Result<(), serde_json::Error>;
}

impl<C: Connector> ErasedConnector for C {
    fn name(&self) -> &'static str {
        self.name()
    }

    fn validate_schema(&self, s: &str) -> Result<(), serde_json::Error> {
        self.parse_schema(s).map(|_| ())
    }

    fn validate_table(&self, s: &str) -> Result<(), serde_json::Error> {
        self.parse_table(s).map(|_| ())
    }

    fn register(
        &self,
        name: &str,
        config: &str,
        table: &str,
        schema: Option<SourceSchema>,
        schema_provider: &mut ArroyoSchemaProvider,
    ) -> Result<(), serde_json::Error> {
        self.register(
            name,
            self.parse_schema(config).unwrap(),
            self.parse_table(table)?,
            schema,
            schema_provider,
        );

        Ok(())
    }
}

pub fn connector_for_type(t: &str) -> Option<Box<dyn ErasedConnector>> {
    match t {
        "kafka" => Some(Box::new(KafkaConnector {})),
        "http" => todo!(),
        _ => None,
    }
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

pub fn serialization_mode(schema: &SourceSchema) -> OperatorConfigSerializationMode {
    match &schema.schema {
        Some(s) => match s {
            arroyo_rpc::grpc::api::source_schema::Schema::Builtin(_) => todo!(),
            arroyo_rpc::grpc::api::source_schema::Schema::JsonSchema(_)
            | arroyo_rpc::grpc::api::source_schema::Schema::JsonFields(_) => {
                if schema.kafka_schema_registry {
                    OperatorConfigSerializationMode::JsonSchemaRegistry
                } else {
                    OperatorConfigSerializationMode::Json
                }
            }
            arroyo_rpc::grpc::api::source_schema::Schema::Protobuf(_) => todo!(),
            arroyo_rpc::grpc::api::source_schema::Schema::RawJson(_) => {
                OperatorConfigSerializationMode::RawJson
            }
        },
        None => todo!(),
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

pub fn schema_fields(name: &str, schema: &SourceSchema) -> anyhow::Result<Vec<StructField>> {
    use arroyo_rpc::grpc::api::source_schema;
    match schema.schema.as_ref().unwrap() {
        source_schema::Schema::Builtin(_) => todo!(),
        source_schema::Schema::JsonSchema(j) => {
            Ok(json_schema::convert_json_schema(name, &j.json_schema)
                .map_err(|e| anyhow::anyhow!("Failed to use json schema: {}", e))?
                .iter()
                .map(|f| f.into())
                .collect())
        }
        source_schema::Schema::JsonFields(def) => {
            let fs: Result<Vec<SchemaField>, String> =
                def.fields.iter().map(|t| t.clone().try_into()).collect();
            let fs = fs.map_err(|e| anyhow::anyhow!("Failed to convert schema fields: {}", e))?;

            Ok(fs.iter().map(|f| f.into()).collect())
        }
        source_schema::Schema::Protobuf(_) => todo!(),
        source_schema::Schema::RawJson(_) => {
            Ok(raw_schema().fields().iter().map(|t| t.into()).collect())
        }
    }
}

pub fn schema_type(name: &str, schema: &SourceSchema) -> Option<String> {
    match schema.schema.as_ref().unwrap() {
        arroyo_rpc::grpc::api::source_schema::Schema::Builtin(_) => todo!(),
        arroyo_rpc::grpc::api::source_schema::Schema::JsonSchema(_) => {
            Some(format!("{}::{}", name, json_schema::ROOT_NAME))
        }
        arroyo_rpc::grpc::api::source_schema::Schema::JsonFields(_) => None,
        arroyo_rpc::grpc::api::source_schema::Schema::Protobuf(_) => todo!(),
        arroyo_rpc::grpc::api::source_schema::Schema::RawJson(_) => {
            Some("arroyo_types::RawJson".to_string())
        }
    }
}

pub fn schema_defs(name: &str, schema: &SourceSchema) -> Option<String> {
    match schema.schema.as_ref().unwrap() {
        arroyo_rpc::grpc::api::source_schema::Schema::Builtin(_) => todo!(),
        arroyo_rpc::grpc::api::source_schema::Schema::JsonSchema(s) => {
            Some(json_schema::get_defs(&name, &s.json_schema).unwrap())
        }
        arroyo_rpc::grpc::api::source_schema::Schema::JsonFields(_) => None,
        arroyo_rpc::grpc::api::source_schema::Schema::Protobuf(_) => None,
        arroyo_rpc::grpc::api::source_schema::Schema::RawJson(_) => None,
    }
}
