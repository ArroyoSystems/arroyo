use serde::{Serialize, de::DeserializeOwned, Deserialize};

use self::kafka::KafkaConnector;

pub mod kafka;
pub mod http;

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
}

pub trait ErasedConnector {
    fn name(&self) -> &'static str;

    fn validate_schema(&self, s: &str) -> Result<(), serde_json::Error>;

    fn validate_table(&self, s: &str) -> Result<(), serde_json::Error>;
}

impl <C: Connector> ErasedConnector for C {
    fn name(&self) -> &'static str {
        self.name()
    }

    fn validate_schema(&self, s: &str) -> Result<(), serde_json::Error> {
        self.parse_schema(s).map(|_| ())
    }

    fn validate_table(&self, s: &str) -> Result<(), serde_json::Error> {
        self.parse_table(s).map(|_| ())
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
    JsonSchema {

    },
    JsonFields {

    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SchemaType {
    Raw(RawConfig),
    JSON(JsonConfig)
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
