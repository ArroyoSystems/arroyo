use std::collections::HashMap;

use anyhow::anyhow;
use arroyo_rpc::{
    grpc::{
        self,
        api::{
            connection_schema::Definition, source_field_type, SourceField, SourceFieldType,
            TableType, TestSourceMessage,
        },
    },
    primitive_to_sql,
};
use arroyo_types::formats::{Format, JsonFormat, ParquetFormat};
use blackhole::BlackholeConnector;
use fluvio::FluvioConnector;
use impulse::ImpulseConnector;
use nexmark::NexmarkConnector;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sse::SSEConnector;
use tokio::sync::mpsc::Sender;
use tonic::Status;
use websocket::WebsocketConnector;

use self::kafka::KafkaConnector;

pub mod blackhole;
pub mod filesystem;
pub mod fluvio;
pub mod impulse;
pub mod kafka;
pub mod nexmark;
pub mod sse;
pub mod websocket;

pub fn connectors() -> HashMap<&'static str, Box<dyn ErasedConnector>> {
    let mut m: HashMap<&'static str, Box<dyn ErasedConnector>> = HashMap::new();
    m.insert("kafka", Box::new(KafkaConnector {}));
    m.insert("sse", Box::new(SSEConnector {}));
    m.insert("nexmark", Box::new(NexmarkConnector {}));
    m.insert("impulse", Box::new(ImpulseConnector {}));
    m.insert("blackhole", Box::new(BlackholeConnector {}));
    m.insert("websocket", Box::new(WebsocketConnector {}));
    m.insert("fluvio", Box::new(FluvioConnector {}));
    m.insert("filesystem", Box::new(filesystem::FileSystemConnector {}));

    m
}

#[derive(Serialize, Deserialize)]
pub struct EmptyConfig {}

#[derive(Debug, Copy, Clone)]
pub enum ConnectionType {
    Source,
    Sink,
}

#[derive(Debug, Clone)]
pub struct Connection {
    pub id: Option<i64>,
    pub name: String,
    pub connection_type: ConnectionType,
    pub schema: ConnectionSchema,
    pub operator: String,
    pub config: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionSchema {
    pub format: Option<Format>,
    pub struct_name: Option<String>,
    pub fields: Vec<SourceField>,
    pub definition: Option<Definition>,
}

impl TryFrom<grpc::api::ConnectionSchema> for ConnectionSchema {
    type Error = String;

    fn try_from(schema: grpc::api::ConnectionSchema) -> Result<Self, Self::Error> {
        let confluent_schema_registry = schema
            .format_options
            .as_ref()
            .filter(|t| t.confluent_schema_registry)
            .is_some();
        let include_schema = schema
            .format_options
            .as_ref()
            .filter(|t| t.include_schema)
            .is_some();

        let format = if schema.format.is_some() {
            Some(match schema.format() {
                grpc::api::Format::JsonFormat | grpc::api::Format::DebeziumJsonFormat => {
                    Format::Json(JsonFormat {
                        confluent_schema_registry,
                        include_schema,
                        debezium: matches!(schema.format(), grpc::api::Format::DebeziumJsonFormat),
                        unstructured: matches!(
                            schema.definition,
                            Some(Definition::RawSchema { .. })
                        ),
                        timestamp_format: arroyo_types::formats::TimestampFormat::default(),
                    })
                }
                grpc::api::Format::RawStringFormat => Format::Json(JsonFormat {
                    confluent_schema_registry: false,
                    include_schema,
                    debezium: false,
                    unstructured: true,
                    timestamp_format: arroyo_types::formats::TimestampFormat::default(),
                }),
                grpc::api::Format::ParquetFormat => Format::Parquet(ParquetFormat {}),
                grpc::api::Format::ProtobufFormat => {
                    return Err("Protobuf is not yet supported".to_string());
                }
                grpc::api::Format::AvroFormat => {
                    return Err("Avro is not yet supported".to_string());
                }
            })
        } else {
            None
        };

        Ok(ConnectionSchema {
            format,
            struct_name: schema.struct_name,
            fields: schema.fields,
            definition: schema.definition,
        })
    }
}

pub trait Connector: Send {
    type ConfigT: DeserializeOwned + Serialize;
    type TableT: DeserializeOwned + Serialize;

    fn name(&self) -> &'static str;

    #[allow(unused)]
    fn config_description(&self, config: Self::ConfigT) -> String {
        "".to_string()
    }

    fn parse_config(&self, s: &str) -> Result<Self::ConfigT, serde_json::Error> {
        serde_json::from_str(if s.is_empty() { "{}" } else { s })
    }

    fn parse_table(&self, s: &str) -> Result<Self::TableT, serde_json::Error> {
        serde_json::from_str(s)
    }

    fn metadata(&self) -> arroyo_rpc::types::Connector;

    fn table_type(&self, config: Self::ConfigT, table: Self::TableT) -> TableType;

    #[allow(unused)]
    fn get_schema(
        &self,
        config: Self::ConfigT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        schema.cloned()
    }

    fn test(
        &self,
        name: &str,
        config: Self::ConfigT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        tx: Sender<Result<TestSourceMessage, Status>>,
    );

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection>;

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ConfigT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection>;
}

pub trait ErasedConnector: Send {
    fn name(&self) -> &'static str;

    fn metadata(&self) -> arroyo_rpc::types::Connector;

    fn validate_config(&self, s: &str) -> Result<(), serde_json::Error>;

    fn validate_table(&self, s: &str) -> Result<(), serde_json::Error>;

    fn table_type(&self, config: &str, table: &str) -> Result<TableType, serde_json::Error>;

    fn config_description(&self, s: &str) -> Result<String, serde_json::Error>;

    fn get_schema(
        &self,
        config: &str,
        table: &str,
        schema: Option<&ConnectionSchema>,
    ) -> Result<Option<ConnectionSchema>, serde_json::Error>;

    fn test(
        &self,
        name: &str,
        config: &str,
        table: &str,
        schema: Option<&ConnectionSchema>,
        tx: Sender<Result<TestSourceMessage, Status>>,
    ) -> Result<(), serde_json::Error>;

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection>;

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: &str,
        table: &str,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection>;
}

impl<C: Connector> ErasedConnector for C {
    fn name(&self) -> &'static str {
        self.name()
    }

    fn metadata(&self) -> arroyo_rpc::types::Connector {
        self.metadata()
    }

    fn config_description(&self, s: &str) -> Result<String, serde_json::Error> {
        Ok(self.config_description(self.parse_config(s)?))
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

    fn get_schema(
        &self,
        config: &str,
        table: &str,
        schema: Option<&ConnectionSchema>,
    ) -> Result<Option<ConnectionSchema>, serde_json::Error> {
        Ok(self.get_schema(self.parse_config(config)?, self.parse_table(table)?, schema))
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

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        self.from_options(name, options, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: &str,
        table: &str,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        self.from_config(
            id,
            name,
            self.parse_config(config)?,
            self.parse_table(table)?,
            schema,
        )
    }
}

pub(crate) fn pull_opt(name: &str, opts: &mut HashMap<String, String>) -> anyhow::Result<String> {
    opts.remove(name)
        .ok_or_else(|| anyhow!("required option '{}' not set", name))
}

pub fn connector_for_type(t: &str) -> Option<Box<dyn ErasedConnector>> {
    connectors().remove(t)
}

pub(crate) fn source_field(name: &str, field_type: source_field_type::Type) -> SourceField {
    SourceField {
        field_name: name.to_string(),
        field_type: Some(SourceFieldType {
            sql_name: match field_type {
                source_field_type::Type::Primitive(p) => Some(
                    primitive_to_sql(grpc::api::PrimitiveType::from_i32(p).unwrap()).to_string(),
                ),
                source_field_type::Type::Struct(_) => None,
            },
            r#type: Some(field_type),
        }),
        nullable: false,
    }
}

pub(crate) fn nullable_field(name: &str, field_type: source_field_type::Type) -> SourceField {
    SourceField {
        field_name: name.to_string(),
        field_type: Some(SourceFieldType {
            sql_name: None,
            r#type: Some(field_type),
        }),
        nullable: true,
    }
}
