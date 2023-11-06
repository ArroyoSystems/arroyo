use crate::formats::{Format, Framing};
use anyhow::bail;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use utoipa::{IntoParams, ToSchema};

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Connector {
    pub id: String,
    pub name: String,
    pub icon: String,
    pub description: String,
    pub table_config: String,
    pub enabled: bool,
    pub source: bool,
    pub sink: bool,
    pub custom_schemas: bool,
    pub testing: bool,
    pub hidden: bool,
    pub connection_config: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionProfile {
    pub id: String,
    pub name: String,
    pub connector: String,
    pub config: serde_json::Value,
    pub description: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionProfilePost {
    pub name: String,
    pub connector: String,
    pub config: serde_json::Value,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionType {
    Source,
    Sink,
}

impl Display for ConnectionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionType::Source => write!(f, "SOURCE"),
            ConnectionType::Sink => write!(f, "SINK"),
        }
    }
}

impl TryFrom<String> for ConnectionType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "source" => Ok(ConnectionType::Source),
            "sink" => Ok(ConnectionType::Sink),
            _ => Err(format!("Invalid connection type: {}", value)),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PrimitiveType {
    Int32,
    Int64,
    UInt32,
    UInt64,
    F32,
    F64,
    Bool,
    String,
    Bytes,
    UnixMillis,
    UnixMicros,
    UnixNanos,
    DateTime,
    Json,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StructType {
    pub name: Option<String>,
    pub fields: Vec<SourceField>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Primitive(PrimitiveType),
    Struct(StructType),
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceFieldType {
    pub r#type: FieldType,
    pub sql_name: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceField {
    pub field_name: String,
    pub field_type: SourceFieldType,
    pub nullable: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaDefinition {
    JsonSchema(String),
    ProtobufSchema(String),
    AvroSchema(String),
    RawSchema(String),
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSchema {
    pub format: Option<Format>,
    pub framing: Option<Framing>,
    pub struct_name: Option<String>,
    pub fields: Vec<SourceField>,
    pub definition: Option<SchemaDefinition>,
}

impl ConnectionSchema {
    pub fn try_new(
        format: Option<Format>,
        framing: Option<Framing>,
        struct_name: Option<String>,
        fields: Vec<SourceField>,
        definition: Option<SchemaDefinition>,
    ) -> anyhow::Result<Self> {
        let s = ConnectionSchema {
            format,
            framing,
            struct_name,
            fields,
            definition,
        };

        s.validate()
    }

    pub fn validate(self) -> anyhow::Result<Self> {
        match &self.format {
            Some(Format::RawString(_)) => {
                if self.fields.len() != 1
                    || self.fields.get(0).unwrap().field_type.r#type
                        != FieldType::Primitive(PrimitiveType::String)
                {
                    bail!("raw_string format requires a schema with a single field of type TEXT");
                }
            }
            _ => {}
        }

        Ok(self)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionTable {
    #[serde(skip_serializing)]
    pub id: i64,
    #[serde(rename = "id")]
    pub pub_id: String,
    pub name: String,
    pub created_at: u64,
    pub connector: String,
    pub connection_profile: Option<ConnectionProfile>,
    pub table_type: ConnectionType,
    pub config: serde_json::Value,
    pub schema: ConnectionSchema,
    pub consumers: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionTablePost {
    pub name: String,
    pub connector: String,
    pub connection_profile_id: Option<String>,
    pub config: serde_json::Value,
    pub schema: Option<ConnectionSchema>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TestSourceMessage {
    pub error: bool,
    pub done: bool,
    pub message: String,
}
impl TestSourceMessage {
    pub fn info(message: impl Into<String>) -> Self {
        Self {
            error: false,
            done: false,
            message: message.into(),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            error: true,
            done: false,
            message: message.into(),
        }
    }

    pub fn done(message: impl Into<String>) -> Self {
        Self {
            error: false,
            done: true,
            message: message.into(),
        }
    }

    pub fn fail(message: impl Into<String>) -> Self {
        Self {
            error: true,
            done: true,
            message: message.into(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfluentSchema {
    pub schema: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ConfluentSchemaQueryParams {
    pub endpoint: String,
    pub topic: String,
}
