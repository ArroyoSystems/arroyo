use crate::formats::{BadData, Format, Framing};
use crate::primitive_to_sql;
use anyhow::bail;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_types::ArroyoExtensionType;
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

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq, Hash)]
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

#[derive(Serialize, Deserialize, Clone, Copy, Debug, ToSchema, PartialEq, Eq)]
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
#[serde(rename_all = "camelCase")]
pub enum FieldType {
    Primitive(PrimitiveType),
    Struct(StructType),
    List(Box<SourceField>),
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

impl From<SourceField> for Field {
    fn from(f: SourceField) -> Self {
        let (t, ext) = match f.field_type.r#type {
            FieldType::Primitive(pt) => match pt {
                PrimitiveType::Int32 => (DataType::Int32, None),
                PrimitiveType::Int64 => (DataType::Int64, None),
                PrimitiveType::UInt32 => (DataType::UInt32, None),
                PrimitiveType::UInt64 => (DataType::UInt64, None),
                PrimitiveType::F32 => (DataType::Float32, None),
                PrimitiveType::F64 => (DataType::Float64, None),
                PrimitiveType::Bool => (DataType::Boolean, None),
                PrimitiveType::String => (DataType::Utf8, None),
                PrimitiveType::Bytes => (DataType::Binary, None),
                PrimitiveType::UnixMillis => {
                    (DataType::Timestamp(TimeUnit::Millisecond, None), None)
                }
                PrimitiveType::UnixMicros => {
                    (DataType::Timestamp(TimeUnit::Microsecond, None), None)
                }
                PrimitiveType::UnixNanos => (DataType::Timestamp(TimeUnit::Nanosecond, None), None),
                PrimitiveType::DateTime => (DataType::Timestamp(TimeUnit::Microsecond, None), None),
                PrimitiveType::Json => (DataType::Utf8, Some(ArroyoExtensionType::JSON)),
            },
            FieldType::Struct(s) => (
                DataType::Struct(Fields::from(
                    s.fields
                        .into_iter()
                        .map(|t| t.into())
                        .collect::<Vec<Field>>(),
                )),
                None,
            ),
            FieldType::List(t) => (DataType::List(Arc::new((*t).into())), None),
        };

        ArroyoExtensionType::add_metadata(ext, Field::new(f.field_name, t, f.nullable))
    }
}

impl TryFrom<Field> for SourceField {
    type Error = String;

    fn try_from(f: Field) -> Result<Self, Self::Error> {
        let field_type = match (f.data_type(), ArroyoExtensionType::from_map(f.metadata())) {
            (DataType::Boolean, None) => FieldType::Primitive(PrimitiveType::Bool),
            (DataType::Int32, None) => FieldType::Primitive(PrimitiveType::Int32),
            (DataType::Int64, None) => FieldType::Primitive(PrimitiveType::Int64),
            (DataType::UInt32, None) => FieldType::Primitive(PrimitiveType::UInt32),
            (DataType::UInt64, None) => FieldType::Primitive(PrimitiveType::UInt64),
            (DataType::Float32, None) => FieldType::Primitive(PrimitiveType::F32),
            (DataType::Float64, None) => FieldType::Primitive(PrimitiveType::F64),
            (DataType::Binary, None) | (DataType::LargeBinary, None) => {
                FieldType::Primitive(PrimitiveType::Bytes)
            }
            (DataType::Timestamp(TimeUnit::Millisecond, _), None) => {
                FieldType::Primitive(PrimitiveType::UnixMillis)
            }
            (DataType::Timestamp(TimeUnit::Microsecond, _), None) => {
                FieldType::Primitive(PrimitiveType::UnixMicros)
            }
            (DataType::Timestamp(TimeUnit::Nanosecond, _), None) => {
                FieldType::Primitive(PrimitiveType::UnixNanos)
            }
            (DataType::Utf8, None) => FieldType::Primitive(PrimitiveType::String),
            (DataType::Utf8, Some(ArroyoExtensionType::JSON)) => {
                FieldType::Primitive(PrimitiveType::Json)
            }
            (DataType::Struct(fields), None) => {
                let fields: Result<_, String> = fields
                    .into_iter()
                    .map(|f| (**f).clone().try_into())
                    .collect();

                let st = StructType {
                    name: None,
                    fields: fields?,
                };

                FieldType::Struct(st)
            }
            (DataType::List(item), None) => FieldType::List(Box::new((**item).clone().try_into()?)),
            dt => {
                return Err(format!("Unsupported data type {:?}", dt));
            }
        };

        let sql_name = match &field_type {
            FieldType::Primitive(pt) => Some(primitive_to_sql(*pt).to_string()),
            _ => None,
        };

        Ok(SourceField {
            field_name: f.name().clone(),
            field_type: SourceFieldType {
                r#type: field_type,
                sql_name,
            },
            nullable: f.is_nullable(),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaDefinition {
    JsonSchema(String),
    ProtobufSchema(String),
    AvroSchema(String),
    RawSchema(String),
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSchema {
    pub format: Option<Format>,
    pub bad_data: Option<BadData>,
    pub framing: Option<Framing>,
    pub struct_name: Option<String>,
    pub fields: Vec<SourceField>,
    pub definition: Option<SchemaDefinition>,
    pub inferred: Option<bool>,
}

impl ConnectionSchema {
    pub fn try_new(
        format: Option<Format>,
        bad_data: Option<BadData>,
        framing: Option<Framing>,
        struct_name: Option<String>,
        fields: Vec<SourceField>,
        definition: Option<SchemaDefinition>,
        inferred: Option<bool>,
    ) -> anyhow::Result<Self> {
        let s = ConnectionSchema {
            format,
            bad_data,
            framing,
            struct_name,
            fields,
            definition,
            inferred,
        };

        s.validate()
    }

    pub fn validate(self) -> anyhow::Result<Self> {
        match &self.format {
            Some(Format::RawString(_)) => {
                if self.fields.len() != 1
                    || self.fields.first().unwrap().field_type.r#type
                        != FieldType::Primitive(PrimitiveType::String)
                    || self.fields.first().unwrap().field_name != "value"
                {
                    bail!("raw_string format requires a schema with a single field called `value` of type TEXT");
                }
            }
            _ => {
                // Right now only RawString has checks, but we may add checks for other formats in the future
            }
        }

        Ok(self)
    }
    pub fn arroyo_schema(&self) -> ArroyoSchemaRef {
        let fields: Vec<Field> = self.fields.iter().map(|f| f.clone().into()).collect();
        Arc::new(ArroyoSchema::from_fields(fields))
    }
}

impl From<ConnectionSchema> for ArroyoSchema {
    fn from(val: ConnectionSchema) -> Self {
        let fields: Vec<Field> = val.fields.into_iter().map(|f| f.into()).collect();
        ArroyoSchema::from_fields(fields)
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
pub struct ConnectionAutocompleteResp {
    pub values: BTreeMap<String, Vec<String>>,
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
