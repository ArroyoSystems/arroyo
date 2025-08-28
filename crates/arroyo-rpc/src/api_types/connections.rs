use crate::df::{ArroyoSchema, ArroyoSchemaRef};
use crate::formats::{BadData, Format, Framing};
use crate::MetadataField;
use ahash::HashSet;
use anyhow::bail;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use arroyo_types::ArroyoExtensionType;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
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

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionType {
    Source,
    Sink,
    Lookup,
}

impl Display for ConnectionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionType::Source => write!(f, "SOURCE"),
            ConnectionType::Sink => write!(f, "SINK"),
            ConnectionType::Lookup => write!(f, "LOOKUP"),
        }
    }
}

impl TryFrom<String> for ConnectionType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "source" => Ok(ConnectionType::Source),
            "sink" => Ok(ConnectionType::Sink),
            _ => Err(format!("Invalid connection type: {value}")),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum FieldType {
    #[schema(title = "Int32")]
    Int32,
    #[schema(title = "Int64")]
    Int64,
    #[schema(title = "UInt32")]
    UInt32,
    #[schema(title = "UInt64")]
    UInt64,
    #[schema(title = "F32")]
    F32,
    #[schema(title = "F64")]
    F64,
    #[schema(title = "Bool")]
    Bool,
    #[serde(alias = "utf8")]
    #[schema(title = "String")]
    String,
    #[serde(alias = "binary")]
    #[schema(title = "Bytes")]
    Bytes,
    #[schema(title = "Timestamp")]
    Timestamp(TimestampField),
    #[schema(title = "Json")]
    Json,
    #[schema(title = "Struct")]
    Struct(StructField),
    #[schema(title = "List")]
    List(ListField),
}

impl FieldType {
    pub fn sql_type(&self) -> String {
        match self {
            FieldType::Int32 => "INTEGER".into(),
            FieldType::Int64 => "BIGINT".into(),
            FieldType::UInt32 => "INTEGER UNSIGNED".into(),
            FieldType::UInt64 => "BIGINT UNSIGNED".into(),
            FieldType::F32 => "FLOAT".into(),
            FieldType::F64 => "DOUBLE".into(),
            FieldType::Bool => "BOOLEAN".into(),
            FieldType::String => "TEXT".into(),
            FieldType::Bytes => "BINARY".into(),
            FieldType::Timestamp(t) => format!("TIMESTAMP({})", t.unit.precision()),
            FieldType::Json => "JSON".into(),
            FieldType::List(item) => {
                format!("{}[]", item.items.field_type.sql_type())
            }
            FieldType::Struct(StructField { fields, .. }) => {
                format!(
                    "STRUCT <{}>",
                    fields
                        .iter()
                        .map(|f| format!("{} {}", f.name, f.field_type.sql_type()))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum TimestampUnit {
    #[serde(alias = "s")]
    Second,

    #[default]
    #[serde(alias = "ms")]
    Millisecond,

    #[serde(alias = "µs", alias = "us")]
    Microsecond,

    #[serde(alias = "ns")]
    Nanosecond,
}

impl TimestampUnit {
    pub fn precision(&self) -> u8 {
        match self {
            TimestampUnit::Second => 0,
            TimestampUnit::Millisecond => 3,
            TimestampUnit::Microsecond => 6,
            TimestampUnit::Nanosecond => 9,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TimestampField {
    #[serde(default)]
    pub unit: TimestampUnit,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StructField {
    pub name: Option<String>,
    pub fields: Vec<SourceField>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ListField {
    pub items: Box<SourceField>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceField {
    pub name: String,
    #[serde(flatten)]
    pub field_type: FieldType,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub sql_name: String,
    #[serde(default)]
    pub metadata_key: Option<String>,
}

impl From<SourceField> for Field {
    fn from(f: SourceField) -> Self {
        let (t, ext) = match f.field_type {
            FieldType::Int32 => (DataType::Int32, None),
            FieldType::Int64 => (DataType::Int64, None),
            FieldType::UInt32 => (DataType::UInt32, None),
            FieldType::UInt64 => (DataType::UInt64, None),
            FieldType::F32 => (DataType::Float32, None),
            FieldType::F64 => (DataType::Float64, None),
            FieldType::Bool => (DataType::Boolean, None),
            FieldType::String => (DataType::Utf8, None),
            FieldType::Bytes => (DataType::Binary, None),
            FieldType::Timestamp(TimestampField {
                unit: TimestampUnit::Second,
            }) => (DataType::Timestamp(TimeUnit::Second, None), None),
            FieldType::Timestamp(TimestampField {
                unit: TimestampUnit::Millisecond,
            }) => (DataType::Timestamp(TimeUnit::Millisecond, None), None),
            FieldType::Timestamp(TimestampField {
                unit: TimestampUnit::Microsecond,
            }) => (DataType::Timestamp(TimeUnit::Microsecond, None), None),
            FieldType::Timestamp(TimestampField {
                unit: TimestampUnit::Nanosecond,
            }) => (DataType::Timestamp(TimeUnit::Nanosecond, None), None),
            FieldType::Json => (DataType::Utf8, Some(ArroyoExtensionType::JSON)),
            FieldType::Struct(s) => (
                DataType::Struct(Fields::from(
                    s.fields
                        .into_iter()
                        .map(|t| t.into())
                        .collect::<Vec<Field>>(),
                )),
                None,
            ),
            FieldType::List(t) => (DataType::List(Arc::new((*t.items).into())), None),
        };

        ArroyoExtensionType::add_metadata(ext, Field::new(f.name, t, !f.required))
    }
}

impl TryFrom<Field> for SourceField {
    type Error = String;

    fn try_from(f: Field) -> Result<Self, Self::Error> {
        let field_type = match (f.data_type(), ArroyoExtensionType::from_map(f.metadata())) {
            (DataType::Boolean, None) => FieldType::Bool,
            (DataType::Int32, None) => FieldType::Int32,
            (DataType::Int64, None) => FieldType::Int64,
            (DataType::UInt32, None) => FieldType::UInt32,
            (DataType::UInt64, None) => FieldType::UInt64,
            (DataType::Float32, None) => FieldType::F32,
            (DataType::Float64, None) => FieldType::F64,
            (DataType::Binary, None) | (DataType::LargeBinary, None) => FieldType::Bytes,
            (DataType::Timestamp(TimeUnit::Second, _), None) => {
                FieldType::Timestamp(TimestampField {
                    unit: TimestampUnit::Second,
                })
            }
            (DataType::Timestamp(TimeUnit::Millisecond, _), None) => {
                FieldType::Timestamp(TimestampField {
                    unit: TimestampUnit::Millisecond,
                })
            }
            (DataType::Timestamp(TimeUnit::Microsecond, _), None) => {
                FieldType::Timestamp(TimestampField {
                    unit: TimestampUnit::Microsecond,
                })
            }
            (DataType::Timestamp(TimeUnit::Nanosecond, _), None) => {
                FieldType::Timestamp(TimestampField {
                    unit: TimestampUnit::Nanosecond,
                })
            }
            (DataType::Utf8, None) => FieldType::String,
            (DataType::Utf8, Some(ArroyoExtensionType::JSON)) => FieldType::Json,
            (DataType::Struct(fields), None) => {
                let fields: Result<_, String> = fields
                    .into_iter()
                    .map(|f| (**f).clone().try_into())
                    .collect();

                let st = StructField {
                    name: None,
                    fields: fields?,
                };

                FieldType::Struct(st)
            }
            (DataType::List(item), None) => FieldType::List(ListField {
                items: Box::new((**item).clone().try_into()?),
            }),
            dt => {
                return Err(format!("Unsupported data type {dt:?}"));
            }
        };

        let sql_name = field_type.sql_type();
        Ok(SourceField {
            name: f.name().clone(),
            field_type,
            required: !f.is_nullable(),
            metadata_key: None,
            sql_name,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum SchemaDefinition {
    #[schema(title = "JsonSchema")]
    JsonSchema { schema: String },
    #[schema(title = "Protobuf")]
    ProtobufSchema {
        schema: String,
        #[serde(default)]
        dependencies: HashMap<String, String>,
    },
    #[schema(title = "Avro")]
    AvroSchema { schema: String },
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSchema {
    pub format: Option<Format>,
    #[serde(default)]
    pub bad_data: Option<BadData>,
    #[serde(default)]
    pub framing: Option<Framing>,
    #[serde(default)]
    pub fields: Vec<SourceField>,
    #[serde(default)]
    pub definition: Option<SchemaDefinition>,
    #[serde(default)]
    pub inferred: Option<bool>,
    #[serde(default)]
    pub primary_keys: HashSet<String>,
}

impl ConnectionSchema {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        format: Option<Format>,
        bad_data: Option<BadData>,
        framing: Option<Framing>,
        fields: Vec<SourceField>,
        definition: Option<SchemaDefinition>,
        inferred: Option<bool>,
        primary_keys: HashSet<String>,
    ) -> anyhow::Result<Self> {
        let s = ConnectionSchema {
            format,
            bad_data,
            framing,
            fields,
            definition,
            inferred,
            primary_keys,
        };

        s.validate()
    }

    pub fn validate(self) -> anyhow::Result<Self> {
        let non_metadata_fields: Vec<_> = self
            .fields
            .iter()
            .filter(|f| f.metadata_key.is_none())
            .collect();

        match &self.format {
            Some(Format::RawString(_)) => {
                if non_metadata_fields.len() != 1
                    || non_metadata_fields.first().unwrap().field_type != FieldType::String
                    || non_metadata_fields.first().unwrap().name != "value"
                {
                    bail!("raw_string format requires a schema with a single field called `value` of type TEXT");
                }
            }
            Some(Format::Json(json_format)) => {
                if json_format.unstructured
                    && (non_metadata_fields.len() != 1
                        || non_metadata_fields.first().unwrap().field_type != FieldType::Json
                        || non_metadata_fields.first().unwrap().name != "value")
                {
                    bail!("json format with unstructured flag enabled requires a schema with a single field called `value` of type JSON");
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

    pub fn metadata_fields(&self) -> Vec<MetadataField> {
        self.fields
            .iter()
            .filter_map(|f| {
                Some(MetadataField {
                    field_name: f.name.clone(),
                    key: f.metadata_key.clone()?,
                    data_type: Some(Field::from(f.clone()).data_type().clone()),
                })
            })
            .collect()
    }
}

impl From<ConnectionSchema> for ArroyoSchema {
    fn from(val: ConnectionSchema) -> Self {
        let fields: Vec<Field> = val.fields.into_iter().map(|f| f.into()).collect();
        ArroyoSchema::from_fields(fields)
    }
}

#[derive(Serialize, Clone, Debug, ToSchema, IntoParams)]
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
