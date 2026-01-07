use crate::df::{ArroyoSchema, ArroyoSchemaRef};
use crate::formats::{BadData, Format, Framing};
use crate::MetadataField;
use ahash::HashSet;
use anyhow::bail;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use arroyo_types::ArroyoExtensionType;
use serde::__private::ser::FlatMapSerializer;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "snake_case")]
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
#[serde(rename_all = "snake_case")]
pub struct ConnectionProfile {
    pub id: String,
    pub name: String,
    pub connector: String,
    pub config: serde_json::Value,
    pub description: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct ConnectionProfilePost {
    pub name: String,
    pub connector: String,
    pub config: serde_json::Value,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, ToSchema, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "snake_case")]
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
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FieldType {
    #[schema(title = "Int32")]
    Int32,
    #[schema(title = "Int64")]
    Int64,
    #[schema(title = "UInt32")]
    Uint32,
    #[schema(title = "UInt64")]
    Uint64,
    #[schema(title = "Float32")]
    #[serde(alias = "f32")]
    Float32,
    #[schema(title = "Float64")]
    #[serde(alias = "f64")]
    Float64,
    #[schema(title = "Decimal128")]
    Decimal128(DecimalField),
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
            FieldType::Uint32 => "INTEGER UNSIGNED".into(),
            FieldType::Uint64 => "BIGINT UNSIGNED".into(),
            FieldType::Float32 => "FLOAT".into(),
            FieldType::Float64 => "DOUBLE".into(),
            FieldType::Decimal128(f) => format!("DECIMAL({}, {})", f.precision, f.scale),
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
#[serde(rename_all = "snake_case")]
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
#[serde(rename_all = "snake_case")]
pub struct TimestampField {
    #[serde(default)]
    pub unit: TimestampUnit,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DecimalField {
    pub precision: u8,
    pub scale: i8,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct StructField {
    pub fields: Vec<SourceField>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct ListField {
    pub items: Box<ListFieldItem>,
}

fn default_item_name() -> String {
    "item".to_string()
}

#[derive(Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct ListFieldItem {
    #[serde(default = "default_item_name")]
    pub name: String,

    #[serde(flatten)]
    pub field_type: FieldType,

    #[serde(default)]
    pub required: bool,

    #[serde(default)]
    #[schema(read_only)]
    pub sql_name: Option<String>,
}

impl From<ListFieldItem> for Field {
    fn from(value: ListFieldItem) -> Self {
        SourceField {
            name: value.name,
            field_type: value.field_type,
            required: value.required,
            sql_name: None,
            metadata_key: None,
        }
        .into()
    }
}

impl Serialize for ListFieldItem {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut f = Serializer::serialize_map(s, None)?;
        f.serialize_entry("name", &self.name)?;
        self.field_type.serialize(FlatMapSerializer(&mut f))?;
        f.serialize_entry("required", &self.required)?;
        f.serialize_entry("sql_name", &self.field_type.sql_type())?;
        f.end()
    }
}

impl TryFrom<Field> for ListFieldItem {
    type Error = String;

    fn try_from(value: Field) -> Result<Self, Self::Error> {
        let source_field: SourceField = value.try_into()?;
        Ok(Self {
            name: source_field.name,
            field_type: source_field.field_type,
            required: source_field.required,
            sql_name: None,
        })
    }
}

#[derive(Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SourceField {
    pub name: String,
    #[serde(flatten)]
    pub field_type: FieldType,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    #[schema(read_only)]
    pub sql_name: Option<String>,
    #[serde(default)]
    pub metadata_key: Option<String>,
}

impl Serialize for SourceField {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut f = Serializer::serialize_map(s, None)?;
        f.serialize_entry("name", &self.name)?;
        self.field_type.serialize(FlatMapSerializer(&mut f))?;
        f.serialize_entry("required", &self.required)?;
        if let Some(metadata_key) = &self.metadata_key {
            f.serialize_entry("metadata_key", metadata_key)?;
        }
        f.serialize_entry("sql_name", &self.field_type.sql_type())?;
        f.end()
    }
}

impl From<SourceField> for Field {
    fn from(f: SourceField) -> Self {
        let (t, ext) = match f.field_type {
            FieldType::Int32 => (DataType::Int32, None),
            FieldType::Int64 => (DataType::Int64, None),
            FieldType::Uint32 => (DataType::UInt32, None),
            FieldType::Uint64 => (DataType::UInt64, None),
            FieldType::Float32 => (DataType::Float32, None),
            FieldType::Float64 => (DataType::Float64, None),
            FieldType::Bool => (DataType::Boolean, None),
            FieldType::String => (DataType::Utf8, None),
            FieldType::Bytes => (DataType::Binary, None),
            FieldType::Decimal128(f) => (DataType::Decimal128(f.precision, f.scale), None),
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
            (DataType::UInt32, None) => FieldType::Uint32,
            (DataType::UInt64, None) => FieldType::Uint64,
            (DataType::Float32, None) => FieldType::Float32,
            (DataType::Float64, None) => FieldType::Float64,
            (DataType::Decimal128(p, s), None) => FieldType::Decimal128(DecimalField {
                precision: *p,
                scale: *s,
            }),
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

                let st = StructField { fields: fields? };

                FieldType::Struct(st)
            }
            (DataType::List(item), None) => FieldType::List(ListField {
                items: Box::new((**item).clone().try_into()?),
            }),
            dt => {
                return Err(format!("Unsupported data type {dt:?}"));
            }
        };

        Ok(SourceField {
            name: f.name().clone(),
            field_type,
            required: !f.is_nullable(),
            sql_name: None,
            metadata_key: None,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "snake_case", tag = "type")]
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
#[serde(rename_all = "snake_case")]
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

        s.validate(None)
    }

    pub fn validate(self, connection_type: Option<ConnectionType>) -> anyhow::Result<Self> {
        // Validate format compatibility
        if let (Some(format), Some(conn_type)) = (&self.format, connection_type) {
            match conn_type {
                ConnectionType::Source | ConnectionType::Lookup => {
                    // Only allow formats supported for deserialization
                    // See arroyo-formats/src/de.rs for implementation
                    match format {
                        Format::Json(_)
                        | Format::Avro(_)
                        | Format::Protobuf(_)
                        | Format::RawString(_)
                        | Format::RawBytes(_) => {
                            // Supported input formats
                        }
                        Format::Parquet(_) => {
                            bail!("parquet format is not supported for source connections");
                        }
                    }
                }
                ConnectionType::Sink => {
                    // Only allow formats supported for serialization
                    // See arroyo-formats/src/ser.rs for implementation
                    match format {
                        Format::Json(_)
                        | Format::Avro(_)
                        | Format::Parquet(_) // Filesystem sinks only, validated by connector
                        | Format::RawString(_)
                        | Format::RawBytes(_) => {
                            // Supported output formats
                        }
                        Format::Protobuf(_) => {
                            bail!("protobuf format is not yet supported for sink connections");
                        }
                    }
                }
            }
        }

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
#[serde(rename_all = "snake_case")]
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
#[serde(rename_all = "snake_case")]
pub struct ConnectionTablePost {
    pub name: String,
    pub connector: String,
    pub connection_profile_id: Option<String>,
    pub config: serde_json::Value,
    pub schema: Option<ConnectionSchema>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct ConnectionAutocompleteResp {
    pub values: BTreeMap<String, Vec<String>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "snake_case")]
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
#[serde(rename_all = "snake_case")]
pub struct ConfluentSchema {
    pub schema: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, IntoParams)]
#[serde(rename_all = "snake_case")]
pub struct ConfluentSchemaQueryParams {
    pub endpoint: String,
    pub topic: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field as ArrowField, TimeUnit};
    use serde_json::{json, Value as J};
    use std::sync::Arc;

    fn af(name: &str, dt: DataType, nullable: bool) -> ArrowField {
        ArrowField::new(name, dt, nullable)
    }

    #[test]
    fn sql_type_struct_and_list() {
        let st = FieldType::Struct(StructField {
            fields: vec![
                SourceField {
                    name: "a".into(),
                    field_type: FieldType::Int32,
                    required: true,
                    sql_name: None,
                    metadata_key: None,
                },
                SourceField {
                    name: "b".into(),
                    field_type: FieldType::String,
                    required: false,
                    sql_name: None,
                    metadata_key: None,
                },
            ],
        });
        assert_eq!(st.sql_type(), "STRUCT <a INTEGER, b TEXT>");

        let lf = FieldType::List(ListField {
            items: Box::new(ListFieldItem {
                name: "item".into(),
                field_type: FieldType::Bool,
                required: false,
                sql_name: None,
            }),
        });
        assert_eq!(lf.sql_type(), "BOOLEAN[]");
    }

    #[test]
    fn sourcefield_custom_serialize() {
        let sf = SourceField {
            name: "x".into(),
            field_type: FieldType::Timestamp(TimestampField {
                unit: TimestampUnit::Microsecond,
            }),
            required: true,
            sql_name: None,
            metadata_key: None,
        };
        let j = serde_json::to_value(&sf).unwrap();

        assert_eq!(j.get("name"), Some(&J::String("x".into())));
        assert_eq!(j.get("required"), Some(&J::Bool(true)));
        assert_eq!(j.get("type"), Some(&J::String("timestamp".into())));
        assert_eq!(j.pointer("/unit"), Some(&J::String("microsecond".into())));
        assert_eq!(j.get("sql_name"), Some(&J::String("TIMESTAMP(6)".into())));
    }

    #[test]
    fn sourcefield_to_arrow_and_back_primitives() {
        let sf = SourceField {
            name: "flag".into(),
            field_type: FieldType::Bool,
            required: true,
            sql_name: None,
            metadata_key: None,
        };

        let arrow: ArrowField = sf.clone().into();
        assert_eq!(arrow.name(), "flag");
        assert_eq!(arrow.data_type(), &DataType::Boolean);
        assert!(!arrow.is_nullable());

        let round: SourceField = arrow.try_into().unwrap();
        assert_eq!(round, sf);
    }

    #[test]
    fn json_extension_roundtrip() {
        let sf = SourceField {
            name: "payload".into(),
            field_type: FieldType::Json,
            required: false,
            sql_name: None,
            metadata_key: None,
        };

        let arrow: ArrowField = sf.clone().into();
        assert_eq!(arrow.data_type(), &DataType::Utf8);
        let back: SourceField = arrow.try_into().unwrap();
        assert_eq!(back.field_type, FieldType::Json);
    }

    #[test]
    fn struct_roundtrip() {
        let sf = SourceField {
            name: "s".into(),
            field_type: FieldType::Struct(StructField {
                fields: vec![
                    SourceField {
                        name: "id".into(),
                        field_type: FieldType::Int64,
                        required: true,
                        sql_name: None,
                        metadata_key: None,
                    },
                    SourceField {
                        name: "name".into(),
                        field_type: FieldType::String,
                        required: false,
                        sql_name: None,
                        metadata_key: None,
                    },
                ],
            }),
            required: true,
            sql_name: None,
            metadata_key: None,
        };

        let arrow: ArrowField = sf.clone().into();
        match arrow.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "id");
                assert_eq!(fields[1].name(), "name");
            }
            other => panic!("expected Struct, got {other:?}"),
        }

        let round: SourceField = arrow.try_into().unwrap();
        assert_eq!(round, sf);
    }

    #[test]
    fn list_item_conversions() {
        let item = ListFieldItem {
            name: "elem".into(),
            field_type: FieldType::Int32,
            required: true,
            sql_name: None,
        };
        let as_field: ArrowField = Field::from(SourceField {
            name: item.name.clone(),
            field_type: item.field_type.clone(),
            required: item.required,
            sql_name: None,
            metadata_key: None,
        });

        let list_arrow = af("nums", DataType::List(Arc::new(as_field.clone())), true);

        let recovered_item: ListFieldItem = (**match list_arrow.data_type() {
            DataType::List(inner) => inner,
            _ => panic!("expected list"),
        })
        .clone()
        .try_into()
        .unwrap();

        assert_eq!(recovered_item.name, "elem");
        assert_eq!(recovered_item.field_type, FieldType::Int32);
        assert!(recovered_item.required);
    }

    #[test]
    fn list_without_name() {
        let json = json!({
            "name": "my_list",
            "type": "list",
            "items": {
                "type": "int32",
                "required": true
            }
        });

        let field: SourceField = serde_json::from_value(json).unwrap();
        assert_eq!(field.name.as_str(), "my_list");
        assert!(!field.required);

        assert_eq!(
            field.field_type,
            FieldType::List(ListField {
                items: Box::new(ListFieldItem {
                    name: "item".to_string(),
                    field_type: FieldType::Int32,
                    required: true,
                    sql_name: None,
                }),
            })
        );
    }

    #[test]
    fn list_roundtrip_from_fieldtype() {
        let schema_field = SourceField {
            name: "tags".into(),
            field_type: FieldType::List(ListField {
                items: Box::new(ListFieldItem {
                    name: "item".into(),
                    field_type: FieldType::String,
                    required: false,
                    sql_name: None,
                }),
            }),
            required: false,
            sql_name: None,
            metadata_key: None,
        };

        let arrow: ArrowField = schema_field.clone().into();
        match arrow.data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.name(), "item");
                assert_eq!(inner.data_type(), &DataType::Utf8);
            }
            dt => panic!("expected List, got {dt:?}"),
        }

        let round: SourceField = arrow.try_into().unwrap();
        assert_eq!(round, schema_field);
    }

    #[test]
    fn connection_schema_arroyo_schema_mapping() {
        let fields = vec![
            SourceField {
                name: "id".into(),
                field_type: FieldType::Int64,
                required: true,
                sql_name: None,
                metadata_key: None,
            },
            SourceField {
                name: "ts".into(),
                field_type: FieldType::Timestamp(TimestampField {
                    unit: TimestampUnit::Millisecond,
                }),
                required: false,
                sql_name: None,
                metadata_key: None,
            },
        ];

        let cs = ConnectionSchema {
            format: None,
            bad_data: None,
            framing: None,
            fields: fields.clone(),
            definition: None,
            inferred: Some(false),
            primary_keys: ["id".to_string()].into_iter().collect(),
        };

        let arroyo = cs.arroyo_schema();
        let a_fields = arroyo.schema.fields();
        assert_eq!(a_fields.len(), 3);
        assert_eq!(a_fields[0].name(), "id");
        assert_eq!(a_fields[1].name(), "ts");
        assert_eq!(a_fields[0].data_type(), &DataType::Int64);
        assert_eq!(
            a_fields[1].data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
    }

    #[test]
    fn arrow_roundtrip_timestamp_units() {
        for (unit, tu) in [
            (TimeUnit::Second, TimestampUnit::Second),
            (TimeUnit::Millisecond, TimestampUnit::Millisecond),
            (TimeUnit::Microsecond, TimestampUnit::Microsecond),
            (TimeUnit::Nanosecond, TimestampUnit::Nanosecond),
        ] {
            let sf = SourceField {
                name: "ts".into(),
                field_type: FieldType::Timestamp(TimestampField { unit: tu.clone() }),
                required: true,
                sql_name: None,
                metadata_key: None,
            };
            let a: ArrowField = sf.clone().into();
            assert_eq!(a.data_type(), &DataType::Timestamp(unit, None));
            let back: SourceField = a.try_into().unwrap();
            assert_eq!(back, sf);
        }
    }

    #[test]
    fn test_format_validation_for_connection_type() {
        use crate::formats::{Format, ParquetCompression, ParquetFormat};

        let schema = ConnectionSchema {
            format: Some(Format::Parquet(ParquetFormat {
                compression: ParquetCompression::Zstd,
                row_group_bytes: None,
            })),
            bad_data: None,
            framing: None,
            fields: vec![],
            definition: None,
            inferred: None,
            primary_keys: Default::default(),
        };

        // Parquet should be rejected for sources
        let result = schema.clone().validate(Some(ConnectionType::Source));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "parquet format is not supported for source connections"
        );

        // Parquet should be accepted for sinks
        let result = schema.clone().validate(Some(ConnectionType::Sink));
        assert!(result.is_ok());

        // No validation when connection_type is None
        let result = schema.validate(None);
        assert!(result.is_ok());
    }
}
