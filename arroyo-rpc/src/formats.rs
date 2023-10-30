use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use utoipa::ToSchema;

#[derive(
    Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default, Hash, PartialOrd, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TimestampFormat {
    #[default]
    #[serde(rename = "rfc3339")]
    RFC3339,
    UnixMillis,
}

impl TryFrom<&str> for TimestampFormat {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "RFC3339" => Ok(TimestampFormat::RFC3339),
            "UnixMillis" | "unix_millis" => Ok(TimestampFormat::UnixMillis),
            _ => Err(()),
        }
    }
}

#[derive(
    Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default, Hash, PartialOrd, ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct JsonFormat {
    #[serde(default)]
    pub confluent_schema_registry: bool,

    #[serde(default)]
    pub include_schema: bool,

    #[serde(default)]
    pub debezium: bool,

    #[serde(default)]
    pub unstructured: bool,

    #[serde(default)]
    pub timestamp_format: TimestampFormat,
}

impl JsonFormat {
    fn from_opts(debezium: bool, opts: &mut HashMap<String, String>) -> Result<Self, String> {
        let confluent_schema_registry = opts
            .remove("json.confluent_schema_registry")
            .filter(|t| t == "true")
            .is_some();

        let include_schema = opts
            .remove("json.include_schema")
            .filter(|t| t == "true")
            .is_some();

        let unstructured = opts
            .remove("json.unstructured")
            .filter(|t| t == "true")
            .is_some();

        let timestamp_format: TimestampFormat = opts
            .remove("json.timestamp_format")
            .map(|t| t.as_str().try_into())
            .transpose()
            .map_err(|_| "json.timestamp_format".to_string())?
            .unwrap_or_else(|| {
                if debezium {
                    TimestampFormat::UnixMillis
                } else {
                    TimestampFormat::default()
                }
            });

        Ok(Self {
            confluent_schema_registry,
            include_schema,
            debezium,
            unstructured,
            timestamp_format,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RawStringFormat {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
pub struct ConfluentSchemaRegistryConfig {
    endpoint: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SerializableAvroSchema(pub apache_avro::Schema);

impl Eq for SerializableAvroSchema {}

impl Hash for SerializableAvroSchema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.canonical_form().hash(state);
    }
}

impl PartialOrd for SerializableAvroSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0
            .canonical_form()
            .partial_cmp(&other.0.canonical_form())
    }
}

impl From<SerializableAvroSchema> for apache_avro::Schema {
    fn from(value: SerializableAvroSchema) -> Self {
        value.0
    }
}

impl<'a> From<&'a SerializableAvroSchema> for &'a apache_avro::Schema {
    fn from(value: &'a SerializableAvroSchema) -> Self {
        &value.0
    }
}

impl Serialize for SerializableAvroSchema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.canonical_form().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SerializableAvroSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self(
            apache_avro::Schema::parse_str(&String::deserialize(deserializer)?)
                .map_err(|e| serde::de::Error::custom(format!("Invalid avro schema: {:?}", e)))?,
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AvroFormat {
    #[serde(default)]
    pub confluent_schema_registry: bool,

    #[serde(default)]
    pub embedded_schema: bool,

    #[serde(default)]
    pub into_unstructured_json: bool,

    #[serde(default)]
    #[schema(read_only, value_type = String)]
    pub reader_schema: Option<SerializableAvroSchema>,
}

impl AvroFormat {
    pub fn new(
        confluent_schema_registry: bool,
        embedded_schema: bool,
        into_unstructured_json: bool,
    ) -> Self {
        Self {
            confluent_schema_registry,
            embedded_schema,
            into_unstructured_json,
            reader_schema: None,
        }
    }

    pub fn from_opts(opts: &mut HashMap<String, String>) -> Result<Self, String> {
        Ok(Self::new(
            opts.remove("avro.confluent_schema_registry")
                .filter(|t| t == "true")
                .is_some(),
            opts.remove("avro.include_schema")
                .filter(|t| t == "true")
                .is_some(),
            opts.remove("avro.into_unstructured_json")
                .filter(|t| t == "true")
                .is_some(),
        ))
    }

    pub fn add_reader_schema(&mut self, schema: apache_avro::Schema) {
        self.reader_schema = Some(SerializableAvroSchema(schema));
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ParquetFormat {}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Json(JsonFormat),
    Avro(AvroFormat),
    Parquet(ParquetFormat),
    RawString(RawStringFormat),
}

impl Format {
    pub fn from_opts(opts: &mut HashMap<String, String>) -> Result<Option<Self>, String> {
        let Some(name) = opts.remove("format") else {
            return Ok(None);
        };

        Ok(Some(match name.as_str() {
            "json" => Format::Json(JsonFormat::from_opts(false, opts)?),
            "debezium_json" => Format::Json(JsonFormat::from_opts(true, opts)?),
            "protobuf" => return Err("protobuf is not yet supported".to_string()),
            "avro" => Format::Avro(AvroFormat::from_opts(opts)?),
            "raw_string" => Format::RawString(RawStringFormat {}),
            "parquet" => Format::Parquet(ParquetFormat {}),
            f => return Err(format!("Unknown format '{}'", f)),
        }))
    }

    pub fn is_updating(&self) -> bool {
        match self {
            Format::Json(JsonFormat { debezium: true, .. }) => true,
            Format::Json(_) | Format::Avro(_) | Format::Parquet(_) | Format::RawString(_) => false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Framing {
    pub method: FramingMethod,
}

impl Framing {
    pub fn from_opts(opts: &mut HashMap<String, String>) -> Result<Option<Self>, String> {
        let Some(method) = opts.remove("framing") else {
            return Ok(None);
        };

        let method = match method.as_str() {
            "newline" => FramingMethod::Newline(NewlineDelimitedFraming::from_opts(opts)?),
            f => return Err(format!("Unknown framing method '{}'", f)),
        };

        Ok(Some(Framing { method }))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NewlineDelimitedFraming {
    pub max_line_length: Option<u64>,
}

impl NewlineDelimitedFraming {
    pub fn from_opts(opts: &mut HashMap<String, String>) -> Result<Self, String> {
        let max_line_length = opts
            .remove("framing.newline.max_length")
            .map(|t| u64::from_str(&t))
            .transpose()
            .map_err(|_| {
                format!("invalid value for framing.newline.max_length; must be an unsigned integer")
            })?;

        Ok(NewlineDelimitedFraming { max_line_length })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum FramingMethod {
    Newline(NewlineDelimitedFraming),
}
