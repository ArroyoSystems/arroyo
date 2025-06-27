use crate::ConnectorOptions;
use datafusion::common::{plan_datafusion_err, plan_err, Result as DFResult};
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;
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

    #[serde(default, alias = "confluent_schema_version")]
    pub schema_id: Option<u32>,

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
    fn from_opts(debezium: bool, opts: &mut ConnectorOptions) -> DFResult<Self> {
        let confluent_schema_registry = opts
            .pull_opt_bool("json.confluent_schema_registry")?
            .unwrap_or(false);

        let include_schema = opts.pull_opt_bool("json.include_schema")?.unwrap_or(false);

        if include_schema && confluent_schema_registry {
            return plan_err!("at most one of `json.confluent_schema_registry` and `json.include_schema` may be set");
        }

        let unstructured = opts.pull_opt_bool("json.unstructured")?.unwrap_or(false);

        let timestamp_format: TimestampFormat = opts
            .pull_opt_str("json.timestamp_format")?
            .map(|t| t.as_str().try_into())
            .transpose()
            .map_err(|_| plan_datafusion_err!("invalid value for `json.timestamp_format`"))?
            .unwrap_or_else(|| {
                if debezium {
                    TimestampFormat::UnixMillis
                } else {
                    TimestampFormat::default()
                }
            });

        Ok(Self {
            confluent_schema_registry,
            schema_id: None,
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
#[serde(rename_all = "camelCase")]
pub struct RawBytesFormat {}

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
                .map_err(|e| serde::de::Error::custom(format!("Invalid avro schema: {e:?}")))?,
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AvroFormat {
    #[serde(default)]
    pub confluent_schema_registry: bool,

    #[serde(default)]
    pub raw_datums: bool,

    #[serde(default)]
    pub into_unstructured_json: bool,

    #[serde(default)]
    #[schema(read_only, value_type = String)]
    pub reader_schema: Option<SerializableAvroSchema>,

    #[serde(default)]
    #[schema(read_only)]
    pub schema_id: Option<u32>,
}

impl AvroFormat {
    pub fn new(
        confluent_schema_registry: bool,
        raw_datums: bool,
        into_unstructured_json: bool,
    ) -> Self {
        Self {
            confluent_schema_registry,
            raw_datums,
            into_unstructured_json,
            reader_schema: None,
            schema_id: None,
        }
    }

    pub fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Self> {
        Ok(Self::new(
            opts.pull_opt_bool("avro.confluent_schema_registry")?
                .unwrap_or(false),
            opts.pull_opt_bool("avro.raw_datums")?.unwrap_or(false),
            opts.pull_opt_bool("avro.into_unstructured_json")?
                .unwrap_or(false),
        ))
    }

    pub fn add_reader_schema(&mut self, schema: apache_avro::Schema) {
        self.reader_schema = Some(SerializableAvroSchema(schema));
    }

    pub fn sanitize_field(s: &str) -> String {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| Regex::new(r"[^a-zA-Z0-9_.]").unwrap());

        re.replace_all(s, "_").replace('.', "__")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ParquetFormat {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProtobufFormat {
    #[serde(default)]
    pub into_unstructured_json: bool,

    #[serde(default)]
    pub message_name: Option<String>,

    #[serde(default)]
    pub compiled_schema: Option<Vec<u8>>,

    #[serde(default)]
    pub confluent_schema_registry: bool,

    #[serde(default)]
    pub length_delimited: bool,
}

impl ProtobufFormat {
    pub fn from_opts(_opts: &mut ConnectorOptions) -> DFResult<Self> {
        plan_err!("Protobuf is not yet supported in CREATE TABLE statements")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Json(JsonFormat),
    Avro(AvroFormat),
    Protobuf(ProtobufFormat),
    Parquet(ParquetFormat),
    RawString(RawStringFormat),
    RawBytes(RawBytesFormat),
}

impl Format {
    pub fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Option<Self>> {
        let Some(name) = opts.pull_opt_str("format")? else {
            return Ok(None);
        };

        Ok(Some(match name.as_str() {
            "json" => Format::Json(JsonFormat::from_opts(false, opts)?),
            "debezium_json" => Format::Json(JsonFormat::from_opts(true, opts)?),
            "protobuf" => Format::Protobuf(ProtobufFormat::from_opts(opts)?),
            "avro" => Format::Avro(AvroFormat::from_opts(opts)?),
            "raw_string" => Format::RawString(RawStringFormat {}),
            "raw_bytes" => Format::RawBytes(RawBytesFormat {}),
            "parquet" => Format::Parquet(ParquetFormat {}),
            f => return plan_err!("unknown format '{}'", f),
        }))
    }

    pub fn is_updating(&self) -> bool {
        match self {
            Format::Json(JsonFormat { debezium: true, .. }) => true,
            Format::Json(_)
            | Format::Avro(_)
            | Format::Parquet(_)
            | Format::RawString(_)
            | Format::Protobuf(_) => false,
            Format::RawBytes(_) => false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BadData {
    Fail {},
    Drop {},
}

impl Default for BadData {
    fn default() -> Self {
        BadData::Fail {}
    }
}

impl BadData {
    pub fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Option<Self>> {
        let Some(method) = opts.pull_opt_str("bad_data")? else {
            return Ok(None);
        };

        let method = match method.as_str() {
            "drop" => BadData::Drop {},
            "fail" => BadData::Fail {},
            f => {
                return plan_err!(
                    "invalid value for 'bad_data': `{}`; expected one of 'drop' or 'fail'",
                    f
                )
            }
        };

        Ok(Some(method))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Framing {
    pub method: FramingMethod,
}

impl Framing {
    pub fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Option<Self>> {
        let Some(method) = opts.pull_opt_str("framing")? else {
            return Ok(None);
        };

        let method = match method.as_str() {
            "newline" => FramingMethod::Newline(NewlineDelimitedFraming::from_opts(opts)?),
            f => return plan_err!("Unknown framing method '{}'", f),
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
    pub fn from_opts(opts: &mut ConnectorOptions) -> DFResult<Self> {
        let max_line_length = opts.pull_opt_u64("framing.newline.max_length")?;

        Ok(NewlineDelimitedFraming { max_line_length })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum FramingMethod {
    Newline(NewlineDelimitedFraming),
}
