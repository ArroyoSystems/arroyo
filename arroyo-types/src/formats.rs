use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default, Hash, PartialOrd)]
pub enum TimestampFormat {
    #[default]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default, Hash, PartialOrd)]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
pub struct RawStringFormat {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
pub struct AvroFormat {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
pub struct ParquetFormat {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
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
            "avro" => return Err("avro is not yet supported".to_string()),
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
