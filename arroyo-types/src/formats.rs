use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct JsonFormat {
    #[serde(default)]
    pub confluent_schema_registry: bool,

    #[serde(default)]
    pub include_schema: bool,

    #[serde(default)]
    pub debezium: bool,

    #[serde(default)]
    pub unstructured: bool,
}

impl JsonFormat {
    fn from_opts(debezium: bool, opts: &mut HashMap<String, String>) -> Self {
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

        Self {
            confluent_schema_registry,
            include_schema,
            debezium,
            unstructured,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RawEncoding {
    None,
    Utf8,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawFormat {
    encoding: RawEncoding,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AvroFormat {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParquetFormat {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Format {
    Json(JsonFormat),
    Avro(AvroFormat),
    Parquet(ParquetFormat),
    Raw(RawFormat),
}

impl Format {
    pub fn from_opts(opts: &mut HashMap<String, String>) -> Result<Option<Self>, String> {
        let Some(name) = opts.remove("format") else {
            return Ok(None);
        };

        Ok(Some(match name.as_str() {
            "json" => Format::Json(JsonFormat::from_opts(false, opts)),
            "debezium_json" => Format::Json(JsonFormat::from_opts(true, opts)),
            "protobuf" => return Err("protobuf is not yet supported".to_string()),
            "avro" => return Err("avro is not yet supported".to_string()),
            "raw_string" => return Err("raw_string is not yet supported".to_string()),
            "parquet" => Format::Parquet(ParquetFormat {}),
            f => return Err(format!("Unknown format '{}'", f)),
        }))
    }

    pub fn is_updating(&self) -> bool {
        match self {
            Format::Json(JsonFormat { debezium: true, .. }) => true,
            Format::Json(_) | Format::Avro(_) | Format::Parquet(_) | Format::Raw(_) => false,
        }
    }
}
