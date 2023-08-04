use std::collections::HashMap;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

use crate::UserError;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaJson {
    pub schema: Value,
    pub payload: Value,
}

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
    fn deserialize_slice_int<T: DeserializeOwned>(&self, msg: &[u8]) -> Result<T, String> {
        let msg = if self.confluent_schema_registry {
            &msg[5..]
        } else {
            msg
        };

        if self.unstructured {
            let j = if self.include_schema {
                // we need to deserialize it to pull out the payload
                let v: Value = serde_json::from_slice(&msg)
                    .map_err(|e| format!("Failed to deserialize json: {:?}", e))?;
                let payload = v.get("payload").ok_or_else(|| {
                    "`include_schema` set to true, but record does not have a payload field"
                        .to_string()
                })?;

                json! {
                    { "value": serde_json::to_string(payload).unwrap() }
                }
            } else {
                json! {
                    { "value": String::from_utf8_lossy(msg) }
                }
            };

            // TODO: this is inefficient, because we know that T is RawJson in this case and can much more directly
            //  produce that value. However, without specialization I don't know how to get the compiler to emit
            //  the optimized code for that case.
            serde_json::from_value(j).unwrap()
        } else {
            serde_json::from_slice(msg)
                .map_err(|e| format!("Failed to deserialize JSON into schema: {:?}", e))?
        }
    }

    fn deserialize_slice<T: DeserializeOwned>(&self, msg: &[u8]) -> Result<T, UserError> {
        self.deserialize_slice_int(msg).map_err(|e| {
            UserError::new(
                "Deserialization failed",
                format!(
                    "Failed to deserialize: '{}': {}",
                    String::from_utf8_lossy(&msg),
                    e
                ),
            )
        })
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
        /*         if let Some(f) = options.remove("format") {
                   format = Some(match f.as_str() {
                       "json" => Format::JsonFormat,
                       "debezium_json" => Format::DebeziumJsonFormat,
                       "protobuf" => Format::ProtobufFormat,
                       "avro" => Format::AvroFormat,
                       "raw_string" => Format::RawStringFormat,
                       "parquet" => Format::ParquetFormat,
                       f => bail!("Unknown format '{}'", f),
                   });
               }

               let schema_registry = options
                   .remove("format_options.confluent_schema_registry")
                   .map(|f| f == "true")
                   .unwrap_or(false);
        */

        todo!()
    }

    pub fn is_updating(&self) -> bool {
        match self {
            Format::Json(JsonFormat { debezium: true, .. }) => true,
            Format::Json(_) | Format::Avro(_) | Format::Parquet(_) | Format::Raw(_) => false,
        }
    }

    pub fn deserialize_slice<T: DeserializeOwned>(&self, msg: &[u8]) -> Result<T, UserError> {
        match self {
            Format::Json(json) => json.deserialize_slice(msg),
            Format::Avro(_) => todo!(),
            Format::Parquet(_) => todo!(),
            Format::Raw(_) => todo!(),
        }
    }
}
