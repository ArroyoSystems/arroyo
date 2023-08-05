use std::marker::PhantomData;

use arroyo_types::{
    formats::{Format, JsonFormat},
    OperatorConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub trait FormatSerializer<T> {
    fn serialize(&self, record: &T) -> Vec<u8>;
}

pub struct JsonSerializer<T: Serialize> {
    format: JsonFormat,
    schema: Value,
    _t: PhantomData<T>,
}

impl<T: Serialize> FormatSerializer<T> for JsonSerializer<T> {
    fn serialize(&self, record: &T) -> Vec<u8> {
        let v = if self.format.include_schema {
            let record = json! {{
                "schema": self.schema,
                "payload": record
            }};

            serde_json::to_vec(&record).unwrap()
        } else {
            serde_json::to_vec(record).unwrap()
        };

        if self.format.confluent_schema_registry {
            todo!("Serializing to confluent schema registry is not yet supported");
        }

        v
    }
}

pub fn get_serializer<T>(config: &OperatorConfig) -> Box<dyn FormatSerializer<T>> {
    match &config.format {
        Some(Format::Json(json)) => Box::new(JsonSerializer {}),
        Some(Format::Avro(..)) => todo!(),
        Some(Format::Parquet(..)) => todo!(),
        Some(Format::Raw(..)) => todo!(),
        None => panic!("No format in operator config; cannot produce serializer"),
    }
}
