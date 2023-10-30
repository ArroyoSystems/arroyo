mod avro;
pub mod json;

use apache_avro::Schema;
use std::sync::Arc;
use std::{collections::HashMap, marker::PhantomData};

use arroyo_rpc::formats::{AvroFormat, Format, Framing, FramingMethod};
use arroyo_rpc::schema_resolver::{FailingSchemaResolver, FixedSchemaResolver, SchemaResolver};
use arroyo_types::UserError;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::SchemaData;

fn deserialize_raw_string<T: DeserializeOwned>(msg: &[u8]) -> Result<T, String> {
    let json = json! {
        { "value": String::from_utf8_lossy(msg) }
    };
    Ok(serde_json::from_value(json).unwrap())
}

pub struct FramingIterator<'a> {
    framing: Option<Arc<Framing>>,
    buf: &'a [u8],
    offset: usize,
}

impl<'a> FramingIterator<'a> {
    pub fn new(framing: Option<Arc<Framing>>, buf: &'a [u8]) -> Self {
        Self {
            framing,
            buf,
            offset: 0,
        }
    }
}

impl<'a> Iterator for FramingIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.buf.len() {
            return None;
        }

        match &self.framing {
            Some(framing) => {
                match &framing.method {
                    FramingMethod::Newline(newline) => {
                        let end = memchr::memchr('\n' as u8, &self.buf[self.offset..])
                            .map(|i| self.offset + i)
                            .unwrap_or(self.buf.len());

                        let prev = self.offset;
                        self.offset = end + 1;

                        // enforce max len if set
                        let length =
                            (end - prev).min(newline.max_line_length.unwrap_or(u64::MAX) as usize);

                        Some(&self.buf[prev..(prev + length)])
                    }
                }
            }
            None => {
                self.offset = self.buf.len();
                Some(self.buf)
            }
        }
    }
}

#[derive(Clone)]
pub struct DataDeserializer<T: SchemaData> {
    format: Arc<Format>,
    framing: Option<Arc<Framing>>,
    schema_registry: Arc<Mutex<HashMap<u32, Schema>>>,
    schema_resolver: Arc<dyn SchemaResolver + Sync>,
    _t: PhantomData<T>,
}

impl<T: SchemaData> DataDeserializer<T> {
    pub fn new(format: Format, framing: Option<Framing>) -> Self {
        let resolver = if let Format::Avro(AvroFormat {
            reader_schema: Some(schema),
            ..
        }) = &format
        {
            Arc::new(FixedSchemaResolver::new(0, schema.clone().into()))
                as Arc<dyn SchemaResolver + Sync>
        } else {
            Arc::new(FailingSchemaResolver::new()) as Arc<dyn SchemaResolver + Sync>
        };

        Self::with_schema_resolver(format, framing, resolver)
    }

    pub fn with_schema_resolver(
        format: Format,
        framing: Option<Framing>,
        schema_resolver: Arc<dyn SchemaResolver + Sync>,
    ) -> Self {
        Self {
            format: Arc::new(format),
            framing: framing.map(|f| Arc::new(f)),
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
            schema_resolver,
            _t: PhantomData,
        }
    }

    pub async fn deserialize_slice<'a>(
        &mut self,
        msg: &'a [u8],
    ) -> impl Iterator<Item = Result<T, UserError>> + 'a + Send {
        match &*self.format {
            Format::Avro(avro) => {
                let schema_registry = self.schema_registry.clone();
                let schema_resolver = self.schema_resolver.clone();
                match avro::deserialize_slice_avro(avro, schema_registry, schema_resolver, msg)
                    .await
                {
                    Ok(iter) => Box::new(iter),
                    Err(e) => Box::new(
                        vec![Err(UserError::new("Avro deserialization failed", e))].into_iter(),
                    )
                        as Box<dyn Iterator<Item = Result<T, UserError>> + Send>,
                }
            }
            _ => {
                let new_self = self.clone();
                Box::new(
                    FramingIterator::new(self.framing.clone(), msg)
                        .map(move |t| new_self.deserialize_single(t)),
                ) as Box<dyn Iterator<Item = Result<T, UserError>> + Send>
            }
        }
    }

    fn deserialize_single(&self, msg: &[u8]) -> Result<T, UserError> {
        match &*self.format {
            Format::Json(json) => json::deserialize_slice_json(json, msg),
            Format::Avro(_) => unreachable!("avro should be handled by here"),
            Format::Parquet(_) => todo!("parquet is not supported as an input format"),
            Format::RawString(_) => deserialize_raw_string(msg),
        }
        .map_err(|e| {
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

pub struct DataSerializer<T: SchemaData> {
    kafka_schema: Value,
    #[allow(unused)]
    json_schema: Value,
    format: Format,
    _t: PhantomData<T>,
}

impl<T: SchemaData> DataSerializer<T> {
    pub fn new(format: Format) -> Self {
        Self {
            kafka_schema: json::arrow_to_kafka_json(T::name(), T::schema().fields()),
            json_schema: json::arrow_to_json_schema(T::schema().fields()),
            format,
            _t: PhantomData,
        }
    }

    pub fn to_vec(&self, record: &T) -> Option<Vec<u8>> {
        match &self.format {
            Format::Json(json) => {
                let v = if json.include_schema {
                    let record = json! {{
                        "schema": self.kafka_schema,
                        "payload": record
                    }};

                    serde_json::to_vec(&record).unwrap()
                } else {
                    serde_json::to_vec(record).unwrap()
                };

                if json.confluent_schema_registry {
                    todo!("Serializing to confluent schema registry is not yet supported");
                }

                Some(v)
            }
            Format::Avro(_) => todo!(),
            Format::Parquet(_) => todo!(),
            Format::RawString(_) => record.to_raw_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::formats::FramingIterator;
    use arroyo_rpc::formats::{Framing, FramingMethod, NewlineDelimitedFraming};
    use std::sync::Arc;

    #[test]
    fn test_line_framing() {
        let framing = Some(Arc::new(Framing {
            method: FramingMethod::Newline(NewlineDelimitedFraming {
                max_line_length: None,
            }),
        }));

        let result: Vec<_> = FramingIterator::new(framing.clone(), "one block".as_bytes())
            .map(|t| String::from_utf8(t.to_vec()).unwrap())
            .collect();

        assert_eq!(vec!["one block".to_string()], result);

        let result: Vec<_> = FramingIterator::new(
            framing.clone(),
            "one block\ntwo block\nthree block".as_bytes(),
        )
        .map(|t| String::from_utf8(t.to_vec()).unwrap())
        .collect();

        assert_eq!(
            vec![
                "one block".to_string(),
                "two block".to_string(),
                "three block".to_string()
            ],
            result
        );

        let result: Vec<_> = FramingIterator::new(
            framing.clone(),
            "one block\ntwo block\nthree block\n".as_bytes(),
        )
        .map(|t| String::from_utf8(t.to_vec()).unwrap())
        .collect();

        assert_eq!(
            vec![
                "one block".to_string(),
                "two block".to_string(),
                "three block".to_string()
            ],
            result
        );
    }

    #[test]
    fn test_max_line_length() {
        let framing = Some(Arc::new(Framing {
            method: FramingMethod::Newline(NewlineDelimitedFraming {
                max_line_length: Some(5),
            }),
        }));

        let result: Vec<_> =
            FramingIterator::new(framing, "one block\ntwo block\nwhole".as_bytes())
                .map(|t| String::from_utf8(t.to_vec()).unwrap())
                .collect();

        assert_eq!(
            vec![
                "one b".to_string(),
                "two b".to_string(),
                "whole".to_string()
            ],
            result
        );
    }
}
