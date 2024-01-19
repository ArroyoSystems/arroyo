use crate::{avro, json, FramingIterator, SchemaData};
use arroyo_rpc::formats::{AvroFormat, Format, Framing};
use arroyo_rpc::schema_resolver::{FailingSchemaResolver, FixedSchemaResolver, SchemaResolver};
use arroyo_types::SourceError;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::Mutex;

fn deserialize_raw_string<T: DeserializeOwned>(msg: &[u8]) -> Result<T, String> {
    let json = json! {
        { "value": String::from_utf8_lossy(msg) }
    };
    Ok(serde_json::from_value(json).unwrap())
}

#[derive(Clone)]
pub struct DataDeserializer<T: SchemaData> {
    format: Arc<Format>,
    framing: Option<Arc<Framing>>,
    schema_registry: Arc<Mutex<HashMap<u32, apache_avro::schema::Schema>>>,
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
    ) -> impl Iterator<Item = Result<T, SourceError>> + 'a + Send {
        match &*self.format {
            Format::Avro(avro) => {
                unimplemented!()
            }
            _ => {
                let new_self = self.clone();
                Box::new(
                    FramingIterator::new(self.framing.clone(), msg)
                        .map(move |t| new_self.deserialize_single(t)),
                ) as Box<dyn Iterator<Item = Result<T, SourceError>> + Send>
            }
        }
    }

    pub fn get_format(&self) -> Arc<Format> {
        self.format.clone()
    }

    pub fn deserialize_single(&self, msg: &[u8]) -> Result<T, SourceError> {
        match &*self.format {
            Format::Json(json) => unimplemented!(),
            Format::Avro(_) => unreachable!("avro should be handled by here"),
            Format::Parquet(_) => todo!("parquet is not supported as an input format"),
            Format::RawString(_) => deserialize_raw_string(msg),
        }
        .map_err(|e| SourceError::bad_data(format!("Failed to deserialize: {:?}", e)))
    }
}

pub struct DataSerializer<T: SchemaData> {
    kafka_schema: Value,
    #[allow(unused)]
    json_schema: Value,
    avro_schema: apache_avro::schema::Schema,
    schema_id: Option<u32>,
    format: Format,
    _t: PhantomData<T>,
}

impl<T: SchemaData> DataSerializer<T> {
    pub fn new(format: Format) -> Self {
        Self {
            kafka_schema: json::arrow_to_kafka_json(T::name(), T::schema().fields()),
            json_schema: json::arrow_to_json_schema(T::schema().fields()),
            avro_schema: avro::arrow_to_avro_schema(T::name(), T::schema().fields()),
            schema_id: match &format {
                Format::Avro(avro) => avro.schema_id,
                _ => None,
            },
            format,
            _t: PhantomData,
        }
    }

    pub fn to_vec(&self, record: &T) -> Option<Vec<u8>> {
        match &self.format {
            Format::Json(json) => {
                let mut writer: Vec<u8> = Vec::with_capacity(128);
                if json.confluent_schema_registry {
                    if json.include_schema {
                        unreachable!("can't include schema when writing to confluent schema registry, should've been caught when creating JsonFormat");
                    }
                    writer.push(0);
                    writer.extend(json.schema_id.expect("must have computed id version to write using confluent schema registry").to_be_bytes());
                }
                if json.include_schema {
                    let record = json! {{
                        "schema": self.kafka_schema,
                        "payload": record
                    }};

                    serde_json::to_writer(&mut writer, &record).unwrap();
                } else {
                    serde_json::to_writer(&mut writer, record).unwrap();
                };
                Some(writer)
            }
            Format::Avro(f) => Some(avro::to_vec(record, f, &self.avro_schema, self.schema_id)),
            Format::Parquet(_) => todo!(),
            Format::RawString(_) => record.to_raw_string(),
        }
    }
}
