extern crate core;

use anyhow::bail;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::cast::AsArray;
use arrow_array::{RecordBatch, StringArray};
use arroyo_rpc::formats::{AvroFormat, Format, Framing, FramingMethod};
use arroyo_rpc::schema_resolver::{FailingSchemaResolver, FixedSchemaResolver, SchemaResolver};
use arroyo_types::{Data, Debezium, RawJson, UserError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod avro;
pub mod json;

pub trait SchemaData: Data + Serialize + DeserializeOwned {
    fn name() -> &'static str;
    fn schema() -> arrow::datatypes::Schema;

    fn iterator_from_record_batch(
        _record_batch: RecordBatch,
    ) -> anyhow::Result<Box<dyn Iterator<Item = Self> + Send>> {
        bail!("unimplemented");
    }

    /// Returns the raw string representation of this data, if available for the type
    ///
    /// Implementations should return None if the relevant field is Optional and has
    /// a None value, and should panic if they do not support raw strings (which
    /// indicates a miscompilation).
    fn to_raw_string(&self) -> Option<Vec<u8>>;

    fn to_avro(&self, schema: &apache_avro::Schema) -> apache_avro::types::Value;
}

impl<T: SchemaData> SchemaData for Debezium<T> {
    fn name() -> &'static str {
        "debezium"
    }

    fn schema() -> arrow::datatypes::Schema {
        let subschema = T::schema();

        let fields = vec![
            Field::new(
                "before",
                arrow::datatypes::DataType::Struct(subschema.fields.clone()),
                true,
            ),
            Field::new(
                "after",
                arrow::datatypes::DataType::Struct(subschema.fields),
                true,
            ),
            Field::new("op", arrow::datatypes::DataType::Utf8, false),
        ];

        arrow::datatypes::Schema::new(fields)
    }

    fn to_raw_string(&self) -> Option<Vec<u8>> {
        unimplemented!("debezium data cannot be written as a raw string");
    }

    fn to_avro(&self, schema: &apache_avro::Schema) -> apache_avro::types::Value {
        todo!()
    }
}

impl SchemaData for RawJson {
    fn name() -> &'static str {
        "raw_json"
    }

    fn schema() -> Schema {
        Schema::new(vec![Field::new("value", DataType::Utf8, false)])
    }

    fn to_raw_string(&self) -> Option<Vec<u8>> {
        Some(self.value.as_bytes().to_vec())
    }

    fn iterator_from_record_batch(
        record_batch: RecordBatch,
    ) -> anyhow::Result<Box<dyn Iterator<Item = RawJson> + Send>> {
        Ok(Box::new(RawJsonIterator {
            offset: 0,
            rows: record_batch.num_rows(),
            column: record_batch
                .column_by_name("value")
                .ok_or_else(|| anyhow::anyhow!("missing column value {}", "value"))?
                .as_string()
                .clone(),
        }))
    }

    fn to_avro(&self, schema: &apache_avro::Schema) -> apache_avro::types::Value {
        todo!()
    }
}

struct RawJsonIterator {
    offset: usize,
    rows: usize,
    column: StringArray,
}

impl Iterator for RawJsonIterator {
    type Item = RawJson;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.rows {
            return None;
        }
        let val = self.column.value(self.offset);
        self.offset += 1;
        Some(RawJson {
            value: val.to_string(),
        })
    }
}

impl SchemaData for () {
    fn name() -> &'static str {
        "empty"
    }

    fn schema() -> Schema {
        Schema::empty()
    }

    fn to_raw_string(&self) -> Option<Vec<u8>> {
        None
    }

    fn to_avro(&self, schema: &apache_avro::Schema) -> apache_avro::types::Value {
        apache_avro::types::Value::Record(vec![])
    }
}

// A custom deserializer for json, that takes a json::Value and reserializes it as a string
// where it can then be accessed using SQL JSON functions -- this is currently a bit inefficient
// since we need an owned string.
pub fn deserialize_raw_json<'de, D>(f: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Box<serde_json::value::RawValue> = Box::deserialize(f)?;
    Ok(raw.to_string())
}

pub fn deserialize_raw_json_opt<'de, D>(f: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Box<serde_json::value::RawValue> = Box::deserialize(f)?;
    Ok(Some(raw.to_string()))
}

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

    pub fn get_format(&self) -> Arc<Format> {
        self.format.clone()
    }

    pub fn deserialize_single(&self, msg: &[u8]) -> Result<T, UserError> {
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
    avro_schema: apache_avro::schema::Schema,
    schema_id: Option<i32>,
    format: Format,
    _t: PhantomData<T>,
}

impl<T: SchemaData> DataSerializer<T> {
    pub fn new(format: Format) -> Self {
        Self {
            kafka_schema: json::arrow_to_kafka_json(T::name(), T::schema().fields()),
            json_schema: json::arrow_to_json_schema(T::schema().fields()),
            avro_schema: avro::arrow_to_avro_schema(T::name(), T::schema().fields()),
            format,
            schema_id: Some(1),
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
                    writer.extend(json.confluent_schema_version.expect("must have computed schema version to write using confluent schema registry").to_be_bytes());
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

#[cfg(test)]
mod tests {
    use crate::FramingIterator;
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
