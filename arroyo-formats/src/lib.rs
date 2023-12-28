extern crate core;

use anyhow::bail;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::builder::{make_builder, ArrayBuilder, StringBuilder, TimestampNanosecondBuilder};
use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, StringArray};
use arroyo_rpc::formats::{AvroFormat, Format, Framing, FramingMethod};
use arroyo_rpc::schema_resolver::{FailingSchemaResolver, FixedSchemaResolver, SchemaResolver};
use arroyo_rpc::ArroyoSchema;
use arroyo_types::{to_nanos, Data, Debezium, RawJson, SourceError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;

pub mod avro;
pub mod json;
pub mod old;

pub trait SchemaData: Data + Serialize + DeserializeOwned {
    fn name() -> &'static str;
    fn schema() -> arrow::datatypes::Schema;

    fn iterator_from_record_batch(
        _record_batch: RecordBatch,
    ) -> anyhow::Result<Box<dyn Iterator<Item = Self> + Send>> {
        bail!("unimplemented");
    }
    fn nullable_iterator_from_struct_array(
        _array: &arrow::array::StructArray,
    ) -> anyhow::Result<Box<dyn Iterator<Item = Option<Self>> + Send>> {
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

fn get_subschema<'a>(schema: &'a apache_avro::Schema, field: &str) -> &'a apache_avro::Schema {
    let apache_avro::schema::Schema::Record(record_schmema) = schema else {
        unreachable!();
    };

    let Some(idx) = record_schmema.lookup.get("before") else {
        panic!("field {} not found in avro schema", field);
    };

    &record_schmema.fields[*idx].schema
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
        let mut record = apache_avro::types::Record::new(schema).unwrap();
        if let Some(before) = &self.before {
            record.put("before", before.to_avro(get_subschema(schema, "before")));
        }

        if let Some(after) = &self.after {
            record.put("after", after.to_avro(get_subschema(schema, "after")));
        }

        record.put("op", apache_avro::types::Value::String(self.op.to_string()));

        record.into()
    }
}

impl SchemaData for RawJson {
    fn name() -> &'static str {
        "raw_json"
    }

    fn schema() -> Schema {
        Schema::new(vec![Field::new("value", DataType::Utf8, false)])
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

    fn to_raw_string(&self) -> Option<Vec<u8>> {
        Some(self.value.as_bytes().to_vec())
    }

    fn to_avro(&self, schema: &apache_avro::Schema) -> apache_avro::types::Value {
        let mut record = apache_avro::types::Record::new(schema).unwrap();
        record.put(
            "value",
            apache_avro::types::Value::String(self.value.clone()),
        );
        record.into()
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

    fn iterator_from_record_batch(
        record_batch: RecordBatch,
    ) -> anyhow::Result<Box<dyn Iterator<Item = Self> + Send>> {
        let len = record_batch.num_rows();
        Ok(Box::new(std::iter::repeat(()).take(len)))
    }

    fn nullable_iterator_from_struct_array(
        array: &arrow::array::StructArray,
    ) -> anyhow::Result<Box<dyn Iterator<Item = Option<Self>> + Send>> {
        let len = array.len();
        Ok(Box::new(std::iter::repeat(None).take(len)))
    }

    fn to_raw_string(&self) -> Option<Vec<u8>> {
        None
    }

    fn to_avro(&self, _: &apache_avro::Schema) -> apache_avro::types::Value {
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
pub struct ArrowDeserializer {
    format: Arc<Format>,
    framing: Option<Arc<Framing>>,
    schema: ArroyoSchema,
    schema_registry: Arc<Mutex<HashMap<u32, apache_avro::schema::Schema>>>,
    schema_resolver: Arc<dyn SchemaResolver + Sync>,
}

pub struct ArrowDeserializerBatch<'a> {
    parent: &'a mut ArrowDeserializer,
    arrays: Vec<Box<dyn ArrayBuilder>>,
    errors: Vec<SourceError>,
}

impl ArrowDeserializer {
    pub fn new(format: Format, schema: ArroyoSchema, framing: Option<Framing>) -> Self {
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

        Self::with_schema_resolver(format, framing, schema, resolver)
    }

    pub fn with_schema_resolver(
        format: Format,
        framing: Option<Framing>,
        schema: ArroyoSchema,
        schema_resolver: Arc<dyn SchemaResolver + Sync>,
    ) -> Self {
        Self {
            format: Arc::new(format),
            framing: framing.map(|f| Arc::new(f)),
            schema,
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
            schema_resolver,
        }
    }

    pub fn batch(&mut self) -> ArrowDeserializerBatch {
        ArrowDeserializerBatch::new(self)
    }
}

impl<'a> ArrowDeserializerBatch<'a> {
    fn new(parent: &'a mut ArrowDeserializer) -> Self {
        let arrays = parent
            .schema
            .schema
            .fields
            .iter()
            .map(|f| make_builder(f.data_type(), 16))
            .collect();

        Self {
            parent,
            arrays,
            errors: vec![],
        }
    }

    pub async fn deserialize_slice(&mut self, msg: &[u8], timestamp: SystemTime) {
        match &*self.parent.format {
            Format::Avro(avro) => {
                // let schema_registry = self.schema_registry.clone();
                // let schema_resolver = self.schema_resolver.clone();
                // match avro::deserialize_slice_avro(avro, schema_registry, schema_resolver, msg)
                //     .await
                // {
                //     Ok(data) => data,
                //     Err(e) => Box::new(
                //         vec![Err(SourceError::other(
                //             "Avro error",
                //             format!("Avro deserialization failed: {}", e),
                //         ))]
                //         .into_iter(),
                //     )
                // }
                todo!("avro")
            }
            _ => {
                FramingIterator::new(self.parent.framing.clone(), msg)
                    .for_each(|t| self.deserialize_single(t, timestamp));
            }
        }
    }

    pub fn finish(self) -> (RecordBatch, Vec<SourceError>) {
        (
            RecordBatch::try_new(
                self.parent.schema.schema.clone(),
                self.arrays.into_iter().map(|mut a| a.finish()).collect(),
            )
            .unwrap(),
            self.errors,
        )
    }

    fn deserialize_single(&mut self, msg: &[u8], timestamp: SystemTime) {
        if let Err(e) = match &*self.parent.format {
            Format::Json(json) => {
                todo!("json")
                //json::deserialize_slice_json(json, msg)
            }
            Format::Avro(_) => unreachable!("avro should be handled by here"),
            Format::Parquet(_) => todo!("parquet is not supported as an input format"),
            Format::RawString(_) => {
                self.deserialize_raw_string(msg, timestamp);
                Ok(())
            }
        }
        .map_err(|e: String| SourceError::bad_data(format!("Failed to deserialize: {:?}", e)))
        {
            self.errors.push(e);
        }

        self.add_timestamp(timestamp);
    }

    fn deserialize_raw_string(&mut self, msg: &[u8], timestamp: SystemTime) {
        let (col, _) = self
            .parent
            .schema
            .schema
            .column_with_name("value")
            .expect("no 'value' column for RawString format");
        self.arrays[col]
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .expect("'value' column has incorrect type")
            .append_value(String::from_utf8_lossy(msg));
    }

    fn add_timestamp(&mut self, timestamp: SystemTime) {
        self.arrays[self.parent.schema.timestamp_index]
            .as_any_mut()
            .downcast_mut::<TimestampNanosecondBuilder>()
            .expect("_timestamp column has incorrect type")
            .append_value(to_nanos(timestamp) as i64);
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
                "three block".to_string(),
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
                "three block".to_string(),
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
                "whole".to_string(),
            ],
            result
        );
    }
}
