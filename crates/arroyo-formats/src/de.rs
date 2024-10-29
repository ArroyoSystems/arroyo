use crate::avro::de;
use crate::proto::schema::get_pool;
use crate::{proto, should_flush};
use arrow::array::{Int32Builder, Int64Builder};
use arrow::compute::kernels;
use arrow_array::builder::{
    ArrayBuilder, GenericByteBuilder, StringBuilder, TimestampNanosecondBuilder,
};
use arrow_array::types::GenericBinaryType;
use arrow_array::RecordBatch;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::formats::{
    AvroFormat, BadData, Format, Framing, FramingMethod, JsonFormat, ProtobufFormat,
};
use arroyo_rpc::schema_resolver::{FailingSchemaResolver, FixedSchemaResolver, SchemaResolver};
use arroyo_types::{to_nanos, SourceError};
use prost_reflect::DescriptorPool;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub enum FieldValueType<'a> {
    Int64(i64),
    Int32(i32),
    String(&'a String),
    // Extend with more types as needed
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
                        let end = memchr::memchr(b'\n', &self.buf[self.offset..])
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

pub struct ArrowDeserializer {
    format: Arc<Format>,
    framing: Option<Arc<Framing>>,
    schema: ArroyoSchema,
    bad_data: BadData,
    json_decoder: Option<(arrow::json::reader::Decoder, TimestampNanosecondBuilder)>,
    buffered_count: usize,
    buffered_since: Instant,
    schema_registry: Arc<Mutex<HashMap<u32, apache_avro::schema::Schema>>>,
    proto_pool: DescriptorPool,
    schema_resolver: Arc<dyn SchemaResolver + Sync>,
    additional_fields_builder: Option<HashMap<String, Box<dyn ArrayBuilder>>>,
}

impl ArrowDeserializer {
    pub fn new(
        format: Format,
        schema: ArroyoSchema,
        framing: Option<Framing>,
        bad_data: BadData,
    ) -> Self {
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

        Self::with_schema_resolver(format, framing, schema, bad_data, resolver)
    }

    pub fn with_schema_resolver(
        format: Format,
        framing: Option<Framing>,
        schema: ArroyoSchema,
        bad_data: BadData,
        schema_resolver: Arc<dyn SchemaResolver + Sync>,
    ) -> Self {
        let proto_pool = if let Format::Protobuf(ProtobufFormat {
            compiled_schema: Some(schema),
            ..
        }) = &format
        {
            get_pool(schema).expect("unable to handle protobuf schema")
        } else {
            DescriptorPool::global()
        };

        Self {
            json_decoder: matches!(
                format,
                Format::Json(..)
                    | Format::Avro(AvroFormat {
                        into_unstructured_json: false,
                        ..
                    })
                    | Format::Protobuf(ProtobufFormat {
                        into_unstructured_json: false,
                        ..
                    })
            )
            .then(|| {
                // exclude the timestamp field
                (
                    arrow_json::reader::ReaderBuilder::new(Arc::new(
                        schema.schema_without_timestamp(),
                    ))
                    .with_limit_to_batch_size(false)
                    .with_strict_mode(false)
                    .with_allow_bad_data(matches!(bad_data, BadData::Drop { .. }))
                    .build_decoder()
                    .unwrap(),
                    TimestampNanosecondBuilder::new(),
                )
            }),
            format: Arc::new(format),
            framing: framing.map(Arc::new),
            schema,
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
            bad_data,
            schema_resolver,
            proto_pool,
            buffered_count: 0,
            buffered_since: Instant::now(),
            additional_fields_builder: None,
        }
    }

    pub async fn deserialize_slice(
        &mut self,
        buffer: &mut [Box<dyn ArrayBuilder>],
        msg: &[u8],
        timestamp: SystemTime,
        additional_fields: Option<HashMap<&String, FieldValueType<'_>>>,
    ) -> Vec<SourceError> {
        match &*self.format {
            Format::Avro(_) => self.deserialize_slice_avro(buffer, msg, timestamp).await,
            _ => FramingIterator::new(self.framing.clone(), msg)
                .map(|t| self.deserialize_single(buffer, t, timestamp, additional_fields.clone()))
                .filter_map(|t| t.err())
                .collect(),
        }
    }

    pub fn should_flush(&self) -> bool {
        should_flush(self.buffered_count, self.buffered_since)
    }

    pub fn flush_buffer(&mut self) -> Option<Result<RecordBatch, SourceError>> {
        let (decoder, timestamp) = self.json_decoder.as_mut()?;
        self.buffered_since = Instant::now();
        self.buffered_count = 0;
        match self.bad_data {
            BadData::Fail { .. } => Some(
                decoder
                    .flush()
                    .map_err(|e| {
                        SourceError::bad_data(format!("JSON does not match schema: {:?}", e))
                    })
                    .transpose()?
                    .map(|batch| {
                        let mut columns = batch.columns().to_vec();
                        columns.insert(self.schema.timestamp_index, Arc::new(timestamp.finish()));
                        flush_additional_fields_builders(
                            &mut self.additional_fields_builder,
                            &self.schema,
                            &mut columns,
                        );
                        RecordBatch::try_new(self.schema.schema.clone(), columns).unwrap()
                    }),
            ),
            BadData::Drop { .. } => Some(
                decoder
                    .flush_with_bad_data()
                    .map_err(|e| {
                        SourceError::bad_data(format!(
                            "Something went wrong decoding JSON: {:?}",
                            e
                        ))
                    })
                    .transpose()?
                    .map(|(batch, mask, _)| {
                        let mut columns = batch.columns().to_vec();
                        let timestamp =
                            kernels::filter::filter(&timestamp.finish(), &mask).unwrap();

                        columns.insert(self.schema.timestamp_index, Arc::new(timestamp));
                        flush_additional_fields_builders(
                            &mut self.additional_fields_builder,
                            &self.schema,
                            &mut columns,
                        );
                        RecordBatch::try_new(self.schema.schema.clone(), columns).unwrap()
                    }),
            ),
        }
    }

    fn deserialize_single(
        &mut self,
        buffer: &mut [Box<dyn ArrayBuilder>],
        msg: &[u8],
        timestamp: SystemTime,
        additional_fields: Option<HashMap<&String, FieldValueType>>,
    ) -> Result<(), SourceError> {
        match &*self.format {
            Format::RawString(_)
            | Format::Json(JsonFormat {
                unstructured: true, ..
            }) => {
                self.deserialize_raw_string(buffer, msg);
                add_timestamp(buffer, self.schema.timestamp_index, timestamp);
                if let Some(fields) = additional_fields {
                    for (k, v) in fields.iter() {
                        add_additional_fields(buffer, &self.schema, k, v);
                    }
                }
            }
            Format::RawBytes(_) => {
                self.deserialize_raw_bytes(buffer, msg);
                add_timestamp(buffer, self.schema.timestamp_index, timestamp);
                if let Some(fields) = additional_fields {
                    for (k, v) in fields.iter() {
                        add_additional_fields(buffer, &self.schema, k, v);
                    }
                }
            }
            Format::Json(json) => {
                let msg = if json.confluent_schema_registry {
                    &msg[5..]
                } else {
                    msg
                };

                let Some((decoder, timestamp_builder)) = &mut self.json_decoder else {
                    panic!("json decoder not initialized");
                };

                if self.additional_fields_builder.is_none() {
                    if let Some(fields) = additional_fields.as_ref() {
                        let mut builders = HashMap::new();
                        for (key, value) in fields.iter() {
                            let builder: Box<dyn ArrayBuilder> = match value {
                                FieldValueType::Int32(_) => Box::new(Int32Builder::new()),
                                FieldValueType::Int64(_) => Box::new(Int64Builder::new()),
                                FieldValueType::String(_) => Box::new(StringBuilder::new()),
                            };
                            builders.insert(key, builder);
                        }
                        self.additional_fields_builder = Some(
                            builders
                                .into_iter()
                                .map(|(k, v)| ((*k).clone(), v))
                                .collect(),
                        );
                    }
                }

                decoder
                    .decode(msg)
                    .map_err(|e| SourceError::bad_data(format!("invalid JSON: {:?}", e)))?;
                timestamp_builder.append_value(to_nanos(timestamp) as i64);

                add_additional_fields_using_builder(
                    additional_fields,
                    &mut self.additional_fields_builder,
                );
                self.buffered_count += 1;
            }
            Format::Protobuf(proto) => {
                let json = proto::de::deserialize_proto(&mut self.proto_pool, proto, msg)?;

                if proto.into_unstructured_json {
                    self.decode_into_json(buffer, json, timestamp);
                } else {
                    let Some((decoder, timestamp_builder)) = &mut self.json_decoder else {
                        panic!("json decoder not initialized");
                    };

                    decoder
                        .decode(json.to_string().as_bytes())
                        .map_err(|e| SourceError::bad_data(format!("invalid JSON: {:?}", e)))?;
                    timestamp_builder.append_value(to_nanos(timestamp) as i64);

                    add_additional_fields_using_builder(
                        additional_fields,
                        &mut self.additional_fields_builder,
                    );

                    self.buffered_count += 1;
                }
            }
            Format::Avro(_) => unreachable!("this should not be called for avro"),
            Format::Parquet(_) => todo!("parquet is not supported as an input format"),
        }

        Ok(())
    }

    fn decode_into_json(
        &mut self,
        builders: &mut [Box<dyn ArrayBuilder>],
        value: Value,
        timestamp: SystemTime,
    ) {
        let (idx, _) = self
            .schema
            .schema
            .column_with_name("value")
            .expect("no 'value' column for unstructured avro");
        let array = builders[idx]
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .expect("'value' column has incorrect type");

        array.append_value(value.to_string());
        add_timestamp(builders, self.schema.timestamp_index, timestamp);
        self.buffered_count += 1;
    }

    pub async fn deserialize_slice_avro<'a>(
        &mut self,
        builders: &mut [Box<dyn ArrayBuilder>],
        msg: &'a [u8],
        timestamp: SystemTime,
    ) -> Vec<SourceError> {
        let Format::Avro(format) = &*self.format else {
            unreachable!("not avro");
        };

        let messages = match de::avro_messages(
            format,
            &self.schema_registry,
            &self.schema_resolver,
            msg,
        )
        .await
        {
            Ok(messages) => messages,
            Err(e) => {
                return vec![e];
            }
        };

        let into_json = format.into_unstructured_json;

        messages
            .into_iter()
            .map(|record| {
                let value = record.map_err(|e| {
                    SourceError::bad_data(format!("failed to deserialize from avro: {:?}", e))
                })?;

                if into_json {
                    self.decode_into_json(builders, de::avro_to_json(value), timestamp);
                } else {
                    // for now round-trip through json in order to handle unsupported avro features
                    // as that allows us to rely on raw json deserialization
                    let json = de::avro_to_json(value).to_string();

                    let Some((decoder, timestamp_builder)) = &mut self.json_decoder else {
                        panic!("json decoder not initialized");
                    };

                    decoder
                        .decode(json.as_bytes())
                        .map_err(|e| SourceError::bad_data(format!("invalid JSON: {:?}", e)))?;
                    self.buffered_count += 1;
                    timestamp_builder.append_value(to_nanos(timestamp) as i64);
                }

                Ok(())
            })
            .filter_map(|r: Result<(), SourceError>| r.err())
            .collect()
    }

    fn deserialize_raw_string(&mut self, buffer: &mut [Box<dyn ArrayBuilder>], msg: &[u8]) {
        let (col, _) = self
            .schema
            .schema
            .column_with_name("value")
            .expect("no 'value' column for RawString format");
        buffer[col]
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .expect("'value' column has incorrect type")
            .append_value(String::from_utf8_lossy(msg));
    }

    fn deserialize_raw_bytes(&mut self, buffer: &mut [Box<dyn ArrayBuilder>], msg: &[u8]) {
        let (col, _) = self
            .schema
            .schema
            .column_with_name("value")
            .expect("no 'value' column for RawBytes format");
        buffer[col]
            .as_any_mut()
            .downcast_mut::<GenericByteBuilder<GenericBinaryType<i32>>>()
            .expect("'value' column has incorrect type")
            .append_value(msg);
    }

    pub fn bad_data(&self) -> &BadData {
        &self.bad_data
    }
}

pub(crate) fn add_timestamp(
    builder: &mut [Box<dyn ArrayBuilder>],
    idx: usize,
    timestamp: SystemTime,
) {
    builder[idx]
        .as_any_mut()
        .downcast_mut::<TimestampNanosecondBuilder>()
        .expect("_timestamp column has incorrect type")
        .append_value(to_nanos(timestamp) as i64);
}

pub(crate) fn add_additional_fields(
    builder: &mut [Box<dyn ArrayBuilder>],
    schema: &ArroyoSchema,
    key: &str,
    value: &FieldValueType<'_>,
) {
    let (idx, _) = schema
        .schema
        .column_with_name(key)
        .unwrap_or_else(|| panic!("no '{}' column for additional fields", key));
    match value {
        FieldValueType::Int32(i) => {
            builder[idx]
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .expect("additional field has incorrect type")
                .append_value(*i);
        }
        FieldValueType::Int64(i) => {
            builder[idx]
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .expect("additional field has incorrect type")
                .append_value(*i);
        }
        FieldValueType::String(s) => {
            builder[idx]
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("additional field has incorrect type")
                .append_value(s);
        }
    }
}

pub(crate) fn add_additional_fields_using_builder(
    additional_fields: Option<HashMap<&String, FieldValueType<'_>>>,
    additional_fields_builder: &mut Option<HashMap<String, Box<dyn ArrayBuilder>>>,
) {
    if let Some(fields) = additional_fields {
        for (k, v) in fields.iter() {
            if let Some(builder) = additional_fields_builder
                .as_mut()
                .and_then(|b| b.get_mut(*k))
            {
                match v {
                    FieldValueType::Int32(i) => {
                        builder
                            .as_any_mut()
                            .downcast_mut::<Int32Builder>()
                            .expect("additional field has incorrect type")
                            .append_value(*i);
                    }
                    FieldValueType::Int64(i) => {
                        builder
                            .as_any_mut()
                            .downcast_mut::<Int64Builder>()
                            .expect("additional field has incorrect type")
                            .append_value(*i);
                    }
                    FieldValueType::String(s) => {
                        builder
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .expect("additional field has incorrect type")
                            .append_value(s);
                    }
                }
            }
        }
    }
}

pub(crate) fn flush_additional_fields_builders(
    additional_fields_builder: &mut Option<HashMap<String, Box<dyn ArrayBuilder>>>,
    schema: &ArroyoSchema,
    columns: &mut [Arc<dyn arrow::array::Array>],
) {
    if let Some(additional_fields) = additional_fields_builder.take() {
        for (field_name, mut builder) in additional_fields {
            if let Some((idx, _)) = schema.schema.column_with_name(&field_name) {
                let expected_type = schema.schema.fields[idx].data_type();
                let built_column = builder.as_mut().finish();
                let actual_type = built_column.data_type();
                if expected_type != actual_type {
                    panic!(
                        "Type mismatch for column '{}': expected {:?}, got {:?}",
                        field_name, expected_type, actual_type
                    );
                }
                columns[idx] = Arc::new(built_column);
            } else {
                panic!("Field '{}' not found in schema", field_name);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::de::{ArrowDeserializer, FieldValueType, FramingIterator};
    use arrow::datatypes::Int32Type;
    use arrow_array::builder::{make_builder, ArrayBuilder};
    use arrow_array::cast::AsArray;
    use arrow_array::types::{GenericBinaryType, Int64Type, TimestampNanosecondType};
    use arrow_array::RecordBatch;
    use arrow_schema::{Schema, TimeUnit};
    use arroyo_rpc::df::ArroyoSchema;
    use arroyo_rpc::formats::{
        BadData, Format, Framing, FramingMethod, JsonFormat, NewlineDelimitedFraming,
        RawBytesFormat,
    };
    use arroyo_types::{to_nanos, SourceError};
    use serde_json::json;
    use std::sync::Arc;
    use std::time::SystemTime;

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

    fn setup_deserializer(bad_data: BadData) -> (Vec<Box<dyn ArrayBuilder>>, ArrowDeserializer) {
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("x", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new(
                "_timestamp",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let arrays: Vec<_> = schema
            .fields
            .iter()
            .map(|f| make_builder(f.data_type(), 16))
            .collect();

        let schema = ArroyoSchema::from_schema_unkeyed(schema).unwrap();

        let deserializer = ArrowDeserializer::new(
            Format::Json(JsonFormat {
                confluent_schema_registry: false,
                schema_id: None,
                include_schema: false,
                debezium: false,
                unstructured: false,
                timestamp_format: Default::default(),
            }),
            schema,
            None,
            bad_data,
        );

        (arrays, deserializer)
    }

    #[tokio::test]
    async fn test_bad_data_drop() {
        let (mut arrays, mut deserializer) = setup_deserializer(BadData::Drop {});

        let now = SystemTime::now();

        assert_eq!(
            deserializer
                .deserialize_slice(
                    &mut arrays[..],
                    json!({ "x": 5 }).to_string().as_bytes(),
                    now,
                    None,
                )
                .await,
            vec![]
        );
        assert_eq!(
            deserializer
                .deserialize_slice(
                    &mut arrays[..],
                    json!({ "x": "hello" }).to_string().as_bytes(),
                    now,
                    None,
                )
                .await,
            vec![]
        );

        let batch = deserializer.flush_buffer().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.columns()[0].as_primitive::<Int64Type>().value(0), 5);
        assert_eq!(
            batch.columns()[1]
                .as_primitive::<TimestampNanosecondType>()
                .value(0),
            to_nanos(now) as i64
        );
    }

    #[tokio::test]
    async fn test_bad_data_fail() {
        let (mut arrays, mut deserializer) = setup_deserializer(BadData::Fail {});

        assert_eq!(
            deserializer
                .deserialize_slice(
                    &mut arrays[..],
                    json!({ "x": 5 }).to_string().as_bytes(),
                    SystemTime::now(),
                    None,
                )
                .await,
            vec![]
        );
        assert_eq!(
            deserializer
                .deserialize_slice(
                    &mut arrays[..],
                    json!({ "x": "hello" }).to_string().as_bytes(),
                    SystemTime::now(),
                    None,
                )
                .await,
            vec![]
        );

        let err = deserializer.flush_buffer().unwrap().unwrap_err();

        assert!(matches!(err, SourceError::BadData { .. }));
    }

    #[tokio::test]
    async fn test_raw_bytes() {
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("value", arrow_schema::DataType::Binary, false),
            arrow_schema::Field::new(
                "_timestamp",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let mut arrays: Vec<_> = schema
            .fields
            .iter()
            .map(|f| make_builder(f.data_type(), 16))
            .collect();

        let arroyo_schema = ArroyoSchema::from_schema_unkeyed(schema.clone()).unwrap();

        let mut deserializer = ArrowDeserializer::new(
            Format::RawBytes(RawBytesFormat {}),
            arroyo_schema,
            None,
            BadData::Fail {},
        );

        let time = SystemTime::now();
        let result = deserializer
            .deserialize_slice(&mut arrays, &[0, 1, 2, 3, 4, 5], time, None)
            .await;
        assert!(result.is_empty());

        let arrays: Vec<_> = arrays.into_iter().map(|mut a| a.finish()).collect();
        let batch = RecordBatch::try_new(schema, arrays).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.columns()[0]
                .as_bytes::<GenericBinaryType<i32>>()
                .value(0),
            &[0, 1, 2, 3, 4, 5]
        );
        assert_eq!(
            batch.columns()[1]
                .as_primitive::<TimestampNanosecondType>()
                .value(0),
            to_nanos(time) as i64
        );
    }

    #[tokio::test]
    async fn test_additional_fields_deserialisation() {
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("x", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new("y", arrow_schema::DataType::Int32, true),
            arrow_schema::Field::new("z", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new(
                "_timestamp",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let mut arrays: Vec<_> = schema
            .fields
            .iter()
            .map(|f| make_builder(f.data_type(), 16))
            .collect();

        let arroyo_schema = ArroyoSchema::from_schema_unkeyed(schema.clone()).unwrap();

        let mut deserializer = ArrowDeserializer::new(
            Format::Json(JsonFormat {
                confluent_schema_registry: false,
                schema_id: None,
                include_schema: false,
                debezium: false,
                unstructured: false,
                timestamp_format: Default::default(),
            }),
            arroyo_schema,
            None,
            BadData::Drop {},
        );

        let time = SystemTime::now();
        let mut additional_fields = std::collections::HashMap::new();
        let binding = "y".to_string();
        additional_fields.insert(&binding, FieldValueType::Int32(5));
        let z_value = "hello".to_string();
        let binding = "z".to_string();
        additional_fields.insert(&binding, FieldValueType::String(&z_value));

        let result = deserializer
            .deserialize_slice(
                &mut arrays,
                json!({ "x": 5 }).to_string().as_bytes(),
                time,
                Some(additional_fields),
            )
            .await;
        assert!(result.is_empty());

        let batch = deserializer.flush_buffer().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.columns()[0].as_primitive::<Int64Type>().value(0), 5);
        assert_eq!(batch.columns()[1].as_primitive::<Int32Type>().value(0), 5);
        assert_eq!(batch.columns()[2].as_string::<i32>().value(0), "hello");
        assert_eq!(
            batch.columns()[3]
                .as_primitive::<TimestampNanosecondType>()
                .value(0),
            to_nanos(time) as i64
        );
    }
}
