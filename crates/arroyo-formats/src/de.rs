use crate::avro::de;
use crate::proto::schema::get_pool;
use crate::{proto, should_flush};
use arrow::array::{Int32Builder, Int64Builder};
use arrow::compute::kernels;
use arrow_array::builder::{
    make_builder, ArrayBuilder, BinaryBuilder, GenericByteBuilder, StringBuilder,
    TimestampNanosecondBuilder, UInt64Builder,
};
use arrow_array::types::GenericBinaryType;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::formats::{
    AvroFormat, BadData, Format, Framing, FramingMethod, JsonFormat, ProtobufFormat,
};
use arroyo_rpc::schema_resolver::{FailingSchemaResolver, FixedSchemaResolver, SchemaResolver};
use arroyo_rpc::{MetadataField, TIMESTAMP_FIELD};
use arroyo_types::{to_nanos, SourceError, LOOKUP_KEY_INDEX_FIELD};
use prost_reflect::DescriptorPool;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub enum FieldValueType<'a> {
    Int64(i64),
    UInt64(u64),
    Int32(i32),
    String(&'a str),
    // Extend with more types as needed
}

struct ContextBuffer {
    buffer: Vec<Box<dyn ArrayBuilder>>,
    created: Instant,
}

impl ContextBuffer {
    fn new(schema: SchemaRef) -> Self {
        let buffer = schema
            .fields
            .iter()
            .map(|f| make_builder(f.data_type(), 16))
            .collect();

        Self {
            buffer,
            created: Instant::now(),
        }
    }

    pub fn size(&self) -> usize {
        self.buffer.iter().map(|b| b.len()).max().unwrap()
    }

    pub fn should_flush(&self) -> bool {
        should_flush(self.size(), self.created)
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        self.buffer.iter_mut().map(|a| a.finish()).collect()
    }
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

enum BufferDecoder {
    Buffer(ContextBuffer),
    JsonDecoder {
        decoder: arrow::json::reader::Decoder,
        buffered_count: usize,
        buffered_since: Instant,
    },
}

impl BufferDecoder {
    fn should_flush(&self) -> bool {
        match self {
            BufferDecoder::Buffer(b) => b.should_flush(),
            BufferDecoder::JsonDecoder {
                buffered_count,
                buffered_since,
                ..
            } => should_flush(*buffered_count, *buffered_since),
        }
    }

    #[allow(clippy::type_complexity)]
    fn flush(
        &mut self,
        bad_data: &BadData,
    ) -> Option<Result<(Vec<ArrayRef>, Option<BooleanArray>), SourceError>> {
        match self {
            BufferDecoder::Buffer(buffer) => {
                if buffer.size() > 0 {
                    Some(Ok((buffer.finish(), None)))
                } else {
                    None
                }
            }
            BufferDecoder::JsonDecoder {
                decoder,
                buffered_since,
                buffered_count,
            } => {
                *buffered_since = Instant::now();
                *buffered_count = 0;
                Some(match bad_data {
                    BadData::Fail { .. } => decoder
                        .flush()
                        .map_err(|e| {
                            SourceError::bad_data(format!("JSON does not match schema: {:?}", e))
                        })
                        .transpose()?
                        .map(|batch| (batch.columns().to_vec(), None)),
                    BadData::Drop { .. } => decoder
                        .flush_with_bad_data()
                        .map_err(|e| {
                            SourceError::bad_data(format!(
                                "Something went wrong decoding JSON: {:?}",
                                e
                            ))
                        })
                        .transpose()?
                        .map(|(batch, mask, _)| (batch.columns().to_vec(), Some(mask))),
                })
            }
        }
    }

    fn decode_json(&mut self, msg: &[u8]) -> Result<(), SourceError> {
        match self {
            BufferDecoder::Buffer(_) => {
                unreachable!("Tried to decode JSON for non-JSON deserializer");
            }
            BufferDecoder::JsonDecoder {
                decoder,
                buffered_count,
                ..
            } => {
                decoder
                    .decode(msg)
                    .map_err(|e| SourceError::bad_data(format!("invalid JSON: {:?}", e)))?;

                *buffered_count += 1;

                Ok(())
            }
        }
    }

    fn get_buffer(&mut self) -> &mut ContextBuffer {
        match self {
            BufferDecoder::Buffer(buffer) => buffer,
            BufferDecoder::JsonDecoder { .. } => {
                panic!("tried to get a raw buffer from a JSON deserializer");
            }
        }
    }

    fn push_null(&mut self, schema: &Schema) {
        match self {
            BufferDecoder::Buffer(b) => {
                for (f, b) in schema.fields.iter().zip(b.buffer.iter_mut()) {
                    match f.data_type() {
                        DataType::Binary => {
                            b.as_any_mut()
                                .downcast_mut::<BinaryBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        DataType::Utf8 => {
                            b.as_any_mut()
                                .downcast_mut::<StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        dt => {
                            unreachable!("unsupported datatype {}", dt);
                        }
                    }
                }
            }
            BufferDecoder::JsonDecoder {
                decoder,
                buffered_count,
                ..
            } => {
                decoder.decode("{}".as_bytes()).unwrap();

                *buffered_count += 1;
            }
        }
    }
}

pub struct ArrowDeserializer {
    format: Arc<Format>,
    framing: Option<Arc<Framing>>,
    final_schema: Arc<Schema>,
    decoder_schema: Arc<Schema>,
    bad_data: BadData,
    schema_registry: Arc<Mutex<HashMap<u32, apache_avro::schema::Schema>>>,
    proto_pool: DescriptorPool,
    schema_resolver: Arc<dyn SchemaResolver + Sync>,
    additional_fields_builder: Option<HashMap<String, Box<dyn ArrayBuilder>>>,
    timestamp_builder: Option<(usize, TimestampNanosecondBuilder)>,
    buffer_decoder: BufferDecoder,
}

impl ArrowDeserializer {
    pub fn new(
        format: Format,
        schema: Arc<ArroyoSchema>,
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

        Self::with_schema_resolver(format, framing, schema, &[], bad_data, resolver)
    }

    pub fn with_schema_resolver(
        format: Format,
        framing: Option<Framing>,
        schema: Arc<ArroyoSchema>,
        metadata_fields: &[MetadataField],
        bad_data: BadData,
        schema_resolver: Arc<dyn SchemaResolver + Sync>,
    ) -> Self {
        Self::with_schema_resolver_and_raw_schema(
            format,
            framing,
            Arc::new(schema.schema_without_timestamp()),
            Some(schema.timestamp_index),
            metadata_fields,
            bad_data,
            schema_resolver,
        )
    }

    pub fn for_lookup(
        format: Format,
        schema: Arc<Schema>,
        metadata_fields: &[MetadataField],
        bad_data: BadData,
        schema_resolver: Arc<dyn SchemaResolver + Sync>,
    ) -> Self {
        let mut metadata_fields = metadata_fields.to_vec();
        metadata_fields.push(MetadataField {
            field_name: LOOKUP_KEY_INDEX_FIELD.to_string(),
            key: LOOKUP_KEY_INDEX_FIELD.to_string(),
            data_type: Some(DataType::UInt64),
        });

        Self::with_schema_resolver_and_raw_schema(
            format,
            None,
            schema,
            None,
            &metadata_fields,
            bad_data,
            schema_resolver,
        )
    }

    fn with_schema_resolver_and_raw_schema(
        format: Format,
        framing: Option<Framing>,
        schema_without_timestamp: Arc<Schema>,
        timestamp_idx: Option<usize>,
        metadata_fields: &[MetadataField],
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

        let metadata_names: HashSet<_> = metadata_fields.iter().map(|f| &f.field_name).collect();

        let schema_without_additional = {
            let fields = schema_without_timestamp
                .fields()
                .iter()
                .filter(|f| !metadata_names.contains(f.name()))
                .cloned()
                .collect::<Vec<_>>();
            Arc::new(Schema::new_with_metadata(
                fields,
                schema_without_timestamp.metadata.clone(),
            ))
        };

        let buffer_decoder = match format {
            Format::Json(..)
            | Format::Avro(AvroFormat {
                into_unstructured_json: false,
                ..
            })
            | Format::Protobuf(ProtobufFormat {
                into_unstructured_json: false,
                ..
            }) => BufferDecoder::JsonDecoder {
                decoder: arrow_json::reader::ReaderBuilder::new(schema_without_additional.clone())
                    .with_limit_to_batch_size(false)
                    .with_strict_mode(false)
                    .with_allow_bad_data(matches!(bad_data, BadData::Drop { .. }))
                    .build_decoder()
                    .unwrap(),
                buffered_count: 0,
                buffered_since: Instant::now(),
            },
            _ => BufferDecoder::Buffer(ContextBuffer::new(schema_without_additional.clone())),
        };

        Self {
            format: Arc::new(format),
            framing: framing.map(Arc::new),
            buffer_decoder,
            timestamp_builder: timestamp_idx
                .map(|i| (i, TimestampNanosecondBuilder::with_capacity(128))),
            final_schema: schema_without_timestamp,
            decoder_schema: schema_without_additional,
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
            bad_data,
            schema_resolver,
            proto_pool,
            additional_fields_builder: None,
        }
    }

    #[must_use]
    pub async fn deserialize_slice(
        &mut self,
        msg: &[u8],
        timestamp: SystemTime,
        additional_fields: Option<&HashMap<&str, FieldValueType<'_>>>,
    ) -> Vec<SourceError> {
        self.deserialize_slice_int(msg, Some(timestamp), additional_fields)
            .await
    }

    #[must_use]
    pub async fn deserialize_without_timestamp(
        &mut self,
        msg: &[u8],
        additional_fields: Option<&HashMap<&str, FieldValueType<'_>>>,
    ) -> Vec<SourceError> {
        self.deserialize_slice_int(msg, None, additional_fields)
            .await
    }

    pub fn deserialize_null(
        &mut self,
        additional_fields: Option<&HashMap<&str, FieldValueType<'_>>>,
    ) {
        self.buffer_decoder.push_null(&self.decoder_schema);
        self.add_additional_fields(additional_fields, 1);
    }

    async fn deserialize_slice_int(
        &mut self,
        msg: &[u8],
        timestamp: Option<SystemTime>,
        additional_fields: Option<&HashMap<&str, FieldValueType<'_>>>,
    ) -> Vec<SourceError> {
        let (count, errors) = match &*self.format {
            Format::Avro(_) => self.deserialize_slice_avro(msg).await,
            _ => {
                let mut count = 0;
                let errors = FramingIterator::new(self.framing.clone(), msg)
                    .map(|t| self.deserialize_single(t))
                    .filter_map(|t| {
                        if t.is_ok() {
                            count += 1;
                        }
                        t.err()
                    })
                    .collect();
                (count, errors)
            }
        };

        self.add_additional_fields(additional_fields, count);

        if let Some(timestamp) = timestamp {
            let (_, b) = self
                .timestamp_builder
                .as_mut()
                .expect("tried to serialize timestamp to a schema without a timestamp column");

            for _ in 0..count {
                b.append_value(to_nanos(timestamp) as i64);
            }
        }

        errors
    }

    fn add_additional_fields(
        &mut self,
        additional_fields: Option<&HashMap<&str, FieldValueType<'_>>>,
        count: usize,
    ) {
        if let Some(additional_fields) = additional_fields {
            if self.additional_fields_builder.is_none() {
                let mut builders = HashMap::new();
                for (key, value) in additional_fields.iter() {
                    let builder: Box<dyn ArrayBuilder> = match value {
                        FieldValueType::Int32(_) => Box::new(Int32Builder::new()),
                        FieldValueType::Int64(_) => Box::new(Int64Builder::new()),
                        FieldValueType::UInt64(_) => Box::new(UInt64Builder::new()),
                        FieldValueType::String(_) => Box::new(StringBuilder::new()),
                    };
                    builders.insert(key.to_string(), builder);
                }
                self.additional_fields_builder = Some(builders);
            }

            let builders = self.additional_fields_builder.as_mut().unwrap();

            for (k, v) in additional_fields {
                add_additional_fields(builders, k, v, count);
            }
        }
    }

    pub fn should_flush(&self) -> bool {
        self.buffer_decoder.should_flush()
    }

    pub fn flush_buffer(&mut self) -> Option<Result<RecordBatch, SourceError>> {
        let (arrays, error_mask) = match self.buffer_decoder.flush(&self.bad_data)? {
            Ok((a, b)) => (a, b),
            Err(e) => return Some(Err(e)),
        };

        let mut arrays: HashMap<_, _> = arrays
            .into_iter()
            .zip(self.decoder_schema.fields.iter())
            .map(|(a, f)| (f.name().as_str(), a))
            .collect();

        if let Some(additional_fields) = &mut self.additional_fields_builder {
            for (name, builder) in additional_fields {
                let mut array = builder.finish();
                if let Some(error_mask) = &error_mask {
                    array = kernels::filter::filter(&array, error_mask).unwrap();
                }

                arrays.insert(name.as_str(), array);
            }
        };

        if let Some((_, timestamp)) = &mut self.timestamp_builder {
            let array = if let Some(error_mask) = &error_mask {
                kernels::filter::filter(&timestamp.finish(), error_mask).unwrap()
            } else {
                Arc::new(timestamp.finish())
            };

            arrays.insert(TIMESTAMP_FIELD, array);
        }

        let arrays = self
            .final_schema
            .fields
            .iter()
            .map(|f| arrays.get(f.name().as_str()).unwrap().clone())
            .collect();

        Some(Ok(
            RecordBatch::try_new(self.final_schema.clone(), arrays).unwrap()
        ))
    }

    fn deserialize_single(&mut self, msg: &[u8]) -> Result<(), SourceError> {
        match &*self.format {
            Format::RawString(_)
            | Format::Json(JsonFormat {
                unstructured: true, ..
            }) => {
                self.deserialize_raw_string(msg);
            }
            Format::RawBytes(_) => {
                self.deserialize_raw_bytes(msg);
            }
            Format::Json(json) => {
                let msg = if json.confluent_schema_registry {
                    &msg[5..]
                } else {
                    msg
                };

                self.buffer_decoder.decode_json(msg)?;
            }
            Format::Protobuf(proto) => {
                let json = proto::de::deserialize_proto(&mut self.proto_pool, proto, msg)?;

                if proto.into_unstructured_json {
                    self.decode_into_json(json);
                } else {
                    self.buffer_decoder
                        .decode_json(json.to_string().as_bytes())
                        .map_err(|e| SourceError::bad_data(format!("invalid JSON: {:?}", e)))?;
                }
            }
            Format::Avro(_) => unreachable!("this should not be called for avro"),
            Format::Parquet(_) => todo!("parquet is not supported as an input format"),
        }

        Ok(())
    }

    fn decode_into_json(&mut self, value: Value) {
        let (idx, _) = self
            .decoder_schema
            .column_with_name("value")
            .expect("no 'value' column for unstructured avro");
        let array = self.buffer_decoder.get_buffer().buffer[idx]
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .expect("'value' column has incorrect type");

        array.append_value(value.to_string());
    }

    async fn deserialize_slice_avro(&mut self, msg: &[u8]) -> (usize, Vec<SourceError>) {
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
                return (0, vec![e]);
            }
        };

        let into_json = format.into_unstructured_json;

        let mut count = 0;
        let errors = messages
            .into_iter()
            .map(|record| {
                let value = record.map_err(|e| {
                    SourceError::bad_data(format!("failed to deserialize from avro: {:?}", e))
                })?;

                if into_json {
                    self.decode_into_json(de::avro_to_json(value));
                } else {
                    // for now round-trip through json in order to handle unsupported avro features
                    // as that allows us to rely on raw json deserialization
                    let json = de::avro_to_json(value).to_string();

                    self.buffer_decoder
                        .decode_json(json.as_bytes())
                        .map_err(|e| SourceError::bad_data(format!("invalid JSON: {:?}", e)))?;
                }

                count += 1;

                Ok(())
            })
            .filter_map(|r: Result<(), SourceError>| r.err())
            .collect();

        (count, errors)
    }

    fn deserialize_raw_string(&mut self, msg: &[u8]) {
        let (col, _) = self
            .decoder_schema
            .column_with_name("value")
            .expect("no 'value' column for RawString format");
        self.buffer_decoder.get_buffer().buffer[col]
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .expect("'value' column has incorrect type")
            .append_value(String::from_utf8_lossy(msg));
    }

    fn deserialize_raw_bytes(&mut self, msg: &[u8]) {
        let (col, _) = self
            .decoder_schema
            .column_with_name("value")
            .expect("no 'value' column for RawBytes format");
        self.buffer_decoder.get_buffer().buffer[col]
            .as_any_mut()
            .downcast_mut::<GenericByteBuilder<GenericBinaryType<i32>>>()
            .expect("'value' column has incorrect type")
            .append_value(msg);
    }

    pub fn bad_data(&self) -> &BadData {
        &self.bad_data
    }
}

fn add_additional_fields(
    builders: &mut HashMap<String, Box<dyn ArrayBuilder>>,
    key: &str,
    value: &FieldValueType<'_>,
    count: usize,
) {
    let builder = builders
        .get_mut(key)
        .unwrap_or_else(|| panic!("unexpected additional field '{}'", key))
        .as_any_mut();
    match value {
        FieldValueType::Int32(i) => {
            let b = builder
                .downcast_mut::<Int32Builder>()
                .expect("additional field has incorrect type");

            for _ in 0..count {
                b.append_value(*i);
            }
        }
        FieldValueType::Int64(i) => {
            let b = builder
                .downcast_mut::<Int64Builder>()
                .expect("additional field has incorrect type");

            for _ in 0..count {
                b.append_value(*i);
            }
        }
        FieldValueType::UInt64(i) => {
            let b = builder
                .downcast_mut::<UInt64Builder>()
                .expect("additional field has incorrect type");

            for _ in 0..count {
                b.append_value(*i);
            }
        }
        FieldValueType::String(s) => {
            let b = builder
                .downcast_mut::<StringBuilder>()
                .expect("additional field has incorrect type");

            for _ in 0..count {
                b.append_value(*s);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::de::{ArrowDeserializer, FieldValueType, FramingIterator};
    use arrow::datatypes::Int32Type;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{GenericBinaryType, Int64Type, TimestampNanosecondType};
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

    fn setup_deserializer(bad_data: BadData) -> ArrowDeserializer {
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("x", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new(
                "_timestamp",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let schema = Arc::new(ArroyoSchema::from_schema_unkeyed(schema).unwrap());

        ArrowDeserializer::new(
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
        )
    }

    #[tokio::test]
    async fn test_bad_data_drop() {
        let mut deserializer = setup_deserializer(BadData::Drop {});

        let now = SystemTime::now();

        assert_eq!(
            deserializer
                .deserialize_slice(json!({ "x": 5 }).to_string().as_bytes(), now, None,)
                .await,
            vec![]
        );
        assert_eq!(
            deserializer
                .deserialize_slice(json!({ "x": "hello" }).to_string().as_bytes(), now, None,)
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
        let mut deserializer = setup_deserializer(BadData::Fail {});

        assert_eq!(
            deserializer
                .deserialize_slice(
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

        let arroyo_schema = Arc::new(ArroyoSchema::from_schema_unkeyed(schema.clone()).unwrap());

        let mut deserializer = ArrowDeserializer::new(
            Format::RawBytes(RawBytesFormat {}),
            arroyo_schema,
            None,
            BadData::Fail {},
        );

        let time = SystemTime::now();
        let result = deserializer
            .deserialize_slice(&[0, 1, 2, 3, 4, 5], time, None)
            .await;
        assert!(result.is_empty());

        let batch = deserializer.flush_buffer().unwrap().unwrap();

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

        let arroyo_schema = Arc::new(ArroyoSchema::from_schema_unkeyed(schema.clone()).unwrap());

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
        additional_fields.insert(binding.as_str(), FieldValueType::Int32(5));
        let z_value = "hello".to_string();
        let binding = "z".to_string();
        additional_fields.insert(binding.as_str(), FieldValueType::String(&z_value));

        let result = deserializer
            .deserialize_slice(
                json!({ "x": 5 }).to_string().as_bytes(),
                time,
                Some(&additional_fields),
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
