use crate::avro::de;
use crate::proto::schema::get_pool;
use crate::{proto, should_flush};
use arrow::array::{Int32Builder, Int64Builder};
use arrow::compute::kernels;
use arrow::json::reader::{FailureKind, JsonType, ValidationError};
use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, GenericByteBuilder, StringBuilder, TimestampNanosecondBuilder,
    UInt64Builder, make_builder,
};
use arrow_array::types::GenericBinaryType;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::{DataflowError, DataflowResult, SourceError};
use arroyo_rpc::formats::{AvroFormat, BadData, Format, Framing, JsonFormat, ProtobufFormat};
use arroyo_rpc::log_event;
use arroyo_rpc::schema_resolver::{FailingSchemaResolver, FixedSchemaResolver, SchemaResolver};
use arroyo_rpc::{MetadataField, TIMESTAMP_FIELD};
use arroyo_types::{LOOKUP_KEY_INDEX_FIELD, to_nanos};
use prost_reflect::DescriptorPool;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::Mutex;

#[derive(Debug, Copy, Clone)]
pub enum FieldValueType<'a> {
    Int64(Option<i64>),
    UInt64(Option<u64>),
    Int32(Option<i32>),
    String(Option<&'a str>),
    Bytes(Option<&'a [u8]>),
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
                match &**framing {
                    Framing::Newline(newline) => {
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

fn failure_kind_to_str(kind: FailureKind) -> &'static str {
    match kind {
        FailureKind::MissingField => "missing_field",
        FailureKind::NullValue => "null_value",
        FailureKind::TypeMismatch => "type_mismatch",
        FailureKind::ParseFailure => "parse_failure",
    }
}

/// Format a validation error without exposing raw data values.
fn format_validation_error(verr: &ValidationError) -> String {
    let field = &verr.field_path;
    let expected = &verr.expected_type;

    match verr.failure_kind {
        FailureKind::MissingField => {
            format!("field '{field}': required field is missing (expected {expected})")
        }
        FailureKind::NullValue => {
            format!("field '{field}': null value for non-nullable field (expected {expected})")
        }
        FailureKind::TypeMismatch | FailureKind::ParseFailure => {
            let type_info = match (verr.actual_type, &verr.actual_value) {
                (Some(actual_type), Some(val)) => match actual_type {
                    JsonType::String => {
                        let inner_len = val.len().saturating_sub(2);
                        format!("got {actual_type} (length: {inner_len})")
                    }
                    JsonType::Number => {
                        format!("got {actual_type} (length: {})", val.len())
                    }
                    _ => format!("got {actual_type}"),
                },
                (Some(actual_type), None) => format!("got {actual_type}"),
                _ => "got incompatible type".to_string(),
            };

            let suffix = if matches!(verr.failure_kind, FailureKind::ParseFailure) {
                " - parse failure"
            } else {
                ""
            };

            format!("field '{field}': expected {expected}, {type_info}{suffix}")
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
    ) -> Option<Result<(Vec<ArrayRef>, Option<BooleanArray>), DataflowError>> {
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
                            SourceError::bad_data(format!("JSON does not match schema: {e:?}"))
                        })
                        .transpose()?
                        .map(|batch| (batch.columns().to_vec(), None)),
                    BadData::Drop { .. } => decoder
                        .flush_with_bad_data()
                        .map_err(|e| {
                            SourceError::bad_data(format!(
                                "Something went wrong decoding JSON: {e:?}"
                            ))
                        })
                        .map(|opt| {
                            opt.map(|(batch, mask, _invalid_records, validation_errors)| {
                                // Report validation errors
                                for verr in validation_errors {
                                    log_event!(
                                        "user_error",
                                        {
                                            "error_family": "deserialization",
                                            "error_type": failure_kind_to_str(verr.failure_kind),
                                            "details": format_validation_error(&verr),
                                        }
                                    );
                                }
                                (batch.columns().to_vec(), Some(mask))
                            })
                        })
                        .transpose()?,
                })
            }
        }
    }

    fn decode_json(&mut self, msg: &[u8]) -> DataflowResult<()> {
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
                    .map_err(|e| SourceError::bad_data(format!("invalid JSON: {e:?}")))?;

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
        metadata_fields: &[MetadataField],
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

        Self::with_schema_resolver(format, framing, schema, metadata_fields, bad_data, resolver)
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
            schema.schema.clone(),
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
        schema: Arc<Schema>,
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
            let fields = schema
                .fields()
                .iter()
                .filter(|f| !metadata_names.contains(f.name()) && f.name() != TIMESTAMP_FIELD)
                .cloned()
                .collect::<Vec<_>>();
            Arc::new(Schema::new_with_metadata(fields, schema.metadata.clone()))
        };

        let buffer_decoder = match format {
            Format::Json(JsonFormat {
                unstructured: false,
                ..
            })
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
            final_schema: schema,
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
    ) -> Vec<DataflowError> {
        self.deserialize_slice_int(msg, Some(timestamp), additional_fields)
            .await
    }

    #[must_use]
    pub async fn deserialize_without_timestamp(
        &mut self,
        msg: &[u8],
        additional_fields: Option<&HashMap<&str, FieldValueType<'_>>>,
    ) -> Vec<DataflowError> {
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
    ) -> Vec<DataflowError> {
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
                        FieldValueType::Bytes(_) => Box::new(BinaryBuilder::new()),
                    };
                    builders.insert(key.to_string(), builder);
                }
                self.additional_fields_builder = Some(builders);
            }

            let builders = self.additional_fields_builder.as_mut().unwrap();

            for (k, v) in additional_fields {
                add_additional_fields(builders, k, *v, count);
            }
        }
    }

    pub fn should_flush(&self) -> bool {
        self.buffer_decoder.should_flush()
    }

    pub fn flush_buffer(&mut self) -> (Option<RecordBatch>, Vec<DataflowError>) {
        let (arrays, error_mask) = match self.buffer_decoder.flush(&self.bad_data) {
            Some(Ok((a, b))) => (a, b),
            Some(Err(e)) => return (None, vec![e]),
            None => return (None, vec![]),
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

        let mut source_errors = vec![];

        if let Some((_, timestamp)) = &mut self.timestamp_builder {
            let array = if let Some(error_mask) = &error_mask {
                let errors = error_mask.false_count();
                if errors > 0 {
                    source_errors.push(SourceError::bad_data_count(
                        "Some records could not be deserialized from JSON",
                        errors,
                    ));
                }

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

        (
            Some(RecordBatch::try_new(self.final_schema.clone(), arrays).unwrap()),
            source_errors,
        )
    }

    fn deserialize_single(&mut self, msg: &[u8]) -> DataflowResult<()> {
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
                        .map_err(|e| SourceError::bad_data(format!("invalid JSON: {e:?}")))?;
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

    async fn deserialize_slice_avro(&mut self, msg: &[u8]) -> (usize, Vec<DataflowError>) {
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
                    SourceError::bad_data(format!("failed to deserialize from avro: {e:?}"))
                })?;

                if into_json {
                    self.decode_into_json(de::avro_to_json(value));
                } else {
                    // for now round-trip through json in order to handle unsupported avro features
                    // as that allows us to rely on raw json deserialization
                    let json = de::avro_to_json(value).to_string();

                    self.buffer_decoder
                        .decode_json(json.as_bytes())
                        .map_err(|e| SourceError::bad_data(format!("invalid JSON: {e:?}")))?;
                }

                count += 1;

                Ok(())
            })
            .filter_map(|r: Result<(), DataflowError>| r.err())
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

macro_rules! append_repeated_value {
    ($builder:expr, $builder_ty:ty, $value:expr, $count:expr) => {{
        let b = $builder
            .downcast_mut::<$builder_ty>()
            .expect("additional field has incorrect type");

        if let Some(v) = $value {
            for _ in 0..$count {
                b.append_value(v);
            }
        } else {
            for _ in 0..$count {
                b.append_null();
            }
        }
    }};
}

fn add_additional_fields(
    builders: &mut HashMap<String, Box<dyn ArrayBuilder>>,
    key: &str,
    value: FieldValueType<'_>,
    count: usize,
) {
    let builder = builders
        .get_mut(key)
        .unwrap_or_else(|| panic!("unexpected additional field '{key}'"))
        .as_any_mut();

    match value {
        FieldValueType::Int32(v) => {
            append_repeated_value!(builder, Int32Builder, v, count);
        }
        FieldValueType::Int64(v) => {
            append_repeated_value!(builder, Int64Builder, v, count);
        }
        FieldValueType::UInt64(v) => {
            append_repeated_value!(builder, UInt64Builder, v, count);
        }
        FieldValueType::String(v) => {
            append_repeated_value!(builder, StringBuilder, v, count);
        }
        FieldValueType::Bytes(v) => {
            append_repeated_value!(builder, BinaryBuilder, v, count);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::de::{ArrowDeserializer, FieldValueType, FramingIterator};
    use arrow::datatypes::Int32Type;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{GenericBinaryType, Int64Type, TimestampNanosecondType};
    use arrow_schema::{DataType, Schema, TimeUnit};
    use arroyo_rpc::MetadataField;
    use arroyo_rpc::df::ArroyoSchema;
    use arroyo_rpc::errors::DataflowError;
    use arroyo_rpc::formats::{
        BadData, Format, Framing, JsonFormat, NewlineDelimitedFraming, RawBytesFormat,
    };
    use arroyo_types::to_nanos;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::SystemTime;

    #[test]
    fn test_line_framing() {
        let framing = Some(Arc::new(Framing::Newline(NewlineDelimitedFraming {
            max_line_length: None,
        })));

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
        let framing = Some(Arc::new(Framing::Newline(NewlineDelimitedFraming {
            max_line_length: Some(5),
        })));

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
                decimal_encoding: Default::default(),
            }),
            schema,
            &[],
            None,
            bad_data,
        )
    }

    #[tokio::test]
    async fn test_bad_data_drop() {
        let mut deserializer = setup_deserializer(BadData::Drop {});

        let now = SystemTime::now();

        assert!(
            deserializer
                .deserialize_slice(json!({ "x": 5 }).to_string().as_bytes(), now, None,)
                .await
                .is_empty()
        );
        assert!(
            deserializer
                .deserialize_slice(json!({ "x": "hello" }).to_string().as_bytes(), now, None,)
                .await
                .is_empty()
        );

        let batch = deserializer.flush_buffer().0.unwrap();
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

        assert!(
            deserializer
                .deserialize_slice(
                    json!({ "x": 5 }).to_string().as_bytes(),
                    SystemTime::now(),
                    None,
                )
                .await
                .is_empty()
        );
        assert!(
            deserializer
                .deserialize_slice(
                    json!({ "x": "hello" }).to_string().as_bytes(),
                    SystemTime::now(),
                    None,
                )
                .await
                .is_empty()
        );

        let err = deserializer.flush_buffer().1.remove(0);

        assert!(matches!(err, DataflowError::DataError { .. }));
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
            &[],
            None,
            BadData::Fail {},
        );

        let time = SystemTime::now();
        let result = deserializer
            .deserialize_slice(&[0, 1, 2, 3, 4, 5], time, None)
            .await;
        assert!(result.is_empty());

        let batch = deserializer.flush_buffer().0.unwrap();

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
    async fn test_additional_fields_deserialization() {
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
                decimal_encoding: Default::default(),
            }),
            arroyo_schema,
            &[
                MetadataField {
                    field_name: "y".to_string(),
                    key: "y".to_string(),
                    data_type: Some(DataType::Int64),
                },
                MetadataField {
                    field_name: "z".to_string(),
                    key: "z".to_string(),
                    data_type: Some(DataType::Utf8),
                },
            ],
            None,
            BadData::Drop {},
        );

        let time = SystemTime::now();
        let mut additional_fields = std::collections::HashMap::new();
        additional_fields.insert("y", FieldValueType::Int32(Some(5)));
        additional_fields.insert("z", FieldValueType::String(Some("hello")));

        let result = deserializer
            .deserialize_slice(
                json!({ "x": 5 }).to_string().as_bytes(),
                time,
                Some(&additional_fields),
            )
            .await;
        assert!(result.is_empty());

        let batch = deserializer.flush_buffer().0.unwrap();
        println!("batch ={batch:?}");
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

    #[test]
    fn test_format_validation_error() {
        use super::format_validation_error;
        use arrow::json::reader::{FailureKind, JsonType, ValidationError};
        use arrow_schema::DataType;

        let cases = vec![
            (
                ValidationError {
                    row_index: 0,
                    field_path: "user_id".to_string(),
                    failure_kind: FailureKind::MissingField,
                    expected_type: Arc::new(DataType::Int64),
                    actual_type: None,
                    actual_value: None,
                },
                "field 'user_id': required field is missing (expected Int64)",
            ),
            (
                ValidationError {
                    row_index: 0,
                    field_path: "age".to_string(),
                    failure_kind: FailureKind::NullValue,
                    expected_type: Arc::new(DataType::Int32),
                    actual_type: Some(JsonType::Null),
                    actual_value: Some("null".to_string()),
                },
                "field 'age': null value for non-nullable field (expected Int32)",
            ),
            // type_mismatch with string — length excludes quotes
            (
                ValidationError {
                    row_index: 0,
                    field_path: "user_id".to_string(),
                    failure_kind: FailureKind::TypeMismatch,
                    expected_type: Arc::new(DataType::Int64),
                    actual_type: Some(JsonType::String),
                    actual_value: Some("\"john.doe@company.com\"".to_string()),
                },
                "field 'user_id': expected Int64, got string (length: 20)",
            ),
            // type_mismatch with number
            (
                ValidationError {
                    row_index: 0,
                    field_path: "name".to_string(),
                    failure_kind: FailureKind::TypeMismatch,
                    expected_type: Arc::new(DataType::Utf8),
                    actual_type: Some(JsonType::Number),
                    actual_value: Some("12345".to_string()),
                },
                "field 'name': expected Utf8, got number (length: 5)",
            ),
            // type_mismatch with object — no length
            (
                ValidationError {
                    row_index: 0,
                    field_path: "value".to_string(),
                    failure_kind: FailureKind::TypeMismatch,
                    expected_type: Arc::new(DataType::Int64),
                    actual_type: Some(JsonType::Object),
                    actual_value: Some("{...}".to_string()),
                },
                "field 'value': expected Int64, got object",
            ),
            // type_mismatch with array — no length
            (
                ValidationError {
                    row_index: 0,
                    field_path: "count".to_string(),
                    failure_kind: FailureKind::TypeMismatch,
                    expected_type: Arc::new(DataType::Int32),
                    actual_type: Some(JsonType::Array),
                    actual_value: Some("[...]".to_string()),
                },
                "field 'count': expected Int32, got array",
            ),
            // type_mismatch with boolean — no length
            (
                ValidationError {
                    row_index: 0,
                    field_path: "score".to_string(),
                    failure_kind: FailureKind::TypeMismatch,
                    expected_type: Arc::new(DataType::Float64),
                    actual_type: Some(JsonType::Boolean),
                    actual_value: Some("true".to_string()),
                },
                "field 'score': expected Float64, got boolean",
            ),
            // parse_failure with string
            (
                ValidationError {
                    row_index: 0,
                    field_path: "created_at".to_string(),
                    failure_kind: FailureKind::ParseFailure,
                    expected_type: Arc::new(DataType::Utf8),
                    actual_type: Some(JsonType::String),
                    actual_value: Some("\"not-a-date\"".to_string()),
                },
                "field 'created_at': expected Utf8, got string (length: 10) - parse failure",
            ),
            // type_mismatch with no actual_value
            (
                ValidationError {
                    row_index: 0,
                    field_path: "field_a".to_string(),
                    failure_kind: FailureKind::TypeMismatch,
                    expected_type: Arc::new(DataType::Int64),
                    actual_type: Some(JsonType::String),
                    actual_value: None,
                },
                "field 'field_a': expected Int64, got string",
            ),
            // type_mismatch with no type or value
            (
                ValidationError {
                    row_index: 0,
                    field_path: "field_b".to_string(),
                    failure_kind: FailureKind::TypeMismatch,
                    expected_type: Arc::new(DataType::Int64),
                    actual_type: None,
                    actual_value: None,
                },
                "field 'field_b': expected Int64, got incompatible type",
            ),
            // array index field path
            (
                ValidationError {
                    row_index: 0,
                    field_path: "items[2]".to_string(),
                    failure_kind: FailureKind::TypeMismatch,
                    expected_type: Arc::new(DataType::Int32),
                    actual_type: Some(JsonType::String),
                    actual_value: Some("\"hello\"".to_string()),
                },
                "field 'items[2]': expected Int32, got string (length: 5)",
            ),
            // empty string — length 0
            (
                ValidationError {
                    row_index: 0,
                    field_path: "name".to_string(),
                    failure_kind: FailureKind::ParseFailure,
                    expected_type: Arc::new(DataType::Int64),
                    actual_type: Some(JsonType::String),
                    actual_value: Some("\"\"".to_string()),
                },
                "field 'name': expected Int64, got string (length: 0) - parse failure",
            ),
        ];

        for (i, (input, expected)) in cases.iter().enumerate() {
            let result = format_validation_error(input);
            assert_eq!(&result, expected, "case {i} failed");
        }
    }
}
