use crate::avro::schema;
use crate::json::encoders::ArroyoEncoderFactory;
use crate::{avro, json};
use arrow_array::cast::AsArray;
use arrow_array::types::GenericBinaryType;
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_json::writer::make_encoder;
use arrow_json::EncoderOptions;
use arrow_schema::{ArrowError, DataType, Field};
use arroyo_rpc::formats::{
    AvroFormat, DecimalEncoding, Format, JsonFormat, RawBytesFormat, RawStringFormat,
    TimestampFormat,
};
use arroyo_rpc::TIMESTAMP_FIELD;
use serde_json::Value;
use std::sync::Arc;

/// Writes a record batch as a Vec of encoded JSON rows
pub fn record_batch_to_vec(
    batch: &RecordBatch,
    explicit_nulls: bool,
    timestamp_format: TimestampFormat,
    decimal_encoding: DecimalEncoding,
) -> Result<Vec<Vec<u8>>, ArrowError> {
    let array = StructArray::from(batch.clone());

    let options = EncoderOptions::default()
        .with_explicit_nulls(explicit_nulls)
        .with_encoder_factory(Arc::new(ArroyoEncoderFactory {
            timestamp_format,
            decimal_encoding,
        }));

    let field = Arc::new(Field::new_struct(
        "",
        batch.schema().fields().clone(),
        false,
    ));

    let mut encoder = make_encoder(&field, &array, &options)?;

    let mut results = Vec::with_capacity(batch.num_rows());
    for idx in 0..array.len() {
        let mut buffer = Vec::with_capacity(8);
        encoder.encode(idx, &mut buffer);
        if !buffer.is_empty() {
            results.push(buffer);
        }
    }

    Ok(results)
}

pub struct ArrowSerializer {
    kafka_schema: Option<Value>,
    avro_schema: Option<Arc<apache_avro::schema::Schema>>,
    format: Format,
    projection: Vec<usize>,
}

impl ArrowSerializer {
    pub fn new(format: Format) -> Self {
        Self {
            kafka_schema: None,
            avro_schema: None,
            format,
            projection: vec![],
        }
    }

    fn projection(schema: &arrow_schema::Schema) -> Vec<usize> {
        schema
            .fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name() != TIMESTAMP_FIELD)
            .map(|(i, _)| i)
            .collect()
    }

    fn projected_schema(schema: &arrow_schema::Schema) -> Vec<Field> {
        let projection = Self::projection(schema);
        projection
            .iter()
            .map(|i| schema.field(*i).clone())
            .collect()
    }

    pub fn avro_schema(schema: &arrow_schema::Schema) -> apache_avro::Schema {
        schema::to_avro("ArroyoAvro", &Self::projected_schema(schema).into())
    }

    pub fn json_schema(schema: &arrow_schema::Schema) -> Value {
        json::arrow_to_json_schema(&Self::projected_schema(schema).into())
    }

    pub fn kafka_schema(schema: &arrow_schema::Schema) -> Value {
        json::arrow_to_kafka_json("ArroyoJson", &Self::projected_schema(schema).into())
    }

    pub fn serialize(&mut self, batch: &RecordBatch) -> Box<dyn Iterator<Item = Vec<u8>> + Send> {
        if self.projection.is_empty() {
            self.projection = Self::projection(&batch.schema());
        }

        if self.kafka_schema.is_none() {
            self.kafka_schema = Some(Self::kafka_schema(&batch.schema()));
        }

        if self.avro_schema.is_none() {
            self.avro_schema = Some(Arc::new(Self::avro_schema(&batch.schema())));
        }

        let batch = batch
            .project(&self.projection)
            .expect("batch has wrong number of columns");

        match &self.format {
            Format::Json(json) => self.serialize_json(json, &batch),
            Format::Avro(avro) => self.serialize_avro(avro, &batch),
            Format::Parquet(_) => todo!("parquet"),
            Format::RawString(RawStringFormat {}) => self.serialize_raw_string(&batch),
            Format::RawBytes(RawBytesFormat {}) => self.serialize_raw_bytes(&batch),
            Format::Protobuf(_) => {
                todo!("protobuf serializer!")
            }
        }
    }

    fn serialize_json(
        &self,
        json: &JsonFormat,
        batch: &RecordBatch,
    ) -> Box<dyn Iterator<Item = Vec<u8>> + Send> {
        let header = json.confluent_schema_registry.then(|| {
            if json.include_schema {
                unreachable!("can't include schema when writing to confluent schema registry, should've been caught when creating JsonFormat");
            }
            let mut v = [0; 5];
            v[1..].iter_mut().zip(json.schema_id.expect("must have computed id version to write using confluent schema registry").to_be_bytes())
                .for_each(|(a, b)| *a = b);

            v
        });

        let rows =
            record_batch_to_vec(batch, true, json.timestamp_format, json.decimal_encoding).unwrap();

        let include_schema = json.include_schema.then(|| self.kafka_schema.clone());

        Box::new(rows.into_iter().map(move |row| {
            if let Some(schema) = &include_schema {
                let parsed: serde_json::Value = serde_json::from_slice(&row).unwrap();
                let record = json! {{
                    "schema": schema,
                    "payload": parsed,
                }};

                let mut buf = if let Some(header) = header {
                    header.to_vec()
                } else {
                    vec![]
                };

                serde_json::to_writer(&mut buf, &record).unwrap();
                buf
            } else if let Some(header) = header {
                let mut buf = header.to_vec();
                buf.extend(&row);
                buf
            } else {
                row
            }
        }))
    }

    fn serialize_raw_string(
        &self,
        batch: &RecordBatch,
    ) -> Box<dyn Iterator<Item = Vec<u8>> + Send> {
        let value_idx = batch.schema().index_of("value").unwrap_or_else(|_| {
            panic!(
                "invalid schema for raw_string serializer: {}; a VALUE column is required",
                batch.schema()
            )
        });

        if *batch.schema().field(value_idx).data_type() != DataType::Utf8 {
            panic!("invalid schema for raw_string serializer: {}; a must have a column VALUE of type TEXT", batch.schema());
        }

        let values: Vec<Vec<u8>> = batch
            .column(value_idx)
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(|v| v.as_bytes().to_vec()).unwrap_or_default())
            .collect();

        Box::new(values.into_iter())
    }

    fn serialize_raw_bytes(&self, batch: &RecordBatch) -> Box<dyn Iterator<Item = Vec<u8>> + Send> {
        let value_idx = batch.schema().index_of("value").unwrap_or_else(|_| {
            panic!(
                "invalid schema for raw_string serializer: {}; a VALUE column is required",
                batch.schema()
            )
        });

        if *batch.schema().field(value_idx).data_type() != DataType::Binary {
            panic!("invalid schema for raw_string serializer: {}; a must have a column VALUE of type BYTEA", batch.schema());
        }

        let values: Vec<Vec<u8>> = batch
            .column(value_idx)
            .as_bytes::<GenericBinaryType<i32>>()
            .iter()
            .map(|v| v.map(|v| v.to_vec()).unwrap_or_default())
            .collect();

        Box::new(values.into_iter())
    }

    fn serialize_avro(
        &self,
        format: &AvroFormat,
        batch: &RecordBatch,
    ) -> Box<dyn Iterator<Item = Vec<u8>> + Send> {
        let schema = self
            .avro_schema
            .as_ref()
            .expect("must have avro schema set for avro format")
            .clone();

        let items = avro::ser::serialize(&schema, batch);

        if format.raw_datums || format.confluent_schema_registry {
            let schema_id = format.confluent_schema_registry.then(|| {
                format
                    .schema_id
                    .expect("must have schema id for confluent schema registry")
                    .to_be_bytes()
            });

            Box::new(items.into_iter().map(move |v| {
                let record = apache_avro::to_avro_datum(&schema, v.clone())
                    .expect("avro serialization failed");
                if let Some(schema_id) = schema_id {
                    // TODO: this would be more efficient if we could use the internal write_avro_datum to avoid
                    // allocating the buffer twice
                    let mut buf = Vec::with_capacity(record.len() + 5);
                    buf.push(0);
                    buf.extend(schema_id);
                    buf.extend(record);
                    buf
                } else {
                    record
                }
            }))
        } else {
            let mut buf = Vec::with_capacity(128);
            let mut writer = apache_avro::Writer::new(&schema, &mut buf);
            for v in items {
                writer.append(v).expect("avro serialization failed");
            }
            Box::new(vec![buf].into_iter())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ser::ArrowSerializer;
    use arrow_array::builder::{Decimal128Builder, TimestampNanosecondBuilder};
    use arrow_schema::{Schema, TimeUnit};
    use arroyo_rpc::formats::{
        DecimalEncoding, Format, JsonFormat, RawBytesFormat, RawStringFormat, TimestampFormat,
    };
    use arroyo_types::to_nanos;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_raw_string() {
        let mut serializer = ArrowSerializer::new(Format::RawString(RawStringFormat {}));

        let data: Vec<_> = ["a", "b", "blah", "whatever"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let ts: Vec<_> = data
            .iter()
            .enumerate()
            .map(|(i, _)| to_nanos(SystemTime::now() + Duration::from_secs(i as u64)) as i64)
            .collect();

        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("value", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new(
                "_timestamp",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::StringArray::from(data)),
                Arc::new(arrow_array::TimestampNanosecondArray::from(ts)),
            ],
        )
        .unwrap();

        let mut iter = serializer.serialize(&batch);
        assert_eq!(iter.next().unwrap(), b"a");
        assert_eq!(iter.next().unwrap(), b"b");
        assert_eq!(iter.next().unwrap(), b"blah");
        assert_eq!(iter.next().unwrap(), b"whatever");
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_raw_bytes() {
        let mut serializer = ArrowSerializer::new(Format::RawBytes(RawBytesFormat {}));

        let data = [b"0123".to_vec(), b"hello".to_vec(), vec![0, 1, 2, 4]];
        let ts: Vec<_> = data
            .iter()
            .enumerate()
            .map(|(i, _)| to_nanos(SystemTime::now() + Duration::from_secs(i as u64)) as i64)
            .collect();

        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("value", arrow_schema::DataType::Binary, false),
            arrow_schema::Field::new(
                "_timestamp",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::BinaryArray::from(
                    data.iter().map(|t| t.as_slice()).collect::<Vec<_>>(),
                )),
                Arc::new(arrow_array::TimestampNanosecondArray::from(ts)),
            ],
        )
        .unwrap();

        let mut iter = serializer.serialize(&batch);
        assert_eq!(iter.next().unwrap(), b"0123");
        assert_eq!(iter.next().unwrap(), b"hello");
        assert_eq!(iter.next().unwrap(), vec![0, 1, 2, 4]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_json() {
        let mut serializer = ArrowSerializer::new(Format::Json(arroyo_rpc::formats::JsonFormat {
            confluent_schema_registry: false,
            schema_id: None,
            include_schema: false,
            debezium: false,
            unstructured: false,
            timestamp_format: Default::default(),
            decimal_encoding: Default::default(),
        }));

        let text: Vec<_> = ["a", "b", "blah", "whatever"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let numbers = vec![1, 2, 3, 4];

        let ts: Vec<_> = text
            .iter()
            .enumerate()
            .map(|(i, _)| to_nanos(SystemTime::now() + Duration::from_secs(i as u64)) as i64)
            .collect();

        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("value", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("number", arrow_schema::DataType::Int32, false),
            arrow_schema::Field::new(
                "_timestamp",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::StringArray::from(text)),
                Arc::new(arrow_array::Int32Array::from(numbers)),
                Arc::new(arrow_array::TimestampNanosecondArray::from(ts)),
            ],
        )
        .unwrap();

        let mut iter = serializer.serialize(&batch);
        assert_eq!(iter.next().unwrap(), br#"{"value":"a","number":1}"#);
        assert_eq!(iter.next().unwrap(), br#"{"value":"b","number":2}"#);
        assert_eq!(iter.next().unwrap(), br#"{"value":"blah","number":3}"#);
        assert_eq!(iter.next().unwrap(), br#"{"value":"whatever","number":4}"#);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_json_unix_ts() {
        let mut serializer = ArrowSerializer::new(Format::Json(arroyo_rpc::formats::JsonFormat {
            confluent_schema_registry: false,
            schema_id: None,
            include_schema: false,
            debezium: false,
            unstructured: false,
            timestamp_format: TimestampFormat::UnixMillis,
            decimal_encoding: Default::default(),
        }));

        let mut timestamp_array = TimestampNanosecondBuilder::new();
        timestamp_array.append_value(1612274910045331968);
        timestamp_array.append_null();
        timestamp_array.append_value(1712274910045331968);
        let ts = Arc::new(timestamp_array.finish());

        let event_times: Vec<_> = ts
            .iter()
            .enumerate()
            .map(|(i, _)| to_nanos(SystemTime::now() + Duration::from_secs(i as u64)) as i64)
            .collect();

        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new(
                "value",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            arrow_schema::Field::new(
                "_timestamp",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                ts,
                Arc::new(arrow_array::TimestampNanosecondArray::from(event_times)),
            ],
        )
        .unwrap();

        let mut iter = serializer.serialize(&batch);
        assert_eq!(iter.next().unwrap(), br#"{"value":1612274910045}"#);
        assert_eq!(iter.next().unwrap(), br#"{"value":null}"#);
        assert_eq!(iter.next().unwrap(), br#"{"value":1712274910045}"#);
    }

    #[test]
    fn test_json_decimal() {
        let mut serializer = ArrowSerializer::new(Format::Json(arroyo_rpc::formats::JsonFormat {
            confluent_schema_registry: false,
            schema_id: None,
            include_schema: false,
            debezium: false,
            unstructured: false,
            timestamp_format: TimestampFormat::UnixMillis,
            decimal_encoding: DecimalEncoding::Number,
        }));

        let mut decimal_array = Decimal128Builder::new()
            .with_precision_and_scale(5, 3)
            .unwrap();
        decimal_array.append_value(10);
        decimal_array.append_value(51);
        decimal_array.append_value(-399);
        let vs = Arc::new(decimal_array.finish());

        let event_times: Vec<_> = vs
            .iter()
            .map(|_| to_nanos(SystemTime::now()) as i64)
            .collect();

        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("value", arrow_schema::DataType::Decimal128(5, 3), true),
            arrow_schema::Field::new(
                "_timestamp",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                vs,
                Arc::new(arrow_array::TimestampNanosecondArray::from(event_times)),
            ],
        )
        .unwrap();

        // number
        let mut iter = serializer.serialize(&batch);
        assert_eq!(
            String::from_utf8(iter.next().unwrap()).unwrap(),
            r#"{"value":0.010}"#.to_string()
        );
        assert_eq!(
            String::from_utf8(iter.next().unwrap()).unwrap(),
            r#"{"value":0.051}"#.to_string()
        );
        assert_eq!(
            String::from_utf8(iter.next().unwrap()).unwrap(),
            r#"{"value":-0.399}"#.to_string()
        );

        // string
        match &mut serializer.format {
            Format::Json(JsonFormat {
                decimal_encoding, ..
            }) => {
                *decimal_encoding = DecimalEncoding::String;
            }
            _ => {
                unreachable!();
            }
        }
        let mut iter = serializer.serialize(&batch);
        assert_eq!(
            String::from_utf8(iter.next().unwrap()).unwrap(),
            r#"{"value":"0.010"}"#.to_string()
        );
        assert_eq!(
            String::from_utf8(iter.next().unwrap()).unwrap(),
            r#"{"value":"0.051"}"#.to_string()
        );
        assert_eq!(
            String::from_utf8(iter.next().unwrap()).unwrap(),
            r#"{"value":"-0.399"}"#.to_string()
        );

        // bytes
        match &mut serializer.format {
            Format::Json(JsonFormat {
                decimal_encoding, ..
            }) => {
                *decimal_encoding = DecimalEncoding::Bytes;
            }
            _ => {
                unreachable!();
            }
        }
        let mut iter = serializer.serialize(&batch);
        assert_eq!(
            String::from_utf8(iter.next().unwrap()).unwrap(),
            r#"{"value":"AAAAAAAAAAAAAAAAAAAACg=="}"#.to_string()
        );
        assert_eq!(
            String::from_utf8(iter.next().unwrap()).unwrap(),
            r#"{"value":"AAAAAAAAAAAAAAAAAAAAMw=="}"#.to_string()
        );
        assert_eq!(
            String::from_utf8(iter.next().unwrap()).unwrap(),
            r#"{"value":"///////////////////+cQ=="}"#.to_string()
        );
    }

    #[test]
    fn test_json_binary() {
        let mut serializer = ArrowSerializer::new(Format::Json(arroyo_rpc::formats::JsonFormat {
            ..Default::default()
        }));

        let binary = vec![
            b"hello".as_slice(),
            b"123123".as_slice(),
            [0, 1, 2, 3, 4].as_slice(),
        ];

        let ts: Vec<_> = binary
            .iter()
            .enumerate()
            .map(|(i, _)| to_nanos(SystemTime::now() + Duration::from_secs(i as u64)) as i64)
            .collect();

        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("value", arrow_schema::DataType::Binary, false),
            arrow_schema::Field::new(
                "_timestamp",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::GenericBinaryArray::<i32>::from(binary)),
                Arc::new(arrow_array::TimestampNanosecondArray::from(ts)),
            ],
        )
        .unwrap();

        let mut iter = serializer.serialize(&batch);
        assert_eq!(iter.next().unwrap(), br#"{"value":"aGVsbG8="}"#);
        assert_eq!(iter.next().unwrap(), br#"{"value":"MTIzMTIz"}"#);
        assert_eq!(iter.next().unwrap(), br#"{"value":"AAECAwQ="}"#);
        assert_eq!(iter.next(), None);
    }
}
