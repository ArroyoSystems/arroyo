use crate::{avro, json};
use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use arrow_json::writer::record_batches_to_json_rows;
use arrow_schema::DataType;
use arroyo_rpc::formats::{Format, JsonFormat, RawStringFormat};
use arroyo_rpc::TIMESTAMP_FIELD;
use serde_json::Value;

pub struct ArrowSerializer {
    kafka_schema: Option<Value>,
    avro_schema: Option<apache_avro::schema::Schema>,
    format: Format,
}

impl ArrowSerializer {
    pub fn new(format: Format) -> Self {
        Self {
            kafka_schema: None,
            avro_schema: None,
            format,
        }
    }

    pub fn serialize(&mut self, batch: &RecordBatch) -> Box<dyn Iterator<Item = Vec<u8>> + Send> {
        if self.kafka_schema.is_none() {
            self.kafka_schema = Some(json::arrow_to_kafka_json(
                "ArroyoJson",
                batch.schema().fields(),
            ));
        }
        if self.avro_schema.is_none() {
            self.avro_schema = Some(avro::arrow_to_avro_schema(
                "ArroyoAvro",
                batch.schema().fields(),
            ));
        }

        match &self.format {
            Format::Json(json) => self.serialize_json(json, batch),
            Format::Avro(_) => todo!("avro"),
            Format::Parquet(_) => todo!("parquet"),
            Format::RawString(RawStringFormat {}) => self.serialize_raw_string(batch),
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

        let rows = record_batches_to_json_rows(&[batch]).unwrap();

        let include_schema = json.include_schema.then(|| self.kafka_schema.clone());

        Box::new(rows.into_iter().map(move |mut row| {
            row.remove(TIMESTAMP_FIELD);

            let mut buf: Vec<u8> = Vec::with_capacity(128);
            if let Some(header) = header {
                buf.extend(&header);
            }

            if let Some(schema) = &include_schema {
                let record = json! {{
                    "schema": schema,
                    "payload": row
                }};
                serde_json::to_writer(&mut buf, &record).unwrap();
            } else {
                serde_json::to_writer(&mut buf, &row).unwrap();
            };

            buf
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
}

#[cfg(test)]
mod tests {
    use crate::serialize::ArrowSerializer;
    use arrow_schema::{Schema, TimeUnit};
    use arroyo_rpc::formats::{Format, RawStringFormat};
    use arroyo_types::to_nanos;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_raw_string() {
        let mut serializer = ArrowSerializer::new(Format::RawString(RawStringFormat {}));

        let data: Vec<_> = vec!["a", "b", "blah", "whatever"]
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
    fn test_json() {
        let mut serializer = ArrowSerializer::new(Format::Json(arroyo_rpc::formats::JsonFormat {
            confluent_schema_registry: false,
            schema_id: None,
            include_schema: false,
            debezium: false,
            unstructured: false,
            timestamp_format: Default::default(),
        }));

        let text: Vec<_> = vec!["a", "b", "blah", "whatever"]
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
}
