use arrow_array::RecordBatch;
use arrow_json::writer::record_batches_to_json_rows;
use serde_json::Value;
use arroyo_rpc::{TIMESTAMP_FIELD};
use arroyo_rpc::formats::{Format, JsonFormat};
use crate::{avro, json};

pub struct ArrowSerializer {
    kafka_schema: Option<Value>,
    avro_schema: Option<apache_avro::schema::Schema>,
    schema_id: Option<u32>,
    format: Format,
}

impl ArrowSerializer {
    pub fn new(format: Format) -> Self {
        Self {
            kafka_schema: None,
            avro_schema: None,
            schema_id: match &format {
                Format::Avro(avro) => avro.schema_id,
                _ => None,
            },
            format,
        }
    }

    pub fn serialize(&mut self, batch: &RecordBatch) -> Box<dyn Iterator<Item = Vec<u8>> + Send> {
        if self.kafka_schema.is_none() {
            self.kafka_schema = Some(json::arrow_to_kafka_json("ArroyoJson", batch.schema().fields()));
        }
        if self.avro_schema.is_none() {
            self.avro_schema = Some(avro::arrow_to_avro_schema("ArroyoAvro", batch.schema().fields()));
        }

        match &self.format {
            Format::Json(json) => {
                self.serialize_json(json, batch)
            }
            Format::Avro(_) => todo!("avro"),
            Format::Parquet(_) => todo!("parquet"),
            Format::RawString(_) => todo!("raw string"),
        }
    }

    fn serialize_json(&self, json: &JsonFormat, batch: &RecordBatch) -> Box<dyn Iterator<Item = Vec<u8>> + Send> {
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
}
