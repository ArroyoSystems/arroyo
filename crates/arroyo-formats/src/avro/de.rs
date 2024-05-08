use apache_avro::types::{Value, Value as AvroValue};
use apache_avro::{from_avro_datum, AvroResult, Reader, Schema};
use arroyo_rpc::formats::AvroFormat;
use arroyo_rpc::schema_resolver::SchemaResolver;
use arroyo_types::SourceError;
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub(crate) async fn avro_messages(
    format: &AvroFormat,
    schema_registry: &Arc<Mutex<HashMap<u32, Schema>>>,
    resolver: &Arc<dyn SchemaResolver + Sync>,
    mut msg: &[u8],
) -> Result<Vec<AvroResult<Value>>, SourceError> {
    let id = if format.confluent_schema_registry {
        let magic_byte = msg[0];
        if magic_byte != 0 {
            return Err(SourceError::bad_data(format!(
                "data was not encoded with schema registry wire format; \
                magic byte has unexpected value: {}",
                magic_byte
            )));
        }

        let id = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        msg = &msg[5..];
        id
    } else {
        // this should be kept in sync with the id configured when we construct the
        // FixedSchemaResolver
        0
    };

    let mut registry = schema_registry.lock().await;

    let messages = if format.raw_datums || format.confluent_schema_registry {
        let schema = if let std::collections::hash_map::Entry::Vacant(e) = registry.entry(id) {
            let new_schema = resolver
                .resolve_schema(id)
                .await
                .map_err(|e| SourceError::other("schema registry error", e))?
                .ok_or_else(|| {
                    SourceError::bad_data(format!(
                        "could not resolve schema for message with id {}",
                        id
                    ))
                })?;

            let new_schema = Schema::parse_str(&new_schema).map_err(|e| {
                SourceError::other(
                    "schema registry error",
                    format!(
                        "schema from Confluent Schema registry is not valid: {:?}",
                        e
                    ),
                )
            })?;

            info!("Loaded new schema with id {} from Schema Registry", id);
            e.insert(new_schema);

            registry.get(&id).unwrap()
        } else {
            registry.get(&id).unwrap()
        };

        let mut buf = msg;
        vec![from_avro_datum(
            schema,
            &mut buf,
            format.reader_schema.as_ref().map(|t| t.into()),
        )]
    } else {
        Reader::new(msg)
            .map_err(|e| SourceError::bad_data(format!("invalid Avro schema in message: {:?}", e)))?
            .collect()
    };
    Ok(messages)
}

fn convert_float(f: f64) -> JsonValue {
    match serde_json::Number::from_f64(f) {
        Some(n) => JsonValue::Number(n),
        None => JsonValue::String(
            (if f.is_infinite() && f.is_sign_positive() {
                "+Inf"
            } else if f.is_infinite() {
                "-Inf"
            } else {
                "NaN"
            })
            .to_string(),
        ),
    }
}

fn encode_vec(v: Vec<u8>) -> JsonValue {
    JsonValue::String(v.into_iter().map(char::from).collect())
}

pub(crate) fn avro_to_json(value: AvroValue) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Boolean(b) => JsonValue::Bool(b),
        Value::Int(i) | Value::Date(i) | Value::TimeMillis(i) => {
            JsonValue::Number(serde_json::Number::from(i))
        }
        Value::Long(i)
        | Value::TimeMicros(i)
        | Value::TimestampMillis(i)
        | Value::TimestampMicros(i)
        | Value::LocalTimestampMillis(i)
        | Value::LocalTimestampMicros(i) => JsonValue::Number(serde_json::Number::from(i)),
        Value::Float(f) => convert_float(f as f64),
        Value::Double(f) => convert_float(f),
        Value::String(s) | Value::Enum(_, s) => JsonValue::String(s),
        // this isn't the standard Avro json encoding, which just
        Value::Bytes(b) | Value::Fixed(_, b) => encode_vec(b),
        Value::Union(_, b) => avro_to_json(*b),
        Value::Array(a) => JsonValue::Array(a.into_iter().map(avro_to_json).collect()),
        Value::Map(m) => {
            JsonValue::Object(m.into_iter().map(|(k, v)| (k, avro_to_json(v))).collect())
        }
        Value::Record(rec) => {
            JsonValue::Object(rec.into_iter().map(|(k, v)| (k, avro_to_json(v))).collect())
        }

        Value::Decimal(d) => {
            let b: Vec<u8> = d.try_into().unwrap_or_else(|_| vec![]);
            encode_vec(b)
        }
        Value::Duration(d) => {
            json!({
               "months": u32::from(d.months()),
               "days": u32::from(d.days()),
               "milliseconds": u32::from(d.millis())
            })
        }
        Value::Uuid(u) => JsonValue::String(u.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use crate::avro::schema::to_arrow;
    use crate::de::ArrowDeserializer;
    use arrow_array::builder::{make_builder, ArrayBuilder};
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use arroyo_rpc::df::ArroyoSchema;
    use arroyo_rpc::formats::{AvroFormat, BadData, Format};
    use arroyo_rpc::schema_resolver::{FailingSchemaResolver, FixedSchemaResolver, SchemaResolver};
    use serde_json::json;
    use std::sync::Arc;
    use std::time::SystemTime;

    const SCHEMA: &str = r#"
        {
  "connect.name": "pizza_orders.pizza_orders",
  "fields": [
    {
      "name": "store_id",
      "type": "int"
    },
    {
      "name": "store_order_id",
      "type": "int"
    },
    {
      "name": "coupon_code",
      "type": "int"
    },
    {
      "name": "date",
      "type": {
        "connect.name": "org.apache.kafka.connect.data.Date",
        "connect.version": 1,
        "logicalType": "date",
        "type": "int"
      }
    },
    {
      "name": "status",
      "type": "string"
    },
    {
      "name": "order_lines",
      "type": {
        "items": {
          "connect.name": "pizza_orders.order_line",
          "fields": [
            {
              "name": "product_id",
              "type": "int"
            },
            {
              "name": "category",
              "type": "string"
            },
            {
              "name": "quantity",
              "type": "int"
            },
            {
              "name": "unit_price",
              "type": "double"
            },
            {
              "name": "net_price",
              "type": "double"
            }
          ],
          "name": "order_line",
          "type": "record"
        },
        "type": "array"
      }
    }
  ],
  "name": "pizza_orders",
  "namespace": "pizza_orders",
  "type": "record"
}"#;

    fn deserializer_with_schema(
        format: AvroFormat,
        writer_schema: Option<&str>,
    ) -> (ArrowDeserializer, Vec<Box<dyn ArrayBuilder>>, ArroyoSchema) {
        let arrow_schema = if format.into_unstructured_json {
            Schema::new(vec![Field::new("value", DataType::Utf8, false)])
        } else {
            to_arrow(
                &format
                    .reader_schema
                    .as_ref()
                    .expect("no reader schema")
                    .0
                    .canonical_form(),
            )
            .expect("invalid schema")
        };

        let arroyo_schema = {
            let mut fields = arrow_schema.fields.to_vec();
            fields.push(Arc::new(Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )));
            ArroyoSchema::from_schema_keys(Arc::new(Schema::new(fields)), vec![]).unwrap()
        };

        let builders: Vec<_> = arroyo_schema
            .schema
            .fields
            .iter()
            .map(|f| make_builder(f.data_type(), 8))
            .collect();

        let resolver: Arc<dyn SchemaResolver + Sync> = if let Some(schema) = &writer_schema {
            Arc::new(FixedSchemaResolver::new(
                if format.confluent_schema_registry {
                    1
                } else {
                    0
                },
                apache_avro::Schema::parse_str(schema).unwrap(),
            ))
        } else {
            Arc::new(FailingSchemaResolver::new())
        };

        (
            ArrowDeserializer::with_schema_resolver(
                Format::Avro(format),
                None,
                arroyo_schema.clone(),
                BadData::Fail {},
                resolver,
            ),
            builders,
            arroyo_schema,
        )
    }

    async fn deserialize_with_schema(
        format: AvroFormat,
        writer_schema: Option<&str>,
        message: &[u8],
    ) -> Vec<serde_json::Map<String, serde_json::Value>> {
        let (mut deserializer, mut builders, arroyo_schema) =
            deserializer_with_schema(format.clone(), writer_schema);

        let errors = deserializer
            .deserialize_slice(&mut builders, message, SystemTime::now())
            .await;
        assert_eq!(errors, vec![]);

        let batch = if format.into_unstructured_json {
            RecordBatch::try_new(
                arroyo_schema.schema,
                builders.into_iter().map(|mut b| b.finish()).collect(),
            )
            .unwrap()
        } else {
            deserializer.flush_buffer().unwrap().unwrap()
        };

        #[allow(deprecated)]
        arrow_json::writer::record_batches_to_json_rows(&[&batch])
            .unwrap()
            .into_iter()
            .map(|mut r| {
                r.remove("_timestamp");
                r
            })
            .collect()
    }

    #[tokio::test]
    async fn test_avro_deserialization() {
        let message = [
            0u8, 0, 0, 0, 1, 8, 200, 223, 1, 144, 31, 186, 159, 2, 16, 97, 99, 99, 101, 112, 116,
            101, 100, 4, 156, 1, 10, 112, 105, 122, 122, 97, 4, 102, 102, 102, 102, 102, 230, 38,
            64, 102, 102, 102, 102, 102, 230, 54, 64, 84, 14, 100, 101, 115, 115, 101, 114, 116, 2,
            113, 61, 10, 215, 163, 112, 26, 64, 113, 61, 10, 215, 163, 112, 26, 64, 0, 10,
        ];

        let mut format = AvroFormat::new(true, false, false);
        format.add_reader_schema(apache_avro::Schema::parse_str(SCHEMA).unwrap());
        let row = deserialize_with_schema(format, Some(SCHEMA), &message)
            .await
            .remove(0);
        assert_eq!(*row.get("store_id").unwrap(), json!(4));
        assert_eq!(*row.get("store_order_id").unwrap(), json!(14308));
        assert_eq!(*row.get("coupon_code").unwrap(), json!(1992));
        assert_eq!(*row.get("date").unwrap(), json!(18397));
        assert_eq!(*row.get("status").unwrap(), json!("accepted"));

        let order_line_expected = json!(
            [{"category":"pizza","net_price":22.9,"product_id":78,"quantity":2,"unit_price":11.45},{"category":"dessert","net_price":6.61,"product_id":42,"quantity":1,"unit_price":6.61}]
        );

        assert_eq!(*row.get("order_lines").unwrap(), order_line_expected);
    }

    #[tokio::test]
    async fn test_add_field() {
        // test schema evolution for adding a field (new_field)
        let reader_schema = r#"{"namespace": "example.avro",
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "favorite_number", "type": "int"},
                {"name": "favorite_color", "type": ["string", "null"]}
            ]
        }"#;

        let writer_schema = r#"{"namespace": "example.avro",
            "type": "record",
            "name": "User",
            "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number", "type": "int"},
            {"name": "favorite_color", "type": ["string", "null"]},
            {"name": "new_field", "type": "string", "default": "hello!"}
            ]
        }"#;

        let mut format = AvroFormat::new(true, false, false);
        format.add_reader_schema(apache_avro::Schema::parse_str(reader_schema).unwrap());

        let schema = apache_avro::Schema::parse_str(writer_schema).unwrap();
        let mut value = apache_avro::types::Record::new(&schema).unwrap();
        value.put(
            "name",
            apache_avro::types::Value::String("Alyssa".to_string()),
        );
        value.put("favorite_number", apache_avro::types::Value::Int(256));
        value.put(
            "favorite_color",
            apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::Null)),
        );
        value.put(
            "new_field",
            apache_avro::types::Value::String("new".to_string()),
        );

        let mut bytes = vec![0, 0, 0, 0, 1];
        bytes.extend_from_slice(
            &apache_avro::to_avro_datum(
                &apache_avro::Schema::parse_str(writer_schema).unwrap(),
                value,
            )
            .unwrap(),
        );

        let v = deserialize_with_schema(format, Some(writer_schema), &bytes[..]).await;
        assert_eq!(
            serde_json::to_value(v).unwrap(),
            json!([{
                "name": "Alyssa",
                "favorite_number": 256,
            }])
        );
    }

    #[tokio::test]
    async fn test_remove_field() {
        // test schema evolution for removing a field with a default value
        let writer_schema = r#"{"namespace": "example.avro",
            "type": "record",
            "name": "User",
            "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number", "type": "int"},
            {"name": "favorite_color", "type": ["string", "null"]}
            ]
        }"#;

        let reader_schema = r#"{"namespace": "example.avro",
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "favorite_number", "type": "int"},
                {"name": "favorite_color", "type": ["string", "null"]},
                {"name": "removed_field", "type": "string", "default": "hello!"}
            ]
        }"#;

        let schema = apache_avro::Schema::parse_str(writer_schema).unwrap();
        let mut value = apache_avro::types::Record::new(&schema).unwrap();
        value.put(
            "name",
            apache_avro::types::Value::String("Alyssa".to_string()),
        );
        value.put("favorite_number", apache_avro::types::Value::Int(256));

        let mut bytes = vec![0, 0, 0, 0, 1];
        bytes.extend_from_slice(
            &apache_avro::to_avro_datum(
                &apache_avro::Schema::parse_str(writer_schema).unwrap(),
                value,
            )
            .unwrap(),
        );

        let mut format = AvroFormat::new(true, false, false);
        format.add_reader_schema(apache_avro::Schema::parse_str(reader_schema).unwrap());

        let v = deserialize_with_schema(format, Some(writer_schema), bytes.as_slice()).await;
        assert_eq!(
            serde_json::to_value(v).unwrap(),
            json!([{
                "name": "Alyssa",
                "favorite_number": 256,
                "removed_field": "hello!"
            }])
        );
    }

    #[tokio::test]
    async fn test_embedded() {
        let data = [
            79u8, 98, 106, 1, 4, 20, 97, 118, 114, 111, 46, 99, 111, 100, 101, 99, 8, 110, 117,
            108, 108, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, 186, 3, 123, 34, 116,
            121, 112, 101, 34, 58, 32, 34, 114, 101, 99, 111, 114, 100, 34, 44, 32, 34, 110, 97,
            109, 101, 34, 58, 32, 34, 85, 115, 101, 114, 34, 44, 32, 34, 110, 97, 109, 101, 115,
            112, 97, 99, 101, 34, 58, 32, 34, 101, 120, 97, 109, 112, 108, 101, 46, 97, 118, 114,
            111, 34, 44, 32, 34, 102, 105, 101, 108, 100, 115, 34, 58, 32, 91, 123, 34, 116, 121,
            112, 101, 34, 58, 32, 34, 115, 116, 114, 105, 110, 103, 34, 44, 32, 34, 110, 97, 109,
            101, 34, 58, 32, 34, 110, 97, 109, 101, 34, 125, 44, 32, 123, 34, 116, 121, 112, 101,
            34, 58, 32, 91, 34, 105, 110, 116, 34, 44, 32, 34, 110, 117, 108, 108, 34, 93, 44, 32,
            34, 110, 97, 109, 101, 34, 58, 32, 34, 102, 97, 118, 111, 114, 105, 116, 101, 95, 110,
            117, 109, 98, 101, 114, 34, 125, 44, 32, 123, 34, 116, 121, 112, 101, 34, 58, 32, 91,
            34, 115, 116, 114, 105, 110, 103, 34, 44, 32, 34, 110, 117, 108, 108, 34, 93, 44, 32,
            34, 110, 97, 109, 101, 34, 58, 32, 34, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99,
            111, 108, 111, 114, 34, 125, 93, 125, 0, 52, 104, 70, 176, 108, 101, 199, 71, 44, 76,
            126, 49, 211, 19, 204, 87, 4, 44, 12, 65, 108, 121, 115, 115, 97, 0, 128, 4, 2, 6, 66,
            101, 110, 0, 14, 0, 6, 114, 101, 100, 52, 104, 70, 176, 108, 101, 199, 71, 44, 76, 126,
            49, 211, 19, 204, 87,
        ];

        let format = AvroFormat::new(false, false, true);
        let vs = deserialize_with_schema(format, None, &data).await;

        let expected = vec![
            json!({ "name": "Alyssa", "favorite_number": 256, "favorite_color": null }),
            json!({ "name": "Ben", "favorite_number": 7, "favorite_color": "red" }),
        ];

        for (a, b) in vs.iter().zip(expected) {
            let v: serde_json::Value =
                serde_json::from_str(a.get("value").unwrap().as_str().unwrap()).unwrap();
            assert_eq!(v, b);
        }
    }

    #[tokio::test]
    async fn test_datum_static_schema() {
        let data = [12, 65, 108, 121, 115, 115, 97, 0, 128, 4, 2];

        let schema_str = r#"{"namespace": "example.avro",
            "type": "record",
            "name": "User",
            "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number",  "type": ["int", "null"]},
            {"name": "favorite_color", "type": ["string", "null"]}
            ]
        }"#;

        let mut format = AvroFormat::new(false, true, true);
        format.add_reader_schema(apache_avro::Schema::parse_str(schema_str).unwrap());
        let vs = deserialize_with_schema(format, Some(schema_str), &data).await;

        let expected = json!({ "name": "Alyssa", "favorite_number": 256, "favorite_color": null });

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                vs[0].get("value").unwrap().as_str().unwrap()
            )
            .unwrap(),
            expected
        );
    }
}
