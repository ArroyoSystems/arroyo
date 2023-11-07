use apache_avro::types::{Value as AvroValue, Value};
use apache_avro::{from_avro_datum, Reader, Schema};
use arroyo_rpc::formats::AvroFormat;
use arroyo_rpc::schema_resolver::SchemaResolver;
use arroyo_types::UserError;
use serde::de::DeserializeOwned;
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub async fn deserialize_slice_avro<'a, T: DeserializeOwned>(
    format: &AvroFormat,
    schema_registry: Arc<Mutex<HashMap<u32, Schema>>>,
    resolver: Arc<dyn SchemaResolver + Sync>,
    mut msg: &'a [u8],
) -> Result<impl Iterator<Item = Result<T, UserError>> + 'a, String> {
    let id = if format.confluent_schema_registry {
        let magic_byte = msg[0];
        if magic_byte != 0 {
            return Err(format!("data was not encoded with schema registry wire format; magic byte has unexpected value: {}", magic_byte));
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

    let messages = if format.embedded_schema {
        Reader::new(&msg[..])
            .map_err(|e| format!("invalid Avro schema in message: {:?}", e))?
            .collect()
    } else {
        let schema = if registry.contains_key(&id) {
            registry.get(&id).unwrap()
        } else {
            let new_schema = resolver
                .resolve_schema(id)
                .await?
                .ok_or_else(|| format!("could not resolve schema for message with id {}", id))?;

            let new_schema = Schema::parse_str(&new_schema).map_err(|e| {
                format!(
                    "schema from Confluent Schema registry is not valid: {:?}",
                    e
                )
            })?;

            info!("Loaded new schema with id {} from Schema Registry", id);
            registry.insert(id, new_schema);

            registry.get(&id).unwrap()
        };

        let mut buf = &msg[..];
        vec![from_avro_datum(
            schema,
            &mut buf,
            format.reader_schema.as_ref().map(|t| t.into()),
        )]
    };

    let into_json = format.into_unstructured_json;
    Ok(messages.into_iter().map(move |record| {
        let value = record.map_err(|e| {
            UserError::new(
                "Deserialization failed",
                format!("Failed to deserialize from avro: {:?}", e),
            )
        })?;

        if into_json {
            Ok(serde_json::from_value(json!({"value": avro_to_json(value).to_string()})).unwrap())
        } else {
            // for now round-trip through json in order to handle unsupported avro features
            // as that allows us to rely on raw json deserialization
            serde_json::from_value(avro_to_json(value)).map_err(|e| {
                UserError::new(
                    "Deserialization failed",
                    format!("Failed to convert avro message into struct type: {:?}", e),
                )
            })
        }
    }))
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

fn avro_to_json(value: AvroValue) -> JsonValue {
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
        Value::Array(a) => JsonValue::Array(a.into_iter().map(|v| avro_to_json(v)).collect()),
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
    use crate::formats::DataDeserializer;
    use crate::SchemaData;
    use apache_avro::Schema;
    use arroyo_rpc::formats::{AvroFormat, Format};
    use arroyo_rpc::schema_resolver::{FailingSchemaResolver, FixedSchemaResolver};
    use arroyo_types::RawJson;
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_avro_deserialization() {
        #[derive(
            Clone,
            Debug,
            bincode::Encode,
            bincode::Decode,
            PartialEq,
            PartialOrd,
            serde::Serialize,
            serde::Deserialize,
        )]
        pub struct ArroyoAvroRoot {
            pub store_id: i32,
            pub store_order_id: i32,
            pub coupon_code: i32,
            #[serde(deserialize_with = "crate::deserialize_raw_json")]
            pub date: String,
            pub status: String,
            #[serde(deserialize_with = "crate::deserialize_raw_json")]
            pub order_lines: String,
        }
        impl SchemaData for ArroyoAvroRoot {
            fn name() -> &'static str {
                "ArroyoAvroRoot"
            }
            fn schema() -> arrow::datatypes::Schema {
                let fields: Vec<arrow::datatypes::Field> = vec![
                    arrow::datatypes::Field::new(
                        "store_id",
                        arrow::datatypes::DataType::Int32,
                        false,
                    ),
                    arrow::datatypes::Field::new(
                        "store_order_id",
                        arrow::datatypes::DataType::Int32,
                        false,
                    ),
                    arrow::datatypes::Field::new(
                        "coupon_code",
                        arrow::datatypes::DataType::Int32,
                        false,
                    ),
                    arrow::datatypes::Field::new("date", arrow::datatypes::DataType::Utf8, false),
                    arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, false),
                    arrow::datatypes::Field::new(
                        "order_lines",
                        arrow::datatypes::DataType::Utf8,
                        false,
                    ),
                ];
                arrow::datatypes::Schema::new(fields)
            }
            fn to_raw_string(&self) -> Option<Vec<u8>> {
                unimplemented!("to_raw_string is not implemented for this type")
            }
        }

        let schema = r#"
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

        let message = [
            0u8, 0, 0, 0, 1, 8, 200, 223, 1, 144, 31, 186, 159, 2, 16, 97, 99, 99, 101, 112, 116,
            101, 100, 4, 156, 1, 10, 112, 105, 122, 122, 97, 4, 102, 102, 102, 102, 102, 230, 38,
            64, 102, 102, 102, 102, 102, 230, 54, 64, 84, 14, 100, 101, 115, 115, 101, 114, 116, 2,
            113, 61, 10, 215, 163, 112, 26, 64, 113, 61, 10, 215, 163, 112, 26, 64, 0, 10,
        ];

        let mut deserializer = DataDeserializer::with_schema_resolver(
            Format::Avro(AvroFormat::new(true, false, false)),
            None,
            Arc::new(FixedSchemaResolver::new(
                1,
                Schema::parse_str(schema).unwrap(),
            )),
        );

        let v: Vec<Result<ArroyoAvroRoot, _>> =
            deserializer.deserialize_slice(&message[..]).await.collect();

        let expected = ArroyoAvroRoot {
            store_id: 4,
            store_order_id: 14308,
            coupon_code: 1992,
            date: "18397".to_string(),
            status: "accepted".to_string(),
            order_lines: "[{\"category\":\"pizza\",\"net_price\":22.9,\"product_id\":78,\"quantity\":2,\"unit_price\":11.45},{\"category\":\"dessert\",\"net_price\":6.61,\"product_id\":42,\"quantity\":1,\"unit_price\":6.61}]".to_string()
        };

        assert_eq!(v.len(), 1);
        let v = v.into_iter().next().unwrap().unwrap();
        // compare field-by-field because the json encoding for order_lines may not yield the
        // same object field order
        assert_eq!(expected.store_id, v.store_id);
        assert_eq!(expected.store_order_id, v.store_order_id);
        assert_eq!(expected.coupon_code, v.coupon_code);
        assert_eq!(expected.date, v.date);
        assert_eq!(expected.status, v.status);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&expected.order_lines).unwrap(),
            serde_json::from_str::<serde_json::Value>(&v.order_lines).unwrap()
        );
    }

    #[tokio::test]
    async fn test_backwards_compatible() {
        #[derive(
            Clone,
            Debug,
            bincode::Encode,
            bincode::Decode,
            PartialEq,
            PartialOrd,
            serde::Serialize,
            serde::Deserialize,
        )]
        pub struct ArroyoAvroRoot {
            pub name: String,
            pub favorite_number: Option<i32>,
            pub favorite_color: Option<String>,
            pub new_field: String,
        }
        impl SchemaData for ArroyoAvroRoot {
            fn name() -> &'static str {
                "ArroyoAvroRoot"
            }
            fn schema() -> arrow::datatypes::Schema {
                unimplemented!()
            }
            fn to_raw_string(&self) -> Option<Vec<u8>> {
                unimplemented!("to_raw_string is not implemented for this type")
            }
        }

        // test schema evolution for adding a field (new_field)
        let writer_schema = r#"{"namespace": "example.avro",
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "favorite_number",  "type": ["int", "null"]},
                {"name": "favorite_color", "type": ["string", "null"]}
            ]
        }"#;

        let reader_schema = r#"{"namespace": "example.avro",
            "type": "record",
            "name": "User",
            "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number",  "type": ["int", "null"]},
            {"name": "favorite_color", "type": ["string", "null"]},
            {"name": "new_field", "type": "string", "default": "hello!"}
            ]
        }"#;

        let mut format = AvroFormat::new(true, false, false);
        format.add_reader_schema(Schema::parse_str(reader_schema).unwrap());

        let mut deserializer = DataDeserializer::with_schema_resolver(
            Format::Avro(format),
            None,
            Arc::new(FixedSchemaResolver::new(
                1,
                Schema::parse_str(writer_schema).unwrap(),
            )),
        );

        let data = [0, 0, 0, 0, 1, 12, 65, 108, 121, 115, 115, 97, 0, 128, 4, 2];

        let v: Result<Vec<ArroyoAvroRoot>, _> =
            deserializer.deserialize_slice(&data[..]).await.collect();

        let v = v.unwrap();
        println!("{:?}", v);
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

        let mut deserializer = DataDeserializer::with_schema_resolver(
            Format::Avro(AvroFormat::new(false, true, true)),
            None,
            Arc::new(FailingSchemaResolver::new()),
        );

        let v: Result<Vec<RawJson>, _> = deserializer.deserialize_slice(&data[..]).await.collect();

        let v: Vec<_> = v
            .unwrap()
            .into_iter()
            .map(|t| serde_json::from_str::<serde_json::Value>(&t.value).unwrap())
            .collect();

        let expected = vec![
            json!({ "name": "Alyssa", "favorite_number": 256, "favorite_color": null }),
            json!({ "name": "Ben", "favorite_number": 7, "favorite_color": "red" }),
        ];

        assert_eq!(v, expected);
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

        let mut format = AvroFormat::new(false, false, true);
        format.add_reader_schema(Schema::parse_str(&schema_str).unwrap());
        let mut deserializer = DataDeserializer::new(Format::Avro(format), None);

        let v: Result<Vec<RawJson>, _> = deserializer.deserialize_slice(&data[..]).await.collect();

        let expected = json!({ "name": "Alyssa", "favorite_number": 256, "favorite_color": null });

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&v.unwrap()[0].value).unwrap(),
            expected
        );
    }
}
