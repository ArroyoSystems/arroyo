use crate::SchemaData;
use anyhow::{anyhow, bail};
use apache_avro::types::{Record, Value as AvroValue, Value};
pub use apache_avro::Schema;
use apache_avro::{from_avro_datum, AvroResult, Reader, Writer};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    BooleanType, Float16Type, Float32Type, Float64Type, Int32Type, Int64Type, Int8Type,
    TimestampNanosecondType, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch, TimestampNanosecondArray};
use arroyo_rpc::formats::AvroFormat;
use arroyo_rpc::schema_resolver::SchemaResolver;
use arroyo_types::{from_nanos, to_micros, to_nanos, ArroyoExtensionType, SourceError};
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
        let schema = if registry.contains_key(&id) {
            registry.get(&id).unwrap()
        } else {
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
            registry.insert(id, new_schema);

            registry.get(&id).unwrap()
        };

        let mut buf = &msg[..];
        vec![from_avro_datum(
            schema,
            &mut buf,
            format.reader_schema.as_ref().map(|t| t.into()),
        )]
    } else {
        Reader::new(&msg[..])
            .map_err(|e| SourceError::bad_data(format!("invalid Avro schema in message: {:?}", e)))?
            .collect()
    };
    Ok(messages)
}

pub fn to_vec<T: SchemaData>(
    record: &T,
    format: &AvroFormat,
    schema: &Schema,
    version: Option<u32>,
) -> Vec<u8> {
    let v = record.to_avro(schema);

    if format.raw_datums || format.confluent_schema_registry {
        let record =
            apache_avro::to_avro_datum(schema, v.clone()).expect("avro serialization failed");
        if format.confluent_schema_registry {
            // TODO: this would be more efficient if we could use the internal write_avro_datum to avoid
            // allocating the buffer twice
            let mut buf = Vec::with_capacity(record.len() + 5);
            buf.push(0);
            buf.extend(
                version
                    .expect("no schema version for confluent schema avro")
                    .to_be_bytes(),
            );
            buf.extend(record);
            buf
        } else {
            record
        }
    } else {
        let mut buf = Vec::with_capacity(128);
        let mut writer = Writer::new(schema, &mut buf);
        writer.append(v).unwrap();
        buf
    }
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

fn arrow_to_avro(name: &str, dt: &DataType) -> serde_json::value::Value {
    let typ = match dt {
        DataType::Null => unreachable!("null fields are not supported"),
        DataType::Boolean => "boolean",
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            "int"
        }
        // TODO: not all values of u64 can be represented as a long in avro
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "long",
        DataType::Float16 | DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Timestamp(t, tz) => {
            let logical = match (t, tz) {
                (TimeUnit::Microsecond | TimeUnit::Nanosecond, None) => "timestamp-micros",
                (TimeUnit::Microsecond | TimeUnit::Nanosecond, Some(_)) => "local-timestamp-micros",
                (TimeUnit::Millisecond | TimeUnit::Second, None) => "timestamp-millis",
                (TimeUnit::Millisecond | TimeUnit::Second, Some(_)) => "local-timestamp-millis",
            };

            return json!({
                "type": "long",
                "logicalType": logical
            });
        }
        DataType::Date32 | DataType::Date64 => {
            return json!({
                "type": "int",
                "logicalType": "date"
            });
        }
        DataType::Time64(_) | DataType::Time32(_) => {
            todo!("time is not supported")
        }
        DataType::Duration(_) => todo!("duration is not supported"),
        DataType::Interval(_) => todo!("interval is not supported"),
        DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => "bytes",
        DataType::Utf8 | DataType::LargeUtf8 => "string",
        DataType::List(t) | DataType::FixedSizeList(t, _) | DataType::LargeList(t) => {
            return json!({
                "type": "array",
                "items": arrow_to_avro(name, t.data_type())
            });
        }
        DataType::Struct(fields) => {
            let schema = arrow_to_avro_schema(name, fields).canonical_form();
            return serde_json::from_str(&schema).unwrap();
        }
        DataType::Union(_, _) => unimplemented!("unions are not supported"),
        DataType::Dictionary(_, _) => unimplemented!("dictionaries are not supported"),
        DataType::Decimal128(_, _) => unimplemented!("decimal128 is not supported"),
        DataType::Decimal256(_, _) => unimplemented!("decimal256 is not supported"),
        DataType::Map(_, _) => unimplemented!("maps are not supported"),
        DataType::RunEndEncoded(_, _) => unimplemented!("run end encoded is not supported"),
    };

    json!({
        "type": typ
    })
}

fn field_to_avro(name: &str, field: &Field) -> serde_json::value::Value {
    let next_name = format!("{}_{}", name, &field.name());
    let mut schema = arrow_to_avro(&next_name, field.data_type());

    if field.is_nullable() {
        schema = json!({
            "type": ["null", schema]
        })
    }

    json!({
        "name": AvroFormat::sanitize_field(field.name()),
        "type": schema
    })
}

/// Computes an avro schema from an arrow schema
pub fn arrow_to_avro_schema(name: &str, fields: &Fields) -> Schema {
    let fields: Vec<_> = fields.iter().map(|f| field_to_avro(name, &**f)).collect();

    let schema = json!({
        "type": "record",
        "name": name,
        "fields": fields,
    });

    Schema::parse_str(&schema.to_string()).unwrap()
}

pub fn to_arrow(name: &str, schema: &str) -> anyhow::Result<arrow_schema::Schema> {
    let schema =
        Schema::parse_str(schema).map_err(|e| anyhow!("avro schema is not valid: {:?}", e))?;

    let (dt, _, _) = to_arrow_datatype(name, &schema);
    let fields = match dt {
        DataType::Struct(fields) => fields,
        _ => {
            bail!("top-level schema must be a record")
        }
    };

    Ok(arrow_schema::Schema::new(fields))
}

fn serialize_column(
    schema: &Schema,
    values: &mut Vec<Option<Record>>,
    name: &str,
    column: &ArrayRef,
    nullable: bool,
) {
    macro_rules! write_arrow_value {
        ($as_call:path, $value_variant:path, $converter:expr) => {{
            $as_call(column)
                .iter()
                .zip(values.iter_mut())
                .for_each(|(v, r)| {
                    if let Some(r) = r.as_mut() {
                        if nullable {
                            r.put(
                                name,
                                Value::Union(
                                    v.is_some() as u32,
                                    Box::new(
                                        v.map(|v| $value_variant($converter(v)))
                                            .unwrap_or(Value::Null),
                                    ),
                                ),
                            );
                        } else {
                            r.put(name, $value_variant($converter(v.expect("cannot be none"))));
                        }
                    }
                })
        }};
    }

    macro_rules! write_primitive {
        ($primitive_type:ty, $rust_type:ty, $value_variant:path) => {
            write_arrow_value!(
                ArrayRef::as_primitive::<$primitive_type>,
                $value_variant,
                |v| Into::<$rust_type>::into(v)
            )
        };
    }

    match column.data_type() {
        DataType::Utf8 => {
            write_arrow_value!(ArrayRef::as_string::<i32>, Value::String, |v: &str| v
                .into())
        }
        DataType::Boolean => write_arrow_value!(ArrayRef::as_boolean, Value::Boolean, |v| v),

        DataType::Int8 => write_primitive!(Int8Type, i32, Value::Int),
        DataType::Int32 => write_primitive!(Int32Type, i32, Value::Int),
        DataType::Int64 => write_primitive!(Int64Type, i64, Value::Long),

        DataType::UInt8 => write_primitive!(UInt8Type, i32, Value::Int),
        DataType::UInt32 => write_primitive!(UInt32Type, i64, Value::Long),
        DataType::UInt64 => {
            write_arrow_value!(ArrayRef::as_primitive::<UInt64Type>, Value::Long, |v| v
                as i64)
        }

        DataType::Float16 => write_primitive!(Float16Type, f32, Value::Float),
        DataType::Float32 => write_primitive!(Float32Type, f32, Value::Float),
        DataType::Float64 => write_primitive!(Float64Type, f64, Value::Double),

        DataType::Timestamp(TimeUnit::Nanosecond, _) => write_arrow_value!(
            ArrayRef::as_primitive::<TimestampNanosecondType>,
            Value::TimestampMicros,
            |v| to_micros(from_nanos(v as u128)) as i64
        ),

        DataType::Date32 => {
            write_arrow_value!(ArrayRef::as_primitive::<Int32Type>, Value::Date, |v| v)
        }
        DataType::Date64 => write_arrow_value!(
            ArrayRef::as_primitive::<Int64Type>,
            Value::Date,
            |v| (v / 86400000) as i32
        ),

        DataType::Binary => {
            write_arrow_value!(ArrayRef::as_binary::<i32>, Value::Bytes, |v: &[u8]| v
                .to_vec())
        }

        DataType::Struct(fields) => {
            let Schema::Record(record_schema) = schema else {
                panic!(
                    "invalid avro schema -- struct field {name} should correspond to record schema"
                );
            };

            let record_field_number = record_schema.lookup.get(name).unwrap();
            let schema = &record_schema.fields[*record_field_number].schema;

            if nullable {
                let Schema::Union(__union_schema) = schema else {
                    panic!(
                        "invalid avro schema -- struct field {name} is nullable and should be represented by a union"
                    );
                };
                let schema = __union_schema
                    .variants()
                    .get(1)
                    .unwrap_or_else(||
                        panic!("invalid avro schema -- struct field {name} should be a union with two variants")
                    );
                let nulls = column.nulls().unwrap();

                let mut struct_values = nulls
                    .iter()
                    .map(|null| null.then(|| Record::new(schema).unwrap()))
                    .collect::<Vec<_>>();

                for (field, column) in fields.iter().zip(column.as_struct().columns()) {
                    let name = AvroFormat::sanitize_field(field.name());

                    serialize_column(
                        schema,
                        &mut struct_values,
                        &name,
                        column,
                        field.is_nullable(),
                    );
                }

                for ((struct_v, outer_v), notnull) in struct_values
                    .into_iter()
                    .zip(values.iter_mut())
                    .zip(column.nulls().unwrap().iter())
                {
                    if let Some(outer_v) = outer_v.as_mut() {
                        outer_v.put(
                            name,
                            Value::Union(
                                notnull as u32,
                                Box::new(if notnull {
                                    struct_v.expect("should not be null").into()
                                } else {
                                    Value::Null
                                }),
                            ),
                        );
                    }
                }
            } else {
                let mut struct_values = (0..column.len())
                    .map(|_| Some(Record::new(schema).unwrap()))
                    .collect::<Vec<_>>();

                for (field, column) in fields.iter().zip(column.as_struct().columns()) {
                    let name = AvroFormat::sanitize_field(field.name());

                    serialize_column(
                        schema,
                        &mut struct_values,
                        &name,
                        column,
                        field.is_nullable(),
                    );
                }

                for (struct_v, outer_v) in struct_values.into_iter().zip(values.iter_mut()) {
                    if let Some(outer_v) = outer_v.as_mut() {
                        outer_v.put(name, Into::<Value>::into(struct_v.expect("not null")));
                    }
                }
            }
        }

        _ => unimplemented!("unsupported data type: {}", column.data_type()),
    };
}

pub fn serialize(schema: &Schema, batch: &RecordBatch) -> Vec<Value> {
    let mut values = (0..batch.num_rows())
        .map(|_| Some(Record::new(schema).unwrap()))
        .collect::<Vec<_>>();

    for i in 0..batch.num_columns() {
        let column = batch.column(i);
        let field = &batch.schema().fields[i];

        let name = AvroFormat::sanitize_field(field.name());
        serialize_column(schema, &mut values, &name, column, field.is_nullable());
    }

    values
        .into_iter()
        .filter_map(|f| f)
        .map(|r| r.into())
        .collect()
}

fn to_arrow_datatype(
    source_name: &str,
    schema: &Schema,
) -> (DataType, bool, Option<ArroyoExtensionType>) {
    match schema {
        Schema::Null => (DataType::Null, false, None),
        Schema::Boolean => (DataType::Boolean, false, None),
        Schema::Int | Schema::TimeMillis => (DataType::Int32, false, None),
        Schema::Long
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros => (DataType::Int64, false, None),
        Schema::Float => (DataType::Float32, false, None),
        Schema::Double => (DataType::Float64, false, None),
        Schema::Bytes | Schema::Fixed(_) | Schema::Decimal(_) => (DataType::Binary, false, None),
        Schema::String | Schema::Enum(_) | Schema::Uuid => (DataType::Utf8, false, None),
        Schema::Union(union) => {
            // currently just support unions that have [t, null] as variants, which is the
            // avro way to represent optional fields

            let (nulls, not_nulls): (Vec<_>, Vec<_>) = union
                .variants()
                .iter()
                .partition(|v| matches!(v, Schema::Null));

            if nulls.len() == 1 && not_nulls.len() == 1 {
                let (dt, _, ext) = to_arrow_datatype(source_name, not_nulls[0]);
                (dt, true, ext)
            } else {
                (DataType::Utf8, false, Some(ArroyoExtensionType::JSON))
            }
        }
        Schema::Record(record) => {
            let fields = record
                .fields
                .iter()
                .map(|f| {
                    let (dt, nullable, extension) = to_arrow_datatype(source_name, &f.schema);
                    Arc::new(ArroyoExtensionType::add_metadata(
                        extension,
                        Field::new(&f.name, dt, nullable),
                    ))
                })
                .collect();

            (DataType::Struct(fields), false, None)
        }
        _ => (DataType::Utf8, false, Some(ArroyoExtensionType::JSON)),
    }
}

#[cfg(test)]
mod tests {
    use super::{arrow_to_avro_schema, serialize, to_arrow, to_vec};
    use crate::{avro, ArrowDeserializer, SchemaData};
    use arrow_array::builder::{make_builder, ArrayBuilder, StringBuilder, StructBuilder};
    use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, FieldRef, Schema, TimeUnit};
    use arroyo_rpc::df::ArroyoSchema;
    use arroyo_rpc::formats::{AvroFormat, BadData, Format};
    use arroyo_rpc::schema_resolver::{FailingSchemaResolver, FixedSchemaResolver, SchemaResolver};
    use arroyo_rpc::TIMESTAMP_FIELD;
    use arroyo_types::to_nanos;
    use arroyo_types::ArrowMessage::Data;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

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
                "source",
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
                avro::Schema::parse_str(schema).unwrap(),
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
            .deserialize_slice(&mut builders, &message, SystemTime::now())
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
        format.add_reader_schema(avro::Schema::parse_str(SCHEMA).unwrap());
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
        format.add_reader_schema(avro::Schema::parse_str(reader_schema).unwrap());

        let schema = avro::Schema::parse_str(writer_schema).unwrap();
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
            &apache_avro::to_avro_datum(&avro::Schema::parse_str(writer_schema).unwrap(), value)
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

        let schema = avro::Schema::parse_str(writer_schema).unwrap();
        let mut value = apache_avro::types::Record::new(&schema).unwrap();
        value.put(
            "name",
            apache_avro::types::Value::String("Alyssa".to_string()),
        );
        value.put("favorite_number", apache_avro::types::Value::Int(256));

        let mut bytes = vec![0, 0, 0, 0, 1];
        bytes.extend_from_slice(
            &apache_avro::to_avro_datum(&avro::Schema::parse_str(writer_schema).unwrap(), value)
                .unwrap(),
        );

        let mut format = AvroFormat::new(true, false, false);
        format.add_reader_schema(avro::Schema::parse_str(reader_schema).unwrap());

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
        format.add_reader_schema(avro::Schema::parse_str(&schema_str).unwrap());
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

    #[test]
    fn test_writing() {
        use apache_avro::types::Value::*;

        let address_fields = vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ];

        let second_address_fields = vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ];

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("favorite_number", DataType::Int32, false),
            Field::new("favorite_color", DataType::Utf8, true),
            Field::new(
                "address",
                DataType::Struct(address_fields.clone().into()),
                false,
            ),
            Field::new(
                "second_address",
                DataType::Struct(second_address_fields.clone().into()),
                true,
            ),
        ]));

        let names = vec!["Alyssa", "Ben", "Charlie"];
        let favorite_numbers = vec![256, 7, 0];
        let favorite_colors = vec![None, Some("red"), None];

        let mut address_builder = StructBuilder::from_fields(address_fields, 3);
        let mut second_address_builder = StructBuilder::from_fields(second_address_fields, 3);

        address_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("123 Elm St");
        address_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("Springfield");
        address_builder.append(true);
        second_address_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("321 Pine St");
        second_address_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("Sacramento");
        second_address_builder
            .field_builder::<StringBuilder>(2)
            .unwrap()
            .append_null();
        second_address_builder.append(true);

        address_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("456 Oak St");
        address_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("Boston");
        address_builder.append(true);
        second_address_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("645 Glen Ave");
        second_address_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("Santa Cruz");
        second_address_builder
            .field_builder::<StringBuilder>(2)
            .unwrap()
            .append_value("Ben");
        second_address_builder.append(true);

        address_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("789 Pine St");
        address_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("Calgary");
        address_builder.append(true);
        second_address_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_null();
        second_address_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_null();
        second_address_builder
            .field_builder::<StringBuilder>(2)
            .unwrap()
            .append_null();
        second_address_builder.append(false);

        let avro_schema = arrow_to_avro_schema("User", &arrow_schema.fields);

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(arrow_array::StringArray::from(names)),
                Arc::new(arrow_array::Int32Array::from(favorite_numbers)),
                Arc::new(arrow_array::StringArray::from(favorite_colors)),
                Arc::new(address_builder.finish()),
                Arc::new(second_address_builder.finish()),
            ],
        )
        .unwrap();

        let result: Vec<apache_avro::types::Value> = serialize(&avro_schema, &batch);

        assert_eq!(
            result,
            vec![
                Record(vec![
                    ("name".to_string(), String("Alyssa".to_string())),
                    ("favorite_number".to_string(), Int(256)),
                    ("favorite_color".to_string(), Union(0, Box::new(Null))),
                    (
                        "address".to_string(),
                        Record(vec![
                            ("street".to_string(), String("123 Elm St".to_string())),
                            ("city".to_string(), String("Springfield".to_string())),
                        ])
                    ),
                    (
                        "second_address".to_string(),
                        Union(
                            1,
                            Box::new(Record(vec![
                                ("street".to_string(), String("321 Pine St".to_string())),
                                ("city".to_string(), String("Sacramento".to_string())),
                                ("name".to_string(), Union(0, Box::new(Null))),
                            ]))
                        )
                    )
                ]),
                Record(vec![
                    ("name".to_string(), String("Ben".to_string())),
                    ("favorite_number".to_string(), Int(7)),
                    (
                        "favorite_color".to_string(),
                        Union(1, Box::new(String("red".to_string())))
                    ),
                    (
                        "address".to_string(),
                        Record(vec![
                            ("street".to_string(), String("456 Oak St".to_string())),
                            ("city".to_string(), String("Boston".to_string())),
                        ])
                    ),
                    (
                        "second_address".to_string(),
                        Union(
                            1,
                            Box::new(Record(vec![
                                ("street".to_string(), String("645 Glen Ave".to_string())),
                                ("city".to_string(), String("Santa Cruz".to_string())),
                                (
                                    "name".to_string(),
                                    Union(1, Box::new(String("Ben".to_string())))
                                ),
                            ]))
                        )
                    )
                ]),
                Record(vec![
                    ("name".to_string(), String("Charlie".to_string())),
                    ("favorite_number".to_string(), Int(0)),
                    ("favorite_color".to_string(), Union(0, Box::new(Null))),
                    (
                        "address".to_string(),
                        Record(vec![
                            ("street".to_string(), String("789 Pine St".to_string())),
                            ("city".to_string(), String("Calgary".to_string())),
                        ])
                    ),
                    ("second_address".to_string(), Union(0, Box::new(Null)))
                ]),
            ]
        )
    }
}
