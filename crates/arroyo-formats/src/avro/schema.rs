use anyhow::{anyhow, bail};
use apache_avro::Schema;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use arroyo_rpc::formats::AvroFormat;
use arroyo_types::ArroyoExtensionType;
use serde_json::json;
use std::sync::Arc;

/// Computes an avro schema from an arrow schema
pub fn to_avro(name: &str, fields: &Fields) -> Schema {
    let fields: Vec<_> = fields.iter().map(|f| field_to_avro(name, f)).collect();

    let schema = json!({
        "type": "record",
        "name": name,
        "fields": fields,
    });

    Schema::parse_str(&schema.to_string()).unwrap()
}

/// Computes an arrow schema from an avro schema
pub fn to_arrow(schema: &str) -> anyhow::Result<arrow_schema::Schema> {
    let schema =
        Schema::parse_str(schema).map_err(|e| anyhow!("avro schema is not valid: {:?}", e))?;

    let (dt, _, _) = to_arrow_datatype(&schema);
    let fields = match dt {
        DataType::Struct(fields) => fields,
        _ => {
            bail!("top-level schema must be a record")
        }
    };

    Ok(arrow_schema::Schema::new(fields))
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
                "items": field_to_avro("item", t),
            });
        }
        DataType::Struct(fields) => {
            let schema = to_avro(name, fields).canonical_form();
            return serde_json::from_str(&schema).unwrap();
        }
        DataType::Union(_, _) => unimplemented!("unions are not supported"),
        DataType::Dictionary(_, _) => unimplemented!("dictionaries are not supported"),
        DataType::Decimal128(_, _) => unimplemented!("decimal128 is not supported"),
        DataType::Decimal256(_, _) => unimplemented!("decimal256 is not supported"),
        DataType::Map(_, _) => unimplemented!("maps are not supported"),
        DataType::RunEndEncoded(_, _) => unimplemented!("run end encoded is not supported"),
        DataType::BinaryView => unimplemented!("binary view is not supported"),
        DataType::Utf8View => unimplemented!("utf8 view is not suported"),
        DataType::ListView(_) => unimplemented!("list view is not supported"),
        DataType::LargeListView(_) => unimplemented!("large list view is not suported"),
    };

    json!({
        "type": typ
    })
}

fn to_arrow_datatype(schema: &Schema) -> (DataType, bool, Option<ArroyoExtensionType>) {
    match schema {
        Schema::Null => (DataType::Null, false, None),
        Schema::Boolean => (DataType::Boolean, false, None),
        Schema::Int | Schema::TimeMillis => (DataType::Int32, false, None),
        Schema::Long => (DataType::Int64, false, None),
        Schema::TimeMicros => (DataType::Time64(TimeUnit::Microsecond), false, None),
        Schema::TimestampMillis | Schema::LocalTimestampMillis => (
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
            None,
        ),
        Schema::TimestampMicros | Schema::LocalTimestampMicros => (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
            None,
        ),
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
                let (dt, _, ext) = to_arrow_datatype(not_nulls[0]);
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
                    let (dt, nullable, extension) = to_arrow_datatype(&f.schema);
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
