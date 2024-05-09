use arrow::datatypes::{Field, Fields, SchemaRef};
use arrow_array::builder::{ArrayBuilder, StringBuilder};
use arrow_schema::DataType;
use arroyo_rpc::formats::JsonFormat;
use serde_json::{json, Value};
use std::collections::HashMap;

pub mod schema;

pub fn deserialize_slice_json(
    schema: &SchemaRef,
    buffer: &mut [Box<dyn ArrayBuilder>],
    format: &JsonFormat,
    msg: &[u8],
) -> Result<(), String> {
    let msg = if format.confluent_schema_registry {
        &msg[5..]
    } else {
        msg
    };

    if format.unstructured {
        let (idx, _) = schema
            .column_with_name("value")
            .expect("no 'value' column for unstructured json format");
        let array = buffer[idx]
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .expect("'value' column has incorrect type");

        if format.include_schema {
            // we need to deserialize it to pull out the payload
            let v: Value = serde_json::from_slice(msg)
                .map_err(|e| format!("Failed to deserialize json: {:?}", e))?;
            let payload = v.get("payload").ok_or_else(|| {
                "`include_schema` set to true, but record does not have a payload field".to_string()
            })?;

            array.append_value(serde_json::to_string(payload).unwrap());
        } else {
            array.append_value(
                String::from_utf8(msg.to_vec()).map_err(|_| "data is not valid UTF-8")?,
            );
        };
    } else {
        serde_json::from_slice(msg)
            .map_err(|e| format!("Failed to deserialize JSON into schema: {:?}", e))?;
    }

    Ok(())
}

pub fn field_to_json_schema(field: &Field) -> Value {
    match field.data_type() {
        arrow::datatypes::DataType::Null => {
            json! {{ "type": "null" }}
        }
        arrow::datatypes::DataType::Boolean => {
            json! {{ "type": "boolean" }}
        }
        arrow::datatypes::DataType::Int8
        | arrow::datatypes::DataType::Int16
        | arrow::datatypes::DataType::Int32
        | arrow::datatypes::DataType::Int64
        | arrow::datatypes::DataType::UInt8
        | arrow::datatypes::DataType::UInt16
        | arrow::datatypes::DataType::UInt32
        | arrow::datatypes::DataType::UInt64 => {
            // TODO: integer bounds
            json! {{ "type": "integer" }}
        }
        arrow::datatypes::DataType::Float16
        | arrow::datatypes::DataType::Float32
        | arrow::datatypes::DataType::Float64 => {
            json! {{ "type": "number" }}
        }
        arrow::datatypes::DataType::Timestamp(_, _) => {
            json! {{ "type": "string", "format": "date-time" }}
        }
        arrow::datatypes::DataType::Date32
        | arrow::datatypes::DataType::Date64
        | arrow::datatypes::DataType::Time32(_)
        | arrow::datatypes::DataType::Time64(_) => {
            todo!()
        }
        arrow::datatypes::DataType::Duration(_) => todo!(),
        arrow::datatypes::DataType::Interval(_) => todo!(),
        arrow::datatypes::DataType::Binary
        | arrow::datatypes::DataType::FixedSizeBinary(_)
        | arrow::datatypes::DataType::LargeBinary => {
            json! {{ "type": "array", "items": { "type": "integer" }}}
        }
        arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
            json! {{ "type": "string" }}
        }
        arrow::datatypes::DataType::List(t)
        | arrow::datatypes::DataType::FixedSizeList(t, _)
        | arrow::datatypes::DataType::LargeList(t) => {
            json! {{"type": "array", "items": field_to_json_schema(t) }}
        }
        arrow::datatypes::DataType::Struct(s) => arrow_to_json_schema(s),
        arrow::datatypes::DataType::Union(_, _) => todo!(),
        arrow::datatypes::DataType::Dictionary(_, _) => todo!(),
        arrow::datatypes::DataType::Decimal128(_, _) => todo!(),
        arrow::datatypes::DataType::Decimal256(_, _) => todo!(),
        arrow::datatypes::DataType::Map(_, _) => todo!(),
        arrow::datatypes::DataType::RunEndEncoded(_, _) => todo!(),
        DataType::BinaryView => todo!(),
        DataType::Utf8View => todo!(),
        DataType::ListView(_) => todo!(),
        DataType::LargeListView(_) => todo!(),
    }
}

pub fn arrow_to_json_schema(fields: &Fields) -> Value {
    let props: HashMap<String, Value> = fields
        .iter()
        .map(|f| (f.name().clone(), field_to_json_schema(f)))
        .collect();

    let required: Vec<String> = fields
        .iter()
        .filter(|f| !f.is_nullable())
        .map(|f| f.name().clone())
        .collect();
    json! {{
        "type": "object",
        "properties": props,
        "required": required,
    }}
}

pub fn field_to_kafka_json(field: &Field) -> Value {
    use arrow::datatypes::DataType::*;

    let typ = match field.data_type() {
        Null => todo!(),
        Boolean => "boolean",
        Int8 | UInt8 => "int8",
        Int16 | UInt16 => "int16",
        Int32 | UInt32 => "int32",
        Int64 | UInt64 => "int64",
        Float16 | Float32 => "float",
        Float64 => "double",
        Utf8 | LargeUtf8 => "string",
        Binary | FixedSizeBinary(_) | LargeBinary => "bytes",
        Time32(_) | Time64(_) | Timestamp(_, _) => {
            // as far as I can tell, this is the only way to get timestamps from Arroyo into
            // Kafka connect
            return json! {{
                "type": "int64",
                "field": field.name().clone(),
                "optional": field.is_nullable(),
                "name": "org.apache.kafka.connect.data.Timestamp"
            }};
        }
        Date32 | Date64 => {
            return json! {{
                "type": "int64",
                "field": field.name().clone(),
                "optional": field.is_nullable(),
                "name": "org.apache.kafka.connect.data.Date"
            }}
        }
        Duration(_) => "int64",
        Interval(_) => "int64",
        List(t) | FixedSizeList(t, _) | LargeList(t) => {
            return json! {{
                "type": "array",
                "items": field_to_kafka_json(t),
                "field": field.name().clone(),
                "optional": field.is_nullable(),
            }};
        }
        Struct(s) => {
            let fields: Vec<_> = s.iter().map(|f| field_to_kafka_json(f)).collect();
            return json! {{
                "type": "struct",
                "fields": fields,
                "field": field.name().clone(),
                "optional": field.is_nullable(),
            }};
        }
        Union(_, _) => todo!(),
        Dictionary(_, _) => todo!(),
        Decimal128(_, _) => todo!(),
        Decimal256(_, _) => todo!(),
        Map(_, _) => todo!(),
        RunEndEncoded(_, _) => todo!(),
        BinaryView => todo!(),
        Utf8View => todo!(),
        ListView(_) => todo!(),
        LargeListView(_) => todo!(),
    };

    json! {{
        "type": typ,
        "field": field.name().clone(),
        "optional": field.is_nullable(),
    }}
}

// For some reason Kafka uses it's own bespoke almost-but-not-quite JSON schema format
// https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/#json-schemas
pub fn arrow_to_kafka_json(name: &str, fields: &Fields) -> Value {
    let fields: Vec<_> = fields.iter().map(|f| field_to_kafka_json(f)).collect();
    json! {{
        "type": "struct",
        "name": name,
        "fields": fields,
        "optional": false,
    }}
}
