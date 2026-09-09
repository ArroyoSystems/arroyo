use crate::float_to_json;
use arrow_schema::{DataType, Field, Fields};
use arroyo_rpc::errors::{DataflowError, DataflowResult, SourceError};
use arroyo_rpc::formats::DecimalEncoding;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use rmpv::Value as MsgPackValue;
use serde_json::{Map as JsonMap, Number, Value as JsonValue};
use std::io::Cursor;

pub(crate) fn msgpack_to_json(msg: &[u8]) -> DataflowResult<JsonValue> {
    let mut cursor = Cursor::new(msg);
    let value = rmpv::decode::read_value(&mut cursor)
        .map_err(|e| SourceError::bad_data(format!("failed to deserialize MsgPack: {e:?}")))?;

    if cursor.position() != msg.len() as u64 {
        return Err(SourceError::bad_data(
            "MsgPack message contains trailing bytes",
        ));
    }

    value_to_json(value)
}

pub(crate) fn value_to_json(value: MsgPackValue) -> DataflowResult<JsonValue> {
    Ok(match value {
        MsgPackValue::Nil => JsonValue::Null,
        MsgPackValue::Boolean(b) => JsonValue::Bool(b),
        MsgPackValue::Integer(i) => integer_to_json(i)?,
        MsgPackValue::F32(f) => float_to_json(f as f64),
        MsgPackValue::F64(f) => float_to_json(f),
        MsgPackValue::String(s) => JsonValue::String(msgpack_string_to_string(s)?),
        MsgPackValue::Binary(b) => JsonValue::String(BASE64_STANDARD.encode(b)),
        MsgPackValue::Array(a) => JsonValue::Array(
            a.into_iter()
                .map(value_to_json)
                .collect::<Result<Vec<_>, DataflowError>>()?,
        ),
        MsgPackValue::Map(m) => {
            let mut obj = JsonMap::new();
            for (key, value) in m {
                let key = match key {
                    MsgPackValue::String(s) => msgpack_string_to_string(s)?,
                    _ => {
                        return Err(SourceError::bad_data(
                            "MsgPack map keys must be strings to decode into Arroyo rows",
                        ));
                    }
                };
                obj.insert(key, value_to_json(value)?);
            }
            JsonValue::Object(obj)
        }
        MsgPackValue::Ext(_, _) => {
            return Err(SourceError::bad_data(
                "MsgPack extension values are not supported",
            ));
        }
    })
}

fn msgpack_string_to_string(s: rmpv::Utf8String) -> DataflowResult<String> {
    s.as_str()
        .map(ToString::to_string)
        .ok_or_else(|| SourceError::bad_data("MsgPack string is not valid UTF-8"))
}

fn integer_to_json(i: rmpv::Integer) -> DataflowResult<JsonValue> {
    if let Some(i) = i.as_i64() {
        Ok(JsonValue::Number(Number::from(i)))
    } else if let Some(u) = i.as_u64() {
        Ok(JsonValue::Number(Number::from(u)))
    } else {
        Err(SourceError::bad_data(
            "MsgPack integer is too large to represent in JSON",
        ))
    }
}

pub(crate) fn json_to_msgpack(
    value: &JsonValue,
    field: Option<&Field>,
    decimal_encoding: DecimalEncoding,
) -> Result<MsgPackValue, String> {
    if value.is_null() {
        return Ok(MsgPackValue::Nil);
    }

    let data_type = field.map(Field::data_type);
    match data_type {
        Some(DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)) => {
            return json_base64_to_msgpack_binary(value, "binary field");
        }
        Some(DataType::Decimal128(_, _) | DataType::Decimal256(_, _))
            if matches!(decimal_encoding, DecimalEncoding::Bytes) =>
        {
            return json_base64_to_msgpack_binary(value, "decimal field");
        }
        Some(DataType::Struct(fields)) => {
            return json_object_to_msgpack(value, fields, decimal_encoding);
        }
        Some(
            DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _),
        ) => {
            return json_array_to_msgpack(value, Some(field.as_ref()), decimal_encoding);
        }
        _ => {}
    }

    match value {
        JsonValue::Null => Ok(MsgPackValue::Nil),
        JsonValue::Bool(b) => Ok(MsgPackValue::from(*b)),
        JsonValue::Number(n) => json_number_to_msgpack(n),
        JsonValue::String(s) => Ok(MsgPackValue::from(s.as_str())),
        JsonValue::Array(_) => json_array_to_msgpack(value, None, decimal_encoding),
        JsonValue::Object(_) => json_object_to_msgpack(value, &Fields::empty(), decimal_encoding),
    }
}

fn json_object_to_msgpack(
    value: &JsonValue,
    fields: &Fields,
    decimal_encoding: DecimalEncoding,
) -> Result<MsgPackValue, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "expected JSON object for MessagePack map".to_string())?;

    let mut entries = Vec::with_capacity(obj.len());
    if fields.is_empty() {
        for (key, value) in obj {
            entries.push((
                MsgPackValue::from(key.as_str()),
                json_to_msgpack(value, None, decimal_encoding)?,
            ));
        }
    } else {
        for field in fields {
            if let Some(value) = obj.get(field.name()) {
                entries.push((
                    MsgPackValue::from(field.name().as_str()),
                    json_to_msgpack(value, Some(field), decimal_encoding)?,
                ));
            }
        }
    }

    Ok(MsgPackValue::Map(entries))
}

fn json_array_to_msgpack(
    value: &JsonValue,
    item_field: Option<&Field>,
    decimal_encoding: DecimalEncoding,
) -> Result<MsgPackValue, String> {
    let array = value
        .as_array()
        .ok_or_else(|| "expected JSON array for MessagePack array".to_string())?;

    Ok(MsgPackValue::Array(
        array
            .iter()
            .map(|v| json_to_msgpack(v, item_field, decimal_encoding))
            .collect::<Result<Vec<_>, String>>()?,
    ))
}

fn json_base64_to_msgpack_binary(value: &JsonValue, context: &str) -> Result<MsgPackValue, String> {
    let s = value
        .as_str()
        .ok_or_else(|| format!("expected base64 string for {context}"))?;
    let bytes = BASE64_STANDARD
        .decode(s)
        .map_err(|e| format!("invalid base64 string for {context}: {e}"))?;
    Ok(MsgPackValue::Binary(bytes))
}

fn json_number_to_msgpack(n: &Number) -> Result<MsgPackValue, String> {
    if let Some(i) = n.as_i64() {
        Ok(MsgPackValue::from(i))
    } else if let Some(u) = n.as_u64() {
        Ok(MsgPackValue::from(u))
    } else if let Some(f) = n.as_f64() {
        Ok(MsgPackValue::from(f))
    } else {
        Err("JSON number cannot be represented as MessagePack".to_string())
    }
}
