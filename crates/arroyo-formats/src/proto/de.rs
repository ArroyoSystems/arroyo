use crate::float_to_json;
use arroyo_rpc::formats::ProtobufFormat;
use arroyo_types::SourceError;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use prost_reflect::{DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MapKey, Value};
use serde_json::Value as JsonValue;

pub(crate) fn deserialize_proto(
    pool: &mut DescriptorPool,
    proto: &ProtobufFormat,
    mut msg: &[u8],
) -> Result<serde_json::Value, SourceError> {
    if proto.confluent_schema_registry {
        msg = &msg[5..];
    }

    let message = proto.message_name.as_ref().expect("no message name");
    let descriptor = pool.get_message_by_name(message).expect("no descriptor");

    let msg = DynamicMessage::decode(descriptor, msg)
        .map_err(|e| SourceError::bad_data(format!("failed to deserialize protobuf: {:?}", e)))?;

    Ok(proto_to_json(&msg))
}

pub(crate) fn proto_to_json(message: &DynamicMessage) -> JsonValue {
    let mut obj = serde_json::Map::new();

    for (field, value) in message.fields() {
        obj.insert(field.name().to_string(), proto_value_to_json(&field, value));
    }

    JsonValue::Object(obj)
}

fn proto_value_to_json(field: &FieldDescriptor, value: &Value) -> JsonValue {
    match value {
        Value::Bool(b) => JsonValue::Bool(*b),
        Value::I32(i) => JsonValue::Number((*i).into()),
        Value::I64(i) => JsonValue::Number((*i).into()),
        Value::U32(u) => JsonValue::Number((*u).into()),
        Value::U64(u) => JsonValue::Number((*u).into()),
        Value::F32(f) => float_to_json(*f as f64),
        Value::F64(f) => float_to_json(*f),
        Value::String(s) => JsonValue::String(s.clone()),
        Value::Bytes(b) => JsonValue::String(BASE64_STANDARD.encode(b)),
        Value::EnumNumber(i) => JsonValue::String(
            (match field.kind() {
                Kind::Enum(desc) => desc.get_value(*i).map(|v| v.name().to_string()),
                _ => None,
            })
            .unwrap_or_default(),
        ),
        Value::Message(m) => proto_to_json(m),
        Value::List(l) => {
            JsonValue::Array(l.iter().map(|v| proto_value_to_json(field, v)).collect())
        }
        Value::Map(m) => {
            let value_field = match field.kind() {
                Kind::Message(m) => {
                    if m.is_map_entry() {
                        m.map_entry_value_field()
                    } else {
                        return JsonValue::Null;
                    }
                }
                _ => {
                    return JsonValue::Null;
                }
            };
            let mut map = serde_json::Map::new();
            for (k, v) in m {
                let key = match k {
                    MapKey::Bool(b) => b.to_string(),
                    MapKey::I32(i) => i.to_string(),
                    MapKey::I64(i) => i.to_string(),
                    MapKey::U32(u) => u.to_string(),
                    MapKey::U64(u) => u.to_string(),
                    MapKey::String(s) => s.to_string(),
                };
                map.insert(key, proto_value_to_json(&value_field, v));
            }
            JsonValue::Object(map)
        }
    }
}
