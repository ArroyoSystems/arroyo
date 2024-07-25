use prost_reflect::{MessageDescriptor, FieldDescriptor, Kind, Cardinality};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use arroyo_types::ArroyoExtensionType;

fn protobuf_to_arrow_datatype(field: &FieldDescriptor, in_list: bool) -> (DataType, Option<ArroyoExtensionType>) {
    if field.is_list() && !in_list {
        let (dt, ext) = protobuf_to_arrow_datatype(field, true);
        return (DataType::List(Arc::new(ArroyoExtensionType::add_metadata(ext, Field::new(
            "item",
            dt,
            true
        )))), None);
    }
    (match field.kind() {
        Kind::Bool => DataType::Boolean,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => DataType::Int32,
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => DataType::Int64,
        Kind::Uint32 | Kind::Fixed32 => DataType::UInt32,
        Kind::Uint64 | Kind::Fixed64 => DataType::UInt64,
        Kind::Float => DataType::Float32,
        Kind::Double => DataType::Float64,
        Kind::String | Kind::Bytes => DataType::Utf8,
        Kind::Message(message) => {
            if field.is_map() {
                // we don't currently support maps so treat maps as raw json
                return (DataType::Utf8, Some(ArroyoExtensionType::JSON));
            } else {
                DataType::Struct(fields_for_message(&message).into())
            }
        }
        Kind::Enum(_) => DataType::Utf8,
    }, None)
}

fn fields_for_message(message: &MessageDescriptor) -> Vec<Arc<Field>> {
    message
        .fields()
        .map(|f| {
            let (t, ext) = protobuf_to_arrow_datatype(&f, false);
            Arc::new(ArroyoExtensionType::add_metadata(ext, Field::new(
                f.name(),
                t,
                is_nullable(&f),
            )))
        })
        .collect()
} 

/// Computes an Arrow schema from a protobuf schema
pub fn protobuf_to_arrow(proto_schema: MessageDescriptor) -> anyhow::Result<Schema> {
    let fields = fields_for_message(&proto_schema);
    Ok(Schema::new(fields))
}

fn is_nullable(field: &FieldDescriptor) -> bool {
    field.cardinality() == Cardinality::Optional || field.is_list() || field.is_map()
}