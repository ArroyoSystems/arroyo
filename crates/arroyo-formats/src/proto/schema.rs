use std::path::Path;
use prost_reflect::{MessageDescriptor, FieldDescriptor, Kind, Cardinality};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use anyhow::bail;
use protobuf_native::compiler::{SimpleErrorCollector, SourceTreeDescriptorDatabase, VirtualSourceTree};
use protobuf_native::{DescriptorDatabase, MessageLite};
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
pub fn protobuf_to_arrow(proto_schema: &MessageDescriptor) -> anyhow::Result<Schema> {
    let fields = fields_for_message(proto_schema);
    Ok(Schema::new(fields))
}


fn is_nullable(field: &FieldDescriptor) -> bool { 
    field.cardinality() == Cardinality::Optional || field.is_list() || field.is_map()
}

pub fn schema_file_to_descriptor(schema: &str) -> anyhow::Result<Vec<u8>> {
    let mut source_tree = VirtualSourceTree::new();
    source_tree.as_mut().add_file(Path::new("schema.proto"), schema.as_bytes().to_vec());

    let mut error_collector = SimpleErrorCollector::new();
    let mut db = SourceTreeDescriptorDatabase::new(source_tree.as_mut());
    db.as_mut().record_errors_to(error_collector.as_mut());
    let res = db.as_mut().find_file_by_name(Path::new("schema.proto")).unwrap();
    drop(db);
    let errors: Vec<_> = error_collector.as_mut().collect();
    if !errors.is_empty() {
        bail!("errors parsing proto file:\n{}", 
            errors.iter().map(|e| format!("  * {}", e)).collect::<Vec<_>>().join("\n"))
    }
    Ok(res.serialize().unwrap())
}