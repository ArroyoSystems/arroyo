use prost_reflect::{DescriptorPool};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use arroyo_types::ArroyoExtensionType;
use crate::proto::schema::protobuf_to_arrow;

#[test]
fn test_basic_types() {
    let pool = DescriptorPool::decode(include_bytes!("protos/basic_types.bin").as_ref()).unwrap();
    let message = pool.all_messages().next().unwrap();
    let arrow_schema = protobuf_to_arrow(message).unwrap();

    assert_eq!(arrow_schema.fields().len(), 7);
    assert_field(&arrow_schema, "bool_field", DataType::Boolean, true);
    assert_field(&arrow_schema, "int32_field", DataType::Int32, true);
    assert_field(&arrow_schema, "int64_field", DataType::Int64, true);
    assert_field(&arrow_schema, "uint32_field", DataType::UInt32, true);
    assert_field(&arrow_schema, "uint64_field", DataType::UInt64, true);
    assert_field(&arrow_schema, "float_field", DataType::Float32, true);
    assert_field(&arrow_schema, "double_field", DataType::Float64, true);
}

#[test]
fn test_string_and_bytes() {
    let pool = DescriptorPool::decode(include_bytes!("protos/string_and_bytes.bin").as_ref()).unwrap();
    let message = pool.all_messages().next().unwrap();
    let arrow_schema = protobuf_to_arrow(message).unwrap();

    assert_eq!(arrow_schema.fields().len(), 2);
    assert_field(&arrow_schema, "string_field", DataType::Utf8, true);
    assert_field(&arrow_schema, "bytes_field", DataType::Utf8, true);
}

#[test]
fn test_nested_message() {
    let pool = DescriptorPool::decode(include_bytes!("protos/nested_message.bin").as_ref()).unwrap();
    let message = pool.all_messages().next().unwrap();
    let arrow_schema = protobuf_to_arrow(message).unwrap();

    assert_eq!(arrow_schema.fields().len(), 2);
    assert_field(&arrow_schema, "nested_field", DataType::Struct(vec![
        Arc::new(Field::new("inner_field", DataType::Int32, true))
    ].into()), true);
    assert_field(&arrow_schema, "double_nested_field", DataType::Struct(vec![
        Arc::new(Field::new("inner_nested", DataType::Struct(vec![
            Arc::new(Field::new("inner_field", DataType::Int32, true))
        ].into()), true))
    ].into()), true);
}

#[test]
fn test_repeated_fields() {
    let pool = DescriptorPool::decode(include_bytes!("protos/repeated_fields.bin").as_ref()).unwrap();
    let message = pool.all_messages().next().unwrap();
    let arrow_schema = protobuf_to_arrow(message).unwrap();

    assert_eq!(arrow_schema.fields().len(), 2);
    assert_field(&arrow_schema, "repeated_int", DataType::List(Arc::new(Field::new("item", DataType::Int32, true))), true);
    assert_field(&arrow_schema, "repeated_string", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true);
}

#[test]
fn test_map_fields() {
    let pool = DescriptorPool::decode(include_bytes!("protos/map_fields.bin").as_ref()).unwrap();
    let message = pool.all_messages().next().unwrap();
    
    let arrow_schema = protobuf_to_arrow(message).unwrap();

    assert_eq!(arrow_schema.fields().len(), 2);
    assert_eq!(arrow_schema.field_with_name("int_to_string_map").unwrap(),
        &ArroyoExtensionType::add_metadata(Some(ArroyoExtensionType::JSON), 
            Field::new("int_to_string_map".to_string(), DataType::Utf8, true)));
    assert_eq!(arrow_schema.field_with_name("string_to_message_map").unwrap(),
       &ArroyoExtensionType::add_metadata(Some(ArroyoExtensionType::JSON),
          Field::new("string_to_message_map".to_string(), DataType::Utf8, true)));
}

#[test]
fn test_enum_fields() {
    let pool = DescriptorPool::decode(include_bytes!("protos/enum_fields.bin").as_ref()).unwrap();
    let message = pool.all_messages().next().unwrap();
    let arrow_schema = protobuf_to_arrow(message).unwrap();

    assert_eq!(arrow_schema.fields().len(), 1);
    assert_field(&arrow_schema, "enum_field", DataType::Utf8, true);
}

// Helper function to assert field properties
fn assert_field(schema: &Schema, name: &str, data_type: DataType, nullable: bool) {
    let field = schema.field_with_name(name).unwrap();
    assert_eq!(field.name(), name);
    assert_eq!(field.data_type(), &data_type);
    assert_eq!(field.is_nullable(), nullable);
}