use crate::proto::schema::{
    protobuf_to_arrow, schema_file_to_descriptor, schema_file_to_descriptor_with_resolver,
    ProtoSchemaResolver,
};
use arrow_schema::{DataType, Field, Schema};
use arroyo_types::ArroyoExtensionType;
use prost_reflect::DescriptorPool;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_basic_types() {
    let bytes = schema_file_to_descriptor(
        include_str!("protos/basic_types.proto"),
        &HashMap::default(),
    )
    .await
    .unwrap();

    let pool = DescriptorPool::decode(bytes.as_ref()).unwrap();
    let message = pool.all_messages().next().unwrap();
    let arrow_schema = protobuf_to_arrow(&message).unwrap();

    assert_eq!(arrow_schema.fields().len(), 7);
    assert_field(&arrow_schema, "bool_field", DataType::Boolean, true);
    assert_field(&arrow_schema, "int32_field", DataType::Int32, true);
    assert_field(&arrow_schema, "int64_field", DataType::Int64, true);
    assert_field(&arrow_schema, "uint32_field", DataType::UInt32, true);
    assert_field(&arrow_schema, "uint64_field", DataType::UInt64, true);
    assert_field(&arrow_schema, "float_field", DataType::Float32, true);
    assert_field(&arrow_schema, "double_field", DataType::Float64, true);
}

#[tokio::test]
async fn test_string_and_bytes() {
    let bytes = schema_file_to_descriptor(
        include_str!("protos/string_and_bytes.proto"),
        &HashMap::default(),
    )
    .await
    .unwrap();

    let pool = DescriptorPool::decode(bytes.as_ref()).unwrap();
    let message = pool.all_messages().next().unwrap();
    let arrow_schema = protobuf_to_arrow(&message).unwrap();

    assert_eq!(arrow_schema.fields().len(), 2);
    assert_field(&arrow_schema, "string_field", DataType::Utf8, true);
    assert_field(&arrow_schema, "bytes_field", DataType::Utf8, true);
}

#[tokio::test]
async fn test_nested_message() {
    let bytes = schema_file_to_descriptor(
        include_str!("protos/nested_message.proto"),
        &HashMap::default(),
    )
    .await
    .unwrap();

    let pool = DescriptorPool::decode(bytes.as_ref()).unwrap();

    let message = pool.all_messages().next().unwrap();
    let arrow_schema = protobuf_to_arrow(&message).unwrap();

    assert_eq!(arrow_schema.fields().len(), 2);
    assert_field(
        &arrow_schema,
        "nested_field",
        DataType::Struct(vec![Arc::new(Field::new("inner_field", DataType::Int32, true))].into()),
        true,
    );
    assert_field(
        &arrow_schema,
        "double_nested_field",
        DataType::Struct(
            vec![Arc::new(Field::new(
                "inner_nested",
                DataType::Struct(
                    vec![Arc::new(Field::new("inner_field", DataType::Int32, true))].into(),
                ),
                true,
            ))]
            .into(),
        ),
        true,
    );
}

#[tokio::test]
async fn test_repeated_fields() {
    let bytes = schema_file_to_descriptor(
        include_str!("protos/repeated_fields.proto"),
        &HashMap::default(),
    )
    .await
    .unwrap();

    let pool = DescriptorPool::decode(bytes.as_ref()).unwrap();

    let message = pool.all_messages().next().unwrap();
    let arrow_schema = protobuf_to_arrow(&message).unwrap();

    assert_eq!(arrow_schema.fields().len(), 2);
    assert_field(
        &arrow_schema,
        "repeated_int",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    );
    assert_field(
        &arrow_schema,
        "repeated_string",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    );
}

#[tokio::test]
async fn test_map_fields() {
    let bytes =
        schema_file_to_descriptor(include_str!("protos/map_fields.proto"), &HashMap::default())
            .await
            .unwrap();

    let pool = DescriptorPool::decode(bytes.as_ref()).unwrap();
    let message = pool.all_messages().next().unwrap();

    let arrow_schema = protobuf_to_arrow(&message).unwrap();

    assert_eq!(arrow_schema.fields().len(), 2);
    assert_eq!(
        arrow_schema.field_with_name("int_to_string_map").unwrap(),
        &ArroyoExtensionType::add_metadata(
            Some(ArroyoExtensionType::JSON),
            Field::new("int_to_string_map".to_string(), DataType::Utf8, true)
        )
    );
    assert_eq!(
        arrow_schema
            .field_with_name("string_to_message_map")
            .unwrap(),
        &ArroyoExtensionType::add_metadata(
            Some(ArroyoExtensionType::JSON),
            Field::new("string_to_message_map".to_string(), DataType::Utf8, true)
        )
    );
}

#[tokio::test]
async fn test_enum_fields() {
    let bytes = schema_file_to_descriptor(
        include_str!("protos/enum_fields.proto"),
        &HashMap::default(),
    )
    .await
    .unwrap();

    let pool = DescriptorPool::decode(bytes.as_ref()).unwrap();

    let message = pool.all_messages().next().unwrap();
    let arrow_schema = protobuf_to_arrow(&message).unwrap();

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

#[tokio::test]
async fn test_imports() {
    struct TestResolver {}

    impl ProtoSchemaResolver for TestResolver {
        async fn resolve(&self, path: &str) -> anyhow::Result<Option<String>> {
            if path == "address.proto" {
                Ok(Some(
                    r#"syntax = "proto3";
                    package proto_common;
                    
                    message Address {
                      string street = 1;
                      string city = 2;
                      string state = 3;
                      string postal_code = 4;
                      string country = 5;
                    }"#
                    .to_string(),
                ))
            } else {
                Ok(None)
            }
        }
    }

    schema_file_to_descriptor_with_resolver(
        include_str!("protos/orders.proto"),
        &HashMap::default(),
        TestResolver {},
    )
    .await
    .unwrap();
}
