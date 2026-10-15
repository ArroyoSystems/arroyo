use arrow::array::{Array, ArrayRef, ListArray, RecordBatch, StringViewArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_rpc::formats::{AvroFormat, Format, JsonFormat, RawStringFormat};
use arroyo_types::ArroyoExtensionType;
use serde_json::{Value, json};
use std::sync::Arc;

fn strings(data_type: &DataType) -> ArrayRef {
    let values = StringViewArray::from(vec![
        Some("discarded prefix"),
        Some("short"),
        Some("a string longer than twelve bytes"),
        Some("é水🙂"),
        None,
        Some(""),
    ])
    .slice(1, 5);
    cast(&values, data_type).unwrap()
}

fn nested_batch(data_type: &DataType) -> RecordBatch {
    let values = strings(data_type);
    let item = Arc::new(Field::new("item", data_type.clone(), true));
    let lists = ListArray::new(
        item,
        OffsetBuffer::from_lengths([1; 5]),
        values.clone(),
        None,
    );
    let nested = StructArray::from(vec![(
        Arc::new(Field::new("nested_value", data_type.clone(), true)),
        values.clone(),
    )]);
    RecordBatch::try_from_iter([
        ("value", values),
        ("items", Arc::new(lists) as ArrayRef),
        ("nested", Arc::new(nested) as ArrayRef),
    ])
    .unwrap()
}

#[test]
fn utf8view_raw_string_preserves_values() {
    let batch = RecordBatch::try_from_iter([("value", strings(&DataType::Utf8View))]).unwrap();
    let output: Vec<_> = ArrowSerializer::new(Format::RawString(RawStringFormat {}))
        .serialize(&batch)
        .collect();
    assert_eq!(
        output,
        [
            "short",
            "a string longer than twelve bytes",
            "é水🙂",
            "",
            ""
        ]
        .map(|value| value.as_bytes().to_vec())
    );
}

#[test]
fn utf8view_nested_json_and_avro_match_utf8() {
    let original = nested_batch(&DataType::Utf8);
    let batch = nested_batch(&DataType::Utf8View);
    assert_eq!(
        ArrowSerializer::avro_schema(&batch.schema()),
        ArrowSerializer::avro_schema(&original.schema()),
    );
    assert_eq!(
        ArrowSerializer::json_schema(&batch.schema()),
        ArrowSerializer::json_schema(&original.schema()),
    );
    for format in [
        Format::Json(JsonFormat::default()),
        Format::Json(JsonFormat {
            include_schema: true,
            ..Default::default()
        }),
        Format::Avro(AvroFormat::new(false, true, false)),
    ] {
        let expected: Vec<_> = ArrowSerializer::new(format.clone())
            .serialize(&original)
            .collect();
        let actual: Vec<_> = ArrowSerializer::new(format).serialize(&batch).collect();
        assert_eq!(actual, expected);
    }
}

#[test]
fn utf8view_json_extension_preserves_json_values() {
    let values = StringViewArray::from(vec![
        Some(r#"{"long_property":"é水🙂"}"#),
        Some("[1,true]"),
        Some(r#""text""#),
        None,
    ]);
    let json_field = Arc::new(ArroyoExtensionType::add_metadata(
        Some(ArroyoExtensionType::JSON),
        Field::new("payload", DataType::Utf8View, true),
    ));
    let nested = StructArray::from(vec![(json_field.clone(), Arc::new(values) as ArrayRef)]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "nested",
            nested.data_type().clone(),
            false,
        )])),
        vec![Arc::new(nested)],
    )
    .unwrap();
    let output: Vec<Value> = ArrowSerializer::new(Format::Json(JsonFormat::default()))
        .serialize(&batch)
        .map(|row| serde_json::from_slice(&row).unwrap())
        .collect();
    assert_eq!(
        output,
        vec![
            json!({"nested": {"payload": {"long_property": "é水🙂"}}}),
            json!({"nested": {"payload": [1, true]}}),
            json!({"nested": {"payload": "text"}}),
            json!({"nested": {"payload": null}}),
        ]
    );
}
