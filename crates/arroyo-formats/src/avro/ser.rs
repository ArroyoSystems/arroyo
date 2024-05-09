use apache_avro::types::{Record, Value};
use apache_avro::Schema;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float16Type, Float32Type, Float64Type, Int32Type, Int64Type, Int8Type, TimestampNanosecondType,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use arroyo_rpc::formats::AvroFormat;
use arroyo_types::{from_nanos, to_micros};

trait SerializeTarget {
    fn add(&mut self, i: usize, name: &str, value: Value);
    fn is_some(&self, i: usize) -> bool;
}

impl SerializeTarget for Vec<Option<Record<'_>>> {
    fn add(&mut self, i: usize, name: &str, value: Value) {
        if let Some(r) = &mut self[i] {
            r.put(name, value);
        }
    }

    fn is_some(&self, i: usize) -> bool {
        self[i].is_some()
    }
}

impl SerializeTarget for Vec<Value> {
    fn add(&mut self, _: usize, _: &str, value: Value) {
        self.push(value);
    }

    fn is_some(&self, _: usize) -> bool {
        true
    }
}

fn get_field_schema<'a>(schema: &'a Schema, name: &str, nullable: bool) -> &'a Schema {
    let Schema::Record(record_schema) = schema else {
        panic!("invalid avro schema -- struct field {name} should correspond to record schema");
    };

    let record_field_number = record_schema.lookup.get(name).unwrap();
    let schema = &record_schema.fields[*record_field_number].schema;

    if nullable {
        let Schema::Union(__union_schema) = schema else {
            panic!(
                "invalid avro schema -- struct field {name} is nullable and should be represented by a union"
            );
        };
        __union_schema.variants().get(1).unwrap_or_else(|| {
            panic!("invalid avro schema -- struct field {name} should be a union with two variants")
        })
    } else {
        schema
    }
}

#[allow(clippy::redundant_closure_call)]
fn serialize_column<T: SerializeTarget>(
    schema: &Schema,
    values: &mut T,
    name: &str,
    column: &ArrayRef,
    nullable: bool,
) {
    macro_rules! write_arrow_value {
        ($as_call:path, $value_variant:path, $converter:expr) => {{
            $as_call(column).iter().enumerate().for_each(|(i, v)| {
                if values.is_some(i) {
                    if nullable {
                        values.add(
                            i,
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
                        values.add(
                            i,
                            name,
                            $value_variant($converter(v.expect("cannot be none"))),
                        );
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

        DataType::List(item) => {
            let schema = get_field_schema(schema, name, nullable);
            let Schema::Array(item_schema) = schema else {
                panic!(
                    "invalid avro schema -- list field {} should correspond to array schema but is {:?}",
                    name, schema
                );
            };

            let item_values: Vec<Option<Vec<Value>>> = if let Some(nulls) = column.nulls() {
                nulls
                    .iter()
                    .map(|null| null.then(std::vec::Vec::new))
                    .collect()
            } else {
                (0..column.len()).map(|_| Some(vec![])).collect()
            };

            for ((i, mut v), column) in item_values
                .into_iter()
                .enumerate()
                .zip(column.as_list::<i32>().iter())
            {
                if let Some(v) = &mut v {
                    serialize_column(
                        item_schema,
                        v,
                        "",
                        &column.expect("unmasked null in list"),
                        item.is_nullable(),
                    )
                }

                if nullable {
                    values.add(
                        i,
                        name,
                        Value::Union(
                            v.is_some() as u32,
                            Box::new(v.map(Value::Array).unwrap_or_else(|| Value::Null)),
                        ),
                    );
                } else {
                    values.add(
                        i,
                        name,
                        Value::Array(v.expect("null found in non-nullable list column")),
                    );
                }
            }
        }

        DataType::Struct(fields) => {
            let schema = get_field_schema(schema, name, nullable);
            if nullable {
                let mut struct_values: Vec<_> = if let Some(nulls) = column.nulls() {
                    nulls
                        .iter()
                        .map(|null| null.then(|| Record::new(schema).unwrap()))
                        .collect()
                } else {
                    (0..column.len())
                        .map(|_| Some(Record::new(schema).unwrap()))
                        .collect()
                };

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

                for (i, struct_v) in struct_values.into_iter().enumerate() {
                    values.add(
                        i,
                        name,
                        Value::Union(
                            struct_v.is_some() as u32,
                            Box::new(if let Some(struct_v) = struct_v {
                                struct_v.into()
                            } else {
                                Value::Null
                            }),
                        ),
                    );
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

                for (i, struct_v) in struct_values.into_iter().enumerate() {
                    values.add(i, name, Into::<Value>::into(struct_v.expect("not null")));
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

    values.into_iter().flatten().map(|r| r.into()).collect()
}

#[cfg(test)]
mod tests {
    use crate::avro::schema::to_avro;
    use crate::avro::ser::serialize;
    use arrow_array::builder::{Int64Builder, ListBuilder, StringBuilder, StructBuilder};
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

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
            Field::new(
                "numbers",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
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

        let mut numbers = ListBuilder::new(Int64Builder::new());
        numbers.append_value([Some(1), Some(2), Some(3)]);
        numbers.append_null();
        numbers.append_value([Some(4), Some(5)]);

        let avro_schema = to_avro("User", &arrow_schema.fields);

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(arrow_array::StringArray::from(names)),
                Arc::new(arrow_array::Int32Array::from(favorite_numbers)),
                Arc::new(arrow_array::StringArray::from(favorite_colors)),
                Arc::new(address_builder.finish()),
                Arc::new(second_address_builder.finish()),
                Arc::new(numbers.finish()),
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
                    ),
                    (
                        "numbers".to_string(),
                        Union(
                            1,
                            Box::new(Array(vec![
                                Union(1, Box::new(Long(1))),
                                Union(1, Box::new(Long(2))),
                                Union(1, Box::new(Long(3)))
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
                    ),
                    ("numbers".to_string(), Union(0, Box::new(Null)))
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
                    ("second_address".to_string(), Union(0, Box::new(Null))),
                    (
                        "numbers".to_string(),
                        Union(
                            1,
                            Box::new(Array(vec![
                                Union(1, Box::new(Long(4))),
                                Union(1, Box::new(Long(5)))
                            ]))
                        )
                    )
                ]),
            ]
        )
    }
}
