use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};

use crate::types::{StructDef, StructField, TypeDef};

pub(crate) fn window_arrow_struct() -> DataType {
    DataType::Struct(vec![
        Arc::new(arrow::datatypes::Field::new(
            "start_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )),
        Arc::new(arrow::datatypes::Field::new(
            "end_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )),
    ].into())
}

pub(crate) fn window_type_def() -> TypeDef {
    TypeDef::StructDef(
        StructDef {
            name: Some("arroyo_types::Window".to_string()),
            fields: vec![
                StructField {
                    name: "start_time".to_string(),
                    alias: None,
                    data_type: TypeDef::DataType(
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                },
                StructField {
                    name: "end_time".to_string(),
                    alias: None,
                    data_type: TypeDef::DataType(
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                },
            ],
        },
        false,
    )
}
