use crate::types::{StructDef, StructField, TypeDef};
use arrow::datatypes::{DataType, TimeUnit};
use std::sync::Arc;

pub(crate) fn window_arrow_struct() -> DataType {
    DataType::Struct(
        vec![
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
        ]
        .into(),
    )
}

pub(crate) fn window_type_def() -> TypeDef {
    TypeDef::StructDef(
        StructDef::for_name(
            Some("arroyo_types::Window".to_string()),
            vec![
                StructField::new(
                    "start_time".to_string(),
                    None,
                    TypeDef::DataType(DataType::Timestamp(TimeUnit::Millisecond, None), false),
                ),
                StructField::new(
                    "end_time".to_string(),
                    None,
                    TypeDef::DataType(DataType::Timestamp(TimeUnit::Millisecond, None), false),
                ),
            ],
        ),
        false,
    )
}

// StructField \{\s+name: ([^,])+,\s+alias: ([^,]+), data_type: ([^,]+)\}
pub fn nexmark_fields() -> Vec<StructField> {
    // return the fields of the nexmark schema
    vec![
        StructField::new(
            "person".to_string(),
            None,
            TypeDef::StructDef(
                StructDef::for_name(
                    Some("arroyo_types::nexmark::Person".to_string()),
                    vec![
                        StructField::new(
                            "id".to_string(),
                            None,
                            TypeDef::DataType(DataType::Int64, false),
                        ),
                        StructField::new(
                            "name".to_string(),
                            None,
                            TypeDef::DataType(DataType::Utf8, false),
                        ),
                        StructField::new(
                            "email_address".to_string(),
                            None,
                            TypeDef::DataType(DataType::Utf8, false),
                        ),
                        StructField::new(
                            "credit_card".to_string(),
                            None,
                            TypeDef::DataType(DataType::Utf8, false),
                        ),
                        StructField::new(
                            "city".to_string(),
                            None,
                            TypeDef::DataType(DataType::Utf8, false),
                        ),
                        StructField::new(
                            "state".to_string(),
                            None,
                            TypeDef::DataType(DataType::Utf8, false),
                        ),
                        StructField::new(
                            "datetime".to_string(),
                            None,
                            TypeDef::DataType(
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                false,
                            ),
                        ),
                        StructField::new(
                            "extra".to_string(),
                            None,
                            TypeDef::DataType(DataType::Utf8, false),
                        ),
                    ],
                ),
                true,
            ),
        ),
        StructField::new(
            "bid".to_string(),
            None,
            TypeDef::StructDef(
                StructDef::for_name(
                    Some("arroyo_types::nexmark::Bid".to_string()),
                    vec![
                        StructField::new(
                            "auction".to_string(),
                            None,
                            TypeDef::DataType(DataType::Int64, false),
                        ),
                        StructField::new(
                            "bidder".to_string(),
                            None,
                            TypeDef::DataType(DataType::Int64, false),
                        ),
                        StructField::new(
                            "price".to_string(),
                            None,
                            TypeDef::DataType(DataType::Int64, false),
                        ),
                        StructField::new(
                            "channel".to_string(),
                            None,
                            TypeDef::DataType(DataType::Utf8, false),
                        ),
                        StructField::new(
                            "url".to_string(),
                            None,
                            TypeDef::DataType(DataType::Utf8, false),
                        ),
                        StructField::new(
                            "datetime".to_string(),
                            None,
                            TypeDef::DataType(
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                false,
                            ),
                        ),
                        StructField::new(
                            "extra".to_string(),
                            None,
                            TypeDef::DataType(DataType::Utf8, false),
                        ),
                    ],
                ),
                true,
            ),
        ),
        StructField::new(
            "auction".to_string(),
            None,
            TypeDef::StructDef(
                StructDef::for_name(
                    Some("arroyo_types::nexmark::Auction".to_string()),
                    vec![
                        StructField::new(
                            "id".to_string(),
                            None,
                            TypeDef::DataType(DataType::Int64, false),
                        ),
                        StructField::new(
                            "description".to_string(),
                            None,
                            TypeDef::DataType(DataType::Utf8, false),
                        ),
                        StructField::new(
                            "initial_bid".to_string(),
                            None,
                            TypeDef::DataType(DataType::Int64, false),
                        ),
                        StructField::new(
                            "reserve".to_string(),
                            None,
                            TypeDef::DataType(DataType::Int64, false),
                        ),
                        StructField::new(
                            "datetime".to_string(),
                            None,
                            TypeDef::DataType(
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                false,
                            ),
                        ),
                        StructField::new(
                            "expires".to_string(),
                            None,
                            TypeDef::DataType(
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                false,
                            ),
                        ),
                        StructField::new(
                            "seller".to_string(),
                            None,
                            TypeDef::DataType(DataType::Int64, false),
                        ),
                        StructField::new(
                            "category".to_string(),
                            None,
                            TypeDef::DataType(DataType::Int64, false),
                        ),
                        StructField::new(
                            "extra".to_string(),
                            None,
                            TypeDef::DataType(DataType::Utf8, false),
                        ),
                    ],
                ),
                true,
            ),
        ),
    ]
}
