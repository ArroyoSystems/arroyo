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

pub fn nexmark_fields() -> Vec<StructField> {
    // return the fields of the nexmark schema
    vec![
        StructField {
            name: "person".to_string(),
            alias: None,
            data_type: TypeDef::StructDef(
                StructDef {
                    name: Some("arroyo_types::nexmark::Person".to_string()),
                    fields: vec![
                        StructField {
                            name: "id".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Int64, false),
                        },
                        StructField {
                            name: "name".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Utf8, false),
                        },
                        StructField {
                            name: "email_address".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Utf8, false),
                        },
                        StructField {
                            name: "credit_card".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Utf8, false),
                        },
                        StructField {
                            name: "city".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Utf8, false),
                        },
                        StructField {
                            name: "state".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Utf8, false),
                        },
                        StructField {
                            name: "datetime".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                false,
                            ),
                        },
                        StructField {
                            name: "extra".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Utf8, false),
                        },
                    ],
                },
                true,
            ),
        },
        StructField {
            name: "bid".to_string(),
            alias: None,
            data_type: TypeDef::StructDef(
                StructDef {
                    name: Some("arroyo_types::nexmark::Bid".to_string()),
                    fields: vec![
                        StructField {
                            name: "auction".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Int64, false),
                        },
                        StructField {
                            name: "bidder".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Int64, false),
                        },
                        StructField {
                            name: "price".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Int64, false),
                        },
                        StructField {
                            name: "channel".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Utf8, false),
                        },
                        StructField {
                            name: "url".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Utf8, false),
                        },
                        StructField {
                            name: "datetime".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                false,
                            ),
                        },
                        StructField {
                            name: "extra".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Utf8, false),
                        },
                    ],
                },
                true,
            ),
        },
        StructField {
            name: "auction".to_string(),
            alias: None,
            data_type: TypeDef::StructDef(
                StructDef {
                    name: Some("arroyo_types::nexmark::Auction".to_string()),
                    fields: vec![
                        StructField {
                            name: "id".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Int64, false),
                        },
                        StructField {
                            name: "description".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Utf8, false),
                        },
                        StructField {
                            name: "initial_bid".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Int64, false),
                        },
                        StructField {
                            name: "reserve".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Int64, false),
                        },
                        StructField {
                            name: "datetime".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                false,
                            ),
                        },
                        StructField {
                            name: "expires".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                false,
                            ),
                        },
                        StructField {
                            name: "seller".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Int64, false),
                        },
                        StructField {
                            name: "category".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Int64, false),
                        },
                        StructField {
                            name: "extra".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::Utf8, false),
                        },
                    ],
                },
                true,
            ),
        },
    ]
}
