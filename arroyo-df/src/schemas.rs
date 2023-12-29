use crate::types::{StructDef, StructField, TypeDef};

use arrow::datatypes::{DataType, TimeUnit};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion_common::{DFField, DFSchema, DFSchemaRef, Result as DFResult};
use std::{collections::HashMap, sync::Arc};

pub fn window_arrow_struct() -> DataType {
    DataType::Struct(
        vec![
            Arc::new(arrow::datatypes::Field::new(
                "start",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "end",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
        ]
        .into(),
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

pub(crate) fn add_timestamp_field(schema: DFSchemaRef) -> DFResult<DFSchemaRef> {
    let timestamp_field = DFField::new_unqualified(
        "_timestamp",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    );
    Ok(Arc::new(schema.join(&DFSchema::new_with_metadata(
        vec![timestamp_field],
        HashMap::new(),
    )?)?))
}

pub(crate) fn has_timestamp_field(schema: DFSchemaRef) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| field.name() == "_timestamp")
}

pub(crate) fn has_timestamp_field_arrow(schema: SchemaRef) -> bool {
    schema.fields.iter().any(|f| f.name() == "_timestamp")
}

pub(crate) fn add_timestamp_field_arrow(schema: SchemaRef) -> SchemaRef {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(
        "_timestamp",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )));
    Arc::new(Schema::new(fields))
}

pub(crate) fn add_timestamp_field_if_missing_arrow(schema: SchemaRef) -> SchemaRef {
    if has_timestamp_field_arrow(schema.clone()) {
        schema
    } else {
        add_timestamp_field_arrow(schema)
    }
}
