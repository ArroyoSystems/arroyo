use anyhow::{anyhow, bail};
use arrow::array::{Array, ArrayRef, ListArray, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, UnionFields};
use itertools::Itertools;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::sync::Arc;

/// Add metadata "PARQUET:field_id" to every field , which is required by the arrow -> iceberg
/// schema conversion code from iceberg-rust
pub fn add_parquet_field_ids(schema: &Schema) -> Schema {
    let mut next_id: i32 = 1;
    let new_fields: Fields = schema
        .fields()
        .iter()
        .map(|f| annotate_field(f, &mut next_id))
        .collect();
    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

fn annotate_field(field: &Field, next_id: &mut i32) -> Field {
    let mut md = field.metadata().clone();
    md.insert(PARQUET_FIELD_ID_META_KEY.to_string(), next_id.to_string());
    *next_id += 1;

    let new_dt = match field.data_type() {
        DataType::Struct(children) => {
            let ch: Fields = children
                .iter()
                .map(|c| annotate_field(c, next_id))
                .collect();
            DataType::Struct(ch)
        }
        DataType::List(child) => {
            let c = Arc::new(annotate_field(child, next_id));
            DataType::List(c)
        }
        DataType::LargeList(child) => {
            let c = Arc::new(annotate_field(child, next_id));
            DataType::LargeList(c)
        }
        DataType::FixedSizeList(child, len) => {
            let c = Arc::new(annotate_field(child, next_id));
            DataType::FixedSizeList(c, *len)
        }
        DataType::Map(entry_field, keys_sorted) => {
            let e = Arc::new(annotate_field(entry_field, next_id));
            DataType::Map(e, *keys_sorted)
        }
        DataType::Union(fields, mode) => {
            let new_fields_vec: Vec<Field> = fields
                .iter()
                .map(|(_, f)| annotate_field(f, next_id))
                .collect();
            let type_ids: Vec<i8> = fields.iter().map(|(id, _)| id).collect();
            let uf = UnionFields::new(type_ids, new_fields_vec);
            DataType::Union(uf, *mode)
        }
        other => other.clone(),
    };

    Field::new(field.name(), new_dt, field.is_nullable()).with_metadata(md)
}

/// Iceberg REST catalogs will ignore our field ids, so we need to reassign field
/// ids in our arrow schema to match the iceberg schema
/// (https://github.com/apache/iceberg/issues/13164)
/// this seems like a bug to me, but it's not technically specified that catalogs should
/// *not* ignore field ids, so I guess they're free to...
pub fn update_field_ids_to_iceberg(
    schema: &Schema,
    iceberg: &iceberg::spec::Schema,
) -> anyhow::Result<Schema> {
    let new_fields: Fields = schema
        .fields()
        .iter()
        .map(|f| update_field_id(f, None, iceberg, false))
        .try_collect()?;

    Ok(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ))
}

fn update_field_id(
    field: &Field,
    path: Option<&str>,
    iceberg: &iceberg::spec::Schema,
    parent_is_list: bool,
) -> anyhow::Result<Field> {
    let mut md = field.metadata().clone();
    let mut field_name = field.name().clone();

    if let Some(path) = path {
        field_name = [path, &field_name].join(".");
    }

    let id = iceberg.field_id_by_name(&field_name).or_else(|| {
        if parent_is_list {
            // iceberg catalogs may rename all list fields to "element"
            field_name = [path.unwrap(), "element"].join(".");
            iceberg.field_id_by_name(&field_name)
        } else {
            None
        }
    });

    md.insert(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        id.ok_or_else(|| {
            anyhow!(
                "field `{}.element` not present in Iceberg schema",
                field_name
            )
        })?
        .to_string(),
    );

    let new_dt = match field.data_type() {
        DataType::Struct(children) => {
            let ch: Fields = children
                .iter()
                .map(|c| update_field_id(c, Some(&field_name), iceberg, false))
                .try_collect()?;
            DataType::Struct(ch)
        }
        DataType::List(child) => {
            let c = Arc::new(update_field_id(child, Some(&field_name), iceberg, true)?);
            DataType::List(c)
        }
        DataType::LargeList(child) => {
            let c = Arc::new(update_field_id(child, Some(&field_name), iceberg, true)?);
            DataType::LargeList(c)
        }
        DataType::FixedSizeList(child, len) => {
            let c = Arc::new(update_field_id(child, Some(&field_name), iceberg, true)?);
            DataType::FixedSizeList(c, *len)
        }
        DataType::Map(entry_field, keys_sorted) => {
            let e = Arc::new(update_field_id(
                entry_field,
                Some(&field_name),
                iceberg,
                false,
            )?);
            DataType::Map(e, *keys_sorted)
        }
        DataType::Union(fields, mode) => {
            let new_fields_vec: Vec<Field> = fields
                .iter()
                .map(|(_, f)| update_field_id(f, Some(&field_name), iceberg, false))
                .try_collect()?;
            let type_ids: Vec<i8> = fields.iter().map(|(id, _)| id).collect();
            let uf = UnionFields::new(type_ids, new_fields_vec);
            DataType::Union(uf, *mode)
        }
        other => other.clone(),
    };

    Ok(Field::new(field.name(), new_dt, field.is_nullable()).with_metadata(md))
}

/// Nested fields (structs and arrays) in the batch need to be updated with the parquet field ids
/// metadata in order to be written.
pub fn normalize_batch_to_schema(
    batch: &RecordBatch,
    target: &Schema,
) -> anyhow::Result<RecordBatch> {
    assert_eq!(batch.num_columns(), target.fields().len());
    let cols = batch
        .columns()
        .iter()
        .zip(target.fields().iter())
        .map(|(col, f)| retag_field(col, f))
        .try_collect()?;
    Ok(RecordBatch::try_new(Arc::new(target.clone()), cols)?)
}

fn retag_field(arr: &ArrayRef, schema_field: &Field) -> anyhow::Result<ArrayRef> {
    use DataType::*;

    if !arr.data_type().is_nested() {
        // nothing to do for non-nested fields
        return Ok(arr.clone());
    }

    match (arr.data_type(), schema_field.data_type()) {
        (Struct(_), Struct(schema_children)) => {
            let sa = arr.as_any().downcast_ref::<StructArray>().unwrap();
            let rebuilt_children = sa
                .columns()
                .iter()
                .zip(schema_children.iter())
                .map(|(child, sf)| retag_field(child, sf))
                .try_collect();

            let rebuilt = StructArray::try_new(
                schema_children.clone(),
                rebuilt_children?,
                sa.nulls().cloned(),
            )?;

            Ok(Arc::new(rebuilt))
        }

        (List(_), List(schema_elem)) => {
            let la = arr.as_any().downcast_ref::<ListArray>().unwrap();
            let values = retag_field(la.values(), schema_elem.as_ref())?;
            let rebuilt = ListArray::try_new(
                schema_elem.clone(),
                la.offsets().clone(),
                values,
                la.nulls().cloned(),
            )?;
            Ok(Arc::new(rebuilt))
        }
        _ => {
            bail!("unsupported composite data type {:?}", arr.data_type());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PARQUET_FIELD_ID_META_KEY, normalize_batch_to_schema};
    use super::{add_parquet_field_ids, update_field_ids_to_iceberg};
    use arrow::array::{Int64Array, ListArray, RecordBatch, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema, TimeUnit};
    use arrow::json::ReaderBuilder;
    use iceberg::TableCreation;
    use iceberg::spec::TableMetadataBuilder;
    use std::io::Cursor;
    use std::sync::Arc;

    #[test]
    fn test_adds_parquet_field_ids_recursively() {
        let struct_field = Field::new(
            "props",
            DataType::Struct(
                vec![
                    Field::new("k", DataType::Utf8, true),
                    Field::new("v", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        );

        let list_field = Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        );

        let id_field = Field::new("id", DataType::Int64, false);
        let price_field = Field::new("price", DataType::Decimal128(10, 2), true);
        let active_field = Field::new("active", DataType::Boolean, true);
        let ts_field = Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        );

        let schema = Schema::new(vec![
            id_field,
            price_field,
            active_field,
            ts_field,
            list_field,
            struct_field,
        ]);

        let out = add_parquet_field_ids(&schema);

        let has_id = |f: &Field| {
            f.metadata()
                .get("PARQUET:field_id")
                .map(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit()))
                .unwrap_or(false)
        };

        let fields = out.fields();
        assert!(has_id(&fields[0]), "id missing PARQUET:field_id");
        assert!(has_id(&fields[1]), "price missing PARQUET:field_id");
        assert!(has_id(&fields[2]), "active missing PARQUET:field_id");
        assert!(has_id(&fields[3]), "ts missing PARQUET:field_id");
        assert!(has_id(&fields[4]), "tags (list) missing PARQUET:field_id");
        assert!(
            has_id(&fields[5]),
            "props (struct) missing PARQUET:field_id"
        );

        if let DataType::List(inner) = fields[4].data_type() {
            assert!(has_id(inner), "list inner field missing PARQUET:field_id");
        } else {
            panic!("tags not a List");
        }

        if let DataType::Struct(children) = fields[5].data_type() {
            assert!(has_id(&children[0]), "struct.k missing PARQUET:field_id");
            assert!(has_id(&children[1]), "struct.v missing PARQUET:field_id");
        } else {
            panic!("props not a Struct");
        }
    }

    fn id_of(f: &Field) -> Option<i32> {
        f.metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|s| s.parse::<i32>().ok())
    }

    #[test]
    fn adds_ids_recursively_struct_and_list() {
        let base = Schema::new(vec![Field::new(
            "person",
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int64, false),
                Field::new(
                    "aliases",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ),
            ])),
            true,
        )]);

        let annotated = add_parquet_field_ids(&base);
        let person = annotated.field_with_name("person").unwrap();
        assert!(id_of(person).is_some(), "top-level field id missing");

        let DataType::Struct(children) = person.data_type() else {
            panic!("person not struct")
        };
        let id_f = &children[0];
        let aliases_f = &children[1];
        assert!(id_of(id_f).is_some(), "nested struct child id missing");

        let DataType::List(elem_f) = aliases_f.data_type() else {
            panic!("aliases not list")
        };
        assert!(id_of(elem_f).is_some(), "list element id missing");
    }

    #[test]
    fn normalize_fixes_nested_metadata_and_preserves_data() {
        let target = add_parquet_field_ids(&Schema::new(vec![Field::new(
            "person",
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new(
                    "aliases",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                ),
            ])),
            true,
        )]));

        let id = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let name = Arc::new(StringArray::from(vec![Some("a"), Some("b"), None]));

        let aliases = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
            None,
        ]));

        let unannot_children = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "aliases",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ]);

        let person_struct = Arc::new(
            StructArray::try_new(unannot_children, vec![id, name, aliases], None).unwrap(),
        );

        let incoming = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "person",
                DataType::Struct(Fields::from(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                    Field::new(
                        "aliases",
                        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                        true,
                    ),
                ])),
                true,
            )])),
            vec![person_struct.clone()],
        )
        .unwrap();

        let incoming_person_field = incoming.schema().field(0).clone();
        let target_person_field = target.field(0).clone();
        assert_ne!(
            incoming_person_field, target_person_field,
            "sanity: schemas should differ due to missing metadata"
        );

        let normalized = normalize_batch_to_schema(&incoming, &target).expect("normalize");

        assert_eq!(normalized.schema().as_ref(), &target);

        let person = normalized
            .schema()
            .field_with_name("person")
            .cloned()
            .unwrap();
        let DataType::Struct(children) = person.data_type() else {
            unreachable!()
        };
        assert!(
            id_of(&children[0]).is_some(),
            "id child missing PARQUET:field_id"
        );
        assert!(
            id_of(&children[1]).is_some(),
            "name child missing PARQUET:field_id"
        );
        let DataType::List(elem) = children[2].data_type() else {
            unreachable!()
        };
        assert!(
            id_of(elem).is_some(),
            "list element missing PARQUET:field_id"
        );
    }

    #[test]
    fn normalize_corrects_list_parent_element_ids_to_match_iceberg() {
        let incoming_schema = Arc::new(Schema::new(vec![
            Field::new(
                "__ingest_ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("counter", DataType::Int64, false),
            Field::new(
                "counters",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
            Field::new(
                "nested",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    true,
                ))),
                true,
            ),
        ]));

        let writer_schema = add_parquet_field_ids(&incoming_schema);

        let json_data = r#"
{"__ingest_ts": 1000000, "counter": 10, "counters": [1, 2, 3], "nested": [[1, 2], [3]]}
{"__ingest_ts": 2000000, "counter": 20, "counters": [4, 5], "nested": [[4, 5, 6]]}
{"__ingest_ts": 3000000, "counter": 30, "counters": [6, 7, 8, 9], "nested": [[7], [8, 9]]}
"#;

        let cursor = Cursor::new(json_data);
        let mut reader = ReaderBuilder::new(incoming_schema.clone())
            .build(cursor)
            .unwrap();

        let batch = reader.next().unwrap().unwrap();

        let normalized =
            normalize_batch_to_schema(&batch, &writer_schema).expect("normalize_batch_to_schema");

        let iceberg_schema = iceberg::arrow::arrow_schema_to_schema(&writer_schema).unwrap();

        let counters_field = normalized
            .schema()
            .field_with_name("counters")
            .unwrap()
            .clone();
        assert_eq!(
            id_of(&counters_field).unwrap(),
            iceberg_schema.field_id_by_name("counters").unwrap(),
        );

        let DataType::List(item) = counters_field.data_type() else {
            panic!("not list");
        };
        assert_eq!(
            id_of(item.as_ref()).unwrap(),
            iceberg_schema.field_id_by_name("counters.element").unwrap()
        );

        let nested_field = normalized
            .schema()
            .field_with_name("nested")
            .unwrap()
            .clone();
        assert_eq!(
            id_of(&nested_field).unwrap(),
            iceberg_schema.field_id_by_name("nested").unwrap(),
        );

        let DataType::List(item) = nested_field.data_type() else {
            panic!("not list");
        };

        assert_eq!(
            id_of(item.as_ref()).unwrap(),
            iceberg_schema.field_id_by_name("nested.element").unwrap()
        );

        let DataType::List(item) = item.data_type() else {
            panic!("not list");
        };

        assert_eq!(
            id_of(item.as_ref()).unwrap(),
            iceberg_schema
                .field_id_by_name("nested.element.element")
                .unwrap()
        );
    }

    #[test]
    fn normalize_corrects_struct_field_ids_to_match_iceberg() {
        let incoming_schema = Arc::new(Schema::new(vec![
            Field::new(
                "__ingest_ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("counter", DataType::Int64, false),
            Field::new(
                "person",
                DataType::Struct(
                    vec![
                        Field::new("name", DataType::Utf8, true),
                        Field::new("age", DataType::Int64, true),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new(
                "nested_struct",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int64, true),
                        Field::new(
                            "address",
                            DataType::Struct(
                                vec![
                                    Field::new("street", DataType::Utf8, true),
                                    Field::new("city", DataType::Utf8, true),
                                ]
                                .into(),
                            ),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));
        let writer_schema = add_parquet_field_ids(&incoming_schema);
        let json_data = r#"
{"__ingest_ts": 1000000, "counter": 10, "person": {"name": "Alice", "age": 30}, "nested_struct": {"id": 1, "address": {"street": "Main St", "city": "NYC"}}}
{"__ingest_ts": 2000000, "counter": 20, "person": {"name": "Bob", "age": 25}, "nested_struct": {"id": 2, "address": {"street": "Oak Ave", "city": "LA"}}}
{"__ingest_ts": 3000000, "counter": 30, "person": {"name": "Carol", "age": 35}, "nested_struct": {"id": 3, "address": {"street": "Elm Rd", "city": "SF"}}}
"#;
        let cursor = Cursor::new(json_data);
        let mut reader = ReaderBuilder::new(incoming_schema.clone())
            .build(cursor)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let normalized =
            normalize_batch_to_schema(&batch, &writer_schema).expect("normalize_batch_to_schema");
        let iceberg_schema = iceberg::arrow::arrow_schema_to_schema(&writer_schema).unwrap();

        // Test person struct and its fields
        let person_field = normalized
            .schema()
            .field_with_name("person")
            .unwrap()
            .clone();
        assert_eq!(
            id_of(&person_field).unwrap(),
            iceberg_schema.field_id_by_name("person").unwrap(),
        );
        let DataType::Struct(person_fields) = person_field.data_type() else {
            panic!("not struct");
        };
        let name_field = person_fields.find("name").unwrap().1;
        assert_eq!(
            id_of(name_field).unwrap(),
            iceberg_schema.field_id_by_name("person.name").unwrap()
        );
        let age_field = person_fields.find("age").unwrap().1;
        assert_eq!(
            id_of(age_field).unwrap(),
            iceberg_schema.field_id_by_name("person.age").unwrap()
        );

        // Test nested_struct and its nested fields
        let nested_struct_field = normalized
            .schema()
            .field_with_name("nested_struct")
            .unwrap()
            .clone();
        assert_eq!(
            id_of(&nested_struct_field).unwrap(),
            iceberg_schema.field_id_by_name("nested_struct").unwrap(),
        );
        let DataType::Struct(nested_fields) = nested_struct_field.data_type() else {
            panic!("not struct");
        };
        let id_field = nested_fields.find("id").unwrap().1;
        assert_eq!(
            id_of(id_field).unwrap(),
            iceberg_schema.field_id_by_name("nested_struct.id").unwrap()
        );
        let address_field = nested_fields.find("address").unwrap().1;
        assert_eq!(
            id_of(address_field).unwrap(),
            iceberg_schema
                .field_id_by_name("nested_struct.address")
                .unwrap()
        );
        let DataType::Struct(address_fields) = address_field.data_type() else {
            panic!("not struct");
        };
        let street_field = address_fields.find("street").unwrap().1;
        assert_eq!(
            id_of(street_field).unwrap(),
            iceberg_schema
                .field_id_by_name("nested_struct.address.street")
                .unwrap()
        );
        let city_field = address_fields.find("city").unwrap().1;
        assert_eq!(
            id_of(city_field).unwrap(),
            iceberg_schema
                .field_id_by_name("nested_struct.address.city")
                .unwrap()
        );
    }

    #[test]
    fn test_matching_ids_from_iceberg() {
        let arrow_schema = Schema::new(vec![
            Field::new(
                "__ingest_ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )
            .with_metadata([(PARQUET_FIELD_ID_META_KEY.into(), "1".into())].into()),
            Field::new("counter", DataType::Int64, false)
                .with_metadata([(PARQUET_FIELD_ID_META_KEY.into(), "2".into())].into()),
            Field::new(
                "counters",
                DataType::List(
                    Field::new("item", DataType::Utf8, true)
                        .with_metadata([(PARQUET_FIELD_ID_META_KEY.into(), "5".into())].into())
                        .into(),
                ),
                true,
            )
            .with_metadata([(PARQUET_FIELD_ID_META_KEY.into(), "4".into())].into()),
            Field::new(
                "nested_struct",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int64, true)
                            .with_metadata([(PARQUET_FIELD_ID_META_KEY.into(), "7".into())].into()),
                        Field::new(
                            "address",
                            DataType::Struct(
                                vec![
                                    Field::new("street", DataType::Utf8, true).with_metadata(
                                        [(PARQUET_FIELD_ID_META_KEY.into(), "9".into())].into(),
                                    ),
                                    Field::new("city", DataType::Utf8, true).with_metadata(
                                        [(PARQUET_FIELD_ID_META_KEY.into(), "10".into())].into(),
                                    ),
                                ]
                                .into(),
                            ),
                            true,
                        )
                        .with_metadata([(PARQUET_FIELD_ID_META_KEY.into(), "8".into())].into()),
                    ]
                    .into(),
                ),
                true,
            )
            .with_metadata([(PARQUET_FIELD_ID_META_KEY.into(), "6".into())].into()),
        ]);

        let iceberg_schema = iceberg::arrow::arrow_schema_to_schema(&arrow_schema).unwrap();
        let table = TableMetadataBuilder::from_table_creation(
            TableCreation::builder()
                .name("my_table".into())
                .location("s3://my-bucket".into())
                .schema(iceberg_schema)
                .build(),
        )
        .unwrap()
        .build()
        .unwrap();

        let iceberg_schema_from_catalog = table.metadata.current_schema();
        // this should have been reassigned
        assert_ne!(
            iceberg_schema_from_catalog.field_id_by_name("nested_struct"),
            Some(6)
        );

        let updated_arrow_schema =
            update_field_ids_to_iceberg(&arrow_schema, iceberg_schema_from_catalog).unwrap();

        // this computes the field map from our actual arrow schema
        let updated_iceberg =
            iceberg::arrow::arrow_schema_to_schema(&updated_arrow_schema).unwrap();
        assert_eq!(
            updated_iceberg.field_id_to_name_map(),
            iceberg_schema_from_catalog.field_id_to_name_map()
        );
    }
}
