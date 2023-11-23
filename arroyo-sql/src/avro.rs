use crate::types::{StructDef, StructField, TypeDef};
use anyhow::{anyhow, bail};
use apache_avro::schema::{
    FixedSchema, Name, RecordField, RecordFieldOrder, RecordSchema, UnionSchema,
};
use apache_avro::Schema;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use std::ptr::null;

pub const ROOT_NAME: &str = "ArroyoAvroRoot";

pub fn convert_avro_schema(name: &str, schema: &str) -> anyhow::Result<Vec<StructField>> {
    let schema =
        Schema::parse_str(schema).map_err(|e| anyhow!("avro schema is not valid: {:?}", e))?;

    let (typedef, _) = to_typedef(name, &schema);
    match typedef {
        TypeDef::StructDef(sd, _) => Ok(sd.fields),
        TypeDef::DataType(_, _) => {
            bail!("top-level schema must be a record")
        }
    }
}

pub fn get_defs(name: &str, schema: &str) -> anyhow::Result<String> {
    let fields = convert_avro_schema(name, schema)?;

    let sd = StructDef::new(Some(ROOT_NAME.to_string()), true, fields, None);
    let defs: Vec<_> = sd
        .all_structs_including_named()
        .iter()
        .map(|p| {
            vec![
                syn::parse_str(&p.def(false)).unwrap(),
                p.generate_serializer_items(),
            ]
        })
        .flatten()
        .collect();

    let mod_ident: Ident = syn::parse_str(name).unwrap();
    Ok(quote! {
        mod #mod_ident {
            use super::*;
            #(#defs)
            *
        }
    }
    .to_string())
}

fn to_typedef(source_name: &str, schema: &Schema) -> (TypeDef, Option<String>) {
    match schema {
        Schema::Null => (TypeDef::DataType(DataType::Null, false), None),
        Schema::Boolean => (TypeDef::DataType(DataType::Boolean, false), None),
        Schema::Int | Schema::TimeMillis => (TypeDef::DataType(DataType::Int32, false), None),
        Schema::Long
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros => (TypeDef::DataType(DataType::Int64, false), None),
        Schema::Float => (TypeDef::DataType(DataType::Float32, false), None),
        Schema::Double => (TypeDef::DataType(DataType::Float64, false), None),
        Schema::Bytes | Schema::Fixed(_) | Schema::Decimal(_) => {
            (TypeDef::DataType(DataType::Binary, false), None)
        }
        Schema::String | Schema::Enum(_) | Schema::Uuid => {
            (TypeDef::DataType(DataType::Utf8, false), None)
        }
        Schema::Union(union) => {
            // currently just support unions that have [t, null] as variants, which is the
            // avro way to represent optional fields

            let (nulls, not_nulls): (Vec<_>, Vec<_>) = union
                .variants()
                .iter()
                .partition(|v| matches!(v, Schema::Null));

            if nulls.len() == 1 && not_nulls.len() == 1 {
                let (dt, original) = to_typedef(source_name, not_nulls[0]);
                (dt.to_optional(), original)
            } else {
                (
                    TypeDef::DataType(DataType::Utf8, false),
                    Some("json".to_string()),
                )
            }
        }
        Schema::Record(record) => {
            let fields = record
                .fields
                .iter()
                .map(|f| {
                    let (ft, original) = to_typedef(source_name, &f.schema);
                    StructField::with_rename(f.name.clone(), None, ft, None, original)
                })
                .collect();

            (
                TypeDef::StructDef(
                    StructDef::for_name(
                        Some(format!("{}::{}", source_name, record.name.name)),
                        fields,
                    ),
                    false,
                ),
                None,
            )
        }
        _ => (
            TypeDef::DataType(DataType::Utf8, false),
            Some("json".to_string()),
        ),
    }
}

/// Generates code that serializes an arroyo data struct into avro
///
/// Note that this must align with the schemas constructed in
/// `arroyo-formats::avro::arrow_to_avro_schema`!
pub fn generate_serializer_item(record: &Ident, field: &Ident, td: &TypeDef) -> TokenStream {
    use DataType::*;
    let value = quote!(#record.#field);
    match td {
        TypeDef::StructDef(_, _) => {
            quote!(todo!("structs"))
        }
        TypeDef::DataType(dt, nullable) => {
            let inner = match dt {
                Null => unreachable!("null fields are not supported"),
                Boolean => quote! { Boolean(*v) },
                Int8 | Int16 | Int32 | UInt8 | UInt16 => quote! { Int(*v as i32) },
                Int64 | UInt32 | UInt64 => quote! { Long(*v as i64) },
                Float16 | Float32 => quote! { Float(*v as f32) },
                Float64 => quote! { Double(*v as f64) },
                Timestamp(t, tz) => match (t, tz) {
                    (TimeUnit::Microsecond | TimeUnit::Nanosecond, None) => {
                        quote! { TimestampMicros(arroyo_types::to_micros(*v) as i64) }
                    }
                    (TimeUnit::Microsecond | TimeUnit::Nanosecond, Some(_)) => {
                        quote! { LocalTimestampMicros(arroyo_types::to_micros(*v) as i64) }
                    }
                    (TimeUnit::Millisecond | TimeUnit::Second, None) => {
                        quote! { TimestampMillis(arroyo_types::to_millis(*v) as i64) }
                    }
                    (TimeUnit::Millisecond | TimeUnit::Second, Some(_)) => {
                        quote! { LocalTimestampMillis(arroyo_types::to_millis(*v) as i64) }
                    }
                },
                Date32 | Date64 => quote! { Date(arroyo_types::days_since_epoch(*v)) },
                Time32(_) => todo!("time32 is not supported"),
                Time64(_) => todo!("time64 is not supported"),
                Duration(_) => todo!("duration is not supported"),
                Interval(_) => todo!("interval is not supported"),
                Binary | FixedSizeBinary(_) | LargeBinary => quote! { Bytes(v.clone()) },
                Utf8 | LargeUtf8 => quote! { String(v.clone()) },
                List(_) | FixedSizeList(_, _) | LargeList(_) => {
                    todo!("lists are not supported")
                }
                Struct(_) => unreachable!("typedefs should not contain structs"),
                Union(_, _) => unimplemented!("unions are not supported"),
                Dictionary(_, _) => unimplemented!("dictionaries are not supported"),
                Decimal128(_, _) => unimplemented!("decimal128 is not supported"),
                Decimal256(_, _) => unimplemented!("decimal256 is not supported"),
                Map(_, _) => unimplemented!("maps are not supported"),
                RunEndEncoded(_, _) => unimplemented!("run end encoded is not supported"),
            };

            if *nullable {
                quote! {
                    Union(#value.is_some() as u32, Box::new(#value.as_ref().map(|v| #inner).unwrap_or(Null)))
                }
            } else {
                quote! {{let v = &#value; #inner}}
            }
        }
    }
}
