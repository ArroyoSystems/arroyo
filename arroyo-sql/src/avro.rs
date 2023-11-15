use crate::types::{StructDef, StructField, TypeDef};
use anyhow::{anyhow, bail};
use apache_avro::Schema;
use arrow_schema::DataType;
use proc_macro2::Ident;
use quote::quote;

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
