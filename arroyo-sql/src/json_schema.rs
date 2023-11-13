use arrow_schema::DataType;
use quote::{format_ident, quote};
use schemars::schema::{Metadata, RootSchema, Schema};
use syn::parse_str;
use tracing::warn;
use typify::{Type, TypeDetails, TypeSpace, TypeSpaceSettings};

use crate::types::{StructDef, StructField, TypeDef};

pub const ROOT_NAME: &str = "ArroyoJsonRoot";

fn get_type_space(schema: &str) -> Result<TypeSpace, String> {
    let mut root_schema: RootSchema =
        serde_json::from_str(schema).map_err(|e| format!("Invalid json schema: {:?}", e))?;

    if root_schema.schema.metadata.is_none() {
        root_schema.schema.metadata = Some(Box::new(Metadata::default()));
    }

    root_schema.schema.metadata.as_mut().unwrap().title = Some(ROOT_NAME.to_string());

    let mut type_space = TypeSpace::new(
        TypeSpaceSettings::default()
            .with_derive("bincode::Encode".to_string())
            .with_derive("bincode::Decode".to_string())
            .with_derive("PartialEq".to_string())
            .with_derive("PartialOrd".to_string())
            .with_struct_builder(true),
    );
    type_space.add_ref_types(root_schema.definitions).unwrap();
    type_space
        .add_type(&Schema::Object(root_schema.schema))
        .unwrap();

    Ok(type_space)
}

pub fn convert_json_schema(name: &str, schema: &str) -> Result<Vec<StructField>, String> {
    let type_space = get_type_space(schema)?;

    let s = type_space
        .iter_types()
        .find(|typ| {
            if let TypeDetails::Struct(_) = typ.details() {
                typ.name() == ROOT_NAME
            } else {
                false
            }
        })
        .ok_or_else(|| format!("No top-level struct in json schema {}", name))?;

    if let TypeDef::StructDef(
        StructDef {
            name: Some(_),
            fields,
            ..
        },
        _,
    ) = to_schema_type(&type_space, name, &s).unwrap().0
    {
        Ok(fields)
    } else {
        unreachable!()
    }
}

pub fn get_defs(source_name: &str, schema: &str) -> Result<String, String> {
    fn add_defs(source_name: &str, name: &str, fields: &Vec<StructField>, defs: &mut Vec<String>) {
        let struct_fields: Vec<_> = fields.iter().map(|f| {
            let mut serde_opts = vec![];
            if let Some(opt) = match (&f.data_type, f.original_type.as_ref().map(|s| s.as_str())) {
                (TypeDef::DataType(DataType::Utf8, nullable), Some("json")) => {
                    if *nullable {
                        Some(quote!{
                            #[serde(default)]
                            #[serde(deserialize_with = "arroyo_worker::deserialize_raw_json_opt")]
                        })
                    } else {
                        Some(quote! {
                            #[serde(deserialize_with = "arroyo_worker::deserialize_raw_json")]
                        })
                    }
                },
                (TypeDef::DataType(DataType::Timestamp(_, _), nullable), Some("datetime")) => {
                    if *nullable {
                        Some(quote!{
                            #[serde(default)]
                            #[serde(with = "arroyo_worker::formats::json::opt_timestamp_as_rfc3339")]
                        })
                    } else {
                        Some(quote! {
                            #[serde(with = "arroyo_worker::formats::json::timestamp_as_rfc3339")]
                        })
                    }

                },
                _ => None
            } {
                serde_opts.push(opt);
            };

            if let Some(rename) = &f.renamed_from {
                serde_opts.push(quote!(#[serde(rename = #rename)]));
            }

            let ident = format_ident!("{}", f.name);
            let typ = match &f.data_type {
                TypeDef::DataType(dt, _) => StructField::data_type_name(dt),
                TypeDef::StructDef(sd, _) => {
                    let name = sd.name.as_ref().unwrap().replace(&format!("{}::", source_name), "");

                    add_defs(source_name, &name, &sd.fields, defs);

                    name
                },
            };

            let typ: syn::Type = parse_str(&typ).unwrap();

            let typ = if f.data_type.is_optional() {
                quote! { Option<#typ> }
            } else {
                quote! { #typ }
            };

            quote! {
                #(#serde_opts) *
                pub #ident: #typ
            }
        }).collect();

        let serializer_items = StructDef::new(Some(name.to_string()), true, fields.clone(), None)
            .generate_serializer_items();
        let name = format_ident!("{}", name);
        defs.push(quote!{
            #[derive(Clone, Debug, bincode::Encode, bincode::Decode, PartialEq,  PartialOrd, serde::Serialize, serde::Deserialize)]
            pub struct #name {
                #(#struct_fields)
                ,*
            }

            #serializer_items
        }.to_string());
    }

    let fields = convert_json_schema(source_name, schema)?;

    let mut defs: Vec<String> = vec![];

    add_defs(source_name, ROOT_NAME, &fields, &mut defs);

    Ok(format!(
        "mod {} {{\nuse super::*;\n{}\n}}",
        source_name,
        defs.join("\n")
    ))
}

fn to_schema_type(
    type_space: &TypeSpace,
    source_name: &str,
    t: &Type,
) -> Option<(TypeDef, Option<String>)> {
    match t.details() {
        TypeDetails::Struct(s) => {
            let mut fields = vec![];
            for info in s.properties_info() {
                let field_type = type_space.get_type(&info.type_id).unwrap();
                if let Some((t, original)) = to_schema_type(type_space, source_name, &field_type) {
                    fields.push(StructField::with_rename(
                        info.name.to_string(),
                        None,
                        t,
                        info.rename.map(|t| t.to_string()),
                        original,
                    ));
                }
            }

            Some((
                TypeDef::StructDef(
                    StructDef::for_name(
                        Some(format!("{}::{}", source_name, t.ident().to_string())),
                        fields,
                    ),
                    false,
                ),
                None,
            ))
        }
        TypeDetails::Option(opt) => {
            let t = type_space.get_type(&opt).unwrap();
            let (dt, original) = to_schema_type(type_space, source_name, &t)?;
            Some((dt.to_optional(), original))
        }
        TypeDetails::Builtin(t) => {
            use DataType::*;

            let (data_type, original) = match t {
                "bool" => (Boolean, None),
                "u32" => (UInt32, None),
                "u64" => (UInt64, None),
                "i32" => (Int32, None),
                "i64" => (Int64, None),
                "f32" => (Float32, None),
                "f64" => (Float64, None),
                "chrono::DateTime<chrono::offset::Utc>" => (
                    Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                    Some("datetime".to_string()),
                ),
                _ => {
                    warn!("Unhandled primitive in json-schema: {}", t);
                    return None;
                }
            };
            Some((TypeDef::DataType(data_type, false), original))
        }
        TypeDetails::String => Some((TypeDef::DataType(DataType::Utf8, false), None)),
        TypeDetails::Newtype(t) => {
            let t = type_space.get_type(&t.subtype()).unwrap();
            to_schema_type(type_space, source_name, &t)
        }
        _ => {
            warn!(
                "Unhandled JSON schema type for field {}, converting to raw json",
                t.name()
            );
            Some((
                TypeDef::DataType(DataType::Utf8, false),
                Some("json".to_string()),
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::json_schema::get_defs;

    use super::convert_json_schema;

    #[test]
    fn test() {
        let json_schema = r##"
{
  "type": "object",
  "title": "ksql.orders",
  "properties": {
    "itemid": {
      "type": "string",
      "connect.index": 2
    },
    "address": {
      "type": "object",
      "title": "ksql.address",
      "connect.index": 4,
      "properties": {
        "zipcode": {
          "type": "integer",
          "connect.index": 2,
          "connect.type": "int64"
        },
        "city": {
          "type": "string",
          "connect.index": 0
        },
        "state": {
          "type": "string",
          "connect.index": 1
        },
        "nested": {
            "type": "object",
            "properties": {
                "a": {
                    "type": "integer"
                }
            }
        }
      }
    },
    "orderid": {
      "type": "integer",
      "connect.index": 1,
      "connect.type": "int32"
    },
    "orderunits": {
      "type": "number",
      "connect.index": 3,
      "connect.type": "float64"
    },
    "ordertime": {
      "type": "integer",
      "connect.index": 0,
      "connect.type": "int64"
    }
  }
}            "##;

        let _ = convert_json_schema("nexmark", json_schema).unwrap();
        let _ = get_defs("nexmark", json_schema).unwrap();
    }
}
