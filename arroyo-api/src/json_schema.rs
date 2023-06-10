use std::{collections::hash_map::DefaultHasher, hash::{Hash, Hasher}};

use schemars::schema::{RootSchema, Schema};
use syn::{Type, parse_str};
use tracing::log::warn;
use typify::{TypeDetails, TypeSpace, TypeSpaceSettings};
use quote::{quote, format_ident};

use crate::sources::{PrimitiveType, SchemaField, SchemaFieldType};

pub const ROOT_NAME: &str = "ArroyoJsonRoot";

fn get_type_space(schema: &str) -> Result<TypeSpace, String> {
    let mut root_schema: RootSchema =
        serde_json::from_str(schema).map_err(|e| format!("Invalid json schema: {:?}", e))?;

    root_schema
        .schema
        .metadata
        .as_mut()
        .ok_or_else(|| "Schema metadata is missing".to_string())?
        .title = Some(ROOT_NAME.to_string());

    let mut type_space = TypeSpace::new(
        TypeSpaceSettings::default()
            .with_derive("bincode::Encode".to_string())
            .with_derive("bincode::Decode".to_string())
            .with_derive("PartialEq".to_string())
            .with_derive("PartialOrd".to_string())
            .with_struct_builder(true)

    );
    type_space.add_ref_types(root_schema.definitions).unwrap();
    type_space
        .add_type(&Schema::Object(root_schema.schema))
        .unwrap();

    Ok(type_space)
}

pub fn convert_json_schema(source_name: &str, schema: &str) -> Result<Vec<SchemaField>, String> {
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
        .ok_or_else(|| format!("No top-level struct in json schema {}", source_name))?;

    if let SchemaFieldType::NamedStruct(_, fields) =
        to_schema_type(&type_space, source_name, s.name(), s.details())
            .unwrap()
            .0
    {
        Ok(fields)
    } else {
        unreachable!()
    }
}

pub fn get_defs(source_name: &str, schema: &str) -> Result<String, String> {
    fn add_defs(name: &str, fields: &Vec<SchemaField>, defs: &mut Vec<String>) {
        let struct_fields: Vec<_> = fields.iter().map(|f| {
            let mut serde_opts = vec![];
            if let Some(opt) = match f.typ {
                SchemaFieldType::Primitive(PrimitiveType::Json) => {
                    if f.nullable {
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
                SchemaFieldType::Primitive(PrimitiveType::DateTime) => {
                    if f.nullable {
                        Some(quote!{
                            #[serde(default)]
                            #[serde(deserialize_with = "arroyo_worker::deserialize_rfc3339_datetime_opt")]
                        })
                    } else {
                        Some(quote! {
                            #[serde(deserialize_with = "arroyo_worker::deserialize_rfc3339_datetime")]
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

            let name = format_ident!("{}", f.name);
            let typ = match &f.typ {
                SchemaFieldType::Primitive(p) => p.to_rust().to_string(),
                SchemaFieldType::Struct(fs) => {
                    let mut s = DefaultHasher::new();
                    name.hash(&mut s);
                    let struct_name = format!("generated_struct_{}", s.finish());
                    add_defs(&struct_name, fs, defs);

                    struct_name
                },
                SchemaFieldType::NamedStruct(name, fs) => {
                    add_defs(name, fs, defs);

                    name.clone()
                },
            };

            let typ: Type = parse_str(&typ).unwrap();

            let typ = if f.nullable {
                quote! { Option<#typ> }
            } else {
                quote! { #typ }
            };

            quote! {
                #(#serde_opts) *
                pub #name: #typ
            }
        }).collect();

        let name = format_ident!("{}", name);
        defs.push(quote!{
            #[derive(Clone, Debug, bincode::Encode, bincode::Decode, PartialEq,  PartialOrd, serde::Serialize, serde::Deserialize)]
            pub struct #name {
                #(#struct_fields)
                ,*
            }
        }.to_string());
    }

    let fields = convert_json_schema(source_name, schema)?;

    let mut defs: Vec<String> = vec![];

    add_defs(ROOT_NAME, &fields, &mut defs);

    Ok(format!(
        "mod {} {{\nuse crate::*;\n{}\n}}",
        source_name,
        defs.join("\n")
    ))
}

fn to_schema_type(
    type_space: &TypeSpace,
    source_name: &str,
    type_name: String,
    td: TypeDetails,
) -> Option<(SchemaFieldType, bool)> {
    match td {
        TypeDetails::Struct(s) => {
            let mut fields = vec![];
            for info in s.properties_info() {
                let field_type = type_space.get_type(&info.type_id).unwrap();
                if let Some((t, nullable)) = to_schema_type(
                    type_space,
                    source_name,
                    field_type.name(),
                    field_type.details(),
                ) {
                    fields.push(SchemaField {
                        name: info.name.to_string(),
                        renamed_from: info.rename.map(|t| t.to_string()),
                        typ: t,
                        nullable,
                    });
                }
            }

            Some((
                SchemaFieldType::NamedStruct(format!("{}::{}", source_name, type_name), fields),
                false,
            ))
        }
        TypeDetails::Option(opt) => {
            let t = type_space.get_type(&opt).unwrap();
            Some((
                to_schema_type(type_space, source_name, t.name(), t.details())?.0,
                true,
            ))
        }
        TypeDetails::Builtin(t) => {
            use PrimitiveType::*;
            use SchemaFieldType::*;
            let primitive = match t {
                "bool" => Primitive(Bool),
                "u32" => Primitive(UInt32),
                "u64" => Primitive(UInt64),
                "i32" => Primitive(Int32),
                "i64" => Primitive(Int64),
                "f32" => Primitive(F32),
                "f64" => Primitive(F64),
                "chrono::DateTime<chrono::offset::Utc>" => Primitive(DateTime),
                _ => {
                    warn!("Unhandled primitive in json-schema: {}", t);
                    return None;
                }
            };
            Some((primitive, false))
        }
        TypeDetails::String => Some((SchemaFieldType::Primitive(PrimitiveType::String), false)),
        TypeDetails::Newtype(t) => {
            let t = type_space.get_type(&t.subtype()).unwrap();
            to_schema_type(type_space, source_name, t.name(), t.details())
        },
        _ => {
            warn!("Unhandled JSON schema type for field {}, converting to raw json", type_name);
            Some((SchemaFieldType::Primitive(PrimitiveType::Json), false))
        }
    }
}

#[cfg(test)]
mod test {
    use super::convert_json_schema;

    #[test]
    fn test() {
        convert_json_schema(
            "nexmark",
            r##"
            {
                "$schema": "https://json-schema.org/draft/2019-09/schema",
                "type": "object",
                "default": {},
                "title": "Root Schema",
                "properties": {
                    "auction": { "$ref": "#/definitions/Auction" },
                    "bid": { "$ref": "#/definitions/Bid" }
                },
                "definitions": {
                    "Auction": {
                        "type": "object",
                        "default": {},
                        "required": [
                            "id",
                            "itemName",
                            "description",
                            "initialBid",
                            "reserve",
                            "dateTime",
                            "expires",
                            "seller",
                            "category",
                            "extra"
                        ],
                        "properties": {
                            "id": {
                                "type": "integer"
                            },
                            "itemName": {
                                "type": "string"
                            },
                            "description": {
                                "type": "string"
                            },
                            "initialBid": {
                                "type": "integer"
                            },
                            "reserve": {
                                "type": "integer"
                            },
                            "dateTime": {
                                "type": "number"
                            },
                            "expires": {
                                "type": "number"
                            },
                            "seller": {
                                "type": "integer"
                            },
                            "category": {
                                "type": "integer"
                            },
                            "extra": {
                                "type": "string"
                            }
                        }
                    },
                    "Bid": {
                        "type": "object",
                        "default": {},
                        "required": [
                            "auction",
                            "bidder",
                            "price",
                            "channel",
                            "url",
                            "dateTime",
                            "extra"
                        ],
                        "properties": {
                            "auction": {
                                "type": "integer"
                            },
                            "bidder": {
                                "type": "integer"
                            },
                            "price": {
                                "type": "integer"
                            },
                            "channel": {
                                "type": "string"
                            },
                            "url": {
                                "type": "string"
                            },
                            "dateTime": {
                                "type": "number"
                            },
                            "extra": {
                                "type": "string"
                            }
                        }
                    }
                }
            }
            "##,
        )
        .unwrap();
    }
}
