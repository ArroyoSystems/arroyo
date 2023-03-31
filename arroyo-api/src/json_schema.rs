use schemars::schema::{RootSchema, Schema};
use tracing::log::warn;
use typify::{TypeDetails, TypeSpace, TypeSpaceSettings};

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
            .with_struct_builder(true),
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
    let type_space = get_type_space(schema)?;

    Ok(format!(
        "mod {} {{\nuse crate::*;\n{}\n}}",
        source_name,
        type_space.to_string()
    ))
}

fn to_schema_type(
    type_space: &TypeSpace,
    source_name: &str,
    type_name: String,
    td: TypeDetails,
) -> Option<(SchemaFieldType, bool)> {
    match td {
        TypeDetails::Enum(_) => {
            warn!("Enums are not currently supported; ignoring {}", type_name);
            None
        }
        TypeDetails::Struct(s) => {
            let mut fields = vec![];
            for (n, p) in s.properties() {
                let field_type = type_space.get_type(&p).unwrap();
                if let Some((t, nullable)) = to_schema_type(
                    type_space,
                    source_name,
                    field_type.name(),
                    field_type.details(),
                ) {
                    fields.push(SchemaField {
                        name: n.to_string(),
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
                _ => {
                    warn!("Unhandled primitive in json-schema: {}", t);
                    return None;
                }
            };
            Some((primitive, false))
        }
        TypeDetails::String => Some((SchemaFieldType::Primitive(PrimitiveType::String), false)),
        _ => {
            warn!("Unhandled field type in json-schema {:?}", type_name);
            None
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
            r#"
            {
                "$schema": "https://json-schema.org/draft/2019-09/schema",
                "$id": "http://example.com/example.json",
                "type": "object",
                "default": {},
                "title": "Root Schema",
                "properties": {
                    "auction": { "$ref": "\\#/definitions/Auction" },
                    "bid": { "$ref": "\\#/definitions/Bid" }
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
            "#,
        )
        .unwrap();
    }
}
