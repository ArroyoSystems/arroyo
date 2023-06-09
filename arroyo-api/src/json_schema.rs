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
        type_space.to_stream()
    ))
}

fn to_schema_type(
    type_space: &TypeSpace,
    source_name: &str,
    type_name: String,
    td: TypeDetails,
) -> Option<(SchemaFieldType, bool)> {
    println!("{}", type_name);
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
                "chrono::DateTime<chrono::offset::Utc>" => Primitive(UnixMillis),
                _ => {
                    println!("Unhandled primitive in json-schema: {}", t);
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
        TypeDetails::Array(..) => {
            warn!("Arrays are not currently supported");
            None
        }
        TypeDetails::Map(..) => {
            warn!("Maps are not currently supported");
            None
        }
        TypeDetails::Set(_) => {
            warn!("Sets are not currently supported");
            None
        }
        TypeDetails::Box(_) => {
            warn!("Boxes are not currently supported");
            None
        }
        TypeDetails::Tuple(_) => {
            warn!("Tuples are not currently supported");
            None
        }
        TypeDetails::Unit => {
            warn!("The unit type is not currently supported");
            None
        }
        TypeDetails::Vec(_) => {
            warn!("The vec type is not currently supported");
            None
        },
    }
}

#[cfg(test)]
mod test {
    use super::convert_json_schema;

    #[test]
    fn test() {
        let s = convert_json_schema(
            "nexmark",
            r##"
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "description": "CloudEvents Specification JSON Schema",
  "type": "object",
  "properties": {
    "id": {
      "description": "Identifies the event.",
      "$ref": "#/definitions/iddef",
      "examples": [
        "A234-1234-1234"
      ]
    },
    "source": {
      "description": "Identifies the context in which an event happened.",
      "$ref": "#/definitions/sourcedef",
      "examples" : [
        "https://github.com/cloudevents",
        "mailto:cncf-wg-serverless@lists.cncf.io",
        "urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66",
        "cloudevents/spec/pull/123",
        "/sensors/tn-1234567/alerts",
        "1-555-123-4567"
      ]
    },
    "specversion": {
      "description": "The version of the CloudEvents specification which the event uses.",
      "$ref": "#/definitions/specversiondef",
      "examples": [
        "1.0"
      ]
    },
    "type": {
      "description": "Describes the type of event related to the originating occurrence.",
      "$ref": "#/definitions/typedef",
      "examples" : [
        "com.github.pull_request.opened",
        "com.example.object.deleted.v2"
      ]
    },
    "datacontenttype": {
      "description": "Content type of the data value. Must adhere to RFC 2046 format.",
      "$ref": "#/definitions/datacontenttypedef",
      "examples": [
        "text/xml",
        "application/json",
        "image/png",
        "multipart/form-data"
      ]
    },
    "dataschema": {
      "description": "Identifies the schema that data adheres to.",
      "$ref": "#/definitions/dataschemadef"
    },
    "subject": {
      "description": "Describes the subject of the event in the context of the event producer (identified by source).",
      "$ref": "#/definitions/subjectdef",
      "examples": [
        "mynewfile.jpg"
      ]
    },
    "time": {
      "description": "Timestamp of when the occurrence happened. Must adhere to RFC 3339.",
      "$ref": "#/definitions/timedef",
      "examples": [
        "2018-04-05T17:31:00Z"
      ]
    },
    "data": {
      "description": "The event payload.",
      "$ref": "#/definitions/datadef",
      "examples": [
        "<much wow=\"xml\"/>"
      ]
    },
    "data_base64": {
      "description": "Base64 encoded event payload. Must adhere to RFC4648.",
      "$ref": "#/definitions/data_base64def",
      "examples": [
        "Zm9vYg=="
      ]
    }
  },
  "required": ["id", "source", "specversion", "type"],
  "definitions": {
    "iddef": {
      "type": "string",
      "minLength": 1
    },
    "sourcedef": {
      "type": "string",
      "format": "uri-reference",
      "minLength": 1
    },
    "specversiondef": {
      "type": "string",
      "minLength": 1
    },
    "typedef": {
      "type": "string",
      "minLength": 1
    },
    "datacontenttypedef": {
      "type": ["string", "null"],
      "minLength": 1
    },
    "dataschemadef": {
      "type": ["string", "null"],
      "format": "uri",
      "minLength": 1
    },
    "subjectdef": {
      "type": ["string", "null"],
      "minLength": 1
    },
    "timedef": {
      "type": ["string", "null"],
      "format": "date-time",
      "minLength": 1
    },
    "datadef": {
      "type": ["object", "string", "number", "array", "boolean", "null"]
    },
    "data_base64def": {
      "type": ["string", "null"],
      "contentEncoding": "base64"
    }
  }
}
            "##,
        )
        .unwrap();

    println!("shcema fields: {:?}", s);
    }
}
