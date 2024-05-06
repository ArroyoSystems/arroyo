use anyhow::{anyhow, bail};
use arrow_schema::{DataType, Field, TimeUnit};
use arroyo_types::ArroyoExtensionType;
use schemars::schema::{RootSchema, Schema};
use std::sync::Arc;
use tracing::warn;
use typify::{Type, TypeDetails, TypeSpace, TypeSpaceSettings};

pub const ROOT_NAME: &str = "ArroyoJsonRoot";

fn get_type_space(schema: &str) -> anyhow::Result<TypeSpace> {
    let mut root_schema: RootSchema =
        serde_json::from_str(schema).map_err(|e| anyhow!("Invalid json schema: {:?}", e))?;

    if root_schema.schema.metadata.is_none() {
        root_schema.schema.metadata = Some(Box::default());
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

pub fn to_arrow(name: &str, schema: &str) -> anyhow::Result<arrow_schema::Schema> {
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
        .ok_or_else(|| anyhow!("No top-level struct in json schema {}", name))?;

    let (dt, _, _) = to_arrow_datatype(&type_space, &s, None);

    let fields = match dt {
        DataType::Struct(fields) => fields,
        _ => {
            bail!("top-level schema must be a record")
        }
    };

    Ok(arrow_schema::Schema::new(fields))
}

fn to_arrow_datatype(
    type_space: &TypeSpace,
    t: &Type,
    required: Option<bool>,
) -> (DataType, bool, Option<ArroyoExtensionType>) {
    match t.details() {
        TypeDetails::Struct(s) => {
            let fields = s
                .properties_info()
                .map(|info| {
                    let field_type = type_space.get_type(&info.type_id).unwrap();
                    let (t, nullable, extension) =
                        to_arrow_datatype(type_space, &field_type, Some(info.required));
                    Arc::new(ArroyoExtensionType::add_metadata(
                        extension,
                        Field::new(info.rename.unwrap_or(info.name), t, nullable),
                    ))
                })
                .collect();

            (DataType::Struct(fields), false, None)
        }
        TypeDetails::Option(opt) => {
            let t = type_space.get_type(&opt).unwrap();
            let (dt, _, extension) = to_arrow_datatype(type_space, &t, None);
            (dt, true, extension)
        }
        TypeDetails::Builtin(t) => {
            use DataType::*;

            let data_type = match t {
                "bool" => Boolean,
                "u32" => UInt32,
                "u64" => UInt64,
                "i32" => Int32,
                "i64" => Int64,
                "f32" => Float32,
                "f64" => Float64,
                "chrono::DateTime<chrono::offset::Utc>" => Timestamp(TimeUnit::Nanosecond, None),
                _ => {
                    warn!("Unhandled primitive in json-schema: {}", t);
                    return (Utf8, false, Some(ArroyoExtensionType::JSON));
                }
            };
            (data_type, false, None)
        }
        TypeDetails::String => (DataType::Utf8, false, None),
        TypeDetails::Newtype(t) => {
            let t = type_space.get_type(&t.subtype()).unwrap();
            to_arrow_datatype(type_space, &t, None)
        }
        TypeDetails::Array(t, _) | TypeDetails::Vec(t) => {
            let t = type_space.get_type(&t).unwrap();
            let (t, nullable, extension) = to_arrow_datatype(type_space, &t, None);
            (
                DataType::List(Arc::new(ArroyoExtensionType::add_metadata(
                    extension,
                    Field::new("item", t, nullable),
                ))),
                !required.unwrap_or(true),
                None,
            )
        }
        _ => {
            warn!(
                "Unhandled JSON schema type for field {}, converting to raw json",
                t.name()
            );
            (DataType::Utf8, false, Some(ArroyoExtensionType::JSON))
        }
    }
}

#[cfg(test)]
mod test {
    use super::to_arrow;

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

        let _ = to_arrow("nexmark", json_schema).unwrap();
    }
}
