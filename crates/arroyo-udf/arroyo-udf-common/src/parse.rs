use std::sync::Arc;
use anyhow::{anyhow, bail};
use arrow::datatypes::{DataType, Field, TimeUnit};
use syn::{FnArg, GenericArgument, Item, ItemFn, parse_file, ReturnType, Type, Visibility};
use syn::PathArguments::AngleBracketed;
use regex::Regex;
use syn::__private::{Span, ToTokens};
use syn::parse::{Parse, ParseStream};

/// An Arrow DataType that also carries around its own nullability info
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NullableType {
    pub data_type: DataType,
    pub nullable: bool,
}

impl NullableType {
    pub fn null(data_type: DataType) -> Self {
        Self {
            data_type,
            nullable: true,
        }
    }

    pub fn not_null(data_type: DataType) -> Self {
        Self {
            data_type,
            nullable: false,
        }
    }

    pub fn with_nullability(&self, nullable: bool) -> Self {
        Self {
            data_type: self.data_type.clone(),
            nullable,
        }
    }
}

fn rust_to_arrow(typ: &Type) -> Option<NullableType> {
    match typ {
        Type::Path(pat) => {
            let last = pat.path.segments.last().unwrap();
            if last.ident == "Option" {
                let AngleBracketed(args) = &last.arguments else {
                    return None;
                };

                let GenericArgument::Type(inner) = args.args.first()? else {
                    return None;
                };

                Some(rust_to_arrow(inner)?.with_nullability(true))
            } else {
                Some(NullableType::not_null(rust_primitive_to_arrow(typ)?))
            }
        }
        _ => None,
    }
}

fn rust_primitive_to_arrow(typ: &Type) -> Option<DataType> {
    match typ {
        Type::Path(pat) => {
            let path: Vec<String> = pat
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();

            match path.join("::").as_str() {
                "bool" => Some(DataType::Boolean),
                "i8" => Some(DataType::Int8),
                "i16" => Some(DataType::Int16),
                "i32" => Some(DataType::Int32),
                "i64" => Some(DataType::Int64),
                "u8" => Some(DataType::UInt8),
                "u16" => Some(DataType::UInt16),
                "u32" => Some(DataType::UInt32),
                "u64" => Some(DataType::UInt64),
                "f16" => Some(DataType::Float16),
                "f32" => Some(DataType::Float32),
                "f64" => Some(DataType::Float64),
                "String" => Some(DataType::Utf8),
                "Vec<u8>" => Some(DataType::Binary),
                "SystemTime" | "std::time::SystemTime" => {
                    Some(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
                "Duration" | "std::time::Duration" => {
                    Some(DataType::Duration(TimeUnit::Microsecond))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub struct UdfDef {
    pub args: Vec<NullableType>,
    pub ret: NullableType,
    pub def: String,
    pub dependencies: String,
    pub aggregate: bool,
}

pub struct ParsedUdfFile {
    pub udf: ParsedUdf,
    pub definition: String,
    pub dependencies: String,
}

impl ParsedUdfFile {
    pub fn try_parse(def: &str) -> anyhow::Result<Self> {
        let mut file = parse_file(def)?;

        let mut functions = file.items.iter_mut().filter_map(|item| match item {
            Item::Fn(function) => Some(function),
            _ => None,
        });

        let function = match (functions.next(), functions.next()) {
            (Some(function), None) => function,
            _ => bail!("UDF definition must contain exactly 1 function."),
        };

        let udf = ParsedUdf::try_parse(function)?;
        function.vis = Visibility::Public(Default::default());

        Ok(ParsedUdfFile {
            udf,
            definition: function.into_token_stream().to_string(),
            dependencies: parse_dependencies(def)?,
        })
    }
}

pub struct ParsedUdf {
    pub function: ItemFn,
    pub name: String,
    pub args: Vec<NullableType>,
    pub vec_arguments: usize,
    pub ret_type: NullableType,
}

fn parse_dependencies(definition: &str) -> anyhow::Result<String> {
    // get content of dependencies comment using regex
    let re = Regex::new(r"/\*\n(\[dependencies]\n[\s\S]*?)\*/").unwrap();
    if re.find_iter(definition).count() > 1 {
        bail!("Only one dependencies definition is allowed in a UDF");
    }

    return if let Some(captures) = re.captures(definition) {
        if captures.len() != 2 {
            bail!("Error parsing dependencies");
        }
        Ok(captures.get(1).unwrap().as_str().to_string())
    } else {
        Ok("[dependencies]\n# none defined\n".to_string())
    };
}

impl ParsedUdf {
    fn vec_inner_type(ty: &syn::Type) -> Option<syn::Type> {
        if let syn::Type::Path(syn::TypePath { path, .. }) = ty {
            if let Some(segment) = path.segments.last() {
                if segment.ident == "Vec" {
                    if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                        if args.args.len() == 1 {
                            if let syn::GenericArgument::Type(inner_ty) = &args.args[0] {
                                return Some(inner_ty.clone());
                            }
                        }
                    }
                }
            }
        }
        None
    }
    
    pub fn try_parse(function: &ItemFn) -> anyhow::Result<ParsedUdf> {
        let name = function.sig.ident.to_string();
        let mut args = vec![];
        let mut vec_arguments = 0;
        for (i, arg) in function.sig.inputs.iter().enumerate() {
            match arg {
                FnArg::Receiver(_) => {
                    bail!(
                        "Function {} has a 'self' argument, which is not allowed",
                        name
                    )
                }
                FnArg::Typed(t) => {
                    if let Some(vec_type) = Self::vec_inner_type(&t.ty) {
                        vec_arguments += 1;
                        let vec_type = rust_to_arrow(&vec_type).ok_or_else(|| {
                            anyhow!(
                                "Could not convert function {} inner vector arg {} into an Arrow data type",
                                name,
                                i
                            )
                        })?;

                        args.push(NullableType::not_null(DataType::List(Arc::new(
                            Field::new("item", vec_type.data_type, vec_type.nullable),
                        ))));
                    } else {
                        args.push(rust_to_arrow(&t.ty).ok_or_else(|| {
                            anyhow!(
                                "Could not convert function {} arg {} into a SQL data type",
                                name,
                                i
                            )
                        })?);
                    }
                }
            }
        }

        let ret = match &function.sig.output {
            ReturnType::Default => bail!("Function {} return type must be specified", name),
            ReturnType::Type(_, t) => rust_to_arrow(t).ok_or_else(|| {
                anyhow!(
                    "Could not convert function {} return type into a SQL data type",
                    name
                )
            })?,
        };


        Ok(ParsedUdf {
            function: function.clone(),
            name,
            args,
            vec_arguments,
            ret_type: ret,
        })
    }
}

impl Parse for ParsedUdf {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let function: ItemFn = input.parse()?;
        ParsedUdf::try_parse(&function)
            .map_err(|e| syn::Error::new(Span::call_site(), e.to_string()))
    }
}

pub fn inner_type(dt: &DataType) -> Option<DataType> {
    match dt {
        DataType::List(f) => Some(f.data_type().clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dependencies_valid() {
        let definition = r#"
/*
[dependencies]
serde = "1.0"
*/

pub fn my_udf() -> i64 {
    1
}
        "#;

        assert_eq!(
            parse_dependencies(definition).unwrap(),
            r#"[dependencies]
serde = "1.0"
"#
        );
    }

    #[test]
    fn test_parse_dependencies_none() {
        let definition = r#"
pub fn my_udf() -> i64 {
    1
}
        "#;

        assert_eq!(
            parse_dependencies(definition).unwrap(),
            r#"[dependencies]
# none defined
"#
        );
    }

    #[test]
    fn test_parse_dependencies_multiple() {
        let definition = r#"
/*
[dependencies]
serde = "1.0"
*/

/*
[dependencies]
serde = "1.0"
*/

pub fn my_udf() -> i64 {
    1

        "#;
        assert!(parse_dependencies(definition).is_err());
    }
}
