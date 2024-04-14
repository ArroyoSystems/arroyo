use anyhow::{anyhow, bail};
use arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;
use syn::PathArguments::AngleBracketed;
use syn::__private::ToTokens;
use syn::{FnArg, GenericArgument, ItemFn, ReturnType, Type};

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
    pub aggregate: bool,
    pub is_async: bool,
}

pub struct ParsedUdf {
    pub function: String,
    pub name: String,
    pub args: Vec<NullableType>,
    pub vec_arguments: usize,
    pub ret_type: NullableType,
    pub is_async: bool,
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
            function: function.into_token_stream().to_string(),
            name,
            args,
            vec_arguments,
            ret_type: ret,
            is_async: function.sig.asyncness.is_some(),
        })
    }
}

pub fn inner_type(dt: &DataType) -> Option<DataType> {
    match dt {
        DataType::List(f) => Some(f.data_type().clone()),
        _ => None,
    }
}
