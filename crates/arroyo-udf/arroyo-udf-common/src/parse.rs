use anyhow::{anyhow, bail};
use arrow::datatypes::{DataType, Field, TimeUnit};
use regex::Regex;
use std::sync::Arc;
use std::time::Duration;
use syn::PathArguments::AngleBracketed;
use syn::__private::ToTokens;
use syn::{FnArg, GenericArgument, ItemFn, LitInt, LitStr, ReturnType, Type};

/// An Arrow DataType that also carries around its own nullability info
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NullableType {
    pub data_type: DataType,
    pub nullable: bool,
}

impl NullableType {
    pub fn new(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
        }
    }

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

pub fn is_vec_u8(typ: &Type) -> bool {
    let Some(inner) = ParsedUdf::vec_inner_type(typ) else {
        return false;
    };

    matches!(
        rust_to_arrow(&inner, true),
        Ok(NullableType {
            data_type: DataType::UInt8,
            nullable: false
        })
    )
}

pub(crate) fn rust_to_arrow(typ: &Type, expect_owned: bool) -> anyhow::Result<NullableType> {
    match typ {
        Type::Path(pat) => {
            let last = pat.path.segments.last().unwrap();
            if last.ident == "Option" {
                let AngleBracketed(args) = &last.arguments else {
                    bail!("invalid Rust type; Option must have arguments");
                };

                let Some(GenericArgument::Type(inner)) = args.args.first() else {
                    bail!("invalid Rust type; Option must have an inner type parameter")
                };

                Ok(rust_to_arrow(inner, expect_owned)?.with_nullability(true))
            } else {
                let mut dt = rust_primitive_to_arrow(typ);

                if dt.is_none() {
                    dt = Some(
                        match (
                            render_path(typ)
                                .ok_or_else(|| anyhow!("unsupported Rust type1"))?
                                .as_str(),
                            expect_owned,
                        ) {
                            ("String", true) => DataType::Utf8,
                            ("String", false) => {
                                bail!("expected reference type &str instead of String")
                            }
                            ("Vec<u8>", true) => DataType::Binary,
                            ("Vec<u8>", false) => {
                                bail!("expected reference type &[u8] instead of Vec<u8>")
                            }
                            (t, _) => bail!("unsupported Rust type {}", t),
                        },
                    );
                }

                Ok(NullableType::not_null(
                    dt.ok_or_else(|| anyhow!("unsupported Rust type2"))?,
                ))
            }
        }
        Type::Reference(r) => {
            let t = render_path(&r.elem).ok_or_else(|| anyhow!("unsupported Rust type3"))?;

            let dt = match (t.as_str(), rust_primitive_to_arrow(&r.elem), expect_owned) {
                ("String", _, false) => bail!("expected &str, not &String"),
                ("String", _, true) => {
                    bail!("expected owned String, not &String (hint: remove the &)")
                }
                ("Vec<u8>", _, false) => bail!("expected &[u8], not &Vec<u8>"),
                ("Vec<u8>", _, true) => {
                    bail!("expected owned Vec<u8>, not &Vec<u8> (hint: remove the &)")
                }
                ("str", _, false) => DataType::Utf8,
                ("str", _, true) => bail!("expected owned String, not &str"),
                ("[u8]", _, false) => DataType::Binary,
                ("[u8]", _, true) => bail!("expected owned Vec<u8>, not &[u8]"),
                (t, Some(_), _) => bail!(
                    "unexpected &{}; primitives should be passed by value (hint: remove the &)",
                    t
                ),
                _ => {
                    bail!("unsupported Rust data type")
                }
            };

            Ok(NullableType::not_null(dt))
        }
        _ => bail!("unsupported Rust data type"),
    }
}

fn render_path(typ: &Type) -> Option<String> {
    match typ {
        Type::Path(pat) => {
            let path: Vec<String> = pat
                .path
                .segments
                .iter()
                .map(|s| s.to_token_stream().to_string().replace(' ', ""))
                .collect();

            Some(path.join("::"))
        }
        Type::Slice(t) => Some(format!("[{}]", render_path(&t.elem)?)),
        _ => None,
    }
}

fn rust_primitive_to_arrow(typ: &Type) -> Option<DataType> {
    match render_path(typ)?.as_str() {
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
        "SystemTime" | "std::time::SystemTime" => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        "Duration" | "std::time::Duration" => Some(DataType::Duration(TimeUnit::Microsecond)),
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub struct UdfDef {
    pub args: Vec<NullableType>,
    pub ret: NullableType,
    pub aggregate: bool,
    pub udf_type: UdfType,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AsyncOptions {
    pub ordered: bool,
    pub timeout: Duration,
    pub max_concurrency: usize,
}

impl Default for AsyncOptions {
    fn default() -> Self {
        Self {
            ordered: false,
            timeout: Duration::from_secs(5),
            max_concurrency: 1000,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum UdfType {
    Sync,
    Async(AsyncOptions),
}

impl UdfType {
    pub fn is_async(&self) -> bool {
        !matches!(self, UdfType::Sync)
    }
}

fn parse_duration(input: &str) -> anyhow::Result<Duration> {
    let r = Regex::new(r"^(\d+)\s*([a-zA-Zµ]+)$").unwrap();
    let captures = r
        .captures(input)
        .ok_or_else(|| anyhow!("invalid duration specification '{}'", input))?;
    let mut capture = captures.iter();

    capture.next();

    let n: u64 = capture.next().unwrap().unwrap().as_str().parse().unwrap();
    let unit = capture.next().unwrap().unwrap().as_str();

    Ok(match unit {
        "ns" | "nanos" => Duration::from_nanos(n),
        "µs" | "micros" => Duration::from_micros(n),
        "ms" | "millis" => Duration::from_millis(n),
        "s" | "secs" | "seconds" => Duration::from_secs(n),
        "m" | "mins" | "minutes" => Duration::from_secs(n * 60),
        "h" | "hrs" | "hours" => Duration::from_secs(n * 60 * 60),
        x => bail!("unknown time unit '{}'", x),
    })
}

pub struct ParsedUdf {
    pub function: String,
    pub name: String,
    pub args: Vec<NullableType>,
    pub vec_arguments: usize,
    pub ret_type: NullableType,
    pub udf_type: UdfType,
}

impl ParsedUdf {
    pub fn vec_inner_type(ty: &syn::Type) -> Option<syn::Type> {
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
                    let vec_type = Self::vec_inner_type(&t.ty);
                    if vec_type.is_some() {
                        vec_arguments += 1;
                        let vec_type = rust_to_arrow(vec_type.as_ref().unwrap(), false).map_err(|e| {
                            anyhow!(
                                "Could not convert function {name} inner vector arg {i} into an Arrow data type: {e}",
                            )
                        })?;

                        args.push(NullableType::not_null(DataType::List(Arc::new(
                            Field::new("item", vec_type.data_type, vec_type.nullable),
                        ))));
                    } else {
                        args.push(rust_to_arrow(&t.ty, false).map_err(|e| {
                            anyhow!(
                                "Could not convert function {name} arg {i} into a SQL data type: {e}",
                            )
                        })?);
                    }
                }
            }
        }

        let ret = match &function.sig.output {
            ReturnType::Default => bail!("Function {} return type must be specified", name),
            ReturnType::Type(_, t) => rust_to_arrow(t, true).map_err(|e| {
                anyhow!("Could not convert function {name} return type into a SQL data type: {e}",)
            })?,
        };

        let udf_type = if function.sig.asyncness.is_some() {
            let mut t = AsyncOptions::default();

            if let Some(attr) = function
                .attrs
                .iter()
                .find(|attr| attr.path().is_ident("udf"))
            {
                if attr.meta.require_path_only().is_err() {
                    attr.parse_nested_meta(|meta| {
                        if meta.path.is_ident("ordered") {
                            t.ordered = true;
                        } else if meta.path.is_ident("unordered") {
                            t.ordered = false;
                        } else if meta.path.is_ident("allowed_in_flight") {
                            let value = meta.value()?;
                            let s: LitInt = value.parse()?;
                            let n: usize = s
                                .base10_digits()
                                .parse()
                                .map_err(|_| meta.error("expected number"))?;
                            t.max_concurrency = n;
                        } else if meta.path.is_ident("timeout") {
                            let value = meta.value()?;
                            let s: LitStr = value.parse()?;
                            t.timeout = parse_duration(&s.value()).map_err(|e| meta.error(e))?;
                        } else {
                            return Err(meta.error(format!(
                                "unsupported attribute '{}'",
                                meta.path.to_token_stream()
                            )));
                        }
                        Ok(())
                    })?;
                }
            }

            UdfType::Async(t)
        } else {
            UdfType::Sync
        };

        Ok(ParsedUdf {
            function: function.into_token_stream().to_string(),
            name,
            args,
            vec_arguments,
            ret_type: ret,
            udf_type,
        })
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
    use crate::parse::{parse_duration, rust_to_arrow, NullableType};
    use arrow::datatypes::DataType;
    use std::time::Duration;
    use syn::parse_quote;

    #[test]
    fn test_duration() {
        assert_eq!(Duration::from_secs(5), parse_duration("5s").unwrap());
        assert_eq!(Duration::from_secs(5), parse_duration("5 seconds").unwrap());
        assert_eq!(Duration::from_secs(5), parse_duration("5   secs").unwrap());

        assert_eq!(Duration::from_millis(10), parse_duration("10ms").unwrap());
        assert_eq!(
            Duration::from_millis(110),
            parse_duration("110millis").unwrap()
        );

        assert!(parse_duration("-10ms").is_err());
        assert!(parse_duration("10.0s").is_err());
        assert!(parse_duration("5s what").is_err());
    }

    #[test]
    fn test_rust_to_arrow() {
        assert_eq!(
            rust_to_arrow(&parse_quote!(i32), false).unwrap(),
            NullableType::not_null(DataType::Int32)
        );
        assert_eq!(
            rust_to_arrow(&parse_quote!(Option<i32>), false).unwrap(),
            NullableType::null(DataType::Int32)
        );
        assert_eq!(
            rust_to_arrow(&parse_quote!(Vec<u8>), true).unwrap(),
            NullableType::not_null(DataType::Binary)
        );
        assert_eq!(
            rust_to_arrow(&parse_quote!(&[u8]), false).unwrap(),
            NullableType::not_null(DataType::Binary)
        );
        assert_eq!(
            rust_to_arrow(&parse_quote!(Vec<u8>), true).unwrap(),
            NullableType::not_null(DataType::Binary)
        );

        assert_eq!(
            rust_to_arrow(&parse_quote!(u64), false).unwrap(),
            NullableType::not_null(DataType::UInt64)
        );
        assert_eq!(
            rust_to_arrow(&parse_quote!(f32), false).unwrap(),
            NullableType::not_null(DataType::Float32)
        );
        assert_eq!(
            rust_to_arrow(&parse_quote!(bool), false).unwrap(),
            NullableType::not_null(DataType::Boolean)
        );

        assert_eq!(
            rust_to_arrow(&parse_quote!(Option<f64>), false).unwrap(),
            NullableType::null(DataType::Float64)
        );
        assert_eq!(
            rust_to_arrow(&parse_quote!(Option<bool>), false).unwrap(),
            NullableType::null(DataType::Boolean)
        );

        assert_eq!(
            rust_to_arrow(&parse_quote!(String), true).unwrap(),
            NullableType::not_null(DataType::Utf8)
        );
        assert_eq!(
            rust_to_arrow(&parse_quote!(&str), false).unwrap(),
            NullableType::not_null(DataType::Utf8)
        );

        assert_eq!(
            rust_to_arrow(&parse_quote!(Option<String>), true).unwrap(),
            NullableType::null(DataType::Utf8)
        );
        assert_eq!(
            rust_to_arrow(&parse_quote!(Option<&str>), false).unwrap(),
            NullableType::null(DataType::Utf8)
        );

        assert_eq!(
            rust_to_arrow(&parse_quote!(HashMap<String, i32>), false).ok(),
            None
        );
        assert_eq!(rust_to_arrow(&parse_quote!(CustomStruct), false).ok(), None);

        assert_eq!(rust_to_arrow(&parse_quote!(Vec<u8>), false).ok(), None);
        assert_eq!(rust_to_arrow(&parse_quote!(&[u8]), true).ok(), None);
    }
}
