use crate::ParsedUdf;
use anyhow::bail;
use arrow_schema::DataType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse_quote;

pub fn cargo_toml(dependencies: &str) -> String {
    format!(
        r#"
[package]
name = "udf"
version = "1.0.0"
edition = "2021"

{}
        "#,
        dependencies
    )
}

pub fn lib_rs(definition: &str) -> anyhow::Result<String> {
    let parsed = ParsedUdf::try_parse(definition)?;

    let udf_name = format_ident!("{}", parsed.name);

    let results_builder = if matches!(parsed.ret_type.data_type, DataType::Utf8) {
        quote!(let mut results_builder = array::StringBuilder::with_capacity(batch_size, batch_size * 64);)
    } else {
        let return_type = data_type_to_arrow_type_token(parsed.ret_type.data_type.clone())?;
        quote!(let mut results_builder = array::PrimitiveBuilder::<datatypes::#return_type>::with_capacity(batch_size);)
    };

    let mut arrow_types = vec![];
    for arg in &parsed.args {
        let arrow_type = data_type_to_arrow_type_token(arg.data_type.clone())?;
        arrow_types.push(arrow_type);
    }

    let udaf = parsed
        .args
        .iter()
        .any(|arg| matches!(arg.data_type, DataType::List(_)));

    let (defs, args): (Vec<_>, Vec<_>) = parsed
        .args
        .iter().zip(arrow_types.iter())
        .enumerate()
        .map(|(i, (arg_type, arrow_type))| {
            let id = format_ident!("arg_{}", i);
            let def = match &arg_type.data_type {
                DataType::Utf8 => {
                    quote!(let #id = array::StringArray::from(args.next().unwrap());)
                }
                DataType::List(field) => {
                    let filter = if !field.is_nullable() {
                        quote!(.filter_map(|x| x))
                    } else {
                        quote!()
                    };

                    quote!(let #id = array::PrimitiveArray::<datatypes::#arrow_type>::from(
                        args.next().unwrap()
                    ).iter()#filter.collect();)
                }
                _ => {
                    quote!(let #id = array::PrimitiveArray::<datatypes::#arrow_type>::from(args.next().unwrap());)
                }
            };


            (def, quote!(#id))
        })
        .unzip();

    let unwrapping: Vec<_> = parsed
        .args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let id = format_ident!("arg_{}", i);

            let append_none = if matches!(parsed.ret_type.data_type, DataType::Utf8) {
                quote!(results_builder.append_option(None::<String>);)
            } else {
                quote!(results_builder.append_option(None);)
            };

            if arg_type.nullable {
                quote!()
            } else {
                parse_quote! {
                    let Some(#id) = #id else {
                        #append_none
                        continue;
                    };
                }
            }
        })
        .collect();

    let to_string: Vec<_> = parsed
        .args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let id = format_ident!("arg_{}", i);
            if matches!(arg_type.data_type, DataType::Utf8) {
                quote!(let #id = #id.to_string();)
            } else {
                quote!()
            }
        })
        .collect();

    let mut arg_zip = quote!(arg_0.iter());
    for i in 1..args.len() {
        let next_arg = format_ident!("arg_{}", i);
        arg_zip = quote!(#arg_zip.zip(#next_arg.iter()));
    }

    let call = if parsed.ret_type.nullable {
        quote!(results_builder.append_option(udf::#udf_name(#(#args),*));)
    } else {
        quote!(results_builder.append_option(Some(udf::#udf_name(#(#args),*)));)
    };

    let call_loop = if udaf {
        quote! {
            #call
        }
    } else {
        quote! {
            for (#(#args),*) in #arg_zip {
                #(#unwrapping;)*
                #(#to_string;)*
                #call
            }
        }
    };

    Ok(prettyplease::unparse(&parse_quote! {
        use arrow::array;
        use arrow::array::Array;
        use arrow::datatypes;
        use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi};
        use udf;

        #[repr(C)]
        pub struct FfiArraySchemaPair(FFI_ArrowArray, FFI_ArrowSchema);

        #[no_mangle]
        pub extern "C" fn run(args_ptr: *mut FfiArraySchemaPair, args_len: usize, args_capacity: usize) -> FfiArraySchemaPair {
            let args = unsafe {
                Vec::from_raw_parts(args_ptr, args_len, args_capacity)
            };

            let batch_size = args[0].0.len();

            let mut args = args
                .into_iter()
                .map(|pair| {
                    let FfiArraySchemaPair(array, schema) = pair;
                    unsafe { from_ffi(array, &schema).unwrap() }
                });

            #results_builder

            #(#defs;)*

            #call_loop

            let (array, schema) = to_ffi(&results_builder.finish().to_data()).unwrap();
            FfiArraySchemaPair(array, schema)
        }
    }))
}

fn data_type_to_arrow_type_token(data_type: DataType) -> anyhow::Result<TokenStream> {
    match data_type {
        DataType::Utf8 => Ok(quote!(GenericStringType<i32>)),
        DataType::Boolean => Ok(quote!(BooleanType)),
        DataType::Int16 => Ok(quote!(Int16Type)),
        DataType::Int32 => Ok(quote!(Int32Type)),
        DataType::Int64 => Ok(quote!(Int64Type)),
        DataType::Int8 => Ok(quote!(Int8Type)),
        DataType::UInt8 => Ok(quote!(UInt8Type)),
        DataType::UInt16 => Ok(quote!(UInt16Type)),
        DataType::UInt32 => Ok(quote!(UInt32Type)),
        DataType::UInt64 => Ok(quote!(UInt64Type)),
        DataType::Float32 => Ok(quote!(Float32Type)),
        DataType::Float64 => Ok(quote!(Float64Type)),
        DataType::List(f) => data_type_to_arrow_type_token(f.data_type().clone()),
        _ => bail!("Unsupported data type: {:?}", data_type),
    }
}
