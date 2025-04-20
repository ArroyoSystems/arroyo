use arrow_schema::DataType;
use arroyo_udf_common::parse::{is_vec_u8, ParsedUdf};
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{parse_quote, FnArg, ItemFn};

fn data_type_to_arrow_type_token(data_type: &DataType) -> TokenStream {
    match data_type {
        DataType::Utf8 => quote!(GenericStringType<i32>),
        DataType::Boolean => quote!(BooleanType),
        DataType::Int16 => quote!(Int16Type),
        DataType::Int32 => quote!(Int32Type),
        DataType::Int64 => quote!(Int64Type),
        DataType::Int8 => quote!(Int8Type),
        DataType::UInt8 => quote!(UInt8Type),
        DataType::UInt16 => quote!(UInt16Type),
        DataType::UInt32 => quote!(UInt32Type),
        DataType::UInt64 => quote!(UInt64Type),
        DataType::Float32 => quote!(Float32Type),
        DataType::Float64 => quote!(Float64Type),
        DataType::Binary => quote!(GenericBinaryType<i32>),
        DataType::List(f) => data_type_to_arrow_type_token(f.data_type()),
        _ => panic!("Unsupported data type: {:?}", data_type),
    }
}

struct ParsedFunction(ParsedUdf, ItemFn);

impl Parse for ParsedFunction {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let function: ItemFn = input.parse()?;

        if function.sig.asyncness.is_some() {
            if let Some(vec) = function.sig.inputs.iter().find_map(|t| match t {
                FnArg::Receiver(_) => None,
                FnArg::Typed(t) => {
                    if ParsedUdf::vec_inner_type(&t.ty).is_some() && !is_vec_u8(&t.ty) {
                        Some(t.ty.span())
                    } else {
                        None
                    }
                }
            }) {
                return Err(syn::Error::new(
                    vec.span(),
                    "Async UDAFs are not supported (hint: remove the Vec<_> args)",
                ));
            }
        }

        Ok(ParsedFunction(
            ParsedUdf::try_parse(&function)
                .map_err(|e| syn::Error::new(Span::call_site(), e.to_string()))?,
            function,
        ))
    }
}

#[proc_macro_attribute]
pub fn udf(
    _attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let parsed: ParsedFunction = match syn::parse(input) {
        Ok(parsed) => parsed,
        Err(e) => {
            return e.to_compile_error().into();
        }
    };

    let mangle = Some(quote! { #[no_mangle] });
    let tokens = if parsed.0.udf_type.is_async() {
        async_udf(parsed, mangle)
    } else {
        sync_udf(parsed, mangle)
    };

    (quote! {
        #tokens
    })
    .into()
}

/// Used to generate a statically-linked UDF for testing
#[proc_macro_attribute]
pub fn local_udf(
    attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input_str = input.to_string();
    let def = format!("#[udf({})]{}", attr, input_str);
    let parsed: ParsedFunction = syn::parse(input).unwrap();
    let name = parsed.0.name.clone();

    let (tokens, interface) = if parsed.0.udf_type.is_async() {
        let tokens = async_udf(parsed, None);
        let interface = quote! {
            arroyo_udf_host::UdfInterface::Async(std::sync::Arc::new(arroyo_udf_host::ContainerOrLocal::Local(
                arroyo_udf_host::AsyncUdfDylibInterface::new(
                    __start,
                    __send,
                    __drain_results,
                    __stop_runtime,
                ))))
        };
        (tokens, interface)
    } else {
        let tokens = sync_udf(parsed, None);
        let interface = quote! {
            arroyo_udf_host::UdfInterface::Sync(std::sync::Arc::new(arroyo_udf_host::ContainerOrLocal::Local(
                arroyo_udf_host::UdfDylibInterface::new(__run))))
        };
        (tokens, interface)
    };

    (quote!(
        #tokens

        pub fn __local() -> arroyo_udf_host::LocalUdf {
            let config = arroyo_udf_host::parse::ParsedUdf::try_parse(&syn::parse_str(#input_str).unwrap()).unwrap();

            arroyo_udf_host::LocalUdf {
                def: #def,
                config: arroyo_udf_host::UdfDylib::new(
                    #name.to_string(),
                    datafusion::logical_expr::Signature::exact(
                        config.args.into_iter().map(|a| a.data_type).collect(),
                        datafusion::logical_expr::Volatility::Volatile),
                    config.ret_type.data_type,
                    #interface,
                ),
                is_aggregate: config.vec_arguments > 0,
                is_async: config.udf_type.is_async(),
            }
        }
    )).into()
}

fn arg_vars(parsed: &ParsedUdf) -> (Vec<TokenStream>, Vec<TokenStream>) {
    parsed
        .args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let arrow_type = data_type_to_arrow_type_token(&arg_type.data_type);
            let id = format_ident!("arg_{}", i);
            let def = match &arg_type.data_type {
                DataType::Utf8 => {
                    quote!(let #id = arroyo_udf_plugin::arrow::array::StringArray::from(args.next().unwrap());)
                }
                DataType::Binary => {
                    quote!(let #id = arroyo_udf_plugin::arrow::array::BinaryArray::from(args.next().unwrap());)
                }
                DataType::List(field) => {
                    let filter = if !field.is_nullable() {
                        quote!(.filter_map(|x| x))
                    } else {
                        quote!()
                    };
                    match field.data_type() {
                        DataType::Utf8 => {
                            quote!(
                                let binding = arroyo_udf_plugin::arrow::array::StringArray::from(
                                    args.next().unwrap()
                                );
                                let #id: Vec<&str> = binding.iter()#filter.map(|s| s.as_ref()).collect();
                            )
                        }
                        DataType::Binary => {
                            quote!(let #id = arroyo_udf_plugin::arrow::array::BinaryArray::from(
                                args.next().unwrap()
                            ).iter()#filter.collect();)
                        }
                        _ => {
                            quote!(let #id = arroyo_udf_plugin::arrow::array::PrimitiveArray::<arroyo_udf_plugin::arrow::datatypes::#arrow_type>::from(
                                args.next().unwrap()
                            ).iter()#filter.collect();)
                        }
                    }
                }
                _ => {
                    quote!(let #id = arroyo_udf_plugin::arrow::array::PrimitiveArray::<arroyo_udf_plugin::arrow::datatypes::#arrow_type>::from(args.next().unwrap());)
                }
            };


            (def, quote!(#id))
        })
        .unzip()
}

fn sync_udf(parsed: ParsedFunction, mangle: Option<TokenStream>) -> TokenStream {
    let (parsed, item) = (parsed.0, parsed.1);
    let udf_name = format_ident!("{}", parsed.name);

    let results_builder = match parsed.ret_type.data_type {
        DataType::Utf8 => {
            quote!(let mut results_builder = arroyo_udf_plugin::arrow::array::StringBuilder::with_capacity(batch_size, batch_size * 8);)
        }
        DataType::Binary => {
            quote!(let mut results_builder = arroyo_udf_plugin::arrow::array::GenericByteBuilder::<arroyo_udf_plugin::arrow::array::types::GenericBinaryType<i32>>
                ::with_capacity(batch_size, batch_size * 8);)
        }
        _ => {
            let return_type = data_type_to_arrow_type_token(&parsed.ret_type.data_type);
            quote!(let mut results_builder = arroyo_udf_plugin::arrow::array::PrimitiveBuilder::<arroyo_udf_plugin::arrow::datatypes::#return_type>::with_capacity(batch_size);)
        }
    };

    let (defs, args) = arg_vars(&parsed);

    let udaf = parsed
        .args
        .iter()
        .any(|arg| matches!(arg.data_type, DataType::List(_)));

    let unwrapping: Vec<_> = parsed
        .args
        .iter()
        .enumerate()
        .map(|(i, arg_type)| {
            let id = format_ident!("arg_{}", i);

            let append_none = match parsed.ret_type.data_type {
                DataType::Utf8 => {
                    quote!(results_builder.append_option(None::<String>);)
                }
                DataType::Binary => {
                    quote!(results_builder.append_option(None::<Vec<u8>>);)
                }
                _ => quote!(results_builder.append_option(None);),
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

    let mut arg_destructure = quote!(arg_0);
    let mut arg_zip = quote!(arg_0.iter());
    for i in 1..args.len() {
        let next_arg = format_ident!("arg_{}", i);
        arg_zip = quote!(#arg_zip.zip(#next_arg.iter()));
        arg_destructure = quote!((#arg_destructure, #next_arg))
    }

    let call = if parsed.ret_type.nullable {
        quote!(results_builder.append_option(#udf_name(#(#args),*));)
    } else {
        quote!(results_builder.append_option(Some(#udf_name(#(#args),*)));)
    };

    let call_loop = if udaf {
        quote! {
            #call
        }
    } else {
        quote! {
            for #arg_destructure in #arg_zip {
                #(#unwrapping;)*
                #call
            }
        }
    };

    quote! {
        #item

        #mangle
        pub extern "C-unwind" fn __run(args: arroyo_udf_plugin::FfiArrays) -> arroyo_udf_plugin::RunResult {
            let args = args.into_vec();
            let batch_size = args[0].len();

            let result = std::panic::catch_unwind(|| {
                let mut args = args.into_iter();
                #results_builder

                #(#defs;)*

                #call_loop

                arroyo_udf_plugin::arrow::array::Array::to_data(&results_builder.finish())
            });


            match result {
                Ok(data) => {
                    arroyo_udf_plugin::RunResult::Ok(arroyo_udf_plugin::FfiArraySchema::from_data(data))
                }
                Err(e) => {
                    arroyo_udf_plugin::RunResult::Err
                }
            }
        }
    }
}

fn async_udf(parsed: ParsedFunction, mangle: Option<TokenStream>) -> TokenStream {
    let (parsed, item) = (parsed.0, parsed.1);

    let (defs, args) = arg_vars(&parsed);

    let name = format_ident!("{}", parsed.name);
    let call_args: Vec<_> = args
        .iter()
        .zip(parsed.args)
        .map(|(arg, t)| {
            if t.nullable {
                quote!(if arroyo_udf_plugin::arrow::array::Array::is_null(&#arg, 0) { None } else { Some(#arg.value(0))})
            } else {
                quote!(#arg.value(0))                
            }
        })
        .collect();

    let datum = match parsed.ret_type.data_type {
        DataType::Boolean => quote!(Bool),
        DataType::Int32 => quote!(I32),
        DataType::Int64 => quote!(I64),
        DataType::UInt32 => quote!(U32),
        DataType::UInt64 => quote!(U64),
        DataType::Float32 => quote!(F32),
        DataType::Float64 => quote!(F64),
        DataType::Timestamp(_, _) => quote!(Timestamp),
        DataType::Binary => quote!(Bytes),
        DataType::Utf8 => quote!(String),
        _ => panic!("unsupported return type {}", parsed.ret_type.data_type),
    };

    let wrap_return = if parsed.ret_type.nullable {
        quote!(arroyo_udf_plugin::ArrowDatum::#datum(result))
    } else {
        quote!(arroyo_udf_plugin::ArrowDatum::#datum(Some(result)))
    };

    let wrapper = quote! {
        async fn __wrapper(id: u64, timeout: std::time::Duration, args: Vec<arroyo_udf_plugin::arrow::array::ArrayData>) ->
           (u64, Result<arroyo_udf_plugin::ArrowDatum, arroyo_udf_plugin::async_udf::tokio::time::error::Elapsed>) {
            let mut args = args.into_iter();

            #(#defs;)*

            match arroyo_udf_plugin::async_udf::tokio::time::timeout(timeout, #name(#(#call_args, )*)).await {
                Ok(result) => (id, Ok(#wrap_return)),
                Err(e) => (id, Err(e)),
            }
        }
    };

    let results_builder = match parsed.ret_type.data_type {
        DataType::Utf8 => quote!(arroyo_udf_plugin::arrow::array::StringBuilder::new()),
        DataType::Binary => quote!(arroyo_udf_plugin::arrow::array::GenericByteBuilder::<
            arroyo_udf_plugin::arrow::array::types::GenericBinaryType<i32>,
        >::new()),
        _ => {
            let return_type = data_type_to_arrow_type_token(&parsed.ret_type.data_type);
            quote!(arroyo_udf_plugin::arrow::array::PrimitiveBuilder::<arroyo_udf_plugin::arrow::datatypes::#return_type>::new())
        }
    };

    let start = quote! {
        #mangle
        pub extern "C-unwind" fn __start(ordered: bool, timeout_micros: u64, allowed_in_flight: u32) -> arroyo_udf_plugin::async_udf::SendableFfiAsyncUdfHandle {
            let (x, handle) = arroyo_udf_plugin::async_udf::AsyncUdf::new(
                ordered, std::time::Duration::from_micros(timeout_micros), allowed_in_flight, Box::new(#results_builder), __wrapper
            );

            x.start();

            arroyo_udf_plugin::async_udf::SendableFfiAsyncUdfHandle { ptr: handle.into_ffi() }
        }
    };

    quote! {
        #item

        #wrapper

        #start

        #mangle
        pub extern "C-unwind" fn __send(handle: arroyo_udf_plugin::async_udf::SendableFfiAsyncUdfHandle,
            id: u64, arrays: arroyo_udf_plugin::FfiArrays) -> arroyo_udf_plugin::async_udf::async_ffi::FfiFuture<bool> {
            use arroyo_udf_plugin::async_udf::async_ffi::FutureExt;
            arroyo_udf_plugin::async_udf::send(handle, id, arrays).into_ffi()
        }

        #mangle
        pub extern "C-unwind" fn __drain_results(handle: arroyo_udf_plugin::async_udf::SendableFfiAsyncUdfHandle) -> arroyo_udf_plugin::async_udf::DrainResult {
            arroyo_udf_plugin::async_udf::drain_results(handle)
        }

        #mangle
        pub extern "C-unwind" fn __stop_runtime(handle: arroyo_udf_plugin::async_udf::SendableFfiAsyncUdfHandle) {
            arroyo_udf_plugin::async_udf::stop_runtime(handle);
        }
    }
}
