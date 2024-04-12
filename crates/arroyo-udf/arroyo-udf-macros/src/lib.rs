use arrow_schema::DataType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_quote, Visibility};
use arroyo_udf_common::parse::ParsedUdf;

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
        DataType::List(f) => data_type_to_arrow_type_token(&f.data_type()),
        _ => panic!("Unsupported data type: {:?}", data_type),
    }
}

#[proc_macro_attribute]
pub fn udf(_attr: proc_macro::TokenStream, input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut parsed: ParsedUdf = syn::parse(input).unwrap();
    if parsed.function.sig.asyncness.is_some() {
        async_udf(parsed)
    } else {
        sync_udf(parsed)
    }
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
                    quote!(let #id = arrow::array::StringArray::from(args.next().unwrap());)
                }
                DataType::List(field) => {
                    let filter = if !field.is_nullable() {
                        quote!(.filter_map(|x| x))
                    } else {
                        quote!()
                    };

                    quote!(let #id = arrow::array::PrimitiveArray::<arrow::datatypes::#arrow_type>::from(
                        args.next().unwrap()
                    ).iter()#filter.collect();)
                }
                _ => {
                    quote!(let #id = arrow::array::PrimitiveArray::<arrow::datatypes::#arrow_type>::from(args.next().unwrap());)
                }
            };


            (def, quote!(#id))
        })
        .unzip()
}

fn sync_udf(mut parsed: ParsedUdf) -> proc_macro::TokenStream {
    let udf_name = format_ident!("{}", parsed.name);

    let results_builder = if matches!(parsed.ret_type.data_type, DataType::Utf8) {
        quote!(let mut results_builder = arrow::array::StringBuilder::with_capacity(batch_size, batch_size * 8);)
    } else {
        let return_type = data_type_to_arrow_type_token(&parsed.ret_type.data_type);
        quote!(let mut results_builder = arrow::array::PrimitiveBuilder::<arrow::datatypes::#return_type>::with_capacity(batch_size);)
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
                Some(quote!(let #id = #id.to_string()))
            } else {
                None
            }
        })
        .collect();

    let mut arg_zip = quote!(arg_0.iter());
    for i in 1..args.len() {
        let next_arg = format_ident!("arg_{}", i);
        arg_zip = quote!(#arg_zip.zip(#next_arg.iter()));
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
            for (#(#args),*) in #arg_zip {
                #(#unwrapping;)*
                #(#to_string;)*
                #call
            }
        }
    };

    parsed.function.vis = Visibility::Public(Default::default());
    let function = &mut parsed.function;

    (quote! {
        #function

        #[no_mangle]
        pub extern "C-unwind" fn run(args: arroyo_udf_plugin::FfiArrays) -> arroyo_udf_plugin::RunResult {
            let args = args.into_vec();
            let batch_size = args[0].len();

            let result = std::panic::catch_unwind(|| {
                let mut args = args.into_iter();
                #results_builder

                #(#defs;)*

                #call_loop

                arrow::array::Array::to_data(&results_builder.finish())
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
    }).into()
}

fn async_udf(parsed: ParsedUdf) -> proc_macro::TokenStream {
    let (defs, args) = arg_vars(&parsed);

    let name = format_ident!("{}", parsed.name);
    let call_args: Vec<_> = args.iter()
        .zip(parsed.args)
        .map(|(arg, dt)| match dt.data_type {
            DataType::Utf8 => {
                quote!(#arg.value(0).to_string())
            }
            DataType::Binary => {
                quote!(#arg.value(0).clone())
            }
            _ => {
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
        async fn wrapper(id: usize, args: Vec<arrow::array::ArrayData>) ->
           (usize, Result<arroyo_udf_plugin::ArrowDatum, tokio::time::error::Elapsed>) {
            let mut args = args.into_iter();

            #(#defs;)*

            let result = #name(#(#call_args, )*).await;

            (id, Ok(#wrap_return))
        }
    };

    let results_builder = if matches!(parsed.ret_type.data_type, DataType::Utf8) {
        quote!(arrow::array::StringBuilder::new())
    } else {
        let return_type = data_type_to_arrow_type_token(&parsed.ret_type.data_type);
        quote!(arrow::array::PrimitiveBuilder::<arrow::datatypes::#return_type>::new())
    };

    let start = quote! {
        pub extern "C-unwind" fn start(ordered: bool) -> arroyo_udf_plugin::async_udf::SendableFfiAsyncUdfHandle {
            let (x, handle) = arroyo_udf_plugin::async_udf::AsyncUdf::new(
                ordered, Box::new(#results_builder), wrapper
            );

            x.start();

            arroyo_udf_plugin::async_udf::SendableFfiAsyncUdfHandle { ptr: handle.into_ffi() }
        }
    };

    let function = &parsed.function;

    (quote!{
        #function

        #wrapper

        #start
        
        #[arroyo_udf_plugin::async_udf::async_ffi::async_ffi]
        pub async extern "C-unwind" fn send(handle: arroyo_udf_plugin::async_udf::SendableFfiAsyncUdfHandle, 
            id: usize, arrays: arroyo_udf_plugin::FfiArrays) -> bool {
            arroyo_udf_plugin::async_udf::send(handle, id, arrays).await
        }
        
        #[no_mangle]
        pub extern "C-unwind" fn drain_results(handle: arroyo_udf_plugin::async_udf::SendableFfiAsyncUdfHandle) -> arroyo_udf_plugin::async_udf::DrainResult {
            arroyo_udf_plugin::async_udf::drain_results(handle)
        }
        
        #[no_mangle]
        pub extern "C-unwind" fn stop_runtime(handle: arroyo_udf_plugin::async_udf::SendableFfiAsyncUdfHandle) {
            arroyo_udf_plugin::async_udf::stop_runtime(handle);
        }
    }).into()
}