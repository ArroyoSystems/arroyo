use arrow_schema::{DataType, Field, TimeUnit};
use arroyo_connectors::nexmark::{NexmarkConnector, NexmarkTable};
use arroyo_connectors::{Connector, EmptyConfig};
use arroyo_sql::types::{rust_to_arrow, StructField, TypeDef};
use arroyo_sql::{
    get_test_expression, parse_and_get_program_sync, ArroyoSchemaProvider, SqlConfig,
};
use proc_macro::{TokenStream, Ident};
use quote::{quote, ToTokens};
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, parse_str, DeriveInput, Expr, LitInt, LitStr, Token, Type};

mod connectors;

/// This macro is used to generate a test function for a single test case.
/// Used in the `arroyo-sql-testing` crate.
///
/// # Arguments
///
/// * `test_name` - The name of the test.
/// * `calculation_string` - The calculation string to be tested.
/// * `input_value` - The input value to be used in the calculation.
/// * `expected_result` - The expected result of the calculation.
///
/// # Returns
///
/// A test function that can be used to test a single test case.
///
/// # Example
///
/// ```
/// use arroyo_sql_testing::single_test_codegen;
///
/// single_test_codegen!(
///    "test_name",
///   "calculation_string",
///   1,
///   2
/// );
/// ```
#[proc_macro]
pub fn single_test_codegen(input: TokenStream) -> TokenStream {
    single_test_codegen_internal(input.into()).into()
}

fn single_test_codegen_internal(input: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let test_case: TestCase = syn::parse2(input).unwrap();

    let test_name = &test_case.test_name;
    let calculation_string = &test_case.calculation_string;
    let input_value = &test_case.input_value;
    let expected_result = &test_case.expected_result;

    let function = get_test_expression(
        &test_name.value(),
        &calculation_string.value(),
        input_value,
        expected_result,
    );

    let tokens = quote! {
        #[test]
        #function
    };

    tokens.into()
}

struct TestCase {
    test_name: LitStr,
    calculation_string: LitStr,
    input_value: Expr,
    expected_result: Expr,
}

impl Parse for TestCase {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let test_name = input.parse()?;
        input.parse::<Token![,]>()?;
        let calculation_string = input.parse()?;
        input.parse::<Token![,]>()?;
        let input_value = input.parse()?;
        input.parse::<Token![,]>()?;
        let expected_result = input.parse()?;

        Ok(Self {
            test_name,
            calculation_string,
            input_value,
            expected_result,
        })
    }
}

/// This macro is used to generate the pipeline code for a given query.
/// While it doesn't run the pipeline it validates that the generated code
/// will compile.
/// Used in the `arroyo-sql-testing` crate.
///
/// # Arguments
///
/// * `test_name` - The name of the test.
/// * `query` - The query to compile. Assumes a nexmark source named "nexmark" is available.
/// # Returns
///
/// A module named `test_name`, containing the generated code.
///
/// # Example
///
/// ```
/// use arroyo_sql_testing::full_pipeline_codegen;
///
/// full_pipeline_codegen!{
///    "select_star",
///   "SELECT * FROM nexmark",
/// }
/// ```
#[proc_macro]
pub fn full_pipeline_codegen(input: TokenStream) -> TokenStream {
    full_pipeline_codegen_internal(input.into()).into()
}

fn full_pipeline_codegen_internal(input: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let pipeline_case: PipelineCase = syn::parse2(input).unwrap();
    get_pipeline_module(
        pipeline_case.query.value(),
        pipeline_case.test_name.value(),
        None,
    )
}

struct PipelineCase {
    test_name: LitStr,
    query: LitStr,
}

fn get_pipeline_module(
    query_string: String,
    mod_name: String,
    udfs: Option<String>,
) -> proc_macro2::TokenStream {
    let mod_ident: syn::Ident = parse_str(&mod_name).unwrap();
    let mut schema_provider = ArroyoSchemaProvider::new();

    // Add a Nexmark source named "nexmark";
    let nexmark = (NexmarkConnector {})
        .from_config(
            Some(1),
            "nexmark",
            EmptyConfig {},
            NexmarkTable {
                event_rate: 1000.0,
                runtime: Some(0.1),
            },
            None,
        )
        .unwrap();

    schema_provider.add_connector_table(nexmark);
    schema_provider.add_connector_table(connectors::get_json_schema_source().unwrap());
    schema_provider.add_connector_table(connectors::get_avro_source().unwrap());

    let file = syn::parse_file(&udfs.unwrap_or_default()).unwrap();
    for item in file.items.into_iter() {
        match item {
            syn::Item::Fn(_) => {
                schema_provider
                    .add_rust_udf(&item.to_token_stream().to_string())
                    .unwrap();
            }
            _ => {
                panic!("Expected only functions.")
            }
        }
    }

    // TODO: test with higher parallelism
    let program = parse_and_get_program_sync(
        query_string,
        schema_provider,
        SqlConfig {
            default_parallelism: 1,
        },
    )
    .unwrap()
    .program;

    let function = program.make_graph_function();

    let udfs: Vec<proc_macro2::TokenStream> = program
        .udfs
        .iter()
        .map(|t| parse_str(&t.definition).unwrap())
        .collect();

    let other_defs: Vec<proc_macro2::TokenStream> = program
        .other_defs
        .iter()
        .map(|t| parse_str(t).unwrap())
        .collect();
    quote!(mod #mod_ident {
            use petgraph::graph::DiGraph;
            use arroyo_worker::operators::*;
            use arroyo_worker::connectors;
            use arroyo_worker::operators::sinks::*;
            use arroyo_worker::operators::sinks;
            use arroyo_worker::operators::joins::*;
            use arroyo_worker::operators::windows::*;
            use arroyo_worker::engine::{Program, SubtaskNode};
            use arroyo_worker::{LogicalEdge, LogicalNode};
            use arroyo_sql::types;
            use types::*;
            use chrono;
            use std::time::SystemTime;
            use std::str::FromStr;
            use serde::{Deserialize, Serialize};

            #function

            mod udfs {
               #(#udfs)*
            }

            #(#other_defs)*
        }
    )
}

impl Parse for PipelineCase {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let test_name = input.parse()?;
        input.parse::<Token![,]>()?;
        let query = input.parse()?;
        Ok(Self { test_name, query })
    }
}

struct CorrectnessTestCase {
    test_name: LitStr,
    checkpoint_interval: LitInt,
    query: LitStr,
    udfs: Option<LitStr>,
}

impl Parse for CorrectnessTestCase {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let test_name = input.parse()?;
        input.parse::<Token![,]>()?;
        let checkpoint_interval = input.parse()?;
        input.parse::<Token![,]>()?;
        let query = input.parse()?;
        let udfs = if input.parse::<Token![,]>().is_ok() {
            input.parse()?
        } else {
            None
        };
        Ok(Self {
            test_name,
            checkpoint_interval,
            query,
            udfs,
        })
    }
}

#[proc_macro_derive(GetArrowSchema)]
pub fn get_arrow_schema_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = input.ident;
    let fields = match input.data {
        syn::Data::Struct(s) => s.fields,
        _ => panic!("GetArrowSchema can only be derived for structs"),
    };

    let schema_fields = fields.iter().map(|f| {
        let field_name = &f.ident;
        let arrow_type_call = match &f.ty {
            // Check if field type is an Option
            Type::Path(type_path) if type_path.path.segments.last().unwrap().ident == "Option" => {
                let inner_type = extract_inner_type(&f.ty);
                quote! { <#inner_type as arroyo_sql::types::GetArrowType>::arrow_type() }
            }
            typ => quote! { <#typ as arroyo_sql::types::GetArrowType>::arrow_type() },
        };
        let is_nullable = is_option(&f.ty);

        quote! {
            arrow::datatypes::Field::new(
                stringify!(#field_name),
                #arrow_type_call,
                #is_nullable
            )
        }
    });

    let expanded = quote! {
        impl GetArrowSchema for #name {
            fn arrow_schema() -> arrow::datatypes::Schema {
                arrow::datatypes::Schema::new(vec![#(#schema_fields),*])
            }
        }
    };

    TokenStream::from(expanded)
}

struct RecordBatchFieldTokens {
    definition: proc_macro2::TokenStream,
    initialization: proc_macro2::TokenStream,
    append: proc_macro2::TokenStream,
    append_null: proc_macro2::TokenStream,
    field_flush: proc_macro2::TokenStream,
}

#[proc_macro_derive(GetRecordBatchBuilder)]
pub fn get_record_batch_builder_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = input.ident;
    let fields = match input.data {
        syn::Data::Struct(s) => s.fields,
        _ => panic!("GetArrowSchema can only be derived for structs"),
    };
    let fields: Vec<_> = fields.iter()
    .map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        let inner_type = match &f.ty {
            // Check if field type is an Option
            Type::Path(type_path) if type_path.path.segments.last().unwrap().ident == "Option" => {
                extract_inner_type(&f.ty)
            }
            typ => typ,
        };
        let is_nullable = is_option(&f.ty);
        
        let arrow_type = rust_to_arrow(inner_type);
        match arrow_type {
            Ok(primitive_type) => {
                let primitive_builder_type = primitive_builder(&primitive_type);
                let definition = quote!(#field_name: #primitive_builder_type);
                let initialization = quote!(#primitive_builder_type::new());
                let append = primitive_append(&primitive_type, is_nullable, field_name);
                let append_null = quote!(self.#field_name.append_null());
                let field_flush = quote!(self.#field_name.finish());
                RecordBatchFieldTokens {
                    definition,
                    initialization,
                    append,
                    append_null,
                    field_flush,
                }
            },
            Err(_) => {
                match &f.ty {
                        Type::Path(path) => {
                            let path: Vec<String> = path
                                .path
                                .segments
                                .iter()
                                .map(|s| s.ident.to_string())
                                .collect();
                            let struct_builder_type: proc_macro2::TokenStream = parse_str(&format!("{}RecordBatchBuilder", path.join("::"))).unwrap();
                            let definition = quote!(#field_name: #struct_builder_type);
                            let initialization = if is_nullable {
                                quote!(#struct_builder_type::nullable())
                            } else {
                                quote!(#struct_builder_type::new())
                            };
                            let append = if is_nullable {
                                quote!(self.#field_name.add_data(data.#field_name))
                            } else {
                                quote!(self.#field_name.add_data(data.#field_name))
                            };
                            let append_null = quote!(self.#field_name.add_data(None));
                            let field_flush = quote!( std::sync::Arc::new(self.#field_name.as_struct_array()));
                            RecordBatchFieldTokens {
                                definition,
                                initialization,
                                append,
                                append_null,
                                field_flush                    
                            }
                        }
                        _ => unimplemented!()
                }
            },
        }
    }).collect();

    let field_definitions: Vec<_> = fields.iter().map(|field| field.definition.clone()).collect();
    let field_initializations : Vec<_> = fields.iter().map(|field| field.initialization.clone()).collect();
    let field_appends : Vec<_> = fields.iter().map(|field| field.append.clone()).collect();
    let field_nulls : Vec<_> = fields.iter().map(|field| field.append_null.clone()).collect();
    let field_flushes : Vec<_> = fields.iter().map(|field| field.field_flush.clone()).collect();

    let builder_ident: proc_macro2::Ident = parse_str(&format!("{}RecordBatchBuilder", name.to_string())).unwrap();

    quote! {
        impl GetRecordBatchBuilder<#builder_ident> for #name {
            fn record_batch_builder() -> #builder_ident::new()
        }
            #[derive(Debug)]
            pub struct #builder_ident {
                schema: std::sync::Arc<arrow::datatypes::Schema>,
                _null_buffer_builder: Option<arrow::array::BooleanBufferBuilder>,
                #(#field_definitions,)*
            }

            impl Default for #builder_ident {
                fn default() -> Self {
                    #builder_ident {
                        schema: Self::arrow_schema(),
                        _null_buffer_builder: None,
                        #(#field_initializations,)*
                    }
                }
            }

            impl arroyo_types::RecordBatchBuilder for #builder_ident {
                type Data = #name;
                fn add_data(&mut self, data: Option<#name>) {
                    if let Some(buffer) = self._null_buffer_builder.as_mut() {
                        buffer.append(data.is_some());
                    }
                    match data {
                        Some(data) => {#(#field_appends;)*},
                        None => {#(#field_nulls;)*},
                    }
                }

                fn nullable() -> Self {
                    #builder_ident {
                        schema: #name:arrow_schema(),
                        _null_buffer_builder: Some(arrow::array::BooleanBufferBuilder::new(0)),
                        #(#field_initializations,)*
                    }
                }

                fn as_struct_array(&mut self) -> arrow_array::StructArray {

                    arrow_array::StructArray::new(
                        self.schema.fields().clone(),
                        vec![#(#field_flushes,)*],
                        self._null_buffer_builder.take().as_mut().map(|builder| builder.finish().into()),
                    )
                }

                fn flush(&mut self) -> arrow_array::RecordBatch {
                    arrow_array::RecordBatch::try_new(self.schema.clone(), vec![#(#field_flushes,)*]).unwrap()
                }

                fn schema(&self) -> std::sync::Arc<arrow::datatypes::Schema> {
                    self.schema.clone()
                }
            }
        }.into()
}

fn primitive_append(data_type: &DataType, is_nullable: bool, field_ident: &proc_macro2::Ident) -> proc_macro2::TokenStream {
    match (data_type, is_nullable) {
        (
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    ) => {
        quote!(self.#field_ident.append_option(data.#field_ident.map(|time| arroyo_types::to_millis(time) as i64)))
    },
    (
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    ) => {
        quote!(self.#field_ident.append_value(arroyo_types::to_millis(data.#field_ident) as i64))
    },
    (
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    ) => {
        quote!(self.#field_ident.append_option(data.#field_ident.map(|time| arroyo_types::to_micros(time) as i64)))
    },
    (
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    ) => {
        quote!(self.#field_ident.append_value(arroyo_types::to_micros(data.#field_ident) as i64))
    },
    (
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    ) => {
        quote!(self.#field_ident.append_option(data.#field_ident.map(|time| arroyo_types::to_nanos(time) as i64)))
    },
    (
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    ) => {
        quote!(self.#field_ident.append_value(arroyo_types::to_nanos(data.#field_ident) as i64))
    },
       (_, true) =>  {
        quote!(self.#field_ident.append_option(data.#field_ident))
       },
       (_, false) => {
        quote!(self.#field_ident.append_value(data.#field_ident))
       }
    }
}

fn primitive_builder(data_type: &DataType) -> proc_macro2::TokenStream {
    match data_type {
        DataType::Boolean => quote!(arrow_array::builder::BooleanBuilder),
        DataType::Int8 => {
            quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Int8Type>)
        }
        DataType::Int16 => {
            quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Int16Type>)
        }
        DataType::Int32 => {
            quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Int32Type>)
        }
        DataType::Int64 => {
            quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Int64Type>)
        }
        DataType::UInt8 => {
            quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::UInt8Type>)
        }
        DataType::UInt16 => {
            quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::UInt16Type>)
        }
        DataType::UInt32 => {
            quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::UInt32Type>)
        }
        DataType::UInt64 => {
            quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::UInt64Type>)
        }
        DataType::Float16 => {
            quote!(
                arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Float16Type>
            )
        }
        DataType::Float32 => {
            quote!(
                arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Float32Type>
            )
        }
        DataType::Float64 => {
            quote!(
                arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Float64Type>
            )
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => quote!(
            arrow_array::builder::PrimitiveBuilder::<
                arrow_array::types::TimestampMillisecondType,
            >
        ),
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => quote!(
            arrow_array::builder::PrimitiveBuilder::<
                arrow_array::types::TimestampMicrosecondType,
            >
        ),
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None) => quote!(
            arrow_array::builder::PrimitiveBuilder::<
                arrow_array::types::TimestampNanosecondType,
            >
        ),
        DataType::Utf8 => {
            quote!(
                arrow_array::builder::GenericByteBuilder<
                    arrow_array::types::GenericStringType<i32>,
                >
            )
        },
        _ => unimplemented!(),
    }
}

fn extract_inner_type(ty: &Type) -> &Type {
    if let Type::Path(type_path) = ty {
        if let syn::PathArguments::AngleBracketed(args) =
            &type_path.path.segments.last().unwrap().arguments
        {
            if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                return inner_type;
            }
        }
    }
    panic!("Expected Option type");
}

fn is_option(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if type_path.path.segments.last().unwrap().ident == "Option" {
            true
        } else {
            false
        }
    } else {
        false
    }
}

#[proc_macro]
pub fn correctness_run_codegen(input: TokenStream) -> TokenStream {
    let correctness_case: CorrectnessTestCase =
        syn::parse_macro_input!(input as CorrectnessTestCase);
    let query_string = correctness_case.query.value();
    let test_name = correctness_case.test_name.value();
    let checkpoint_interval: i32 = correctness_case.checkpoint_interval.base10_parse().unwrap();
    // replace $input_file with the current directory and then inputs/query_name.json
    let physical_input_dir = format!(
        "{}/arroyo-sql-testing/inputs/",
        std::env::current_dir()
            .unwrap()
            .to_string_lossy()
            .to_string(),
    );
    let query_string = query_string.replace("$input_dir", &physical_input_dir);
    // replace $output_file with the current directory and then outputs/query_name.json
    let physical_output = format!(
        "{}/arroyo-sql-testing/outputs/{}.json",
        std::env::current_dir()
            .unwrap()
            .to_string_lossy()
            .to_string(),
        test_name
    );
    let query_string = query_string.replace("$output_path", &physical_output);
    let golden_output_location = format!(
        "{}/arroyo-sql-testing/golden_outputs/{}.json",
        std::env::current_dir()
            .unwrap()
            .to_string_lossy()
            .to_string(),
        test_name
    );
    let module_ident: syn::Ident = parse_str(&test_name).unwrap();
    let test_ident: syn::Ident = parse_str(&format!("{}_test", test_name)).unwrap();
    let module: proc_macro2::TokenStream = get_pipeline_module(
        query_string,
        test_name.clone(),
        correctness_case.udfs.map(|udfs| udfs.value()),
    )
    .into();

    quote!(
        #[test(tokio::test)]
        async fn #test_ident() {
            let graph = #module_ident::make_graph();
            let name = #test_name.to_string();
            let checkpoint_interval = #checkpoint_interval;
            let output_location = #physical_output.to_string();
            let golden_output_location = #golden_output_location.to_string();
            run_pipeline_and_assert_outputs(graph, name, checkpoint_interval, output_location, golden_output_location).await;
        }
        #module
    )
    .into()
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use runtime_macros_derive::emulate_macro_expansion_fallible;

    use crate::{full_pipeline_codegen_internal, single_test_codegen_internal};
    #[test]
    fn full_query_code_coverage() {
        let mut path = env::current_dir().unwrap().parent().unwrap().to_path_buf();
        path.push("arroyo-sql-testing");
        path.push("src");
        path.push("full_query_tests.rs");
        let file = fs::File::open(path).unwrap();
        emulate_macro_expansion_fallible(
            file,
            "full_pipeline_codegen",
            full_pipeline_codegen_internal,
        )
        .unwrap();
    }
    #[test]
    fn single_test_code_coverage() {
        let mut path = env::current_dir().unwrap().parent().unwrap().to_path_buf();
        path.push("arroyo-sql-testing");
        path.push("src");
        path.push("lib.rs");
        let file = fs::File::open(path).unwrap();
        emulate_macro_expansion_fallible(file, "single_test_codegen", single_test_codegen_internal)
            .unwrap();
    }
}
