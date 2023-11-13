use arroyo_connectors::nexmark::{NexmarkConnector, NexmarkTable};
use arroyo_connectors::{Connector, EmptyConfig};
use arroyo_sql::{
    get_test_expression, parse_and_get_program_sync, ArroyoSchemaProvider, SqlConfig,
};
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::parse::{Parse, ParseStream};
use syn::{parse_str, Expr, LitInt, LitStr, Token};

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
    let (program, _) = parse_and_get_program_sync(
        query_string,
        schema_provider,
        SqlConfig {
            default_parallelism: 1,
        },
    )
    .unwrap();

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
