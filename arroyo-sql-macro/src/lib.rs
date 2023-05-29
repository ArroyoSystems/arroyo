use arroyo_datastream::{SerializationMode, SourceConfig};
use arroyo_rpc::grpc::api::{Connection, KafkaAuthConfig, KafkaConnection};
use arroyo_sql::{
    get_test_expression, parse_and_get_program_sync, ArroyoSchemaProvider, SqlConfig,
};
use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, parse_str, Expr, LitStr, Token};

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
pub fn single_test_codegen(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let test_case = parse_macro_input!(input as TestCase);

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
    let pipeline_case = parse_macro_input!(input as PipelineCase);

    let test_name = &pipeline_case.test_name.value();
    let query_string = &pipeline_case.query;

    let mut schema_provider = ArroyoSchemaProvider::new();

    // Add a Nexmark source named "nexmark";
    schema_provider.add_saved_source_with_type(
        1,
        "nexmark".to_string(),
        arroyo_sql::schemas::nexmark_fields(),
        Some("arroyo_types::nexmark::Event".to_string()),
        SourceConfig::NexmarkSource {
            event_rate: 10,
            runtime: None,
        },
        SerializationMode::Json,
    );
    schema_provider.add_connection(Connection {
        name: "local".to_string(),
        sources: 0,
        sinks: 0,
        connection_type: Some(arroyo_rpc::grpc::api::connection::ConnectionType::Kafka(
            KafkaConnection {
                bootstrap_servers: "localhost:9090".to_string(),
                auth_config: Some(KafkaAuthConfig {
                    auth_type: Some(arroyo_rpc::grpc::api::kafka_auth_config::AuthType::NoAuth(
                        arroyo_rpc::grpc::api::NoAuth {},
                    )),
                }),
            },
        )),
    });
    let (program, _) =
        parse_and_get_program_sync(query_string.value(), schema_provider, SqlConfig::default())
            .unwrap();

    let mod_name: syn::Ident = parse_str(&test_name).unwrap();

    let function = arroyo_controller::compiler::ProgramCompiler::make_graph_function(&program);
    let other_defs: Vec<proc_macro2::TokenStream> = program
        .other_defs
        .iter()
        .map(|t| parse_str(t).unwrap())
        .collect();
    quote!(mod #mod_name {
        use petgraph::graph::DiGraph;
        use arroyo_worker::operators::*;
        use arroyo_worker::operators::sources::*;
        use arroyo_worker::operators::sinks::*;
        use arroyo_worker::operators::sinks;
        use arroyo_worker::operators::joins::*;
        use arroyo_worker::operators::windows::*;
        use arroyo_worker::engine::{Program, SubtaskNode};
        use arroyo_worker::{LogicalEdge, LogicalNode};
        use chrono;
        use std::time::SystemTime;
        use std::str::FromStr;
        use serde::{Deserialize, Serialize};
        #function

        #(#other_defs)*
    })
    .into()
}

struct PipelineCase {
    test_name: LitStr,
    query: LitStr,
}

impl Parse for PipelineCase {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let test_name = input.parse()?;
        input.parse::<Token![,]>()?;
        let query = input.parse()?;
        Ok(Self { test_name, query })
    }
}
