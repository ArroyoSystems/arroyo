#![allow(clippy::new_without_default)]
use anyhow::{anyhow, bail, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{self, DataType, Field};
use arrow_schema::TimeUnit;
use arroyo_connectors::Connection;
use arroyo_datastream::Program;
use datafusion::physical_plan::functions::make_scalar_function;

mod expressions;
pub mod external;
pub mod json_schema;
mod operators;
mod optimizations;
mod pipeline;
mod plan_graph;
pub mod schemas;
mod tables;
pub mod types;

use datafusion::prelude::create_udf;

use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::{planner::ContextProvider, TableReference};

use datafusion_expr::{
    logical_plan::builder::LogicalTableSource, AggregateUDF, ScalarUDF, TableSource,
};
use datafusion_expr::{
    AccumulatorFunctionImplementation, ReturnTypeFunction, Signature, StateTypeFunction,
    TypeSignature, Volatility,
};
use expressions::Expression;
use external::SqlSink;
use pipeline::{SqlOperator, SqlPipelineBuilder};
use plan_graph::{get_program, PlanGraph};
use schemas::window_arrow_struct;
use tables::{schema_defs, ConnectorTable, Insert, Table};

use crate::types::{StructDef, StructField, TypeDef};
use quote::ToTokens;
use std::time::SystemTime;
use std::{collections::HashMap, sync::Arc};
use syn::{parse_quote, parse_str, FnArg, Item, ReturnType, VisPublic, Visibility};

#[cfg(test)]
mod test;

#[derive(Clone, Debug)]
pub struct UdfDef {
    args: Vec<TypeDef>,
    ret: TypeDef,
    def: String,
}

#[derive(Debug, Clone, Default)]
pub struct ArroyoSchemaProvider {
    pub source_defs: HashMap<String, String>,
    tables: HashMap<String, Table>,
    pub functions: HashMap<String, Arc<ScalarUDF>>,
    pub connections: HashMap<String, Connection>,
    pub udf_defs: HashMap<String, UdfDef>,
    config_options: datafusion::config::ConfigOptions,
}

impl ArroyoSchemaProvider {
    pub fn new() -> Self {
        let tables = HashMap::new();
        let mut functions = HashMap::new();

        let fn_impl = |args: &[ArrayRef]| Ok(Arc::new(args[0].clone()) as ArrayRef);

        let window_return_type = Arc::new(window_arrow_struct());
        functions.insert(
            "hop".to_string(),
            Arc::new(create_udf(
                "hop",
                vec![
                    DataType::Interval(datatypes::IntervalUnit::MonthDayNano),
                    DataType::Interval(datatypes::IntervalUnit::MonthDayNano),
                ],
                window_return_type.clone(),
                Volatility::Volatile,
                make_scalar_function(fn_impl),
            )),
        );
        functions.insert(
            "tumble".to_string(),
            Arc::new(create_udf(
                "tumble",
                vec![DataType::Interval(datatypes::IntervalUnit::MonthDayNano)],
                window_return_type,
                Volatility::Volatile,
                make_scalar_function(fn_impl),
            )),
        );
        functions.insert(
            "get_first_json_object".to_string(),
            Arc::new(create_udf(
                "get_first_json_object",
                vec![DataType::Utf8, DataType::Utf8],
                Arc::new(DataType::Utf8),
                Volatility::Volatile,
                make_scalar_function(fn_impl),
            )),
        );
        functions.insert(
            "get_json_objects".to_string(),
            Arc::new(create_udf(
                "get_json_objects",
                vec![DataType::Utf8, DataType::Utf8],
                Arc::new(DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    false,
                )))),
                Volatility::Volatile,
                make_scalar_function(fn_impl),
            )),
        );
        functions.insert(
            "extract_json_string".to_string(),
            Arc::new(create_udf(
                "extract_json_string",
                vec![DataType::Utf8, DataType::Utf8],
                Arc::new(DataType::Utf8),
                Volatility::Volatile,
                make_scalar_function(fn_impl),
            )),
        );

        Self {
            tables,
            functions,
            source_defs: HashMap::new(),
            connections: HashMap::new(),
            udf_defs: HashMap::new(),
            config_options: datafusion::config::ConfigOptions::new(),
        }
    }

    pub fn add_connector_table(&mut self, connection: Connection) {
        if let Some(def) = schema_defs(&connection.name, &connection.schema) {
            self.source_defs.insert(connection.name.clone(), def);
        }

        self.tables.insert(
            connection.name.clone(),
            Table::ConnectorTable(connection.into()),
        );
    }

    fn insert_table(&mut self, table: Table) {
        self.tables.insert(table.name().to_string(), table);
    }

    fn get_table(&self, table_name: &String) -> Option<&Table> {
        self.tables.get(table_name)
    }

    pub fn add_rust_udf(&mut self, body: &str) -> Result<()> {
        let file = syn::parse_file(body)?;

        for item in file.items {
            let Item::Fn(mut function) = item else {
                bail!("not a function");
            };

            let mut args: Vec<TypeDef> = vec![];
            for (i, arg) in function.sig.inputs.iter().enumerate() {
                match arg {
                    FnArg::Receiver(_) => bail!("self types are not allowed in UDFs"),
                    FnArg::Typed(t) => {
                        args.push((&*t.ty).try_into().map_err(|_| {
                            anyhow!("Could not convert arg {} into a SQL data type", i)
                        })?);
                    }
                }
            }

            let ret: TypeDef = match &function.sig.output {
                ReturnType::Default => bail!("return type must be specified in UDF"),
                ReturnType::Type(_, t) => (&**t)
                    .try_into()
                    .map_err(|_| anyhow!("Could not convert return type into a SQL data type"))?,
            };

            let fn_impl = |args: &[ArrayRef]| Ok(Arc::new(args[0].clone()) as ArrayRef);

            if self
                .functions
                .insert(
                    function.sig.ident.to_string(),
                    Arc::new(create_udf(
                        &function.sig.ident.to_string(),
                        args.iter()
                            .map(|t| t.as_datatype().unwrap().clone())
                            .collect(),
                        Arc::new(ret.as_datatype().unwrap().clone()),
                        Volatility::Volatile,
                        make_scalar_function(fn_impl),
                    )),
                )
                .is_some()
            {
                bail!("Could not register UDF '{}', as there is already a built-in function with that name",
                    function.sig.ident.to_string());
            };

            function.vis = Visibility::Public(VisPublic {
                pub_token: Default::default(),
            });

            self.udf_defs.insert(
                function.sig.ident.to_string(),
                UdfDef {
                    args,
                    ret,
                    def: function.to_token_stream().to_string(),
                },
            );
        }

        Ok(())
    }
}

fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(Arc::new(
        datatypes::Schema::new_with_metadata(fields, HashMap::new()),
    )))
}

impl ContextProvider for ArroyoSchemaProvider {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let table = self.tables.get(&name.to_string()).ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!("Table {} not found", name))
        })?;
        let fields = table.get_fields().map_err(|err| {
            datafusion::error::DataFusionError::Plan(format!(
                "Table {} failed to get fields with {}",
                name, err
            ))
        })?;
        Ok(create_table_source(fields))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        match name {
            "lexographic_max" => {
                let return_type: ReturnTypeFunction = Arc::new(|input_types| {
                    let struct_fields = input_types
                        .iter()
                        .enumerate()
                        .map(|(i, data_type)| {
                            Field::new(format!("_{}", i).as_str(), data_type.clone(), false)
                        })
                        .collect();
                    let result_type: DataType = DataType::Struct(struct_fields);
                    Ok(Arc::new(result_type))
                });
                let accumulator: AccumulatorFunctionImplementation = Arc::new(|_| todo!());
                let state_type: StateTypeFunction = Arc::new(|_| todo!());
                Some(Arc::new(AggregateUDF::new(
                    "lexographic_max",
                    &Signature::one_of(
                        vec![
                            TypeSignature::Any(1),
                            TypeSignature::Any(2),
                            TypeSignature::Any(3),
                            TypeSignature::Any(4),
                        ],
                        Volatility::Immutable,
                    ),
                    &return_type,
                    &accumulator,
                    &state_type,
                )))
            }
            _ => None,
        }
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &datafusion::config::ConfigOptions {
        &self.config_options
    }
}

#[derive(Clone, Debug)]
pub struct SqlConfig {
    pub default_parallelism: usize,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self {
            default_parallelism: 4,
        }
    }
}

pub async fn parse_and_get_program(
    query: &str,
    schema_provider: ArroyoSchemaProvider,
    config: SqlConfig,
) -> Result<(Program, Vec<i64>)> {
    let query = query.to_string();

    if query.trim().is_empty() {
        bail!("Query is empty");
    }

    tokio::spawn(async move { parse_and_get_program_sync(query, schema_provider, config) })
        .await
        .map_err(|_| anyhow!("Something went wrong"))?
}

pub fn parse_and_get_program_sync(
    query: String,
    mut schema_provider: ArroyoSchemaProvider,
    config: SqlConfig,
) -> Result<(Program, Vec<i64>)> {
    let dialect = PostgreSqlDialect {};
    let mut inserts = vec![];
    for statement in Parser::parse_sql(&dialect, &query)? {
        if let Some(table) = Table::try_from_statement(&statement, &schema_provider)? {
            schema_provider.insert_table(table);
        } else {
            inserts.push(Insert::try_from_statement(&statement, &schema_provider)?);
        };
    }

    let mut sql_pipeline_builder = SqlPipelineBuilder::new(&mut schema_provider);
    for insert in inserts {
        sql_pipeline_builder.add_insert(insert)?;
    }

    let mut plan_graph = PlanGraph::new(config.clone());

    // if there are no insert nodes, return an error
    if sql_pipeline_builder.insert_nodes.is_empty() {
        bail!("The provided SQL does not contain a query");
    }

    // If there isn't a sink, add a web sink to the last insert
    if !sql_pipeline_builder
        .insert_nodes
        .iter()
        .any(|n| matches!(n, SqlOperator::Sink(..)))
    {
        let insert = sql_pipeline_builder.insert_nodes.pop().unwrap();
        let struct_def = insert.return_type();
        let sink = Table::ConnectorTable(ConnectorTable {
            id: None,
            name: "web".to_string(),
            connection_type: arroyo_connectors::ConnectionType::Sink,
            fields: struct_def.fields.clone(),
            type_name: None,
            operator: "GrpcSink".to_string(),
            config: "{}".to_string(),
            description: "WebSink".to_string(),
            serialization_mode: if insert.is_updating() {
                arroyo_datastream::SerializationMode::DebeziumJson
            } else {
                arroyo_datastream::SerializationMode::Json
            },
            event_time_field: None,
            watermark_field: None,
        });

        plan_graph.add_sql_operator(sink.as_sql_sink(insert)?);
    }

    for output in sql_pipeline_builder.insert_nodes.into_iter() {
        plan_graph.add_sql_operator(output);
    }

    get_program(plan_graph, sql_pipeline_builder.schema_provider.clone())
}

#[derive(Clone)]
pub struct TestStruct {
    pub non_nullable_i32: i32,
    pub nullable_i32: Option<i32>,
    pub non_nullable_bool: bool,
    pub nullable_bool: Option<bool>,
    pub non_nullable_f32: f32,
    pub nullable_f32: Option<f32>,
    pub non_nullable_f64: f64,
    pub nullable_f64: Option<f64>,
    pub non_nullable_i64: i64,
    pub nullable_i64: Option<i64>,
    pub non_nullable_string: String,
    pub nullable_string: Option<String>,
    pub non_nullable_timestamp: SystemTime,
    pub nullable_timestamp: Option<SystemTime>,
    pub non_nullable_bytes: Vec<u8>,
    pub nullable_bytes: Option<Vec<u8>>,
}

impl Default for TestStruct {
    fn default() -> Self {
        Self {
            non_nullable_i32: Default::default(),
            nullable_i32: Default::default(),
            non_nullable_bool: Default::default(),
            nullable_bool: Default::default(),
            non_nullable_f32: Default::default(),
            nullable_f32: Default::default(),
            non_nullable_f64: Default::default(),
            nullable_f64: Default::default(),
            non_nullable_i64: Default::default(),
            nullable_i64: Default::default(),
            non_nullable_string: Default::default(),
            nullable_string: Default::default(),
            non_nullable_timestamp: SystemTime::UNIX_EPOCH,
            nullable_timestamp: None,
            non_nullable_bytes: Default::default(),
            nullable_bytes: Default::default(),
        }
    }
}

fn test_struct_def() -> StructDef {
    StructDef {
        name: Some("TestStruct".to_string()),
        fields: vec![
            StructField::new(
                "non_nullable_i32".to_string(),
                None,
                TypeDef::DataType(DataType::Int32, false),
            ),
            StructField::new(
                "nullable_i32".to_string(),
                None,
                TypeDef::DataType(DataType::Int32, true),
            ),
            StructField::new(
                "non_nullable_bool".to_string(),
                None,
                TypeDef::DataType(DataType::Boolean, false),
            ),
            StructField::new(
                "nullable_bool".to_string(),
                None,
                TypeDef::DataType(DataType::Boolean, true),
            ),
            StructField::new(
                "non_nullable_f32".to_string(),
                None,
                TypeDef::DataType(DataType::Float32, false),
            ),
            StructField::new(
                "nullable_f32".to_string(),
                None,
                TypeDef::DataType(DataType::Float32, true),
            ),
            StructField::new(
                "non_nullable_f64".to_string(),
                None,
                TypeDef::DataType(DataType::Float64, false),
            ),
            StructField::new(
                "nullable_f64".to_string(),
                None,
                TypeDef::DataType(DataType::Float64, true),
            ),
            StructField::new(
                "non_nullable_i64".to_string(),
                None,
                TypeDef::DataType(DataType::Int64, false),
            ),
            StructField::new(
                "nullable_i64".to_string(),
                None,
                TypeDef::DataType(DataType::Int64, true),
            ),
            StructField::new(
                "non_nullable_string".to_string(),
                None,
                TypeDef::DataType(DataType::Utf8, false),
            ),
            StructField::new(
                "nullable_string".to_string(),
                None,
                TypeDef::DataType(DataType::Utf8, true),
            ),
            StructField::new(
                "non_nullable_timestamp".to_string(),
                None,
                TypeDef::DataType(DataType::Timestamp(TimeUnit::Microsecond, None), false),
            ),
            StructField::new(
                "nullable_timestamp".to_string(),
                None,
                TypeDef::DataType(DataType::Timestamp(TimeUnit::Microsecond, None), true),
            ),
            StructField::new(
                "non_nullable_bytes".to_string(),
                None,
                TypeDef::DataType(DataType::Binary, false),
            ),
            StructField::new(
                "nullable_bytes".to_string(),
                None,
                TypeDef::DataType(DataType::Binary, true),
            ),
        ],
    }
}

pub fn generate_test_code(
    function_suffix: &str,
    generating_expression: &Expression,
    struct_tokens: &syn::Expr,
    result_expression: &syn::Expr,
) -> syn::ItemFn {
    let syn_expr = generating_expression.to_syn_expression();
    let function_name: syn::Ident =
        parse_str(&format!("generated_test_{}", function_suffix)).unwrap();
    parse_quote!(
                fn #function_name() {
                    assert_eq!({let arg = #struct_tokens;#syn_expr}, #result_expression);
    })
}

pub fn get_test_expression(
    test_name: &str,
    calculation_string: &str,
    input_value: &syn::Expr,
    expected_result: &syn::Expr,
) -> syn::ItemFn {
    let struct_def = test_struct_def();

    let mut schema_provider = ArroyoSchemaProvider::new();
    // schema_provider.add_connector_table(
    //     "test_source".to_string(),
    //     "nexmark".to_string(),
    //     struct_def.fields.clone(),
    //     struct_def.name.clone(),
    //     SourceConfig::NexmarkSource {
    //         event_rate: 10,
    //         runtime: None,
    //     },
    //     SerializationMode::Json,
    // );

    // let mut plan = SqlPreprocessor {
    //     schema_provider: &mut schema_provider,
    // }
    // .plan_query(&format!("SELECT {} FROM test_source", calculation_string))
    // .unwrap();

    // let Table::Anonymous{logical_plan: LogicalPlan::Projection(projection)} = plan.remove(0) else {panic!("expect projection")};
    // let ctx = ExpressionContext {
    //     schema_provider: &schema_provider,
    //     input_struct: &struct_def,
    // };

    // let generating_expression = ctx.compile_expr(&projection.expr[0]).unwrap();

    // generate_test_code(
    //     test_name,
    //     &generating_expression,
    //     input_value,
    //     expected_result,
    // );

    todo!()
}
