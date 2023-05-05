#![allow(clippy::new_without_default)]
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{self, DataType, Field};
use arrow_schema::TimeUnit;
use arroyo_datastream::{
    auth_config_to_hashmap, NexmarkSource, OffsetMode, Operator, Program, SerializationMode, Source,
};

use arroyo_rpc::grpc::api::connection::ConnectionType;
use arroyo_rpc::grpc::api::Connection;
use datafusion::optimizer::analyzer::Analyzer;
use datafusion::optimizer::optimizer::Optimizer;
use datafusion::optimizer::OptimizerContext;
use datafusion::physical_plan::functions::make_scalar_function;

mod expressions;
mod operators;
mod optimizations;
mod pipeline;
mod plan_graph;
pub mod schemas;
pub mod types;

use datafusion::prelude::create_udf;
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::{ColumnOption, Statement, Value};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::{planner::ContextProvider, TableReference};
use datafusion_common::config::ConfigOptions;
use datafusion_common::DataFusionError;
use datafusion_expr::{
    logical_plan::builder::LogicalTableSource, AggregateUDF, ScalarUDF, TableSource,
};
use datafusion_expr::{
    AccumulatorFunctionImplementation, LogicalPlan, ReturnTypeFunction, Signature,
    StateTypeFunction, TypeSignature, Volatility,
};
use expressions::Expression;
use pipeline::get_program_from_plan;
use schemas::window_arrow_struct;

use crate::expressions::ExpressionContext;
use crate::types::{convert_data_type, StructDef, StructField, TypeDef};
use quote::ToTokens;
use std::time::SystemTime;
use std::{collections::HashMap, sync::Arc};
use syn::{parse_quote, parse_str, FnArg, Item, ReturnType, VisPublic, Visibility};

#[cfg(test)]
mod test;

pub struct UdfDef {
    args: Vec<TypeDef>,
    ret: TypeDef,
    def: String,
}

pub struct ArroyoSchemaProvider {
    pub source_defs: HashMap<String, String>,
    pub tables: HashMap<String, Arc<dyn TableSource>>,
    pub functions: HashMap<String, Arc<ScalarUDF>>,
    pub sources: HashMap<String, SqlSource>,
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
            sources: HashMap::new(),
            source_defs: HashMap::new(),
            connections: HashMap::new(),
            udf_defs: HashMap::new(),
            config_options: datafusion::config::ConfigOptions::new(),
        }
    }

    pub fn add_connection(&mut self, connection: Connection) {
        self.connections.insert(connection.name.clone(), connection);
    }

    pub fn add_source(
        &mut self,
        id: Option<i64>,
        name: impl Into<String>,
        fields: Vec<StructField>,
        operator: Operator,
    ) {
        self.add_source_with_type(id, name, fields, operator, None);
    }

    pub fn add_source_with_type(
        &mut self,
        id: Option<i64>,
        name: impl Into<String>,
        fields: Vec<StructField>,
        operator: Operator,
        type_name: Option<String>,
    ) {
        let name: String = name.into();
        self.sources.insert(
            name.clone(),
            SqlSource {
                id,
                struct_def: StructDef {
                    name: type_name,
                    fields: fields.clone(),
                },
                operator,
            },
        );

        let arrow_fields = fields.into_iter().map(|f| f.into()).collect();
        self.tables.insert(name, create_table_source(arrow_fields));
    }

    pub fn add_defs(&mut self, source: impl Into<String>, defs: impl Into<String>) {
        self.source_defs.insert(source.into(), defs.into());
    }

    fn get_inlined_source(
        &self,
        query: &Statement,
    ) -> Result<(String, Vec<StructField>, Operator)> {
        let Statement::CreateTable {
            name,
            columns,
            with_options,
            ..
        } = query else {
            bail!("expected create table statement");
        };
        let connection_name = with_options
            .iter()
            .filter_map(|option| match option.name.value.as_str() {
                "connection" => Some(value_to_inner_string(&option.value)),
                _ => None,
            })
            .next()
            .ok_or_else(|| anyhow!("expect connection in with block"))??;
        let connection = self
            .connections
            .get(&connection_name)
            .ok_or_else(|| anyhow!("connection {} not found", connection_name))?;

        let json_fields: Vec<StructField> = columns
            .iter()
            .map(|column| {
                let name = column.name.value.to_string();
                let data_type = convert_data_type(&column.data_type)?;
                let nullable = !column
                    .options
                    .iter()
                    .any(|option| matches!(option.option, ColumnOption::NotNull));
                Ok(StructField {
                    name,
                    alias: None,
                    data_type: TypeDef::DataType(data_type, nullable),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        match connection.connection_type {
            Some(ConnectionType::Kafka(ref kafka_connection)) => {
                let topic: String = with_options
                    .iter()
                    .filter_map(|option| match option.name.value.as_str() {
                        "topic" => Some(value_to_inner_string(&option.value)),
                        _ => None,
                    })
                    .next()
                    .ok_or_else(|| anyhow!("expect topic in with block"))??;
                Ok((
                    name.to_string(),
                    json_fields,
                    Operator::KafkaSource {
                        topic,
                        // split to a vec
                        bootstrap_servers: kafka_connection
                            .bootstrap_servers
                            .split(",")
                            .map(|s| s.to_string())
                            .collect(),
                        offset_mode: OffsetMode::Latest,
                        kafka_input_format: SerializationMode::Json,
                        // TODO: use setting for account
                        messages_per_second: 10000000,
                        client_configs: auth_config_to_hashmap(
                            kafka_connection.auth_config.clone(),
                        ),
                    },
                ))
            }
            Some(ConnectionType::Kinesis(ref _kinesis_connection)) => {
                bail!("Kinesis not yet supported")
            }
            None => bail!("malformed connection, missing type."),
        }
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

fn value_to_inner_string(value: &Value) -> Result<String> {
    match value {
        Value::SingleQuotedString(inner_string)
        | Value::UnQuotedString(inner_string)
        | Value::DoubleQuotedString(inner_string) => Ok(inner_string.clone()),
        _ => bail!("Expected a string value, found {:?}", value),
    }
}

impl ContextProvider for ArroyoSchemaProvider {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
            Some(table) => Ok(table.clone()),
            _ => Err(DataFusionError::Plan(format!(
                "Table not found: {}",
                name.table()
            ))),
        }
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
pub struct SqlSource {
    pub id: Option<i64>,
    pub struct_def: StructDef,
    pub operator: Operator,
}

#[derive(Clone, Debug)]
pub struct SqlConfig {
    pub default_parallelism: usize,
    pub sink: Operator,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self {
            default_parallelism: 4,
            sink: Operator::ConsoleSink,
        }
    }
}

pub async fn parse_and_get_program(
    query: &str,
    mut schema_provider: ArroyoSchemaProvider,
    config: SqlConfig,
) -> Result<(Program, Vec<SqlSource>)> {
    let query = query.to_string();

    if query.trim().is_empty() {
        bail!("Query is empty");
    }

    tokio::spawn(async move {
        // parse the SQL
        let plan = get_plan_from_query(&query, &mut schema_provider)?;
        get_program_from_plan(config, schema_provider, &plan)
    })
    .await
    .map_err(|_| anyhow!("Something went wrong"))?
}

fn get_plan_from_query(
    query: &str,
    schema_provider: &mut ArroyoSchemaProvider,
) -> Result<LogicalPlan> {
    // parse the SQL
    let dialect = PostgreSqlDialect {};
    let mut ast = Parser::parse_sql(&dialect, &query)
        .map_err(|e| match e {
            ParserError::TokenizerError(s) | ParserError::ParserError(s) => anyhow!(s),
            ParserError::RecursionLimitExceeded => anyhow!("recursion limit"),
        })
        .with_context(|| "parse_failure")?;
    if ast.len() == 0 {
        bail!("Query is empty");
    }
    while ast.len() > 1 {
        let statement = ast.remove(0);
        let (name, fields, operator) = schema_provider.get_inlined_source(&statement)?;
        schema_provider.add_source(None, name, fields, operator);
    }
    let statement = ast.remove(0);

    let sql_to_rel = SqlToRel::new(schema_provider);

    let plan = sql_to_rel.sql_statement_to_plan(statement.clone())?;
    let optimizer_config = OptimizerContext::default();
    let analyzer = Analyzer::default();
    let optimizer = Optimizer::new();
    let analyzed_plan =
        analyzer.execute_and_check(&plan, &ConfigOptions::default(), |_plan, _rule| {})?;
    let optimized_plan =
        optimizer.optimize(&analyzed_plan, &optimizer_config, |_plan, _rule| {})?;
    Ok(optimized_plan)
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
            StructField {
                name: "non_nullable_i32".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Int32, false),
            },
            StructField {
                name: "nullable_i32".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Int32, true),
            },
            StructField {
                name: "non_nullable_bool".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Boolean, false),
            },
            StructField {
                name: "nullable_bool".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Boolean, true),
            },
            StructField {
                name: "non_nullable_f32".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Float32, false),
            },
            StructField {
                name: "nullable_f32".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Float32, true),
            },
            StructField {
                name: "non_nullable_f64".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Float64, false),
            },
            StructField {
                name: "nullable_f64".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Float64, true),
            },
            StructField {
                name: "non_nullable_i64".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Int64, false),
            },
            StructField {
                name: "nullable_i64".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Int64, true),
            },
            StructField {
                name: "non_nullable_string".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Utf8, false),
            },
            StructField {
                name: "nullable_string".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Utf8, true),
            },
            StructField {
                name: "non_nullable_timestamp".to_string(),
                alias: None,
                data_type: TypeDef::DataType(
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
            },
            StructField {
                name: "nullable_timestamp".to_string(),
                alias: None,
                data_type: TypeDef::DataType(
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
            },
            StructField {
                name: "non_nullable_bytes".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Binary, false),
            },
            StructField {
                name: "nullable_bytes".to_string(),
                alias: None,
                data_type: TypeDef::DataType(DataType::Binary, true),
            },
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
    schema_provider.add_source_with_type(
        Some(1),
        "test_source".to_string(),
        struct_def.fields.clone(),
        NexmarkSource {
            first_event_rate: 10,
            num_events: Some(100),
        }
        .as_operator(),
        struct_def.name.clone(),
    );
    let plan = get_plan_from_query(
        &format!("SELECT {} FROM test_source", calculation_string),
        &mut schema_provider,
    )
    .unwrap();
    let LogicalPlan::Projection(projection) = plan else {panic!("expect projection")};

    let mut ctx = ExpressionContext {
        schema_provider: &schema_provider,
        input_struct: &struct_def,
    };

    let generating_expression = ctx.compile_expr(&projection.expr[0]).unwrap();
    generate_test_code(
        test_name,
        &generating_expression,
        input_value,
        expected_result,
    )
}
