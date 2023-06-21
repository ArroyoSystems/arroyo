#![allow(clippy::new_without_default)]
use anyhow::{anyhow, bail, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{self, DataType, Field};
use arrow_schema::TimeUnit;
use arroyo_datastream::{Operator, Program, SerializationMode, SinkConfig, SourceConfig};
use arroyo_rpc::grpc::api::Connection;
use datafusion::optimizer::analyzer::Analyzer;
use datafusion::optimizer::optimizer::Optimizer;
use datafusion::optimizer::OptimizerContext;
use datafusion::physical_plan::functions::make_scalar_function;
use std::panic;
mod expressions;
pub mod external;
mod operators;
mod optimizations;
mod pipeline;
mod plan_graph;
pub mod schemas;
pub mod types;

use datafusion::prelude::create_udf;

use datafusion::sql::planner::{PlannerContext, SqlToRel};
use datafusion::sql::sqlparser::ast::{ColumnOption, Statement, Value};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::{planner::ContextProvider, TableReference};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DFField, DFSchema};

use datafusion_expr::{
    logical_plan::builder::LogicalTableSource, AggregateUDF, ScalarUDF, TableSource,
};
use datafusion_expr::{
    AccumulatorFunctionImplementation, CreateMemoryTable, CreateView, DdlStatement, DmlStatement,
    LogicalPlan, ReturnTypeFunction, Signature, StateTypeFunction, TypeSignature, Volatility,
    WriteOp,
};
use expressions::Expression;
use external::SqlSink;
use pipeline::{SqlOperator, SqlPipelineBuilder};
use plan_graph::{get_program, PlanGraph};
use schemas::window_arrow_struct;

use crate::expressions::ExpressionContext;
use crate::types::{convert_data_type, StructDef, StructField, TypeDef};
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

    pub fn add_connection(&mut self, connection: Connection) {
        self.connections.insert(connection.name.clone(), connection);
    }

    pub fn add_saved_source_with_type(
        &mut self,
        id: i64,
        name: String,
        fields: Vec<StructField>,
        type_name: Option<String>,
        source_config: SourceConfig,
        serialization_mode: SerializationMode,
    ) {
        self.tables.insert(
            name.clone(),
            Table::SavedSource {
                name,
                id,
                fields,
                type_name,
                source_config,
                serialization_mode,
            },
        );
    }

    fn insert_table(&mut self, table: Table) {
        if let Some(name) = table.name() {
            self.tables.insert(name, table);
        }
    }
    pub fn add_defs(&mut self, source: impl Into<String>, defs: impl Into<String>) {
        self.source_defs.insert(source.into(), defs.into());
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
pub struct SqlSource {
    pub id: Option<i64>,
    pub struct_def: StructDef,
    pub operator: Operator,
}

#[derive(Clone, Debug)]
pub struct SqlConfig {
    pub default_parallelism: usize,
    pub sink: SinkConfig,
    pub kafka_qps: Option<u32>,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self {
            default_parallelism: 4,
            sink: SinkConfig::Grpc,
            kafka_qps: None,
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
    let mut sql_program_builder = SqlProgramBuilder {
        schema_provider: &mut schema_provider,
    };
    let outputs = sql_program_builder.plan_query(&query)?;
    let mut sql_pipeline_builder = SqlPipelineBuilder::new(sql_program_builder.schema_provider);
    for output in outputs {
        sql_pipeline_builder.insert_table(output)?;
    }
    let mut plan_graph = PlanGraph::new(config.clone());
    let last_output = sql_pipeline_builder
        .output_nodes
        .last()
        .ok_or_else(|| anyhow!("No output nodes"))?;

    // If the last output is not a sink, add the sink passed in.
    if !matches!(last_output, SqlOperator::Sink(..)) {
        let non_sink_output = sql_pipeline_builder.output_nodes.pop().unwrap();
        let struct_def = non_sink_output.return_type();
        let sink = SqlOperator::Sink(
            "default_sink".to_string(),
            SqlSink::new_from_sink_config(struct_def, config.sink),
            Box::new(non_sink_output),
        );
        sql_pipeline_builder.output_nodes.push(sink);
    }

    for output in sql_pipeline_builder.output_nodes.into_iter() {
        plan_graph.add_sql_operator(output);
    }
    get_program(plan_graph, sql_program_builder.schema_provider.clone())
}

struct SqlProgramBuilder<'a> {
    schema_provider: &'a mut ArroyoSchemaProvider,
}

impl<'a> SqlProgramBuilder<'a> {
    fn plan_query(&mut self, query: &str) -> Result<Vec<Table>> {
        let dialect = PostgreSqlDialect {};
        let mut outputs = Vec::new();
        for statement in Parser::parse_sql(&dialect, query)? {
            let table = self.process_statement(statement)?;
            match table.name() {
                Some(_) => self.schema_provider.insert_table(table),
                None => outputs.push(table),
            }
        }
        Ok(outputs)
    }

    fn process_statement(&mut self, statement: Statement) -> Result<Table> {
        // Handle naked create tables separately,
        // As DataFusion doesn't support the WITH clause.
        let sql_to_rel = SqlToRel::new(self.schema_provider);
        if let Statement::CreateTable {
            name,
            columns,
            with_options,
            query: None,
            ..
        } = statement
        {
            let name = name.to_string();
            let mut with_map = HashMap::new();
            for option in with_options {
                with_map.insert(
                    option.name.value.to_string(),
                    value_to_inner_string(&option.value)?,
                );
            }

            let struct_field_tuple = columns
                .iter()
                .map(|column| {
                    let name = column.name.value.to_string();
                    let data_type = convert_data_type(&column.data_type)?;
                    let nullable = !column
                        .options
                        .iter()
                        .any(|option| matches!(option.option, ColumnOption::NotNull));
                    let struct_field = StructField {
                        name,
                        alias: None,
                        data_type: TypeDef::DataType(data_type, nullable),
                    };
                    let generating_expression = column.options.iter().find_map(|option| {
                        if let ColumnOption::Generated {
                            generated_as: _,
                            sequence_options: _,
                            generation_expr,
                        } = &option.option
                        {
                            generation_expr.clone()
                        } else {
                            None
                        }
                    });
                    Ok((struct_field, generating_expression))
                })
                .collect::<Result<Vec<_>>>()?;
            // if there are virtual fields, need to do some additional work
            let has_virtual_fields = struct_field_tuple
                .iter()
                .any(|(_, generating_expression)| generating_expression.is_some());
            let fields: Vec<FieldSpec> = if has_virtual_fields {
                let physical_struct = StructDef {
                    name: None,
                    fields: struct_field_tuple
                        .iter()
                        .filter_map(
                            |(field, generating_expression)| match generating_expression {
                                Some(_) => None,
                                None => Some(field.clone()),
                            },
                        )
                        .collect(),
                };
                let physical_schema = DFSchema::new_with_metadata(
                    physical_struct
                        .fields
                        .iter()
                        .map(|f| {
                            let TypeDef::DataType(data_type, nullable ) = f.data_type.clone() else {
                    bail!("expect data type for generated column")
                };
                            Ok(DFField::new_unqualified(&f.name, data_type, nullable))
                        })
                        .collect::<Result<Vec<_>>>()?,
                    HashMap::new(),
                )?;
                let expression_context = ExpressionContext {
                    input_struct: &physical_struct,
                    schema_provider: self.schema_provider,
                };
                struct_field_tuple
                    .into_iter()
                    .map(|(struct_field, generating_expression)| {
                        match generating_expression {
                            Some(generating_expression) => {
                                // TODO: Implement automatic type coercion here, as we have elsewhere.
                                // It is done by calling the Analyzer which inserts CAST operators where necessary.

                                let df_expr = sql_to_rel.sql_to_expr(
                                    generating_expression,
                                    &physical_schema,
                                    &mut PlannerContext::default(),
                                )?;
                                let expr = expression_context.compile_expr(&df_expr)?;
                                Ok(FieldSpec::VirtualStructField(struct_field, expr))
                            }
                            None => Ok(FieldSpec::StructField(struct_field)),
                        }
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                struct_field_tuple
                    .into_iter()
                    .map(|(struct_field, _)| Ok(FieldSpec::StructField(struct_field)))
                    .collect::<Result<Vec<_>>>()?
            };

            let connection_name = with_map.get("connection");
            match connection_name {
                Some(connection_name) => {
                    let connection = self
                        .schema_provider
                        .connections
                        .get(connection_name)
                        .ok_or_else(|| anyhow!("connection {} not found", connection_name))?
                        .clone();
                    Ok(Table::MemoryTableWithConnectionConfig {
                        name,
                        fields,
                        connection,
                        connection_config: with_map,
                    })
                }
                None => {
                    let fields = fields
                        .into_iter()
                        .map(|field| match field {
                            FieldSpec::StructField(struct_field) => Ok(struct_field),
                            FieldSpec::VirtualStructField(..) => bail!("Virtual fields are not supported in memory tables. Just write a query"),
                        }).collect::<Result<Vec<_>>>()?;
                    Ok(Table::MemoryTable { name, fields })
                }
            }
        } else {
            let plan = sql_to_rel.sql_statement_to_plan(statement.clone())?;

            let optimizer_config = OptimizerContext::default();
            let analyzer = Analyzer::default();
            let optimizer = Optimizer::new();
            let analyzed_plan =
                analyzer.execute_and_check(&plan, &ConfigOptions::default(), |_plan, _rule| {})?;
            let optimized_plan =
                optimizer.optimize(&analyzed_plan, &optimizer_config, |_plan, _rule| {})?;

            match &optimized_plan {
                // views and memory tables are the same now.
                LogicalPlan::Ddl(DdlStatement::CreateView(CreateView { name, input, .. }))
                | LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
                    name,
                    input,
                    ..
                })) => {
                    // Return a TableFromQuery
                    Ok(Table::TableFromQuery {
                        name: name.to_string(),
                        logical_plan: (**input).clone(),
                    })
                }
                LogicalPlan::Dml(DmlStatement {
                    table_name,
                    table_schema: _,
                    op: WriteOp::Insert,
                    input,
                }) => {
                    let sink_name = table_name.to_string();
                    Ok(Table::InsertQuery {
                        sink_name,
                        logical_plan: (**input).clone(),
                    })
                }
                _ => Ok(Table::Anonymous {
                    logical_plan: optimized_plan,
                }),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum FieldSpec {
    StructField(StructField),
    VirtualStructField(StructField, Expression),
}

#[derive(Debug, Clone)]
pub enum Table {
    SavedSource {
        name: String,
        id: i64,
        fields: Vec<StructField>,
        type_name: Option<String>,
        source_config: SourceConfig,
        serialization_mode: SerializationMode,
    },
    SavedSink {
        name: String,
        id: i64,
        sink_config: SinkConfig,
    },
    MemoryTable {
        name: String,
        fields: Vec<StructField>,
    },
    MemoryTableWithConnectionConfig {
        name: String,
        fields: Vec<FieldSpec>,
        connection: Connection,
        connection_config: HashMap<String, String>,
    },
    TableFromQuery {
        name: String,
        logical_plan: LogicalPlan,
    },
    InsertQuery {
        sink_name: String,
        logical_plan: LogicalPlan,
    },
    Anonymous {
        logical_plan: LogicalPlan,
    },
}

impl Table {
    fn name(&self) -> Option<String> {
        match self {
            Table::SavedSource { name, .. } => Some(name.clone()),
            Table::SavedSink { name, .. } => Some(name.clone()),
            Table::MemoryTable { name, .. } => Some(name.clone()),
            Table::MemoryTableWithConnectionConfig { name, .. } => Some(name.clone()),
            Table::TableFromQuery { name, .. } => Some(name.clone()),
            Table::InsertQuery { .. } | Table::Anonymous { .. } => None,
        }
    }

    fn get_fields(&self) -> Result<Vec<Field>> {
        match self {
            Table::MemoryTableWithConnectionConfig { fields, .. } => fields
                .iter()
                .map(|field| match field {
                    FieldSpec::StructField(struct_field) => Ok(struct_field.clone().into()),
                    FieldSpec::VirtualStructField(struct_field, _) => {
                        Ok(struct_field.clone().into())
                    }
                })
                .collect::<Result<Vec<_>>>(),
            Table::MemoryTable { fields, .. } | Table::SavedSource { fields, .. } => fields
                .iter()
                .map(|field| {
                    let field: Field = field.clone().into();
                    Ok(field)
                })
                .collect::<Result<Vec<_>>>(),
            Table::SavedSink {
                name: _,
                id: _,
                sink_config: _,
            } => bail!("saved sinks don't have fields"),
            Table::TableFromQuery { logical_plan, .. }
            | Table::InsertQuery { logical_plan, .. }
            | Table::Anonymous { logical_plan } => logical_plan
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    let field: Field = (**field.field()).clone();
                    Ok(field)
                })
                .collect::<Result<Vec<_>>>(),
        }
    }
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
    schema_provider.add_saved_source_with_type(
        1,
        "test_source".to_string(),
        struct_def.fields.clone(),
        struct_def.name.clone(),
        SourceConfig::NexmarkSource {
            event_rate: 10,
            runtime: None,
        },
        SerializationMode::Json,
    );
    let mut plan = SqlProgramBuilder {
        schema_provider: &mut schema_provider,
    }
    .plan_query(&format!("SELECT {} FROM test_source", calculation_string))
    .unwrap();

    let Table::Anonymous{logical_plan: LogicalPlan::Projection(projection)} = plan.remove(0) else {panic!("expect projection")};
    let ctx = ExpressionContext {
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
