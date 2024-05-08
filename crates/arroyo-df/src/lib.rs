#![allow(clippy::new_without_default)]

pub mod builder;
pub(crate) mod extension;
pub mod external;
mod json;
pub mod logical;
pub mod physical;
mod plan;
mod rewriters;
pub mod schemas;
mod tables;
pub mod types;
pub mod udafs;

#[cfg(test)]
mod test;

use anyhow::{anyhow, bail, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{self, DataType};
use arrow_schema::Schema;
use arroyo_datastream::WindowType;

use datafusion::common::{plan_err, DFField, OwnedTableReference, Result as DFResult, ScalarValue};
use datafusion::datasource::DefaultTableSource;
#[allow(deprecated)]
use datafusion::physical_plan::functions::make_scalar_function;

use datafusion::prelude::create_udf;

use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::{planner::ContextProvider, TableReference};

use datafusion::common::tree_node::TreeNode;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    create_udaf, Expr, Extension, LogicalPlan, ReturnTypeFunction, ScalarFunctionDefinition,
    ScalarUDF, Signature, Volatility, WindowUDF,
};

use datafusion::logical_expr::{AggregateUDF, TableSource};
use logical::LogicalBatchInput;

use schemas::window_arrow_struct;
use tables::{Insert, Table};

use crate::builder::PlanToGraphVisitor;
use crate::extension::sink::SinkExtension;
use crate::plan::ArroyoRewriter;
use arroyo_datastream::logical::{DylibUdfConfig, ProgramConfig};
use arroyo_rpc::api_types::connections::ConnectionProfile;
use datafusion::common::DataFusionError;
use std::collections::HashSet;
use std::fmt::Debug;

use crate::json::get_json_functions;
use crate::rewriters::{SourceMetadataVisitor, TimeWindowUdfChecker, UnnestRewriter};
use crate::types::interval_month_day_nanos_to_duration;

use crate::udafs::EmptyUdaf;
use arroyo_datastream::logical::LogicalProgram;
use arroyo_operator::connector::Connection;
use arroyo_udf_host::parse::{inner_type, UdfDef};
use arroyo_udf_host::ParsedUdfFile;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr;
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use std::time::{Duration, SystemTime};
use std::{collections::HashMap, sync::Arc};
use syn::Item;
use tracing::{info, warn};
use unicase::UniCase;

const DEFAULT_IDLE_TIME: Option<Duration> = Some(Duration::from_secs(5 * 60));
pub const ASYNC_RESULT_FIELD: &str = "__async_result";

#[derive(Clone, Debug)]
pub struct CompiledSql {
    pub program: LogicalProgram,
    pub connection_ids: Vec<i64>,
}

#[derive(Clone, Default)]
pub struct ArroyoSchemaProvider {
    pub source_defs: HashMap<String, String>,
    tables: HashMap<UniCase<String>, Table>,
    pub functions: HashMap<String, Arc<ScalarUDF>>,
    pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    pub connections: HashMap<String, Connection>,
    profiles: HashMap<String, ConnectionProfile>,
    pub udf_defs: HashMap<String, UdfDef>,
    config_options: datafusion::config::ConfigOptions,
    pub dylib_udfs: HashMap<String, DylibUdfConfig>,
    pub function_rewriters: Vec<Arc<dyn FunctionRewrite + Send + Sync>>,
}

impl ArroyoSchemaProvider {
    pub fn new() -> Self {
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
                #[allow(deprecated)]
                make_scalar_function(fn_impl),
            )),
        );
        functions.insert(
            "tumble".to_string(),
            Arc::new(create_udf(
                "tumble",
                vec![DataType::Interval(datatypes::IntervalUnit::MonthDayNano)],
                window_return_type.clone(),
                Volatility::Volatile,
                #[allow(deprecated)]
                make_scalar_function(fn_impl),
            )),
        );
        functions.insert(
            "session".to_string(),
            Arc::new(create_udf(
                "session",
                vec![DataType::Interval(datatypes::IntervalUnit::MonthDayNano)],
                window_return_type,
                Volatility::Volatile,
                #[allow(deprecated)]
                make_scalar_function(fn_impl),
            )),
        );
        functions.insert(
            "unnest".to_string(),
            Arc::new({
                let return_type: ReturnTypeFunction = Arc::new(move |args| {
                    match args.first().ok_or_else(|| {
                        DataFusionError::Plan("unnest takes one argument".to_string())
                    })? {
                        DataType::List(t) => Ok(Arc::new(t.data_type().clone())),
                        _ => Err(DataFusionError::Plan(
                            "unnest may only be called on arrays".to_string(),
                        )),
                    }
                });
                #[allow(deprecated)]
                ScalarUDF::new(
                    "unnest",
                    // This is marked volatile so that DF doesn't try to optimize constants
                    &Signature::any(1, Volatility::Volatile),
                    &return_type,
                    #[allow(deprecated)]
                    &make_scalar_function(fn_impl),
                )
            }),
        );

        functions.extend(get_json_functions());

        let mut registry = Self {
            functions,
            ..Default::default()
        };

        datafusion_functions::register_all(&mut registry).unwrap();
        datafusion::functions_array::register_all(&mut registry).unwrap();

        registry
    }

    pub fn add_connector_table(&mut self, connection: Connection) {
        self.tables.insert(
            UniCase::new(connection.name.clone()),
            Table::ConnectorTable(connection.into()),
        );
    }

    pub fn add_connection_profile(&mut self, profile: ConnectionProfile) {
        self.profiles.insert(profile.name.clone(), profile);
    }

    fn insert_table(&mut self, table: Table) {
        self.tables
            .insert(UniCase::new(table.name().to_string()), table);
    }

    pub fn get_table(&self, table_name: impl Into<String>) -> Option<&Table> {
        self.tables.get(&UniCase::new(table_name.into()))
    }

    pub fn get_table_mut(&mut self, table_name: impl Into<String>) -> Option<&mut Table> {
        self.tables.get_mut(&UniCase::new(table_name.into()))
    }

    pub fn add_rust_udf(&mut self, body: &str, url: &str) -> Result<String> {
        let parsed = ParsedUdfFile::try_parse(body)?;

        if parsed.udf.vec_arguments > 0 && parsed.udf.vec_arguments != parsed.udf.args.len() {
            bail!(
                "In function {}: for a UDAF, all arguments must be Vec<T>",
                parsed.udf.name
            );
        }
        let fn_impl = |args: &[ArrayRef]| Ok(Arc::new(args[0].clone()) as ArrayRef);

        self.dylib_udfs.insert(
            parsed.udf.name.clone(),
            DylibUdfConfig {
                dylib_path: url.to_string(),
                arg_types: parsed
                    .udf
                    .args
                    .iter()
                    .map(|t| t.data_type.clone())
                    .collect(),
                return_type: parsed.udf.ret_type.data_type.clone(),
                aggregate: parsed.udf.vec_arguments > 0,
                is_async: parsed.udf.udf_type.is_async(),
            },
        );

        let replaced = if parsed.udf.vec_arguments > 0 {
            self.aggregate_functions
                .insert(
                    parsed.udf.name.clone(),
                    Arc::new(create_udaf(
                        &parsed.udf.name,
                        parsed
                            .udf
                            .args
                            .iter()
                            .map(|t| inner_type(&t.data_type).expect("UDAF arg is not a vec"))
                            .collect(),
                        Arc::new(parsed.udf.ret_type.data_type.clone()),
                        Volatility::Volatile,
                        Arc::new(|_| Ok(Box::new(EmptyUdaf {}))),
                        Arc::new(
                            parsed
                                .udf
                                .args
                                .iter()
                                .map(|t| t.data_type.clone())
                                .collect(),
                        ),
                    )),
                )
                .is_some()
        } else {
            self.functions
                .insert(
                    parsed.udf.name.clone(),
                    Arc::new(create_udf(
                        &parsed.udf.name,
                        parsed
                            .udf
                            .args
                            .iter()
                            .map(|t| t.data_type.clone())
                            .collect(),
                        Arc::new(parsed.udf.ret_type.data_type.clone()),
                        Volatility::Volatile,
                        #[allow(deprecated)]
                        make_scalar_function(fn_impl),
                    )),
                )
                .is_some()
        };

        if replaced {
            warn!("Global UDF '{}' is being overwritten", parsed.udf.name);
        };

        self.udf_defs.insert(
            parsed.udf.name.clone(),
            UdfDef {
                args: parsed.udf.args,
                ret: parsed.udf.ret_type,
                aggregate: parsed.udf.vec_arguments > 0,
                udf_type: parsed.udf.udf_type,
            },
        );

        Ok(parsed.udf.name)
    }
}

fn create_table(table_name: String, schema: Arc<Schema>) -> Arc<dyn TableSource> {
    let table_provider = LogicalBatchInput { table_name, schema };
    let wrapped = Arc::new(table_provider);
    let provider = DefaultTableSource::new(wrapped);
    Arc::new(provider)
}

impl ContextProvider for ArroyoSchemaProvider {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion::common::Result<Arc<dyn TableSource>> {
        let table = self
            .get_table(name.to_string())
            .ok_or_else(|| DataFusionError::Plan(format!("Table {} not found", name)))?;

        let fields = table.get_fields();
        let schema = Arc::new(Schema::new_with_metadata(fields, HashMap::new()));
        Ok(create_table(name.to_string(), schema))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.aggregate_functions.get(name).cloned()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &datafusion::config::ConfigOptions {
        &self.config_options
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn udfs_names(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }

    fn udafs_names(&self) -> Vec<String> {
        self.aggregate_functions.keys().cloned().collect()
    }

    fn udwfs_names(&self) -> Vec<String> {
        vec![]
    }
}

impl FunctionRegistry for ArroyoSchemaProvider {
    fn udfs(&self) -> HashSet<String> {
        self.udf_defs.keys().map(|k| k.to_string()).collect()
    }

    fn udf(&self, name: &str) -> datafusion::common::Result<Arc<ScalarUDF>> {
        if let Some(f) = self.functions.get(name) {
            Ok(Arc::clone(f))
        } else {
            plan_err!("No UDF with name {name}")
        }
    }

    fn udaf(&self, name: &str) -> datafusion::common::Result<Arc<AggregateUDF>> {
        if let Some(f) = self.aggregate_functions.get(name) {
            Ok(Arc::clone(f))
        } else {
            plan_err!("No UDAF with name {name}")
        }
    }

    fn udwf(&self, name: &str) -> datafusion::common::Result<Arc<WindowUDF>> {
        plan_err!("No UDWF with name {name}")
    }

    fn register_function_rewrite(
        &mut self,
        rewrite: Arc<dyn FunctionRewrite + Send + Sync>,
    ) -> DFResult<()> {
        self.function_rewriters.push(rewrite);
        Ok(())
    }

    fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> DFResult<Option<Arc<ScalarUDF>>> {
        Ok(self.functions.insert(udf.name().to_string(), udf))
    }

    fn register_udaf(&mut self, udaf: Arc<AggregateUDF>) -> DFResult<Option<Arc<AggregateUDF>>> {
        Ok(self
            .aggregate_functions
            .insert(udaf.name().to_string(), udaf))
    }

    fn register_udwf(&mut self, _udaf: Arc<WindowUDF>) -> DFResult<Option<Arc<WindowUDF>>> {
        plan_err!("custom window functions not supported")
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
) -> Result<CompiledSql> {
    let query = query.to_string();

    if query.trim().is_empty() {
        bail!("Query is empty");
    }

    parse_and_get_arrow_program(query, schema_provider, config).await
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum WindowBehavior {
    FromOperator {
        window: WindowType,
        window_field: DFField,
        window_index: usize,
        is_nested: bool,
    },
    InData,
}

fn get_duration(expression: &Expr) -> Result<Duration> {
    match expression {
        Expr::Literal(ScalarValue::IntervalDayTime(Some(val))) => {
            Ok(Duration::from_millis(*val as u64))
        }
        Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(val))) => {
            Ok(interval_month_day_nanos_to_duration(*val))
        }
        _ => bail!(
            "unsupported Duration expression, expect duration literal, not {}",
            expression
        ),
    }
}

fn find_window(expression: &Expr) -> Result<Option<WindowType>> {
    match expression {
        Expr::ScalarFunction(ScalarFunction {
            func_def: ScalarFunctionDefinition::UDF(fun),
            args,
        }) => match fun.name() {
            "hop" => {
                if args.len() != 2 {
                    unreachable!();
                }
                let slide = get_duration(&args[0])?;
                let width = get_duration(&args[1])?;
                if width.as_nanos() % slide.as_nanos() != 0 {
                    bail!(
                        "hop() width {:?} currently must be a multiple of slide {:?}",
                        width,
                        slide
                    );
                }
                Ok(Some(WindowType::Sliding { width, slide }))
            }
            "tumble" => {
                if args.len() != 1 {
                    unreachable!("wrong number of arguments for tumble(), expect one");
                }
                let width = get_duration(&args[0])?;
                Ok(Some(WindowType::Tumbling { width }))
            }
            "session" => {
                if args.len() != 1 {
                    unreachable!("wrong number of arguments for session(), expected one");
                }
                let gap = get_duration(&args[0])?;
                Ok(Some(WindowType::Session { gap }))
            }
            _ => Ok(None),
        },
        Expr::Alias(logical_expr::expr::Alias {
            expr,
            name: _,
            relation: _,
        }) => find_window(expr),
        _ => Ok(None),
    }
}

#[allow(unused)]
fn inspect_plan(logical_plan: LogicalPlan) -> LogicalPlan {
    info!("logical plan = {}", logical_plan.display_graphviz());
    logical_plan
}

pub fn rewrite_plan(
    plan: LogicalPlan,
    schema_provider: &ArroyoSchemaProvider,
) -> DFResult<LogicalPlan> {
    let rewritten_plan = plan
        .rewrite(&mut UnnestRewriter {})?
        .data
        .rewrite(&mut ArroyoRewriter { schema_provider })?;
    // check for window functions
    rewritten_plan.data.visit(&mut TimeWindowUdfChecker {})?;
    Ok(rewritten_plan.data)
}

pub async fn parse_and_get_arrow_program(
    query: String,
    mut schema_provider: ArroyoSchemaProvider,
    // TODO: use config
    _config: SqlConfig,
) -> Result<CompiledSql> {
    let dialect = PostgreSqlDialect {};
    let mut inserts = vec![];
    for statement in Parser::parse_sql(&dialect, &query)? {
        if let Some(table) = Table::try_from_statement(&statement, &schema_provider)? {
            schema_provider.insert_table(table);
        } else {
            inserts.push(Insert::try_from_statement(
                &statement,
                &mut schema_provider,
            )?);
        };
    }

    if inserts.is_empty() {
        bail!("The provided SQL does not contain a query");
    }

    let mut used_connections = HashSet::new();
    let mut extensions = vec![];

    for insert in inserts {
        let (plan, sink_name) = match insert {
            // TODO: implement inserts
            Insert::InsertQuery {
                sink_name,
                logical_plan,
            } => (logical_plan, Some(sink_name)),
            Insert::Anonymous { logical_plan } => (logical_plan, None),
        };

        let plan_rewrite = rewrite_plan(plan, &schema_provider)?;

        let mut metadata = SourceMetadataVisitor::new(&schema_provider);
        plan_rewrite.visit(&mut metadata)?;
        used_connections.extend(metadata.connection_ids.iter());

        let sink = match sink_name {
            Some(sink_name) => {
                let table = schema_provider
                    .get_table_mut(&sink_name)
                    .ok_or_else(|| anyhow!("Connection {} not found", sink_name))?;
                match table {
                    Table::ConnectorTable(_) => SinkExtension::new(
                        OwnedTableReference::bare(sink_name),
                        table.clone(),
                        plan_rewrite.schema().clone(),
                        Arc::new(plan_rewrite),
                    ),
                    Table::MemoryTable { logical_plan, .. } => {
                        if logical_plan.is_some() {
                            bail!("Can only insert into a memory table once");
                        }
                        logical_plan.replace(plan_rewrite);
                        continue;
                    }
                    Table::TableFromQuery { .. } => {
                        bail!("Shouldn't be inserting more data into a table made with CREATE TABLE AS");
                    }
                    Table::PreviewSink { .. } => {
                        bail!("queries shouldn't be able insert into preview sink.")
                    }
                }
            }
            None => SinkExtension::new(
                OwnedTableReference::parse_str("preview"),
                Table::PreviewSink {
                    logical_plan: plan_rewrite.clone(),
                },
                plan_rewrite.schema().clone(),
                Arc::new(plan_rewrite),
            ),
        };
        extensions.push(LogicalPlan::Extension(Extension {
            node: Arc::new(sink?),
        }));
    }
    let mut plan_to_graph_visitor = PlanToGraphVisitor::new(&schema_provider);
    for extension in extensions {
        plan_to_graph_visitor.add_plan(extension)?;
    }
    let graph = plan_to_graph_visitor.into_graph();
    let program = LogicalProgram {
        graph,
        program_config: ProgramConfig {
            udf_dylibs: schema_provider.dylib_udfs.clone(),
        },
    };

    Ok(CompiledSql {
        program,
        connection_ids: used_connections.into_iter().collect(),
    })
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
pub fn has_duplicate_udf_names<'a>(definitions: impl Iterator<Item = &'a String>) -> bool {
    let mut udf_names = HashSet::new();
    for definition in definitions {
        let Ok(file) = syn::parse_file(definition) else {
            warn!("Could not parse UDF definition: {}", definition);
            continue;
        };

        for item in file.items {
            let Item::Fn(function) = item else {
                continue;
            };

            if udf_names.contains(&function.sig.ident.to_string()) {
                return true;
            }

            udf_names.insert(function.sig.ident.to_string());
        }
    }
    false
}
