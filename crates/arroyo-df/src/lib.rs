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
pub mod udfs;

#[cfg(test)]
mod test;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{self, DataType};
use arrow_schema::{Field, Schema};
use arroyo_datastream::WindowType;

use datafusion::datasource::DefaultTableSource;
#[allow(deprecated)]
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion_common::{plan_err, DFField, OwnedTableReference, ScalarValue};

use datafusion::prelude::create_udf;

use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::{planner::ContextProvider, TableReference};

use datafusion_common::tree_node::TreeNode;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    create_udaf, Expr, Extension, LogicalPlan, ReturnTypeFunction, ScalarFunctionDefinition,
    ScalarUDF, Signature, Volatility, WindowUDF,
};

use datafusion_expr::{AggregateUDF, TableSource};
use logical::LogicalBatchInput;

use schemas::window_arrow_struct;
use tables::{Insert, Table};

use crate::builder::PlanToGraphVisitor;
use crate::extension::sink::SinkExtension;
use crate::plan::ArroyoRewriter;
use arroyo_datastream::logical::{DylibUdfConfig, ProgramConfig};
use arroyo_rpc::api_types::connections::ConnectionProfile;
use datafusion_common::DataFusionError;
use prettyplease::unparse;
use regex::Regex;
use std::collections::HashSet;
use std::fmt::Debug;

use crate::json::get_json_functions;
use crate::rewriters::{SourceMetadataVisitor, UnnestRewriter};
use crate::types::{interval_month_day_nanos_to_duration, rust_to_arrow};

use crate::udafs::EmptyUdaf;
use arroyo_datastream::logical::LogicalProgram;
use arroyo_operator::connector::Connection;
use arroyo_types::NullableType;
use datafusion_execution::FunctionRegistry;
use std::time::{Duration, SystemTime};
use std::{collections::HashMap, sync::Arc};
use syn::{parse_file, FnArg, Item, ReturnType, Visibility};
use tracing::{info, warn};
use unicase::UniCase;

const DEFAULT_IDLE_TIME: Option<Duration> = Some(Duration::from_secs(5 * 60));

#[allow(unused)]
#[derive(Clone, Debug)]
pub struct UdfDef {
    pub args: Vec<NullableType>,
    pub ret: NullableType,
    def: String,
    dependencies: String,
    aggregate: bool,
}

#[derive(Clone, Debug)]
pub struct CompiledSql {
    pub program: LogicalProgram,
    pub connection_ids: Vec<i64>,
}

#[derive(Debug, Clone, Default)]
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
}

pub struct ParsedUdf {
    pub name: String,
    pub args: Vec<NullableType>,
    pub vec_arguments: usize,
    pub ret_type: NullableType,
    pub definition: String,
    pub dependencies: String,
}

impl ParsedUdf {
    fn vec_inner_type(ty: &syn::Type) -> Option<syn::Type> {
        if let syn::Type::Path(syn::TypePath { path, .. }) = ty {
            if let Some(segment) = path.segments.last() {
                if segment.ident == "Vec" {
                    if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                        if args.args.len() == 1 {
                            if let syn::GenericArgument::Type(inner_ty) = &args.args[0] {
                                return Some(inner_ty.clone());
                            }
                        }
                    }
                }
            }
        }
        None
    }

    pub fn try_parse(def: &str) -> anyhow::Result<ParsedUdf> {
        let mut file = parse_file(def)?;

        let mut functions = file.items.iter_mut().filter_map(|item| match item {
            Item::Fn(function) => Some(function),
            _ => None,
        });

        let function = match (functions.next(), functions.next()) {
            (Some(function), None) => function,
            _ => bail!("UDF definition must contain exactly 1 function."),
        };

        let name = function.sig.ident.to_string();
        let mut args = vec![];
        let mut vec_arguments = 0;
        for (i, arg) in function.sig.inputs.iter().enumerate() {
            match arg {
                FnArg::Receiver(_) => {
                    bail!(
                        "Function {} has a 'self' argument, which is not allowed",
                        name
                    )
                }
                FnArg::Typed(t) => {
                    if let Some(vec_type) = Self::vec_inner_type(&t.ty) {
                        vec_arguments += 1;
                        let vec_type = rust_to_arrow(&vec_type).ok_or_else(|| {
                            anyhow!(
                                "Could not convert function {} inner vector arg {} into an Arrow data type",
                                name,
                                i
                            )
                        })?;

                        args.push(NullableType::not_null(DataType::List(Arc::new(
                            Field::new("item", vec_type.data_type, vec_type.nullable),
                        ))));
                    } else {
                        args.push(rust_to_arrow(&t.ty).ok_or_else(|| {
                            anyhow!(
                                "Could not convert function {} arg {} into a SQL data type",
                                name,
                                i
                            )
                        })?);
                    }
                }
            }
        }

        let ret = match &function.sig.output {
            ReturnType::Default => bail!("Function {} return type must be specified", name),
            ReturnType::Type(_, t) => rust_to_arrow(t).ok_or_else(|| {
                anyhow!(
                    "Could not convert function {} return type into a SQL data type",
                    name
                )
            })?,
        };

        function.vis = Visibility::Public(Default::default());

        Ok(ParsedUdf {
            name,
            args,
            vec_arguments,
            ret_type: ret,
            definition: unparse(&file),
            dependencies: parse_dependencies(def)?,
        })
    }
}

pub fn inner_type(dt: &DataType) -> Option<DataType> {
    match dt {
        DataType::List(f) => Some(f.data_type().clone()),
        _ => None,
    }
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

        Self {
            tables,
            functions,
            aggregate_functions: HashMap::new(),
            source_defs: HashMap::new(),
            connections: HashMap::new(),
            profiles: HashMap::new(),
            udf_defs: HashMap::new(),
            config_options: datafusion::config::ConfigOptions::new(),
            dylib_udfs: HashMap::new(),
        }
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
        let parsed = ParsedUdf::try_parse(body)?;

        if parsed.vec_arguments > 0 && parsed.vec_arguments != parsed.args.len() {
            bail!("Function {} arguments must be vectors or none", parsed.name);
        }
        let fn_impl = |args: &[ArrayRef]| Ok(Arc::new(args[0].clone()) as ArrayRef);

        self.dylib_udfs.insert(
            parsed.name.clone(),
            DylibUdfConfig {
                dylib_path: url.to_string(),
                arg_types: parsed.args.iter().map(|t| t.data_type.clone()).collect(),
                return_type: parsed.ret_type.data_type.clone(),
                aggregate: parsed.vec_arguments > 0,
            },
        );

        let replaced = if parsed.vec_arguments > 0 {
            self.aggregate_functions
                .insert(
                    parsed.name.clone(),
                    Arc::new(create_udaf(
                        &parsed.name,
                        parsed
                            .args
                            .iter()
                            .map(|t| inner_type(&t.data_type).expect("udaf arg is not a vec"))
                            .collect(),
                        Arc::new(parsed.ret_type.data_type.clone()),
                        Volatility::Volatile,
                        Arc::new(|_| Ok(Box::new(EmptyUdaf {}))),
                        Arc::new(parsed.args.iter().map(|t| t.data_type.clone()).collect()),
                    )),
                )
                .is_some()
        } else {
            self.functions
                .insert(
                    parsed.name.clone(),
                    Arc::new(create_udf(
                        &parsed.name,
                        parsed.args.iter().map(|t| t.data_type.clone()).collect(),
                        Arc::new(parsed.ret_type.data_type.clone()),
                        Volatility::Volatile,
                        #[allow(deprecated)]
                        make_scalar_function(fn_impl),
                    )),
                )
                .is_some()
        };

        if replaced {
            warn!("Global UDF '{}' is being overwritten", parsed.name);
        };

        self.udf_defs.insert(
            parsed.name.clone(),
            UdfDef {
                args: parsed.args,
                ret: parsed.ret_type,
                def: parsed.definition,
                dependencies: parsed.dependencies,
                aggregate: parsed.vec_arguments > 0,
            },
        );

        Ok(parsed.name)
    }
}

pub fn parse_dependencies(definition: &str) -> Result<String> {
    // get content of dependencies comment using regex
    let re = Regex::new(r"\/\*\n(\[dependencies\]\n[\s\S]*?)\*\/").unwrap();
    if re.find_iter(definition).count() > 1 {
        bail!("Only one dependencies definition is allowed in a UDF");
    }

    return if let Some(captures) = re.captures(definition) {
        if captures.len() != 2 {
            bail!("Error parsing dependencies");
        }
        Ok(captures.get(1).unwrap().as_str().to_string())
    } else {
        Ok("[dependencies]\n# none defined\n".to_string())
    };
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
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let table = self.get_table(name.to_string()).ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!("Table {} not found", name))
        })?;

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
}

// pub fn new_registry() -> Registry {
//     let mut registry = Registry::default();
//     registry.add_udf(Arc::new(window_scalar_function()));
//     for json_function in get_json_functions().values() {
//         registry.add_udf(json_function.clone());
//     }
//     registry
// }

impl FunctionRegistry for ArroyoSchemaProvider {
    fn udfs(&self) -> HashSet<String> {
        self.udf_defs.keys().map(|k| k.to_string()).collect()
    }

    fn udf(&self, name: &str) -> datafusion_common::Result<Arc<ScalarUDF>> {
        if let Some(f) = self.functions.get(name) {
            Ok(Arc::clone(f))
        } else {
            plan_err!("No UDF with name {name}")
        }
    }

    fn udaf(&self, name: &str) -> datafusion_common::Result<Arc<AggregateUDF>> {
        if let Some(f) = self.aggregate_functions.get(name) {
            Ok(Arc::clone(f))
        } else {
            plan_err!("No UDAF with name {name}")
        }
    }

    fn udwf(&self, name: &str) -> datafusion_common::Result<Arc<WindowUDF>> {
        plan_err!("No UDWF with name {name}")
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
        Expr::Alias(datafusion_expr::expr::Alias {
            expr,
            name: _,
            relation: _,
        }) => find_window(expr),
        _ => Ok(None),
    }
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
        if let Some(table) = Table::try_from_statement(&statement, &schema_provider)
            .context("failed in try_from statement")?
        {
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
    let mut plan_to_graph_visitor = PlanToGraphVisitor::new(&schema_provider);

    for insert in inserts {
        let (plan, sink_name) = match insert {
            // TODO: implement inserts
            Insert::InsertQuery {
                sink_name,
                logical_plan,
            } => (logical_plan, Some(sink_name)),
            Insert::Anonymous { logical_plan } => (logical_plan, None),
        };

        let plan_rewrite = plan.rewrite(&mut UnnestRewriter {})?;
        let plan_rewrite = plan_rewrite.rewrite(&mut ArroyoRewriter {
            schema_provider: &schema_provider,
        })?;

        let mut metadata = SourceMetadataVisitor::new(&schema_provider);
        plan_rewrite.visit(&mut metadata)?;
        used_connections.extend(metadata.connection_ids.iter());

        info!("Logical plan: {}", plan_rewrite.display_graphviz());

        let sink = match sink_name {
            Some(sink_name) => {
                let table = schema_provider
                    .get_table(&sink_name)
                    .ok_or_else(|| anyhow!("Connection {} not found", sink_name))?;
                let Table::ConnectorTable(_connector_table) = table else {
                    bail!("expected connector table");
                };
                SinkExtension::new(
                    OwnedTableReference::bare(sink_name),
                    table.clone(),
                    plan_rewrite.schema().clone(),
                    Arc::new(plan_rewrite),
                )
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
        plan_to_graph_visitor.add_plan(LogicalPlan::Extension(Extension {
            node: Arc::new(sink),
        }))?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dependencies_valid() {
        let definition = r#"
/*
[dependencies]
serde = "1.0"
*/

pub fn my_udf() -> i64 {
    1
}
        "#;

        assert_eq!(
            parse_dependencies(definition).unwrap(),
            r#"[dependencies]
serde = "1.0"
"#
        );
    }

    #[test]
    fn test_parse_dependencies_none() {
        let definition = r#"
pub fn my_udf() -> i64 {
    1
}
        "#;

        assert_eq!(
            parse_dependencies(definition).unwrap(),
            r#"[dependencies]
# none defined
"#
        );
    }

    #[test]
    fn test_parse_dependencies_multiple() {
        let definition = r#"
/*
[dependencies]
serde = "1.0"
*/

/*
[dependencies]
serde = "1.0"
*/

pub fn my_udf() -> i64 {
    1

        "#;
        assert!(parse_dependencies(definition).is_err());
    }
}
