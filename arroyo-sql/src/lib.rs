#![allow(clippy::new_without_default)]
use anyhow::{anyhow, bail, Result};
use arrow::array::ArrayRef;
use arrow::compute::or;
use arrow::datatypes::{self, DataType, Field};
use arrow_schema::{Schema, TimeUnit};
use arroyo_connectors::kafka::{KafkaConfig, KafkaConnector, KafkaTable};
use arroyo_connectors::{Connection, Connector};
use arroyo_datastream::{Program, WindowType};

use arroyo_rpc::grpc::api::window;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion_common::{Column, OwnedTableReference, Result as DFResult, DFField};
pub mod avro;
pub(crate) mod code_gen;
pub mod expressions;
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

use datafusion_common::tree_node::{
    RewriteRecursion, TreeNode, TreeNodeRewriter, TreeNodeVisitor, VisitRecursion,
};
use datafusion_expr::{
    logical_plan, AccumulatorFactoryFunction, Aggregate, Expr, LogicalPlan, ReturnTypeFunction,
    Signature, StateTypeFunction, TableScan, Volatility, WindowUDF,
};
use datafusion_expr::{
    logical_plan::builder::LogicalTableSource, AggregateUDF, ScalarUDF, TableSource,
};
use expressions::{Expression, ExpressionContext};
use petgraph::data::Build;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::IntoNodeReferences;
use pipeline::{SqlOperator, SqlPipelineBuilder};
use plan_graph::{get_program, PlanGraph};
use schemas::window_arrow_struct;
use tables::{schema_defs, ConnectorTable, Insert, Table};

use crate::code_gen::{CodeGenerator, ValuePointerContext};
use crate::types::{StructDef, StructField, TypeDef};
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, ConnectionType};
use arroyo_rpc::formats::{Format, JsonFormat};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError};
use prettyplease::unparse;
use regex::Regex;
use std::borrow::BorrowMut;
use std::collections::HashSet;
use std::fmt::Debug;

use arroyo_rpc::OperatorConfig;
use std::time::{Duration, SystemTime};
use std::{collections::HashMap, sync::Arc};
use syn::{parse_file, parse_quote, parse_str, FnArg, Item, ReturnType, Visibility};
use tracing::{info, warn};
use unicase::UniCase;

const DEFAULT_IDLE_TIME: Option<Duration> = Some(Duration::from_secs(5 * 60));

#[cfg(test)]
mod test;

#[derive(Clone, Debug)]
pub struct UdfDef {
    args: Vec<TypeDef>,
    ret: TypeDef,
    def: String,
    dependencies: String,
}

#[derive(Clone, Debug)]
pub struct CompiledSql {
    pub program: Program,
    pub connection_ids: Vec<i64>,
    pub schemas: HashMap<String, StructDef>,
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
                window_return_type.clone(),
                Volatility::Volatile,
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
                make_scalar_function(fn_impl),
            )),
        );
        functions.insert(
            "unnest".to_string(),
            Arc::new({
                let return_type: ReturnTypeFunction = Arc::new(move |args| {
                    match args.get(0).ok_or_else(|| {
                        DataFusionError::Plan("unnest takes one argument".to_string())
                    })? {
                        DataType::List(t) => Ok(Arc::new(t.data_type().clone())),
                        _ => Err(DataFusionError::Plan(
                            "unnest may only be called on arrays".to_string(),
                        )),
                    }
                });
                ScalarUDF::new(
                    "unnest",
                    &Signature::any(1, Volatility::Immutable),
                    &return_type,
                    &make_scalar_function(fn_impl),
                )
            }),
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
            "extract_json".to_string(),
            Arc::new(create_udf(
                "extract_json",
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
            aggregate_functions: HashMap::new(),
            source_defs: HashMap::new(),
            connections: HashMap::new(),
            profiles: HashMap::new(),
            udf_defs: HashMap::new(),
            config_options: datafusion::config::ConfigOptions::new(),
        }
    }

    pub fn add_connector_table(&mut self, connection: Connection) {
        if let Some(def) = schema_defs(&connection.name, &connection.schema) {
            self.source_defs.insert(connection.name.clone(), def);
        }

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

    pub fn add_rust_udf(&mut self, body: &str) -> Result<String> {
        let mut file = parse_file(body)?;

        let mut functions = file.items.iter_mut().filter_map(|item| match item {
            Item::Fn(function) => Some(function),
            _ => None,
        });

        let function = match (functions.next(), functions.next()) {
            (Some(function), None) => function,
            _ => bail!("UDF definition must contain exactly 1 function."),
        };

        let name = function.sig.ident.to_string();
        let mut args: Vec<TypeDef> = vec![];
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
                    if let Some(vec_type) = Self::vec_inner_type(&*t.ty) {
                        vec_arguments += 1;
                        args.push((&vec_type).try_into().map_err(|_| {
                                anyhow!(
                                    "Could not convert function {} inner vector arg {} into a SQL data type",
                                    name,
                                    i
                                )
                            })?);
                    } else {
                        args.push((&*t.ty).try_into().map_err(|_| {
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

        let ret: TypeDef = match &function.sig.output {
            ReturnType::Default => bail!("Function {} return type must be specified", name),
            ReturnType::Type(_, t) => (&**t).try_into().map_err(|_| {
                anyhow!(
                    "Could not convert function {} return type into a SQL data type",
                    name
                )
            })?,
        };
        if vec_arguments > 0 && vec_arguments != args.len() {
            bail!("Function {} arguments must be vectors or none", name);
        }
        if vec_arguments > 0 {
            let return_type = Arc::new(ret.as_datatype().unwrap().clone());
            let name = function.sig.ident.to_string();
            let signature = Signature::exact(
                args.iter()
                    .map(|t| t.as_datatype().unwrap().clone())
                    .collect(),
                Volatility::Volatile,
            );
            let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
            let accumulator: AccumulatorFactoryFunction = Arc::new(|_| unreachable!());
            let state_type: StateTypeFunction = Arc::new(|_| unreachable!());
            let udaf =
                AggregateUDF::new(&name, &signature, &return_type, &accumulator, &state_type);
            self.aggregate_functions
                .insert(function.sig.ident.to_string(), Arc::new(udaf));
        } else {
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
                warn!(
                    "Global UDF '{}' is being overwritten",
                    function.sig.ident.to_string()
                );
            };
        }

        function.vis = Visibility::Public(Default::default());

        self.udf_defs.insert(
            function.sig.ident.to_string(),
            UdfDef {
                args,
                ret,
                def: unparse(&file.clone()),
                dependencies: parse_dependencies(&body)?,
            },
        );

        Ok(name)
    }
}

pub fn parse_dependencies(definition: &str) -> Result<String> {
    // get content of dependencies comment using regex
    let re = Regex::new(r"\/\*\n(\[dependencies\]\n[\s\S]*?)\*\/").unwrap();
    if re.find_iter(&definition).count() > 1 {
        bail!("Only one dependencies definition is allowed in a UDF");
    }

    return if let Some(captures) = re.captures(&definition) {
        if captures.len() != 2 {
            bail!("Error parsing dependencies");
        }
        Ok(captures.get(1).unwrap().as_str().to_string())
    } else {
        Ok("[dependencies]\n# none defined\n".to_string())
    };
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
        let table = self.get_table(name.to_string()).ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!("Table {} not found", name))
        })?;

        let fields = table.get_fields();

        Ok(create_table_source(fields))
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

    tokio::spawn(async move { parse_and_get_program_sync(query, schema_provider, config) })
        .await
        .map_err(|_| anyhow!("Something went wrong"))?
}

#[derive(Default, Debug)]
pub(crate) struct QueryToGraphVisitor {
    local_logical_plan_graph: DiGraph<LogicalPlanExtension, DataFusionEdge>,
    table_source_to_nodes: HashMap<OwnedTableReference, NodeIndex>,
}

#[derive(Default)]
struct KeyTimestampRewriter {

}

impl TreeNodeRewriter for KeyTimestampRewriter {
    type N = LogicalPlan;

    fn mutate(&mut self,mut node: Self::N) -> DFResult<Self::N> {
        match  node {
            LogicalPlan::Projection(ref mut projection) => {
                projection.schema = add_timestamp_field(projection.schema.clone())?;
                projection.expr.push(Expr::Column(Column { relation: None, name: "_timestamp".to_string()}));
            },
            LogicalPlan::Aggregate(ref mut aggregate) => {
                aggregate.schema = add_timestamp_field(aggregate.schema.clone())?;
            },
            LogicalPlan::Join(ref mut join) => {
                join.schema = add_timestamp_field(join.schema.clone())?;
            },
            LogicalPlan::Union(ref mut union) => {
                union.schema = add_timestamp_field(union.schema.clone())?;
            },
            LogicalPlan::TableScan(ref mut table_scan) => {
                table_scan.projected_schema = add_timestamp_field(table_scan.projected_schema.clone())?;
            },
            LogicalPlan::SubqueryAlias(ref mut subquery_alias) => {
                let timestamp_field = DFField::new(Some(subquery_alias.alias.clone()), "_timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false);
                subquery_alias.schema = Arc::new(subquery_alias.schema.join(&DFSchema::new_with_metadata(vec![timestamp_field], HashMap::new())?)?);
            }
            _ => {}
        }
        Ok(node)
    }
}

fn add_timestamp_field(schema: DFSchemaRef) -> DFResult<DFSchemaRef> {
    let timestamp_field = DFField::new_unqualified("_timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false);
    Ok(Arc::new(schema.join(&DFSchema::new_with_metadata(vec![timestamp_field], HashMap::new())?)?))
}

#[derive(Debug)]
enum LogicalPlanExtension {
    ValueCalculation(LogicalPlan),
    KeyCalculation(LogicalPlan),
    AggregateCalculation(AggregateCalculation),
    Sink,
}

impl LogicalPlanExtension {
    // used for finding input TableScans, if the variant already manually crafts its edges, return None.
    fn inner_logical_plan(&self) -> Option<&LogicalPlan> {
        match self {
            LogicalPlanExtension::ValueCalculation(inner_plan)
            | LogicalPlanExtension::KeyCalculation(inner_plan) => Some(inner_plan),
            LogicalPlanExtension::AggregateCalculation(_) => None,
            LogicalPlanExtension::Sink => None,
        }
    }
    fn outgoing_edge(&self) -> DataFusionEdge {
        match self {
            LogicalPlanExtension::ValueCalculation(logical_plan) => DataFusionEdge {
                value_schema: logical_plan.schema().clone(),
                key_schema: None,
            },
            LogicalPlanExtension::KeyCalculation(logical_plan) => {
                let value_schema = logical_plan.inputs()[0].schema().clone();
                let key_schema = Some(logical_plan.schema().clone());
                DataFusionEdge {
                    value_schema,
                    key_schema,
                }
            }
            LogicalPlanExtension::AggregateCalculation(aggregate_calculation) => DataFusionEdge {
                value_schema: aggregate_calculation.aggregate.schema.clone(),
                key_schema: None,
            },
            LogicalPlanExtension::Sink => unreachable!()
        }
    }
}

struct AggregateCalculation {
    window: WindowType,
    aggregate: Aggregate,
}

impl Debug for AggregateCalculation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let logical_plan = LogicalPlan::Aggregate(self.aggregate.clone());
        f.debug_struct("AggregateCalculation")
            .field("window", &self.window)
            .field("aggregate", &logical_plan)
            .finish()
    }
}

#[derive(Debug)]
struct DataFusionEdge {
    value_schema: DFSchemaRef,
    key_schema: Option<DFSchemaRef>,
}

impl TreeNodeRewriter for QueryToGraphVisitor {
    type N = LogicalPlan;

    /// Invoked before (Preorder) any children of `node` are rewritten /
    /// visited. Default implementation returns `Ok(Recursion::Continue)`
    fn pre_visit(&mut self, _node: &Self::N) -> DFResult<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }

    /// Invoked after (Postorder) all children of `node` have been mutated and
    /// returns a potentially modified node.
    fn mutate(&mut self, node: Self::N) -> DFResult<Self::N> {
        // we're trying to split out any shuffles and non-datafusion operations.
        // These will be redefined as TableScans for the downstream operation,
        // so we can just use a physical plan
        match node {
            LogicalPlan::Aggregate(Aggregate {
                input,
                mut group_expr,
                aggr_expr,
                schema,
                ..
            }) => {
                let mut window_group_expr: Vec<_> = group_expr
                    .iter()
                    .enumerate()
                    .filter_map(|(i, expr)| {
                        SqlPipelineBuilder::find_window(expr)
                            .map(|option| option.map(|inner| (i, inner)))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()
                    .map_err(|err| DataFusionError::Plan(err.to_string()))?;
                if window_group_expr.len() != 1 {
                    return Err(datafusion_common::DataFusionError::NotImplemented(
                        "require exactly 1 window in group by".to_string(),
                    ));
                }
                let (window_index, window_type) = window_group_expr.pop().unwrap();
                let mut key_fields = schema
                    .fields()
                    .iter()
                    .take(group_expr.len())
                    .cloned()
                    .collect::<Vec<_>>();
                group_expr.remove(window_index);
                let window_field = key_fields.remove(window_index);

                let key_schema = Arc::new(DFSchema::new_with_metadata(
                    key_fields,
                    schema.metadata().clone(),
                )?);
                let key_projection =
                    LogicalPlan::Projection(datafusion_expr::Projection::try_new_with_schema(
                        group_expr.clone(),
                        input.clone(),
                        key_schema.clone(),
                    )?);
                // TODO: copy keys instead of recomputing.

                let key_index = self
                    .local_logical_plan_graph
                    .add_node(LogicalPlanExtension::KeyCalculation(key_projection));

                let mut aggregate_input_fields = schema.fields().clone();
                aggregate_input_fields.remove(window_index);
                // TODO: incorporate the window field in the schema and adjust datafusion.
                //aggregate_input_schema.push(window_field);

                let input_arrow_schema = Arc::new(Schema::from(input.schema().as_ref()));

                let input_table_scan = LogicalPlan::TableScan(TableScan {
                    table_name: OwnedTableReference::parse_str("memory"),
                    source: Arc::new(LogicalTableSource::new(input_arrow_schema)),
                    projection: None,
                    projected_schema: input.schema().clone(),
                    filters: vec![],
                    fetch: None,
                });

                let aggregate_calculation = AggregateCalculation {
                    window: window_type,
                    aggregate: Aggregate::try_new_with_schema(
                        Arc::new(input_table_scan),
                        group_expr,
                        aggr_expr,
                        Arc::new(DFSchema::new_with_metadata(
                            aggregate_input_fields,
                            schema.metadata().clone(),
                        )?),
                    )?,
                };
                let aggregate_index = self.local_logical_plan_graph.add_node(
                    LogicalPlanExtension::AggregateCalculation(aggregate_calculation),
                );
                let table_name = format!("{}", aggregate_index.index());
                self.local_logical_plan_graph.add_edge(
                    key_index,
                    aggregate_index,
                    DataFusionEdge {
                        value_schema: input.schema().clone(),
                        key_schema: Some(key_schema),
                    },
                );
                Ok(LogicalPlan::TableScan(TableScan {
                    table_name: OwnedTableReference::partial("arroyo-virtual", table_name),
                    source: Arc::new(LogicalTableSource::new(Arc::new(schema.as_ref().into()))),
                    projection: None,
                    projected_schema: input.schema().clone(),
                    filters: vec![],
                    fetch: None,
                }))
            }
            LogicalPlan::TableScan(table_scan) => {
                if let Some(projection_indices) = table_scan.projection {
                    let input_table_scan = LogicalPlan::TableScan(TableScan {
                        table_name: table_scan.table_name.clone(),
                        source: table_scan.source.clone(),
                        projection: None,
                        projected_schema: Arc::new(
                            table_scan.source.schema().as_ref().clone().try_into()?,
                        ),
                        filters: table_scan.filters.clone(),
                        fetch: table_scan.fetch,
                    });
                    let projection_expressions: Vec<_> = projection_indices
                        .into_iter()
                        .map(|index| {
                            Expr::Column(Column {
                                relation: None,
                                name: table_scan.source.schema().fields()[index]
                                    .name()
                                    .to_string(),
                            })
                        })
                        .collect();
                    let projection = LogicalPlan::Projection(datafusion_expr::Projection::try_new(
                        projection_expressions,
                        Arc::new(input_table_scan),
                    )?);
                    return projection.rewrite(self);
                }
                let node_index = match self.table_source_to_nodes.get(&table_scan.table_name) {
                    Some(node_index) => *node_index,
                    None => {
                        let original_name = table_scan.table_name.clone();

                        let index = self.local_logical_plan_graph.add_node(
                            LogicalPlanExtension::ValueCalculation(LogicalPlan::TableScan(
                                table_scan.clone(),
                            )),
                        );
                        let Some(LogicalPlanExtension::ValueCalculation(LogicalPlan::TableScan(
                            TableScan { table_name, .. },
                        ))) = self.local_logical_plan_graph.node_weight_mut(index)
                        else {
                            return Err(DataFusionError::Internal(
                                "expect a value node".to_string(),
                            ));
                        };
                        *table_name = OwnedTableReference::partial(
                            "arroyo-virtual",
                            format!("{}", index.index()),
                        );
                        self.table_source_to_nodes.insert(original_name, index);
                        index
                    }
                };
                let Some(LogicalPlanExtension::ValueCalculation(interred_plan)) =
                    self.local_logical_plan_graph.node_weight(node_index)
                else {
                    return Err(DataFusionError::Internal("expect a value node".to_string()));
                };
                Ok(interred_plan.clone())
            }
            other => Ok(other),
        }
    }
}

#[derive(Default)]
struct TableScanFinder {
    input_table_scan_ids: HashSet<usize>,
}

impl TreeNodeVisitor for TableScanFinder {
    type N = LogicalPlan;

    fn post_visit(
        &mut self,
        _node: &Self::N,
    ) -> DFResult<datafusion_common::tree_node::VisitRecursion> {
        Ok(datafusion_common::tree_node::VisitRecursion::Continue)
    }

    fn pre_visit(
        &mut self,
        node: &Self::N,
    ) -> DFResult<datafusion_common::tree_node::VisitRecursion> {
        match node {
            LogicalPlan::TableScan(table_scan) => {
                if let Some(schema) = table_scan.table_name.schema() {
                    if schema == "arroyo-virtual" {
                        self.input_table_scan_ids
                            .insert(table_scan.table_name.table().parse().unwrap());
                    }
                }
                Ok(datafusion_common::tree_node::VisitRecursion::Skip)
            }
            _ => Ok(datafusion_common::tree_node::VisitRecursion::Continue),
        }
    }
}

pub fn rewrite_experiment(
    query: String,
    mut schema_provider: ArroyoSchemaProvider,
    config: SqlConfig,
) -> Result<()> {
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
    let mut rewriter = QueryToGraphVisitor::default();
    for insert in inserts {
        let mut plan = match insert {
            Insert::InsertQuery {
                sink_name,
                logical_plan,
            } => logical_plan,
            Insert::Anonymous { logical_plan } => logical_plan,
        };
        println!("plan {:?}\n\n", plan);
        let plan_rewrite = plan.rewrite(&mut rewriter).unwrap();
        println!("plan rewrite {:?}", plan_rewrite);
        let extended_plan_node = LogicalPlanExtension::ValueCalculation(plan_rewrite);
        let edge = extended_plan_node.outgoing_edge();
        let plan_index = rewriter
            .local_logical_plan_graph
            .add_node(extended_plan_node);

        let sink_index = rewriter.local_logical_plan_graph.add_node(LogicalPlanExtension::Sink);
        rewriter.local_logical_plan_graph.add_edge(plan_index, sink_index, edge);
    
        
        let mut edges = vec![];
        for (node_index, node) in rewriter.local_logical_plan_graph.node_references() {
            let Some(logical_plan) = node.inner_logical_plan() else {
                continue;
            };
            let mut visitor = TableScanFinder::default();
            logical_plan.visit(&mut visitor).unwrap();
            for index in visitor.input_table_scan_ids {
                let table_scan_index = NodeIndex::from(index as u32);
                let edge = rewriter
                    .local_logical_plan_graph
                    .find_edge(table_scan_index, node_index);
                if edge.is_some() || node_index == table_scan_index {
                    continue;
                }
                edges.push((
                    table_scan_index,
                    node_index,
                    rewriter
                        .local_logical_plan_graph
                        .node_weight(table_scan_index)
                        .unwrap()
                        .outgoing_edge(),
                ));
            }
        }
        for (a, b, weight) in edges {
            rewriter.local_logical_plan_graph.add_edge(a, b, weight);
        }
    }
    println!("rewriter: {:?}", rewriter);
    println!(
        "graph: {:?}",
        petgraph::dot::Dot::with_config(&rewriter.local_logical_plan_graph, &[])
    );
    Ok(())
}

pub fn parse_and_get_program_sync(
    query: String,
    mut schema_provider: ArroyoSchemaProvider,
    config: SqlConfig,
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
            connection_type: ConnectionType::Sink,
            fields: struct_def.fields.into_iter().map(|f| f.into()).collect(),
            type_name: None,
            operator: "GrpcSink::<#in_k, #in_t>".to_string(),
            config: serde_json::to_string(&OperatorConfig::default()).unwrap(),
            description: "WebSink".to_string(),
            format: Some(Format::Json(JsonFormat {
                debezium: insert.is_updating(),
                ..Default::default()
            })),
            event_time_field: None,
            watermark_field: None,
            idle_time: DEFAULT_IDLE_TIME,
            inferred_fields: None,
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
    StructDef::for_name(
        Some("TestStruct".to_string()),
        vec![
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
    )
}

pub fn generate_test_code(
    function_suffix: &str,
    generating_expression: &Expression,
    struct_tokens: &syn::Expr,
    result_expression: &syn::Expr,
) -> syn::ItemFn {
    let syn_expr = generating_expression.generate(&ValuePointerContext::new());
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
    let schema = ConnectionSchema {
        format: Some(Format::Json(JsonFormat::default())),
        framing: None,
        struct_name: struct_def.name.clone(),
        fields: struct_def
            .fields
            .iter()
            .map(|s| s.clone().try_into().unwrap())
            .collect(),
        definition: None,
        inferred: None,
    };

    let mut schema_provider = ArroyoSchemaProvider::new();
    let kafka = (KafkaConnector {})
        .from_config(
            Some(1),
            "test_source",
            KafkaConfig {
                authentication: arroyo_connectors::kafka::KafkaConfigAuthentication::None {},
                bootstrap_servers: "localhost:9092".to_string().try_into().unwrap(),
                schema_registry_enum: None,
            },
            KafkaTable {
                topic: "test_topic".to_string(),
                type_: arroyo_connectors::kafka::TableType::Source {
                    offset: arroyo_connectors::kafka::SourceOffset::Latest,
                    read_mode: Some(arroyo_connectors::kafka::ReadMode::ReadUncommitted),
                    group_id: "test-consumer-group".to_string().try_into().unwrap(),
                },
            },
            Some(&schema),
        )
        .unwrap();

    schema_provider.add_connector_table(kafka);

    let mut inserts = vec![];
    for statement in Parser::parse_sql(
        &PostgreSqlDialect {},
        &format!("SELECT {} FROM test_source", calculation_string),
    )
    .unwrap()
    {
        if let Some(table) = Table::try_from_statement(&statement, &schema_provider).unwrap() {
            schema_provider.insert_table(table);
        } else {
            inserts.push(Insert::try_from_statement(&statement, &mut schema_provider).unwrap());
        };
    }

    let Insert::Anonymous {
        logical_plan: LogicalPlan::Projection(projection),
    } = inserts.remove(0)
    else {
        panic!("expect projection")
    };
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
