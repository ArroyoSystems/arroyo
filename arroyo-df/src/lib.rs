#![allow(clippy::new_without_default)]
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{self, DataType, Field};
use arrow_schema::{FieldRef, Schema};
use arroyo_datastream::{ConnectorOp, WindowType};

use datafusion::datasource::DefaultTableSource;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion_common::{
    DFField, JoinType,
};
pub mod external;
pub mod logical;
pub mod physical;
mod plan_graph;
mod rewriters;
pub mod schemas;
mod tables;
pub mod types;
mod watermark_node;

use datafusion::prelude::create_udf;

use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::{planner::ContextProvider, TableReference};

use datafusion_common::tree_node::{
    TreeNode, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion_expr::{
    AccumulatorFactoryFunction, Aggregate, LogicalPlan,
    ReturnTypeFunction, ScalarUDF, Signature, StateTypeFunction
    , Volatility, WindowUDF,
};

use datafusion_expr::{AggregateUDF, TableSource};
use logical::LogicalBatchInput;
use petgraph::graph::NodeIndex;
use petgraph::visit::IntoNodeReferences;
use plan_graph::Planner;
use schemas::{add_timestamp_field, add_timestamp_field_if_missing_arrow, window_arrow_struct};

use rewriters::SourceRewriter;
use tables::{Insert, Table};

use arroyo_rpc::api_types::connections::ConnectionProfile;
use datafusion_common::{DataFusionError, DFSchema, DFSchemaRef, Result as DFResult};
use prettyplease::unparse;
use regex::Regex;
use std::collections::HashSet;
use std::fmt::Debug;

use crate::json::get_json_functions;
use crate::rewriters::{TimestampRewriter, UnnestRewriter};
use crate::types::{NullableType, rust_to_arrow};
use crate::watermark_node::WatermarkNode;
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalProgram};
use arroyo_operator::connector::Connection;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::TIMESTAMP_FIELD;
use std::time::{Duration, SystemTime};
use std::{collections::HashMap, sync::Arc};
use syn::{FnArg, Item, parse_file, ReturnType, Visibility};
use tracing::warn;
use unicase::UniCase;
use graph_rewriter::{QueryToGraphVisitor};

const DEFAULT_IDLE_TIME: Option<Duration> = Some(Duration::from_secs(5 * 60));

mod json;
#[cfg(test)]
mod test;
mod graph_rewriter;

#[allow(unused)]
#[derive(Clone, Debug)]
pub struct UdfDef {
    args: Vec<NullableType>,
    ret: NullableType,
    def: String,
    dependencies: String,
}

#[derive(Clone, Debug)]
pub struct CompiledSql {
    pub program: LogicalProgram,
    pub connection_ids: Vec<i64>,
    pub schemas: HashMap<String, Schema>,
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
                #[allow(deprecated)]
                ScalarUDF::new(
                    "unnest",
                    // This is marked volatile so that DF doesn't try to optimize constants
                    &Signature::any(1, Volatility::Volatile),
                    &return_type,
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
                        args.push(rust_to_arrow(&vec_type).ok_or_else(|| {
                                anyhow!(
                                    "Could not convert function {} inner vector arg {} into a SQL data type",
                                    name,
                                    i
                                )
                            })?);
                    } else {
                        args.push(rust_to_arrow(&*t.ty).ok_or_else(|| {
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
            ReturnType::Type(_, t) => rust_to_arrow(&**t).ok_or_else(|| {
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
            let return_type = Arc::new(ret.data_type.clone());
            let name = function.sig.ident.to_string();
            let signature = Signature::exact(
                args.iter().map(|t| t.data_type.clone()).collect(),
                Volatility::Volatile,
            );
            let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
            let accumulator: AccumulatorFactoryFunction = Arc::new(|_| unreachable!());
            let state_type: StateTypeFunction = Arc::new(|_| unreachable!());
            #[allow(deprecated)]
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
                        args.iter().map(|t| t.data_type.clone()).collect(),
                        Arc::new(ret.data_type.clone()),
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
                dependencies: parse_dependencies(body)?,
            },
        );

        Ok(name)
    }

    fn get_table_source_with_fields(
        &self,
        name: &str,
        fields: Vec<Field>,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let table = self
            .get_table(name)
            .ok_or_else(|| DataFusionError::Plan(format!("Table {} not found", name)))?;

        let fields = table
            .get_fields()
            .iter()
            .filter_map(|field| {
                if fields.contains(field) {
                    Some(field.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let schema = Arc::new(Schema::new_with_metadata(fields, HashMap::new()));
        Ok(create_table(name.to_string(), schema))
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

fn create_table_with_timestamp(table_name: String, fields: Vec<FieldRef>) -> Arc<dyn TableSource> {
    let schema = add_timestamp_field_if_missing_arrow(Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::new(),
    )));
    create_table(table_name, schema)
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

#[derive(Debug)]
enum LogicalPlanExtension {
    TableScan(LogicalPlan),
    ValueCalculation(LogicalPlan),
    KeyCalculation {
        projection: LogicalPlan,
        key_columns: Vec<usize>,
    },
    AggregateCalculation(AggregateCalculation),
    Sink {
        #[allow(unused)]
        name: String,
        connector_op: ConnectorOp,
    },
    WatermarkNode(WatermarkNode),
    Join(JoinCalculation),
    SqlWindow(LogicalPlan),
}

impl LogicalPlanExtension {
    // used for finding input TableScans, if the variant already manually crafts its edges, return None.
    fn inner_logical_plan(&self) -> Option<&LogicalPlan> {
        match self {
            LogicalPlanExtension::TableScan(inner_plan)
            | LogicalPlanExtension::ValueCalculation(inner_plan)
            | LogicalPlanExtension::KeyCalculation {
                projection: inner_plan,
                key_columns: _,
            } => Some(inner_plan),
            LogicalPlanExtension::AggregateCalculation(_) => None,
            LogicalPlanExtension::Sink { .. } => None,
            LogicalPlanExtension::WatermarkNode(_) => None,
            LogicalPlanExtension::Join(_) => None,
            LogicalPlanExtension::SqlWindow(plan) => {}
        }
    }

    fn outgoing_edge(&self) -> DataFusionEdge {
        match self {
            LogicalPlanExtension::TableScan(logical_plan)
            | LogicalPlanExtension::ValueCalculation(logical_plan) => DataFusionEdge::new(
                logical_plan.schema().clone(),
                LogicalEdgeType::Forward,
                vec![],
            )
            .unwrap(),
            LogicalPlanExtension::KeyCalculation {
                projection: logical_plan,
                key_columns,
            } => DataFusionEdge::new(
                logical_plan.schema().clone(),
                LogicalEdgeType::Forward,
                key_columns.clone(),
            )
            .unwrap(),
            LogicalPlanExtension::AggregateCalculation(aggregate_calculation) => {
                let aggregate_schema = aggregate_calculation.aggregate.schema.clone();
                let mut fields = aggregate_schema.fields().clone();

                if let WindowBehavior::FromOperator {
                    window: _,
                    window_field,
                    window_index,
                } = &aggregate_calculation.window_behavior
                {
                    fields.insert(*window_index, window_field.clone());
                }

                let output_schema = add_timestamp_field(Arc::new(
                    DFSchema::new_with_metadata(fields, aggregate_schema.metadata().clone())
                        .unwrap(),
                ))
                .unwrap();

                DataFusionEdge::new(output_schema, LogicalEdgeType::Forward, vec![]).unwrap()
            }
            LogicalPlanExtension::WatermarkNode(n) => {
                DataFusionEdge::new(n.schema.clone(), LogicalEdgeType::Forward, vec![]).unwrap()
            }
            LogicalPlanExtension::Sink { .. } => unreachable!(),
            LogicalPlanExtension::Join(join) => {
                DataFusionEdge::new(join.output_schema.clone(), LogicalEdgeType::Forward, vec![])
                    .unwrap()
            }
        }
    }
}

struct AggregateCalculation {
    window_behavior: WindowBehavior,
    aggregate: Aggregate,
    key_fields: Vec<usize>,
}
#[derive(Debug)]
enum WindowBehavior {
    FromOperator {
        window: WindowType,
        window_field: DFField,
        window_index: usize,
    },
    InData,
}

impl Debug for AggregateCalculation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let logical_plan = LogicalPlan::Aggregate(self.aggregate.clone());
        f.debug_struct("AggregateCalculation")
            .field("window", &self.window_behavior)
            .field("aggregate", &logical_plan)
            .finish()
    }
}
struct JoinCalculation {
    join_type: JoinType,
    left_input_schema: ArroyoSchema,
    right_input_schema: ArroyoSchema,
    output_schema: DFSchemaRef,
    rewritten_join: LogicalPlan,
}

impl Debug for JoinCalculation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinCalculation")
            .field("join_type", &self.join_type)
            .field("left", &self.left_input_schema)
            .field("right", &self.right_input_schema)
            .finish()
    }
}

#[derive(Debug)]
struct DataFusionEdge {
    schema: DFSchemaRef,
    edge_type: LogicalEdgeType,
    timestamp_index: usize,
    key_indices: Vec<usize>,
}

impl DataFusionEdge {
    pub fn new(
        schema: DFSchemaRef,
        edge_type: LogicalEdgeType,
        key_indices: Vec<usize>,
    ) -> anyhow::Result<Self> {
        let Some(timestamp_index) = schema.index_of_column_by_name(None, TIMESTAMP_FIELD)? else {
            bail!("no timestamp field found in schema: {:?}", schema)
        };

        Ok(DataFusionEdge {
            schema,
            edge_type,
            timestamp_index,
            key_indices,
        })
    }
}

impl From<&DataFusionEdge> for LogicalEdge {
    fn from(value: &DataFusionEdge) -> Self {
        let schema = ArroyoSchema {
            schema: Arc::new(Schema::from(&*value.schema)),
            timestamp_index: value.timestamp_index,
            key_indices: value.key_indices.clone(),
        };

        LogicalEdge {
            edge_type: value.edge_type,
            schema,
            projection: None,
        }
    }
}

#[derive(Default)]
pub struct TableScanFinder {
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

    let mut rewriter = QueryToGraphVisitor::default();
    for insert in inserts {
        let (plan, sink_name) = match insert {
            // TODO: implement inserts
            Insert::InsertQuery {
                sink_name,
                logical_plan,
            } => (logical_plan, Some(sink_name)),
            Insert::Anonymous { logical_plan } => (logical_plan, None),
        };

        let plan_rewrite = plan
            .rewrite(&mut SourceRewriter {
                schema_provider: schema_provider.clone(),
            })?
            .rewrite(&mut UnnestRewriter {})?
            .rewrite(&mut TimestampRewriter {})?
            .rewrite(&mut rewriter)?;

        println!("Logical plan: {}", plan_rewrite.display_graphviz());

        for (original_name, index) in &rewriter.table_source_to_nodes {
            let node = rewriter
                .local_logical_plan_graph
                .node_weight(*index)
                .unwrap();
            if let Some(logical_plan) = node.inner_logical_plan() {
                if let LogicalPlan::TableScan(table_scan) = logical_plan {
                    let table = schema_provider
                        .tables
                        .get(&UniCase::new(original_name.to_string()))
                        .unwrap();
                    schema_provider.tables.insert(
                        UniCase::new(table_scan.table_name.to_string()),
                        table.clone(),
                    );
                }
            }
        }

        let extended_plan_node = LogicalPlanExtension::ValueCalculation(plan_rewrite);
        let edge = extended_plan_node.outgoing_edge();

        let plan_index = rewriter
            .local_logical_plan_graph
            .add_node(extended_plan_node);

        let sink = match sink_name {
            Some(sink_name) => {
                let table = schema_provider
                    .get_table(&sink_name)
                    .ok_or_else(|| anyhow!("Connection {} not found", sink_name))?;
                let Table::ConnectorTable(connector_table) = table else {
                    bail!("expected connector table");
                };

                LogicalPlanExtension::Sink {
                    name: sink_name,
                    connector_op: ConnectorOp {
                        connector: connector_table.connector.clone(),
                        config: connector_table.config.clone(),
                        description: connector_table.description.clone(),
                    },
                }
            }
            None => LogicalPlanExtension::Sink {
                name: "preview".to_string(),
                connector_op: arroyo_datastream::ConnectorOp::web_sink(),
            },
        };

        let sink_index = rewriter.local_logical_plan_graph.add_node(sink);

        rewriter
            .local_logical_plan_graph
            .add_edge(plan_index, sink_index, edge);

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
    let planner = Planner::new(schema_provider);
    planner.get_arrow_program(rewriter).await
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
