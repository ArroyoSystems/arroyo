#![allow(clippy::new_without_default)]

extern crate dlopen;
#[macro_use]
extern crate dlopen_derive;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{self, DataType, Field};
use arrow_schema::{FieldRef, Schema, TimeUnit};
use arroyo_datastream::{ConnectorOp, WindowType};

use datafusion::datasource::DefaultTableSource;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion_common::{
    Column, DFField, JoinConstraint, JoinType, OwnedTableReference, Result as DFResult, ScalarValue,
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
    RewriteRecursion, TreeNode, TreeNodeRewriter, TreeNodeVisitor, VisitRecursion,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    AccumulatorFactoryFunction, Aggregate, BinaryExpr, Case, Expr, Extension, Join, LogicalPlan,
    Projection, ReturnTypeFunction, ScalarFunctionDefinition, ScalarUDF, Signature,
    StateTypeFunction, TableScan, Volatility, WindowUDF,
};

use datafusion_expr::{AggregateUDF, TableSource};
use logical::LogicalBatchInput;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::IntoNodeReferences;
use plan_graph::Planner;
use schemas::{add_timestamp_field, add_timestamp_field_if_missing_arrow, window_arrow_struct};

use rewriters::SourceRewriter;
use tables::{Insert, Table};

use crate::types::data_type_to_arrow_type;
use arroyo_datastream::logical::DylibUdfConfig;
use arroyo_rpc::api_types::connections::ConnectionProfile;
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError};
use prettyplease::unparse;
use regex::Regex;
use std::collections::HashSet;
use std::fmt::Debug;

use crate::json::get_json_functions;
use crate::rewriters::{TimestampRewriter, UnnestRewriter};
use crate::types::{interval_month_day_nanos_to_duration, rust_to_arrow};
use crate::watermark_node::WatermarkNode;
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalProgram};
use arroyo_operator::connector::Connection;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::TIMESTAMP_FIELD;
use arroyo_types::{dylib_path, NullableType};
use std::time::{Duration, SystemTime};
use std::{collections::HashMap, sync::Arc};
use syn::{parse_file, FnArg, Item, ReturnType, Visibility};
use tracing::{info, warn};
use unicase::UniCase;

const DEFAULT_IDLE_TIME: Option<Duration> = Some(Duration::from_secs(5 * 60));

mod json;
#[cfg(test)]
mod test;

#[allow(unused)]
#[derive(Clone, Debug)]
pub struct UdfDef {
    pub args: Vec<NullableType>,
    pub ret: NullableType,
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
    pub dylib_udfs: HashMap<String, DylibUdfConfig>,
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

            let arg_types = args
                .iter()
                .map(|arg| data_type_to_arrow_type(&arg.data_type))
                .collect();

            let arg_types = match arg_types {
                Ok(arg_types) => arg_types,
                Err(e) => bail!("Error converting arg types: {}", e),
            };

            self.dylib_udfs.insert(
                function.sig.ident.to_string(),
                DylibUdfConfig {
                    dylib_path: dylib_path(&body),
                    arg_types,
                    return_type: data_type_to_arrow_type(&ret.data_type)?,
                },
            );

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

#[derive(Default, Debug)]
pub(crate) struct QueryToGraphVisitor {
    local_logical_plan_graph: DiGraph<LogicalPlanExtension, DataFusionEdge>,
    table_source_to_nodes: HashMap<OwnedTableReference, NodeIndex>,
    tables_with_windows: HashMap<OwnedTableReference, WindowType>,
}

impl QueryToGraphVisitor {
    fn mutate_aggregate(
        &mut self,
        Aggregate {
            input,
            mut group_expr,
            aggr_expr,
            schema,
            ..
        }: Aggregate,
    ) -> DFResult<LogicalPlan> {
        let mut window_group_expr: Vec<_> = group_expr
            .iter()
            .enumerate()
            .filter_map(|(i, expr)| {
                find_window(expr)
                    .map(|option| option.map(|inner| (i, inner)))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;

        if window_group_expr.len() > 1 {
            return Err(datafusion_common::DataFusionError::NotImplemented(format!(
                "do not support {} window expressions in group by",
                window_group_expr.len()
            )));
        }
        let mut key_fields: Vec<DFField> = schema
            .fields()
            .iter()
            .take(group_expr.len())
            .cloned()
            .map(|field| {
                DFField::new(
                    field.qualifier().cloned(),
                    &format!("_key_{}", field.name()),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>();
        let mut window =
            WindowDetectingVisitor::get_window(&input, self.tables_with_windows.clone())?;
        let window_behavior = match (window.is_some(), !window_group_expr.is_empty()) {
            (true, true) => {
                return Err(DataFusionError::NotImplemented(
                    "query has both a window in group by and input is windowed.".to_string(),
                ))
            }
            (true, false) => WindowBehavior::InData,
            (false, true) => {
                // strip out window from group by, will be handled by operator.
                let (window_index, window_type) = window_group_expr.pop().unwrap();
                group_expr.remove(window_index);
                let window_field = key_fields.remove(window_index);
                window = Some(window_type.clone());
                WindowBehavior::FromOperator {
                    window: window_type,
                    window_field,
                    window_index,
                }
            }
            (false, false) => {
                return Err(DataFusionError::NotImplemented(
                    "must have window in aggregate".to_string(),
                ))
            }
        };

        let key_count = key_fields.len();
        key_fields.extend(input.schema().fields().clone());

        let key_schema = Arc::new(DFSchema::new_with_metadata(
            key_fields,
            schema.metadata().clone(),
        )?);

        let mut key_projection_expressions = group_expr.clone();
        key_projection_expressions.extend(
            input
                .schema()
                .fields()
                .iter()
                .map(|field| Expr::Column(Column::new(field.qualifier().cloned(), field.name()))),
        );

        let key_projection =
            LogicalPlan::Projection(datafusion_expr::Projection::try_new_with_schema(
                key_projection_expressions.clone(),
                input.clone(),
                key_schema.clone(),
            )?);

        let key_index =
            self.local_logical_plan_graph
                .add_node(LogicalPlanExtension::KeyCalculation {
                    projection: key_projection,
                    key_columns: (0..key_count).collect(),
                });

        let mut aggregate_input_fields = schema.fields().clone();
        if let WindowBehavior::FromOperator { window_index, .. } = &window_behavior {
            aggregate_input_fields.remove(*window_index);
        }
        // TODO: incorporate the window field in the schema and adjust datafusion.
        //aggregate_input_schema.push(window_field);

        let input_source = create_table_with_timestamp(
            "memory".into(),
            key_schema
                .fields()
                .iter()
                .map(|field| {
                    Arc::new(Field::new(
                        field.name(),
                        field.data_type().clone(),
                        field.is_nullable(),
                    ))
                })
                .collect(),
        );
        let mut df_fields = key_schema.fields().clone();
        if !df_fields
            .iter()
            .any(|field: &DFField| field.name() == "_timestamp")
        {
            df_fields.push(DFField::new_unqualified(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ));
        }
        let input_df_schema = Arc::new(DFSchema::new_with_metadata(df_fields, HashMap::new())?);

        let input_table_scan = LogicalPlan::TableScan(TableScan {
            table_name: OwnedTableReference::parse_str("memory"),
            source: input_source,
            projection: None,
            projected_schema: input_df_schema.clone(),
            filters: vec![],
            fetch: None,
        });

        let aggregate_calculation = AggregateCalculation {
            window_behavior,
            aggregate: Aggregate::try_new_with_schema(
                Arc::new(input_table_scan),
                group_expr,
                aggr_expr,
                Arc::new(DFSchema::new_with_metadata(
                    aggregate_input_fields,
                    schema.metadata().clone(),
                )?),
            )?,
            key_fields: (0..key_count).collect(),
        };

        let aggregate_index =
            self.local_logical_plan_graph
                .add_node(LogicalPlanExtension::AggregateCalculation(
                    aggregate_calculation,
                ));

        let table_name = format!("{}", aggregate_index.index());
        let keys_without_window = (0..key_count).into_iter().collect();
        self.local_logical_plan_graph.add_edge(
            key_index,
            aggregate_index,
            DataFusionEdge::new(
                input_df_schema,
                LogicalEdgeType::Shuffle,
                keys_without_window,
            )
            .unwrap(),
        );
        let mut schema_with_timestamp = schema.fields().clone();
        if !schema_with_timestamp
            .iter()
            .any(|field| field.name() == "_timestamp")
        {
            schema_with_timestamp.push(DFField::new_unqualified(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ));
        }
        let table_name = OwnedTableReference::partial("arroyo-virtual", table_name.clone());
        self.tables_with_windows
            .insert(table_name.clone(), window.unwrap());
        Ok(LogicalPlan::TableScan(TableScan {
            table_name: table_name.clone(),
            source: create_table_with_timestamp(
                table_name.to_string(),
                schema
                    .fields()
                    .iter()
                    .map(|field| {
                        Arc::new(Field::new(
                            field.name(),
                            field.data_type().clone(),
                            field.is_nullable(),
                        ))
                    })
                    .collect(),
            ),
            projection: None,
            projected_schema: Arc::new(DFSchema::new_with_metadata(
                schema_with_timestamp,
                HashMap::new(),
            )?),
            filters: vec![],
            fetch: None,
        }))
    }

    fn check_join_windowing(&mut self, join: &Join) -> DFResult<bool> {
        let left_window =
            WindowDetectingVisitor::get_window(&join.left, self.tables_with_windows.clone())?;
        let right_window =
            WindowDetectingVisitor::get_window(&join.right, self.tables_with_windows.clone())?;
        match (left_window, right_window) {
            (None, None) => {
                if join.join_type == JoinType::Inner {
                    Ok(false)
                } else {
                    Err(DataFusionError::NotImplemented(
                        "can't handle non-inner joins without windows".into(),
                    ))
                }
            }
            (None, Some(_)) => Err(DataFusionError::NotImplemented(
                "can't handle mixed windowing between left (non-windowed) and right (windowed)."
                    .into(),
            )),
            (Some(_), None) => Err(DataFusionError::NotImplemented(
                "can't handle mixed windowing between left (windowed) and right (non-windowed)."
                    .into(),
            )),
            (Some(left_window), Some(right_window)) => {
                if left_window != right_window {
                    return Err(DataFusionError::NotImplemented(
                        "can't handle mixed windowing between left and right".into(),
                    ));
                }
                // exclude session windows
                if let WindowType::Session { .. } = left_window {
                    return Err(DataFusionError::NotImplemented(
                        "can't handle session windows in joins".into(),
                    ));
                }

                Ok(true)
            }
        }
    }

    fn mutate_join(&mut self, join: Join) -> DFResult<LogicalPlan> {
        let is_instant = self.check_join_windowing(&join)?;
        let Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint: JoinConstraint::On,
            schema,
            null_equals_null: false,
        } = join
        else {
            return Err(DataFusionError::NotImplemented(
                "can't handle join constraint other than ON".into(),
            ));
        };

        info!("On condition: {:?}", on);

        let (left_expressions, right_expressions): (Vec<_>, Vec<_>) =
            on.clone().into_iter().unzip();
        let (left_index, left_table_scan) =
            self.create_join_input(left, left_expressions, "left")?;
        let (right_index, right_table_scan) =
            self.create_join_input(right, right_expressions, "right")?;
        let rewritten_join = LogicalPlan::Join(Join {
            left: Arc::new(left_table_scan.clone()),
            right: Arc::new(right_table_scan.clone()),
            on,
            join_type,
            join_constraint: JoinConstraint::On,
            schema: schema.clone(),
            null_equals_null: false,
            filter,
        });

        let final_logical_plan = self.post_join_timestamp_projection(rewritten_join)?;
        let left_edge = self
            .local_logical_plan_graph
            .node_weight(left_index)
            .unwrap()
            .outgoing_edge();
        let right_edge = self
            .local_logical_plan_graph
            .node_weight(right_index)
            .unwrap()
            .outgoing_edge();
        let final_schema = final_logical_plan.schema().clone();

        let join_calculation = JoinCalculation {
            join_type,
            left_input_schema: left_edge.arroyo_schema(),
            right_input_schema: right_edge.arroyo_schema(),
            output_schema: final_schema.clone(),
            rewritten_join: final_logical_plan,
            is_instant,
        };
        let join_index = self
            .local_logical_plan_graph
            .add_node(LogicalPlanExtension::Join(join_calculation));
        self.local_logical_plan_graph
            .add_edge(left_index, join_index, left_edge);
        self.local_logical_plan_graph
            .add_edge(right_index, join_index, right_edge);
        let table_name = format!("{}", join_index.index());
        Ok(LogicalPlan::TableScan(TableScan {
            table_name: OwnedTableReference::partial("arroyo-virtual", table_name.clone()),
            source: create_table_with_timestamp(
                OwnedTableReference::partial("arroyo-virtual", table_name).to_string(),
                final_schema
                    .fields()
                    .iter()
                    .map(|field| {
                        Arc::new(Field::new(
                            field.name(),
                            field.data_type().clone(),
                            field.is_nullable(),
                        ))
                    })
                    .collect(),
            ),
            projection: None,
            projected_schema: final_schema,
            filters: vec![],
            fetch: None,
        }))
    }

    fn create_join_input(
        &mut self,
        input: Arc<LogicalPlan>,
        join_expressions: Vec<Expr>,
        name: &'static str,
    ) -> DFResult<(NodeIndex, LogicalPlan)> {
        let key_count = join_expressions.len();
        let projection = projection_from_join_keys(input.clone(), join_expressions)?;
        let index = self
            .local_logical_plan_graph
            .add_node(LogicalPlanExtension::KeyCalculation {
                projection,
                key_columns: (0..key_count).collect(),
            });
        let table_scan_plan = LogicalPlan::TableScan(TableScan {
            table_name: OwnedTableReference::parse_str(&name),
            source: create_table(
                name.to_string().to_string(),
                Arc::new(input.schema().as_ref().into()),
            ),
            projection: None,
            projected_schema: input.schema().clone(),
            filters: vec![],
            fetch: None,
        });

        Ok((index, table_scan_plan))
    }

    fn post_join_timestamp_projection(&mut self, input: LogicalPlan) -> DFResult<LogicalPlan> {
        let schema = input.schema().clone();
        let mut schema_with_timestamp = schema.fields().clone();
        let timestamp_fields = schema_with_timestamp
            .iter()
            .filter(|field| field.name() == "_timestamp")
            .cloned()
            .collect::<Vec<_>>();
        if timestamp_fields.len() != 2 {
            return Err(DataFusionError::NotImplemented(
                "join must have two timestamp fields".to_string(),
            ));
        }
        schema_with_timestamp.retain(|field| field.name() != "_timestamp");
        let mut projection_expr = schema_with_timestamp
            .iter()
            .map(|field| {
                Expr::Column(Column {
                    relation: field.qualifier().cloned(),
                    name: field.name().to_string(),
                })
            })
            .collect::<Vec<_>>();
        // add a _timestamp field to the schema
        schema_with_timestamp.push(DFField::new_unqualified(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ));

        let output_schema = Arc::new(DFSchema::new_with_metadata(
            schema_with_timestamp,
            schema.metadata().clone(),
        )?);
        // then take a max of the two timestamp columns
        let left_field = &timestamp_fields[0];
        let left_column = Expr::Column(Column {
            relation: left_field.qualifier().cloned(),
            name: left_field.name().to_string(),
        });
        let right_field = &timestamp_fields[1];
        let right_column = Expr::Column(Column {
            relation: right_field.qualifier().cloned(),
            name: right_field.name().to_string(),
        });
        let max_timestamp = Expr::Case(Case {
            expr: Some(Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left_column.clone()),
                op: datafusion_expr::Operator::GtEq,
                right: Box::new(right_column.clone()),
            }))),
            when_then_expr: vec![
                (
                    Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)))),
                    Box::new(left_column.clone()),
                ),
                (
                    Box::new(Expr::Literal(ScalarValue::Boolean(Some(false)))),
                    Box::new(right_column.clone()),
                ),
            ],
            else_expr: None,
        });

        projection_expr.push(max_timestamp);
        Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
            projection_expr,
            Arc::new(input),
            output_schema.clone(),
        )?))
    }
    fn mutate_table_scan(&mut self, table_scan: TableScan) -> DFResult<LogicalPlan> {
        if table_scan.projection.is_some() {
            return Err(DataFusionError::Internal(
                "Unexpected projection in table scan".to_string(),
            ));
        }

        let node_index = match self.table_source_to_nodes.get(&table_scan.table_name) {
            Some(node_index) => *node_index,
            None => {
                let original_name = table_scan.table_name.clone();

                let index =
                    self.local_logical_plan_graph
                        .add_node(LogicalPlanExtension::TableScan(LogicalPlan::TableScan(
                            table_scan.clone(),
                        )));
                let Some(LogicalPlanExtension::TableScan(LogicalPlan::TableScan(TableScan {
                    table_name,
                    ..
                }))) = self.local_logical_plan_graph.node_weight_mut(index)
                else {
                    return Err(DataFusionError::Internal("expect a value node".to_string()));
                };
                *table_name =
                    TableReference::partial("arroyo-virtual", format!("{}", index.index()));
                self.table_source_to_nodes.insert(original_name, index);
                index
            }
        };
        let Some(LogicalPlanExtension::TableScan(interred_plan)) =
            self.local_logical_plan_graph.node_weight(node_index)
        else {
            return Err(DataFusionError::Internal("expect a value node".to_string()));
        };
        Ok(interred_plan.clone())
    }
    fn mutate_extension(&mut self, extension: Extension) -> DFResult<LogicalPlan> {
        let watermark_node = extension
            .node
            .as_any()
            .downcast_ref::<WatermarkNode>()
            .unwrap()
            .clone();

        let index = self
            .local_logical_plan_graph
            .add_node(LogicalPlanExtension::WatermarkNode(watermark_node.clone()));

        let input = LogicalPlanExtension::ValueCalculation(watermark_node.input);
        let edge = input.outgoing_edge();
        let input_index = self.local_logical_plan_graph.add_node(input);
        self.local_logical_plan_graph
            .add_edge(input_index, index, edge);

        let table_name = format!("{}", index.index());

        Ok(LogicalPlan::TableScan(TableScan {
            table_name: OwnedTableReference::partial("arroyo-virtual", table_name.clone()),
            source: create_table_with_timestamp(
                OwnedTableReference::partial("arroyo-virtual", table_name).to_string(),
                watermark_node
                    .schema
                    .fields()
                    .iter()
                    .map(|field| {
                        Arc::new(Field::new(
                            field.name(),
                            field.data_type().clone(),
                            field.is_nullable(),
                        ))
                    })
                    .collect(),
            ),
            projection: None,
            projected_schema: watermark_node.schema.clone(),
            filters: vec![],
            fetch: None,
        }))
    }
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

                let output_schema = add_timestamp_field(
                    Arc::new(
                        DFSchema::new_with_metadata(fields, aggregate_schema.metadata().clone())
                            .unwrap(),
                    ),
                    None,
                )
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
    is_instant: bool,
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

    pub fn arroyo_schema(&self) -> ArroyoSchema {
        ArroyoSchema {
            schema: Arc::new(self.schema.as_ref().into()),
            timestamp_index: self.timestamp_index,
            key_indices: self.key_indices.clone(),
        }
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

struct WindowDetectingVisitor {
    table_scans_with_windows: HashMap<OwnedTableReference, WindowType>,
    window: Option<WindowType>,
}

impl WindowDetectingVisitor {
    fn get_window(
        logical_plan: &LogicalPlan,
        table_scans_with_windows: HashMap<OwnedTableReference, WindowType>,
    ) -> DFResult<Option<WindowType>> {
        info!("checking {:?} for window", logical_plan);
        let mut visitor = WindowDetectingVisitor {
            window: None,
            table_scans_with_windows,
        };
        logical_plan.visit(&mut visitor)?;
        Ok(visitor.window.take())
    }
}

impl TreeNodeVisitor for WindowDetectingVisitor {
    type N = LogicalPlan;

    fn pre_visit(&mut self, node: &Self::N) -> DFResult<VisitRecursion> {
        match node {
            LogicalPlan::Aggregate(Aggregate {
                input: _,
                group_expr,
                aggr_expr: _,
                schema: _,
                ..
            }) => {
                info!("group_expr: {:?}", group_expr);
                let window_expressions = group_expr
                    .iter()
                    .filter_map(|expr| {
                        find_window(expr)
                            .map_err(|err| DataFusionError::Plan(err.to_string()))
                            .transpose()
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                for window in window_expressions {
                    // if there's already a window they should match
                    if let Some(existing_window) = &self.window {
                        if *existing_window != window {
                            return Err(DataFusionError::Plan(
                                "window expressions do not match".to_string(),
                            ));
                        }
                    } else {
                        self.window = Some(window);
                    }
                }
            }
            LogicalPlan::TableScan(TableScan {
                table_name,
                source: _,
                projection: _,
                projected_schema: _,
                filters: _,
                fetch: _,
            }) => {
                if let Some(window) = self.table_scans_with_windows.get(table_name) {
                    if let Some(existing_window) = &self.window {
                        if *existing_window != *window {
                            return Err(DataFusionError::Plan(
                                "window expressions do not match".to_string(),
                            ));
                        }
                    } else {
                        self.window = Some(window.clone());
                    }
                }
            }
            _ => {}
        }
        Ok(VisitRecursion::Continue)
    }
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
            LogicalPlan::Aggregate(aggregate) => self.mutate_aggregate(aggregate),
            LogicalPlan::Join(join) => self.mutate_join(join),
            LogicalPlan::TableScan(table_scan) => self.mutate_table_scan(table_scan),
            LogicalPlan::Extension(extension) => self.mutate_extension(extension),
            other => Ok(other),
        }
    }
}

fn projection_from_join_keys(
    input: Arc<LogicalPlan>,
    mut join_expressions: Vec<Expr>,
) -> DFResult<LogicalPlan> {
    let key_count = join_expressions.len();
    join_expressions.extend(
        input
            .schema()
            .fields()
            .iter()
            .map(|field| Expr::Column(Column::new(field.qualifier().cloned(), field.name()))),
    );
    // Calculate initial projection with default names
    let mut projection = Projection::try_new(join_expressions, input)?;
    let fields = projection
        .schema
        .fields()
        .into_iter()
        .enumerate()
        .map(|(index, field)| {
            // rename to avoid collisions
            if index < key_count {
                DFField::new(
                    field.qualifier().cloned(),
                    &format!("_key_{}", field.name()),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            } else {
                field.clone()
            }
        });
    let rewritten_schema = Arc::new(DFSchema::new_with_metadata(
        fields.collect(),
        projection.schema.metadata().clone(),
    )?);
    projection.schema = rewritten_schema;
    Ok(LogicalPlan::Projection(projection))
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

        info!("Logical plan: {}", plan_rewrite.display_graphviz());

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
