use crate::types::interval_month_day_nanos_to_duration;
use crate::watermark_node::WatermarkNode;
use crate::{
    AggregateCalculation, DataFusionEdge, JoinCalculation, LogicalPlanExtension, WindowBehavior,
};
use anyhow::bail;
use arrow_schema::{DataType, Field, TimeUnit};
use arroyo_datastream::logical::LogicalEdgeType;
use arroyo_datastream::WindowType;
use arroyo_rpc::df::ArroyoSchema;
use datafusion_common::tree_node::{
    RewriteRecursion, TreeNode, TreeNodeRewriter, TreeNodeVisitor, VisitRecursion,
};
use datafusion_common::{
    Column, DFField, DFSchema, DFSchemaRef, DataFusionError, JoinConstraint, JoinType,
    OwnedTableReference, Result as DFResult, ScalarValue, TableReference,
};
use datafusion_expr::expr::{ScalarFunction, WindowFunction};
use datafusion_expr::{Aggregate, BinaryExpr, BuiltInWindowFunction, Case, Expr, Extension, Join, LogicalPlan, Projection, ScalarFunctionDefinition, TableScan, UserDefinedLogicalNode, Window, WindowFunctionDefinition};
use petgraph::graph::{DiGraph, NodeIndex};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use crate::schemas::add_timestamp_field;

#[derive(Default, Debug)]
pub(crate) struct QueryToGraphVisitor {
    pub local_logical_plan_graph: DiGraph<LogicalPlanExtension, DataFusionEdge>,
    pub table_source_to_nodes: HashMap<OwnedTableReference, NodeIndex>,
    pub tables_with_windows: HashSet<OwnedTableReference>,
}

fn get_duration(expression: &Expr) -> anyhow::Result<Duration> {
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

fn find_window(expression: &Expr) -> anyhow::Result<Option<WindowType>> {
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
    table_scans_with_windows: HashSet<OwnedTableReference>,
    has_window: bool,
}

impl WindowDetectingVisitor {
    fn has_window(
        logical_plan: &LogicalPlan,
        table_scans_with_windows: HashSet<OwnedTableReference>,
    ) -> DFResult<bool> {
        info!("checking {:?} for window", logical_plan);
        let mut visitor = WindowDetectingVisitor {
            has_window: false,
            table_scans_with_windows,
        };
        logical_plan.visit(&mut visitor)?;
        Ok(visitor.has_window)
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
                let window_group_expr: Vec<_> = group_expr
                    .iter()
                    .enumerate()
                    .filter_map(|(i, expr)| {
                        find_window(expr)
                            .map(|option| option.map(|inner| (i, inner)))
                            .transpose()
                    })
                    .collect::<anyhow::Result<Vec<_>>>()
                    .map_err(|err| DataFusionError::Plan(err.to_string()))?;
                if !window_group_expr.is_empty() {
                    self.has_window = true;
                    return Ok(VisitRecursion::Stop);
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
                if self.table_scans_with_windows.contains(table_name) {
                    self.has_window = true;
                    return Ok(VisitRecursion::Stop);
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
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
                ..
            }) => self.rewrite_aggregate(&input, group_expr, aggr_expr, schema),
            LogicalPlan::Join(join) => self.rewrite_join(join),
            LogicalPlan::TableScan(table_scan) => self.rewrite_table_scan(table_scan),
            LogicalPlan::Extension(extension) => self.rewrite_extension(extension),
            LogicalPlan::Window(window) => self.rewrite_window(window),
            other => Err(DataFusionError::Plan(format!(
                "Unsupported logical plan: {:?}",
                other
            ))),
        }
    }
}

impl QueryToGraphVisitor {
    fn rewrite_table_scan(&mut self, table_scan: TableScan) -> DFResult<LogicalPlan> {
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

    fn rewrite_aggregate(
        &mut self,
        input: &Arc<LogicalPlan>,
        mut group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        schema: DFSchemaRef,
    ) -> Result<LogicalPlan, DataFusionError> {
        let mut window_group_expr: Vec<_> = group_expr
            .iter()
            .enumerate()
            .filter_map(|(i, expr)| {
                find_window(expr)
                    .map(|option| option.map(|inner| (i, inner)))
                    .transpose()
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;

        if window_group_expr.len() > 1 {
            return Err(DataFusionError::NotImplemented(format!(
                "do not support {} window expressions in group by",
                window_group_expr.len()
            )));
        }

        let mut key_fields = Self::get_key_fields(&mut group_expr, &schema);

        let input_has_window =
            WindowDetectingVisitor::has_window(&input, self.tables_with_windows.clone())?;
        let window_behavior = match (input_has_window, !window_group_expr.is_empty()) {
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

        let key_len = key_fields.len();

        let (key_schema, key_index) = self.insert_key_by(input, &group_expr, &schema, key_fields)?;

        let mut aggregate_input_fields = schema.fields().clone();
        if let WindowBehavior::FromOperator { window_index, .. } = &window_behavior {
            aggregate_input_fields.remove(*window_index);
        }
        // TODO: incorporate the window field in the schema and adjust datafusion.
        //aggregate_input_schema.push(window_field);

        let input_source = crate::create_table_with_timestamp(
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
            key_fields: (0..key_len).collect(),
        };

        let aggregate_index =
            self.local_logical_plan_graph
                .add_node(LogicalPlanExtension::AggregateCalculation(
                    aggregate_calculation,
                ));

        let table_name = format!("{}", aggregate_index.index());
        let keys_without_window = (0..key_len).into_iter().collect();
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

        let schema_with_timestamp = add_timestamp_field(schema)?;

        let table_name = OwnedTableReference::partial("arroyo-virtual", table_name.clone());
        self.tables_with_windows.insert(table_name.clone());
        Ok(LogicalPlan::TableScan(TableScan {
            table_name: table_name.clone(),
            source: crate::create_table_with_timestamp(
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
            projected_schema: schema_with_timestamp,
            filters: vec![],
            fetch: None,
        }))
    }

    fn insert_key_by(&mut self, input: &Arc<LogicalPlan>, group_expr: &Vec<Expr>, schema: &DFSchemaRef, mut key_fields: Vec<DFField>) -> Result<(Arc<DFSchema>, NodeIndex), DataFusionError> {
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
                    key_columns: (0..key_fields.len()).collect(),
                });
        Ok((key_schema, key_index))
    }

    fn get_key_fields(group_expr: &Vec<Expr>, schema: &DFSchemaRef) -> Vec<DFField> {
        schema
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
            .collect()
    }

    fn rewrite_join(&mut self, join: Join) -> Result<LogicalPlan, DataFusionError> {
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
        let JoinType::Inner = join_type else {
            return Err(DataFusionError::NotImplemented(
                "can't handle join type other than inner".into(),
            ));
        };
        let key_count = on.len();
        let (left_expressions, right_expressions): (Vec<_>, Vec<_>) =
            on.clone().into_iter().unzip();
        let right_projection = projection_from_join_keys(right.clone(), right_expressions)?;
        let left_projection = projection_from_join_keys(left.clone(), left_expressions)?;
        let right_schema = right_projection.schema().clone();
        let left_schema = left_projection.schema().clone();
        let left_index =
            self.local_logical_plan_graph
                .add_node(LogicalPlanExtension::KeyCalculation {
                    projection: left_projection,
                    key_columns: (0..key_count).collect(),
                });
        let right_index =
            self.local_logical_plan_graph
                .add_node(LogicalPlanExtension::KeyCalculation {
                    projection: right_projection,
                    key_columns: (0..key_count).collect(),
                });

        let left_table_scan = LogicalPlan::TableScan(TableScan {
            table_name: OwnedTableReference::parse_str("left"),
            source: crate::create_table(
                "left".to_string(),
                Arc::new(left.schema().as_ref().into()),
            ),
            projection: None,
            projected_schema: left.schema().clone(),
            filters: vec![],
            fetch: None,
        });

        let right_table_scan = LogicalPlan::TableScan(TableScan {
            table_name: OwnedTableReference::parse_str("right"),
            source: crate::create_table(
                "right".to_string(),
                Arc::new(right.schema().as_ref().into()),
            ),
            projection: None,
            projected_schema: right.schema().clone(),
            filters: vec![],
            fetch: None,
        });
        let rewritten_join = LogicalPlan::Join(Join {
            left: Arc::new(left_table_scan.clone()),
            right: Arc::new(right_table_scan.clone()),
            on,
            join_type,
            join_constraint: JoinConstraint::On,
            schema: schema.clone(),
            null_equals_null: false,
            filter: filter.clone(),
        });

        let mut schema_with_timestamp = schema.fields().clone();
        // remove fields named _timestamp
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
        let left_field = left.schema().field_with_unqualified_name("_timestamp")?;
        let left_column = Expr::Column(Column {
            relation: left_field.qualifier().cloned(),
            name: left_field.name().to_string(),
        });
        let right_field = right.schema().field_with_unqualified_name("_timestamp")?;
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
        let final_logical_plan = LogicalPlan::Projection(Projection::try_new_with_schema(
            projection_expr,
            Arc::new(rewritten_join),
            output_schema.clone(),
        )?);
        println!("left schema: {:?}", left_schema);
        let left_arrow_schema = Arc::new(left_schema.as_ref().into());
        println!("left arrow schema: {:?}", left_arrow_schema);

        let join_calculation = JoinCalculation {
            join_type,
            left_input_schema: ArroyoSchema::new(
                left_arrow_schema,
                left_schema.fields().len() - 1,
                (0..key_count).collect(),
            ),
            right_input_schema: ArroyoSchema::new(
                Arc::new(right_schema.as_ref().into()),
                right_schema.fields().len() - 1,
                (0..key_count).collect(),
            ),
            output_schema: final_logical_plan.schema().clone(),
            rewritten_join: final_logical_plan,
        };
        let join_index = self
            .local_logical_plan_graph
            .add_node(LogicalPlanExtension::Join(join_calculation));
        let table_name = format!("{}", join_index.index());
        let left_edge = DataFusionEdge::new(
            left_schema.clone(),
            LogicalEdgeType::LeftJoin,
            (0..key_count).collect(),
        )
        .expect("should be able to create edge");
        let right_edge = DataFusionEdge::new(
            right_schema.clone(),
            LogicalEdgeType::RightJoin,
            (0..key_count).collect(),
        )
        .expect("should be able to create edge");
        self.local_logical_plan_graph
            .add_edge(left_index, join_index, left_edge);
        self.local_logical_plan_graph
            .add_edge(right_index, join_index, right_edge);
        Ok(LogicalPlan::TableScan(TableScan {
            table_name: OwnedTableReference::partial("arroyo-virtual", table_name.clone()),
            source: crate::create_table_with_timestamp(
                OwnedTableReference::partial("arroyo-virtual", table_name).to_string(),
                output_schema
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
            projected_schema: output_schema,
            filters: vec![],
            fetch: None,
        }))
    }

    fn rewrite_extension(&mut self, extension: Extension) -> Result<LogicalPlan, DataFusionError> {
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
            source: crate::create_table_with_timestamp(
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

    fn rewrite_window(&mut self, window: Window) -> DFResult<LogicalPlan> {
        let mut finder = SqlWindowFinder{
            window_fn: None,
        };
        for expr in window.window_expr {
            let mut finder = SqlWindowFinder{
                window_fn: None,
            };
            expr.visit(&mut finder)?;
        }

        let window_fn = finder.window_fn
            .ok_or_else(|| DataFusionError::Plan("Window expression doesn't contain a window function".to_string()))?;

        match &window_fn.fun {
            WindowFunctionDefinition::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber) => {} // ok
            fun => {
                return Err(DataFusionError::Plan(format!("{} window function is not yet supported", fun)));
            }
        }

        let key_fields = Self::get_key_fields(&window_fn.partition_by, &window.schema);
        let (key_schema, key_index) = self.insert_key_by(&window.input, &window_fn.partition_by, &window.schema, key_fields)?;



        Ok()
    }
}

struct SqlWindowFinder {
    window_fn: Option<WindowFunction>,
}

impl TreeNodeVisitor for SqlWindowFinder {
    type N = Expr;

    fn pre_visit(&mut self, node: &Self::N) -> DFResult<VisitRecursion> {
        if let Expr::WindowFunction(w) = &node {
            if self.window_fn.is_some() {
                return Err(DataFusionError::Plan("Multiple window functions aren't supported".to_string()));
            }
            self.window_fn = Some(w.clone());
        }

        Ok(VisitRecursion::Continue)
    }
}

fn find_sql_window(expression: &Expr) -> anyhow::Result<Option<WindowType>> {
    match expression {
        _ => Ok(None),
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
