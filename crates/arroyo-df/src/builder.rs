use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{bail, Result};
use arrow::datatypes::IntervalMonthDayNanoType;

use arroyo_datastream::logical::{
    LogicalEdge, LogicalEdgeType, LogicalGraph, LogicalNode, OperatorName,
};
use arroyo_datastream::WindowType;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::api::{
    ConnectorOp, ExpressionWatermarkConfig, JoinOperator, KeyPlanOperator,
    SessionWindowAggregateOperator, SlidingWindowAggregateOperator,
    TumblingWindowAggregateOperator, ValuePlanOperator,
};

use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::tree_node::{TreeNode, TreeNodeVisitor, VisitRecursion};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, DataFusionError, OwnedTableReference, Result as DFResult,
    ScalarValue,
};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    Aggregate, BuiltinScalarFunction, Expr, Extension, LogicalPlan, UserDefinedLogicalNode,
};
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use petgraph::graph::{DiGraph, NodeIndex};
use prost::Message;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tracing::info;

use crate::physical::{new_registry, ArroyoMemExec, ArroyoPhysicalExtensionCodec, DecodingContext};
use crate::plan::{
    AggregateExtension, JoinExtension, KeyCalculationExtension, RemoteTableExtension,
    SinkExtension, TableSourceExtension,
};
use crate::schemas::add_timestamp_field_arrow;
use crate::watermark_node::WatermarkNode;
use crate::WindowBehavior;
use datafusion_proto::{
    physical_plan::AsExecutionPlan,
    protobuf::{physical_plan_node::PhysicalPlanType, AggregateMode},
};

pub(crate) struct PlanToGraphVisitor {
    graph: DiGraph<LogicalNode, LogicalEdge>,
    output_schemas: HashMap<NodeIndex, ArroyoSchema>,
    named_nodes: HashMap<OwnedTableReference, NodeIndex>,
    // each node that needs to know its inputs should push an empty vec in pre_visit.
    // In post_visit each node should cleanup its vec and push its index to the last vec, if present.
    traversal: Vec<Vec<NodeIndex>>,
    planner: DefaultPhysicalPlanner,
    session_state: SessionState,
}

impl Default for PlanToGraphVisitor {
    fn default() -> Self {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            ArroyoExtensionPlanner {},
        )]);
        let mut config = SessionConfig::new();
        config
            .options_mut()
            .optimizer
            .enable_round_robin_repartition = false;
        config.options_mut().optimizer.repartition_aggregations = false;
        let session_state =
            SessionState::new_with_config_rt(config, Arc::new(RuntimeEnv::default()))
                .with_physical_optimizer_rules(vec![]);
        Self {
            graph: DiGraph::new(),
            output_schemas: HashMap::new(),
            named_nodes: HashMap::new(),
            traversal: vec![],
            planner,
            session_state,
        }
    }
}

struct ArroyoExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for ArroyoExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let schema = node.schema().as_ref().into();
        let name =
            if let Some(key_extension) = node.as_any().downcast_ref::<KeyCalculationExtension>() {
                key_extension.name.clone()
            } else {
                None
            };
        Ok(Some(Arc::new(ArroyoMemExec {
            table_name: name.unwrap_or("memory".to_string()),
            schema: Arc::new(schema),
        })))
    }
}

impl PlanToGraphVisitor {
    fn add_index_to_traversal(&mut self, index: NodeIndex) {
        if let Some(last) = self.traversal.last_mut() {
            last.push(index);
        }
    }

    pub(crate) fn add_plan(&mut self, plan: LogicalPlan) -> DFResult<()> {
        self.traversal.clear();
        plan.visit(self)?;
        Ok(())
    }

    pub fn into_graph(self) -> LogicalGraph {
        self.graph
    }

    pub fn build_source(&mut self, table_source: &TableSourceExtension) -> DFResult<()> {
        if let Some(table_index) = self.named_nodes.get(&table_source.name) {
            self.add_index_to_traversal(*table_index);
            return Ok(());
        }

        let sql_source = table_source.table.as_sql_source().map_err(|e| {
            DataFusionError::Plan(format!("Error turning table into a SQL source: {}", e))
        })?;
        let node = LogicalNode {
            operator_id: format!("source_{}_{}", table_source.name, self.graph.node_count()),
            description: sql_source.source.config.description.clone(),
            operator_name: OperatorName::ConnectorSource,
            operator_config: ConnectorOp::from(sql_source.source.config).encode_to_vec(),
            parallelism: 1,
        };
        let node_index = self.graph.add_node(node);
        self.named_nodes
            .insert(table_source.name.clone(), node_index);
        let schema = Arc::new(table_source.schema.as_ref().into());
        self.output_schemas.insert(
            node_index,
            ArroyoSchema::from_schema_keys(schema, vec![]).unwrap(),
        );
        self.add_index_to_traversal(node_index);
        Ok(())
    }

    pub fn build_watermark(
        &mut self,
        input_nodes: Vec<NodeIndex>,
        watermark_node: &WatermarkNode,
    ) -> DFResult<()> {
        let expression = self.planner.create_physical_expr(
            &watermark_node.watermark_expression,
            &watermark_node.schema,
            &self.session_state,
        )?;

        let expression = PhysicalExprNode::try_from(expression)?;

        let node_index = self.graph.add_node(LogicalNode {
            operator_id: format!("watermark_{}", self.graph.node_count()),
            description: "watermark".to_string(),
            operator_name: OperatorName::ExpressionWatermark,
            parallelism: 1,
            operator_config: ExpressionWatermarkConfig {
                period_micros: 1_000_000,
                idle_time_micros: None,
                expression: expression.encode_to_vec(),
                input_schema: Some(watermark_node.arroyo_schema().try_into().unwrap()),
            }
            .encode_to_vec(),
        });
        self.add_index_to_traversal(node_index);
        let input_index = input_nodes
            .first()
            .ok_or_else(|| DataFusionError::Plan("WatermarkNode must have an input".to_string()))?;
        let input_schema = self
            .output_schemas
            .get(input_index)
            .ok_or_else(|| DataFusionError::Plan("missing input node".to_string()))?;
        self.graph.add_edge(
            *input_index,
            node_index,
            LogicalEdge::project_all(LogicalEdgeType::Forward, input_schema.clone()),
        );
        self.output_schemas
            .insert(node_index, watermark_node.arroyo_schema());
        Ok(())
    }

    pub fn build_sink(
        &mut self,
        input_nodes: Vec<NodeIndex>,
        sink_node: &SinkExtension,
    ) -> DFResult<()> {
        let operator_config = ConnectorOp::from(sink_node.table.connector_op().map_err(|e| {
            DataFusionError::Plan(format!("failed to calculate connector op error: {}", e))
        })?)
        .encode_to_vec();
        let node_index = self.graph.add_node(LogicalNode {
            operator_id: format!("sink_{}", self.graph.node_count()),
            description: sink_node.table.connector_op().unwrap().description.clone(),
            operator_name: OperatorName::ConnectorSink,
            parallelism: 1,
            operator_config,
        });
        self.add_index_to_traversal(node_index);
        let input_index = input_nodes
            .first()
            .ok_or_else(|| DataFusionError::Plan("SinkNode must have an input".to_string()))?;
        let input_schema = self
            .output_schemas
            .get(input_index)
            .ok_or_else(|| DataFusionError::Plan("missing input node".to_string()))?;
        self.graph.add_edge(
            *input_index,
            node_index,
            LogicalEdge::project_all(LogicalEdgeType::Forward, input_schema.clone()),
        );
        Ok(())
    }

    pub fn build_value_node(
        &mut self,
        input_nodes: Vec<NodeIndex>,
        value_node: &LogicalPlan,
    ) -> DFResult<NodeIndex> {
        let physical_plan = self.sync_plan(&value_node)?;
        let physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            physical_plan,
            &ArroyoPhysicalExtensionCodec::default(),
        )?;
        let config = ValuePlanOperator {
            name: "value".into(),
            physical_plan: physical_plan_node.encode_to_vec(),
        };
        let node_index = self.graph.add_node(LogicalNode {
            operator_id: format!("value_{}", self.graph.node_count()),
            description: "value".to_string(),
            operator_name: OperatorName::ArrowValue,
            parallelism: 1,
            operator_config: config.encode_to_vec(),
        });
        let input_index = input_nodes
            .first()
            .ok_or_else(|| DataFusionError::Plan("ValueNode must have an input".to_string()))?;
        let input_schema = self
            .output_schemas
            .get(input_index)
            .ok_or_else(|| DataFusionError::Plan("missing input node".to_string()))?;

        self.graph.add_edge(
            *input_index,
            node_index,
            LogicalEdge::project_all(LogicalEdgeType::Forward, input_schema.clone()),
        );
        self.output_schemas.insert(
            node_index,
            ArroyoSchema::from_schema_keys(Arc::new(value_node.schema().as_ref().into()), vec![])
                .unwrap(),
        );
        Ok(node_index)
    }

    fn sync_plan(&self, plan: &LogicalPlan) -> DFResult<Arc<dyn ExecutionPlan>> {
        let fut = self.planner.create_physical_plan(plan, &self.session_state);
        let (tx, mut rx) = oneshot::channel();
        let _scope = thread::scope(|s| {
            let _handle = tokio::runtime::Handle::current();
            s.spawn(move || {
                let rt = Runtime::new().unwrap();
                rt.block_on(async {
                    let plan = fut.await;
                    tx.send(plan).unwrap();
                });
            });
        });

        let physical_plan = rx.try_recv().unwrap();
        Ok(physical_plan?)
    }

    pub fn build_key_calculation(
        &mut self,
        input_nodes: Vec<NodeIndex>,
        key_calculation: &KeyCalculationExtension,
    ) -> DFResult<()> {
        let physical_plan = self.sync_plan(&key_calculation.input)?;

        let physical_plan_node: PhysicalPlanNode = PhysicalPlanNode::try_from_physical_plan(
            physical_plan,
            &ArroyoPhysicalExtensionCodec::default(),
        )?;
        let config = KeyPlanOperator {
            name: "key".into(),
            physical_plan: physical_plan_node.encode_to_vec(),
            key_fields: key_calculation.keys.iter().map(|k| *k as u64).collect(),
        };

        let node_index = self.graph.add_node(LogicalNode {
            operator_id: format!("key_{}", self.graph.node_count()),
            operator_name: OperatorName::ArrowKey,
            operator_config: config.encode_to_vec(),
            description: format!("ArrowKey<{}>", config.name),
            parallelism: 1,
        });
        self.add_index_to_traversal(node_index);
        let input_index = input_nodes.first().ok_or_else(|| {
            DataFusionError::Plan("KeyCalculationNode must have an input".to_string())
        })?;
        let input_schema = self
            .output_schemas
            .get(input_index)
            .ok_or_else(|| DataFusionError::Plan("missing input node".to_string()))?;
        self.graph.add_edge(
            *input_index,
            node_index,
            LogicalEdge::project_all(LogicalEdgeType::Forward, input_schema.clone()),
        );
        let arrow_schema = Arc::new(key_calculation.input.schema().as_ref().into());
        self.output_schemas.insert(
            node_index,
            ArroyoSchema::from_schema_keys(arrow_schema, key_calculation.keys.clone()).unwrap(),
        );
        Ok(())
    }

    fn split_physical_plan(
        &self,
        key_indices: Vec<usize>,
        aggregate: &LogicalPlan,
    ) -> DFResult<SplitPlanOutput> {
        let physical_plan = self.sync_plan(&aggregate)?;
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::Planning,
        };
        let mut physical_plan_node =
            PhysicalPlanNode::try_from_physical_plan(physical_plan.clone(), &codec)?;
        let PhysicalPlanType::Aggregate(mut final_aggregate_proto) = physical_plan_node
            .physical_plan_type
            .take()
            .ok_or_else(|| DataFusionError::Plan("missing physical plan type".to_string()))?
        else {
            return Err(DataFusionError::Plan(
                "unexpected physical plan type".to_string(),
            ));
        };
        let AggregateMode::Final = final_aggregate_proto.mode() else {
            return Err(DataFusionError::Plan(
                "unexpected physical plan type".to_string(),
            ));
        };
        // pull out the partial aggregation, so we can checkpoint it.
        let partial_aggregation_plan = *final_aggregate_proto
            .input
            .take()
            .ok_or_else(|| DataFusionError::Plan("missing input".to_string()))?;

        // need to convert to ExecutionPlan to get the partial schema.
        let partial_aggregation_exec_plan = partial_aggregation_plan.try_into_physical_plan(
            &new_registry(),
            &RuntimeEnv::new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;
        info!(
            "physical partial aggregation plan: {:?}",
            partial_aggregation_exec_plan
        );
        info!(
            "physical partial aggregation plan schema: {:?}",
            partial_aggregation_exec_plan.schema()
        );

        let partial_schema = partial_aggregation_exec_plan.schema();
        let final_input_table_provider = ArroyoMemExec {
            table_name: "partial".into(),
            schema: partial_schema.clone(),
        };

        final_aggregate_proto.input = Some(Box::new(PhysicalPlanNode::try_from_physical_plan(
            Arc::new(final_input_table_provider),
            &codec,
        )?));

        let finish_plan = PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Aggregate(final_aggregate_proto)),
        };

        let partial_schema = ArroyoSchema::new(
            add_timestamp_field_arrow(partial_schema.clone()),
            partial_schema.fields().len(),
            key_indices,
        );

        Ok(SplitPlanOutput {
            partial_aggregation_plan,
            partial_schema,
            finish_plan,
        })
    }

    pub fn binning_function_proto(
        &self,
        width: Duration,
        input_schema: DFSchemaRef,
    ) -> DFResult<PhysicalExprNode> {
        let date_bin = Expr::ScalarFunction(ScalarFunction {
            func_def: datafusion_expr::ScalarFunctionDefinition::BuiltIn(
                BuiltinScalarFunction::DateBin,
            ),
            args: vec![
                Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(
                    IntervalMonthDayNanoType::make_value(0, 0, width.as_nanos() as i64),
                ))),
                Expr::Column(datafusion_common::Column {
                    relation: None,
                    name: "_timestamp".into(),
                }),
            ],
        });

        let binning_function =
            self.planner
                .create_physical_expr(&date_bin, &input_schema, &self.session_state)?;
        Ok(PhysicalExprNode::try_from(binning_function)?)
    }

    pub fn tumbling_window_config(
        &self,
        input_schema: DFSchemaRef,
        key_indices: Vec<usize>,
        width: Duration,
        aggregate: &LogicalPlan,
        final_plan: &LogicalPlan,
    ) -> Result<LogicalNode> {
        let binning_function_proto = self.binning_function_proto(width, input_schema.clone())?;
        let SplitPlanOutput {
            partial_aggregation_plan,
            partial_schema,
            finish_plan,
        } = self.split_physical_plan(key_indices.clone(), aggregate)?;

        let final_physical_plan = self.sync_plan(final_plan)?;
        let final_physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            final_physical_plan,
            &ArroyoPhysicalExtensionCodec::default(),
        )?;

        let config = TumblingWindowAggregateOperator {
            name: format!("TumblingWindow"),
            width_micros: width.as_micros() as u64,
            binning_function: binning_function_proto.encode_to_vec(),
            input_schema: Some(
                ArroyoSchema::from_schema_keys(
                    Arc::new(input_schema.as_ref().into()),
                    key_indices,
                )?
                .try_into()?,
            ),
            partial_schema: Some(partial_schema.try_into().unwrap()),
            partial_aggregation_plan: partial_aggregation_plan.encode_to_vec(),
            final_aggregation_plan: finish_plan.encode_to_vec(),
            final_projection: Some(final_physical_plan_node.encode_to_vec()),
        };

        Ok(LogicalNode {
            operator_id: format!("tumbling_{}", self.graph.node_count()),
            operator_name: OperatorName::TumblingWindowAggregate,
            operator_config: config.encode_to_vec(),
            description: format!("TumblingWindow<{}>", config.name),
            parallelism: 1,
        })
    }

    pub fn sliding_window_config(
        &self,
        input_schema: DFSchemaRef,
        key_indices: Vec<usize>,
        width: Duration,
        slide: Duration,
        aggregate: &LogicalPlan,
        final_plan: &LogicalPlan,
    ) -> Result<LogicalNode> {
        let binning_function_proto = self.binning_function_proto(slide, input_schema.clone())?;

        let SplitPlanOutput {
            partial_aggregation_plan,
            partial_schema,
            finish_plan,
        } = self.split_physical_plan(key_indices.clone(), &aggregate)?;

        let final_physical_plan = self.sync_plan(final_plan)?;
        let final_physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            final_physical_plan,
            &ArroyoPhysicalExtensionCodec::default(),
        )?;

        let config = SlidingWindowAggregateOperator {
            name: format!("SlidingWindow<{:?}>", width),
            width_micros: width.as_micros() as u64,
            slide_micros: slide.as_micros() as u64,
            binning_function: binning_function_proto.encode_to_vec(),
            input_schema: Some(
                ArroyoSchema::from_schema_keys(
                    Arc::new(input_schema.as_ref().into()),
                    key_indices,
                )?
                .try_into()?,
            ),
            partial_schema: Some(partial_schema.try_into()?),
            partial_aggregation_plan: partial_aggregation_plan.encode_to_vec(),
            final_aggregation_plan: finish_plan.encode_to_vec(),
            final_projection: final_physical_plan_node.encode_to_vec(),
            // TODO add final aggregation.
        };
        Ok(LogicalNode {
            operator_id: format!("sliding_window_{}", self.graph.node_count()),
            description: "sliding window".to_string(),
            operator_name: OperatorName::SlidingWindowAggregate,
            operator_config: config.encode_to_vec(),
            parallelism: 1,
        })
    }

    pub fn session_window_config(
        &self,
        input_schema: DFSchemaRef,
        key_indices: Vec<usize>,
        aggregate: &LogicalPlan,
        window_behavior: &WindowBehavior,
    ) -> Result<LogicalNode> {
        let WindowBehavior::FromOperator {
            window: WindowType::Session { gap },
            window_index,
            window_field,
        } = window_behavior
        else {
            bail!("expected sliding window")
        };
        let output_schema = aggregate.schema().clone();
        let LogicalPlan::Aggregate(agg) = aggregate.clone() else {
            bail!("expected aggregate")
        };
        let key_count = key_indices.len();
        let unkeyed_aggregate_schema = Arc::new(DFSchema::new_with_metadata(
            output_schema.fields()[key_count..].to_vec(),
            output_schema.metadata().clone(),
        )?);

        let unkeyed_aggregate = Aggregate::try_new_with_schema(
            agg.input.clone(),
            vec![],
            agg.aggr_expr.clone(),
            unkeyed_aggregate_schema.clone(),
        )?;
        let aggregate_plan = self.sync_plan(&LogicalPlan::Aggregate(unkeyed_aggregate))?;

        let physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            aggregate_plan,
            &ArroyoPhysicalExtensionCodec::default(),
        )?;
        let input_schema =
            ArroyoSchema::from_schema_keys(Arc::new(input_schema.as_ref().into()), key_indices)?;

        let config = SessionWindowAggregateOperator {
            name: "session_window".into(),
            gap_micros: gap.as_micros() as u64,
            window_field_name: window_field.name().to_string(),
            window_index: *window_index as u64,
            input_schema: Some(input_schema.try_into()?),
            unkeyed_aggregate_schema: None,
            partial_aggregation_plan: vec![],
            final_aggregation_plan: physical_plan_node.encode_to_vec(),
        };

        Ok(LogicalNode {
            operator_id: config.name.clone(),
            description: format!("SessionWindow<{:?}>", gap),
            operator_name: OperatorName::SessionWindowAggregate,
            operator_config: config.encode_to_vec(),
            parallelism: 1,
        })
    }

    pub fn instant_window_config(
        &self,
        input_schema: DFSchemaRef,
        key_indices: Vec<usize>,
        aggregate: &LogicalPlan,
    ) -> Result<LogicalNode> {
        let binning_function = self.planner.create_physical_expr(
            &Expr::Column(Column::new_unqualified("_timestamp".to_string())),
            &input_schema,
            &self.session_state,
        )?;
        let binning_function_proto = PhysicalExprNode::try_from(binning_function)?;

        let SplitPlanOutput {
            partial_aggregation_plan,
            partial_schema,
            finish_plan,
        } = self.split_physical_plan(key_indices.clone(), &aggregate)?;

        let config = TumblingWindowAggregateOperator {
            name: "InstantWindow".to_string(),
            width_micros: 0,
            binning_function: binning_function_proto.encode_to_vec(),
            input_schema: Some(
                ArroyoSchema::from_schema_keys(
                    Arc::new(input_schema.as_ref().into()),
                    key_indices,
                )?
                .try_into()?,
            ),
            partial_schema: Some(partial_schema.try_into()?),
            partial_aggregation_plan: partial_aggregation_plan.encode_to_vec(),
            final_aggregation_plan: finish_plan.encode_to_vec(),
            final_projection: None,
        };

        Ok(LogicalNode {
            operator_id: format!("instant_window_{}", self.graph.node_count()),
            description: "instant window".to_string(),
            operator_name: OperatorName::TumblingWindowAggregate,
            operator_config: config.encode_to_vec(),
            parallelism: 1,
        })
    }

    pub fn build_aggregate(
        &mut self,
        input_nodes: Vec<NodeIndex>,
        aggregate: &AggregateExtension,
    ) -> DFResult<()> {
        let input_schema = self
            .output_schemas
            .get(&input_nodes[0])
            .ok_or_else(|| DataFusionError::Plan("missing input node".to_string()))?
            .clone();
        let input_df_schema =
            Arc::new(DFSchema::try_from(input_schema.schema.as_ref().clone()).unwrap());
        let logical_node = match &aggregate.window_behavior {
            WindowBehavior::FromOperator {
                window,
                window_field: _,
                window_index: _,
            } => match window {
                WindowType::Tumbling { width } => self
                    .tumbling_window_config(
                        input_df_schema,
                        aggregate.key_fields.clone(),
                        width.clone(),
                        &aggregate.aggregate,
                        &aggregate.final_calculation,
                    )
                    .map_err(|err| {
                        DataFusionError::Plan(format!("tumbling window error: {}", err))
                    })?,
                WindowType::Sliding { width, slide } => self
                    .sliding_window_config(
                        input_df_schema,
                        aggregate.key_fields.clone(),
                        *width,
                        *slide,
                        &aggregate.aggregate,
                        &aggregate.final_calculation,
                    )
                    .map_err(|err| {
                        DataFusionError::Plan(format!("sliding window error: {}", err))
                    })?,
                WindowType::Instant => {
                    return Err(DataFusionError::Plan(
                        "instant window not supported".to_string(),
                    ))
                }
                WindowType::Session { gap: _ } => self
                    .session_window_config(
                        input_df_schema,
                        aggregate.key_fields.clone(),
                        &aggregate.aggregate,
                        &aggregate.window_behavior,
                    )
                    .map_err(|err| {
                        DataFusionError::Plan(format!("session window error: {}", err))
                    })?,
            },
            WindowBehavior::InData => self
                .instant_window_config(
                    input_df_schema,
                    aggregate.key_fields.clone(),
                    &aggregate.aggregate,
                )
                .map_err(|err| DataFusionError::Plan(format!("instant window error: {}", err)))?,
        };

        let node_index = self.graph.add_node(logical_node);
        self.add_index_to_traversal(node_index);
        let input_index = input_nodes
            .first()
            .ok_or_else(|| DataFusionError::Plan("AggregateNode must have an input".to_string()))?;
        self.graph.add_edge(
            *input_index,
            node_index,
            LogicalEdge::project_all(LogicalEdgeType::Shuffle, input_schema.clone()),
        );
        let output_schema = aggregate.schema().as_ref().clone().into();
        self.output_schemas.insert(
            node_index,
            ArroyoSchema::from_schema_keys(Arc::new(output_schema), vec![]).unwrap(),
        );
        Ok(())
    }

    fn build_join(
        &mut self,
        left_index: NodeIndex,
        right_index: NodeIndex,
        join_extension: &JoinExtension,
    ) -> Result<()> {
        let join_plan = self.sync_plan(&join_extension.rewritten_join)?;
        let physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            join_plan.clone(),
            &ArroyoPhysicalExtensionCodec::default(),
        )?;
        let operator_name = if join_extension.is_instant {
            OperatorName::InstantJoin
        } else {
            OperatorName::Join
        };
        let left_schema = self.output_schemas.get(&left_index).cloned().unwrap();
        let right_schema = self.output_schemas.get(&right_index).cloned().unwrap();
        let output_schema = ArroyoSchema::from_schema_keys(
            Arc::new(join_extension.schema().as_ref().clone().into()),
            vec![],
        )?;
        let config = JoinOperator {
            name: format!("join_{}", self.graph.node_count()),
            left_schema: Some(left_schema.clone().try_into()?),
            right_schema: Some(right_schema.clone().try_into()?),
            output_schema: Some(output_schema.clone().try_into()?),
            join_plan: physical_plan_node.encode_to_vec(),
        };

        let logical_node = LogicalNode {
            operator_id: format!("join_{}", self.graph.node_count()),
            description: "join".to_string(),
            operator_name,
            operator_config: config.encode_to_vec(),
            parallelism: 1,
        };
        let node_index = self.graph.add_node(logical_node);
        self.add_index_to_traversal(node_index);
        self.graph.add_edge(
            left_index,
            node_index,
            LogicalEdge::project_all(LogicalEdgeType::LeftJoin, left_schema),
        );
        self.graph.add_edge(
            right_index,
            node_index,
            LogicalEdge::project_all(LogicalEdgeType::RightJoin, right_schema),
        );
        self.output_schemas.insert(node_index, output_schema);
        Ok(())
    }
}

impl TreeNodeVisitor for PlanToGraphVisitor {
    type N = LogicalPlan;

    fn pre_visit(&mut self, node: &Self::N) -> DFResult<VisitRecursion> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return Ok(VisitRecursion::Continue);
        };

        if !node.inputs().is_empty() {
            self.traversal.push(vec![]);
        }

        Ok(VisitRecursion::Continue)
    }

    // most of the work sits in post visit so that we can have the inputs of each node
    fn post_visit(&mut self, node: &Self::N) -> DFResult<VisitRecursion> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return Ok(VisitRecursion::Continue);
        };

        let input_nodes = if !node.inputs().is_empty() {
            self.traversal.pop().unwrap_or_default()
        } else {
            vec![]
        };

        match node.name() {
            // TODO: should we unify all of our extensions into a single enum?
            "TableSourceExtension" => {
                let table_source = node
                    .as_any()
                    .downcast_ref::<TableSourceExtension>()
                    .unwrap();
                self.build_source(table_source)?;
            }
            "WatermarkNode" => {
                let watermark_node = node.as_any().downcast_ref::<WatermarkNode>().unwrap();
                self.build_watermark(input_nodes, watermark_node)?;
            }
            "SinkExtension" => {
                let sink_extension = node.as_any().downcast_ref::<SinkExtension>().unwrap();
                match sink_extension.input.as_ref() {
                    LogicalPlan::Extension(_) => {
                        self.build_sink(input_nodes, sink_extension)?;
                    }
                    _ => {
                        // TODO: this is a bit hacky, should probably separate out computation from the SinkExtension so the input will have been automatically materialized.
                        let input_node =
                            self.build_value_node(input_nodes, &sink_extension.input)?;
                        self.build_sink(vec![input_node], sink_extension)?;
                    }
                }
            }
            "KeyCalculationExtension" => {
                let key_calculation = node
                    .as_any()
                    .downcast_ref::<KeyCalculationExtension>()
                    .unwrap();
                self.build_key_calculation(input_nodes, key_calculation)?;
            }
            "AggregateExtension" => {
                let aggregate = node.as_any().downcast_ref::<AggregateExtension>().unwrap();
                self.build_aggregate(input_nodes, aggregate)?;
            }
            "RemoteTableExtension" => {
                let remote_table = node
                    .as_any()
                    .downcast_ref::<RemoteTableExtension>()
                    .unwrap();
                let node_index = self.build_value_node(input_nodes, &remote_table.input)?;
                self.add_index_to_traversal(node_index);
                self.output_schemas.insert(
                    node_index,
                    ArroyoSchema::from_schema_keys(
                        Arc::new(remote_table.schema.as_ref().into()),
                        vec![],
                    )
                    .map_err(|err| {
                        DataFusionError::Plan(format!(
                            "remote table schema error : {}\n plan is {:?}",
                            err, node
                        ))
                    })?,
                );
            }
            "JoinExtension" => {
                let join_extension = node.as_any().downcast_ref::<JoinExtension>().unwrap();
                let left_index = input_nodes[0];
                let right_index = input_nodes[1];
                self.build_join(left_index, right_index, join_extension)
                    .map_err(|err| DataFusionError::Plan(format!("join error: {}", err)))?;
            }
            other => return Err(DataFusionError::Plan(format!("unexpected node: {}", other))),
        }

        Ok(VisitRecursion::Continue)
    }
}

struct SplitPlanOutput {
    partial_aggregation_plan: PhysicalPlanNode,
    partial_schema: ArroyoSchema,
    finish_plan: PhysicalPlanNode,
}
