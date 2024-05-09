use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow::datatypes::IntervalMonthDayNanoType;

use arroyo_datastream::logical::{LogicalEdge, LogicalGraph, LogicalNode};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};

use async_trait::async_trait;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{
    DFSchema, DFSchemaRef, DataFusionError, OwnedTableReference, Result as DFResult, ScalarValue,
};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::functions::datetime::date_bin;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use petgraph::graph::{DiGraph, NodeIndex};
use tokio::runtime::Builder;
use tokio::sync::oneshot;

use crate::extension::debezium::{DEBEZIUM_UNROLLING_EXTENSION_NAME, TO_DEBEZIUM_EXTENSION_NAME};
use crate::extension::key_calculation::KeyCalculationExtension;
use crate::extension::{ArroyoExtension, NodeWithIncomingEdges};
use crate::physical::{
    ArroyoMemExec, ArroyoPhysicalExtensionCodec, DebeziumUnrollingExec, DecodingContext,
    ToDebeziumExec,
};
use crate::schemas::add_timestamp_field_arrow;
use crate::ArroyoSchemaProvider;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::{
    physical_plan::AsExecutionPlan,
    protobuf::{physical_plan_node::PhysicalPlanType, AggregateMode},
};

pub(crate) struct PlanToGraphVisitor<'a> {
    graph: DiGraph<LogicalNode, LogicalEdge>,
    output_schemas: HashMap<NodeIndex, ArroyoSchemaRef>,
    named_nodes: HashMap<NamedNode, NodeIndex>,
    // each node that needs to know its inputs should push an empty vec in pre_visit.
    // In post_visit each node should clean up its vec and push its index to the last vec, if present.
    traversal: Vec<Vec<NodeIndex>>,
    planner: Planner<'a>,
}

impl<'a> PlanToGraphVisitor<'a> {
    pub fn new(schema_provider: &'a ArroyoSchemaProvider) -> Self {
        Self {
            graph: Default::default(),
            output_schemas: Default::default(),
            named_nodes: Default::default(),
            traversal: vec![],
            planner: Planner::new(schema_provider),
        }
    }
}

pub(crate) struct Planner<'a> {
    schema_provider: &'a ArroyoSchemaProvider,
    planner: DefaultPhysicalPlanner,
    session_state: SessionState,
}

impl<'a> Planner<'a> {
    pub(crate) fn new(schema_provider: &'a ArroyoSchemaProvider) -> Self {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            ArroyoExtensionPlanner {},
        )]);
        let mut config = SessionConfig::new();
        config
            .options_mut()
            .optimizer
            .enable_round_robin_repartition = false;
        config.options_mut().optimizer.repartition_aggregations = false;
        config.options_mut().optimizer.repartition_windows = false;
        config.options_mut().optimizer.repartition_sorts = false;
        let session_state =
            SessionState::new_with_config_rt(config, Arc::new(RuntimeEnv::default()))
                .with_physical_optimizer_rules(vec![]);
        Self {
            schema_provider,
            planner,
            session_state,
        }
    }

    pub(crate) fn sync_plan(&self, plan: &LogicalPlan) -> DFResult<Arc<dyn ExecutionPlan>> {
        let fut = self.planner.create_physical_plan(plan, &self.session_state);
        let (tx, mut rx) = oneshot::channel();
        thread::scope(|s| {
            let _handle = tokio::runtime::Handle::current();
            let builder = thread::Builder::new();
            let builder = if cfg!(debug_assertions) {
                // Debug modes can end up with stack overflows because DataFusion is stack heavy.
                builder.stack_size(10_000_000)
            } else {
                builder
            };
            builder
                .spawn_scoped(&s, move || {
                    let rt = Builder::new_current_thread().enable_all().build().unwrap();
                    rt.block_on(async {
                        let plan = fut.await;
                        tx.send(plan).unwrap();
                    });
                })
                .unwrap();
        });

        rx.try_recv().unwrap()
    }
    pub(crate) fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        self.planner
            .create_physical_expr(expr, input_dfschema, &self.session_state)
    }

    // This splits aggregates into two parts, the partial aggregation and the final aggregation.
    // This needs to be done in physical space as that's the only point at which this split is realized.
    pub(crate) fn split_physical_plan(
        &self,
        key_indices: Vec<usize>,
        aggregate: &LogicalPlan,
        add_timestamp_field: bool,
    ) -> DFResult<SplitPlanOutput> {
        let physical_plan = self.sync_plan(aggregate)?;
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
            self.schema_provider,
            &RuntimeEnv::new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;

        let partial_schema = partial_aggregation_exec_plan.schema();
        let final_input_table_provider =
            ArroyoMemExec::new("partial".into(), partial_schema.clone());

        final_aggregate_proto.input = Some(Box::new(PhysicalPlanNode::try_from_physical_plan(
            Arc::new(final_input_table_provider),
            &codec,
        )?));

        let finish_plan = PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Aggregate(final_aggregate_proto)),
        };

        let (partial_schema, timestamp_index) = if add_timestamp_field {
            (
                add_timestamp_field_arrow(partial_schema.clone()),
                partial_schema.fields().len(),
            )
        } else {
            (partial_schema.clone(), partial_schema.fields().len() - 1)
        };

        let partial_schema = ArroyoSchema::new_keyed(partial_schema, timestamp_index, key_indices);

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
        let date_bin = date_bin().call(vec![
            Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(0, 0, width.as_nanos() as i64),
            ))),
            Expr::Column(datafusion::common::Column {
                relation: None,
                name: "_timestamp".into(),
            }),
        ]);

        let binning_function = self.create_physical_expr(&date_bin, &input_schema)?;
        serialize_physical_expr(binning_function, &DefaultPhysicalExtensionCodec {})
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub(crate) enum NamedNode {
    Source(OwnedTableReference),
    Watermark(OwnedTableReference),
    RemoteTable(OwnedTableReference),
}

struct ArroyoExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for ArroyoExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let schema = node.schema().as_ref().into();
        if let Ok::<&dyn ArroyoExtension, _>(arroyo_extension) = node.try_into() {
            if arroyo_extension.transparent() {
                match node.name() {
                    DEBEZIUM_UNROLLING_EXTENSION_NAME => {
                        let input = physical_inputs[0].clone();
                        return Ok(Some(Arc::new(DebeziumUnrollingExec::try_new(input)?)));
                    }
                    TO_DEBEZIUM_EXTENSION_NAME => {
                        let input = physical_inputs[0].clone();
                        return Ok(Some(Arc::new(ToDebeziumExec::try_new(input)?)));
                    }
                    _ => return Ok(None),
                }
            }
        };
        let name =
            if let Some(key_extension) = node.as_any().downcast_ref::<KeyCalculationExtension>() {
                key_extension.name.clone()
            } else {
                None
            };
        Ok(Some(Arc::new(ArroyoMemExec::new(
            name.unwrap_or("memory".to_string()),
            Arc::new(schema),
        ))))
    }
}

impl<'a> PlanToGraphVisitor<'a> {
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

    pub fn build_extension(
        &mut self,
        input_nodes: Vec<NodeIndex>,
        extension: &dyn ArroyoExtension,
    ) -> DFResult<()> {
        if let Some(node_name) = extension.node_name() {
            if self.named_nodes.contains_key(&node_name) {
                // we should've short circuited
                return Err(DataFusionError::Plan(format!(
                    "extension {:?} has already been planned, shouldn't try again.",
                    node_name
                )));
            }
        }
        let input_schemas = input_nodes
            .iter()
            .map(|index| {
                Ok(self
                    .output_schemas
                    .get(index)
                    .ok_or_else(|| DataFusionError::Plan("missing input node".to_string()))?
                    .clone())
            })
            .collect::<DFResult<Vec<_>>>()?;
        let NodeWithIncomingEdges { node, edges } = extension
            .plan_node(&self.planner, self.graph.node_count(), input_schemas)
            .map_err(|e| DataFusionError::Plan(format!("error planning extension: {}", e)))?;
        let node_index = self.graph.add_node(node);
        self.add_index_to_traversal(node_index);
        for (source, edge) in input_nodes.into_iter().zip(edges.into_iter()) {
            self.graph.add_edge(source, node_index, edge);
        }
        self.output_schemas
            .insert(node_index, extension.output_schema().into());
        if let Some(node_name) = extension.node_name() {
            self.named_nodes.insert(node_name, node_index);
        }
        Ok(())
    }
}

impl<'a> TreeNodeVisitor for PlanToGraphVisitor<'a> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let arroyo_extension: &dyn ArroyoExtension = node
            .try_into()
            .map_err(|e: DataFusionError| e.context("converting extension"))?;
        if arroyo_extension.transparent() {
            return Ok(TreeNodeRecursion::Continue);
        }
        if let Some(name) = arroyo_extension.node_name() {
            if let Some(node_index) = self.named_nodes.get(&name) {
                self.add_index_to_traversal(*node_index);
                return Ok(TreeNodeRecursion::Jump);
            }
        }

        if !node.inputs().is_empty() {
            self.traversal.push(vec![]);
        }

        Ok(TreeNodeRecursion::Continue)
    }

    // most of the work sits in post visit so that we can have the inputs of each node
    fn f_up(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };

        let arroyo_extension: &dyn ArroyoExtension = node
            .try_into()
            .map_err(|e| DataFusionError::Plan(format!("error converting extension: {}", e)))?;
        if arroyo_extension.transparent() {
            return Ok(TreeNodeRecursion::Continue);
        }

        if let Some(name) = arroyo_extension.node_name() {
            if let Some(_) = self.named_nodes.get(&name) {
                return Ok(TreeNodeRecursion::Jump);
            }
        }

        let input_nodes = if !node.inputs().is_empty() {
            self.traversal.pop().unwrap_or_default()
        } else {
            vec![]
        };
        let arroyo_extension: &dyn ArroyoExtension = node
            .try_into()
            .map_err(|e: DataFusionError| e.context("converting extension"))?;
        self.build_extension(input_nodes, arroyo_extension)
            .map_err(|e| e.context("building extension"))?;

        Ok(TreeNodeRecursion::Continue)
    }
}

pub(crate) struct SplitPlanOutput {
    pub(crate) partial_aggregation_plan: PhysicalPlanNode,
    pub(crate) partial_schema: ArroyoSchema,
    pub(crate) finish_plan: PhysicalPlanNode,
}
