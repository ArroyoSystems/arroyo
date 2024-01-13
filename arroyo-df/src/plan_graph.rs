use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Schema};
use arroyo_datastream::{ConnectorOp, WindowType};

use datafusion::{
    execution::{
        context::{SessionConfig, SessionState},
        runtime_env::RuntimeEnv,
    },
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use petgraph::{graph::DiGraph, visit::Topo};

use tracing::{info, warn};

use crate::{physical::ArroyoPhysicalExtensionCodec, DataFusionEdge, QueryToGraphVisitor};
use crate::{tables::Table, ArroyoSchemaProvider, CompiledSql};
use anyhow::{anyhow, bail, Context, Result};
use arroyo_datastream::logical::{
    LogicalEdge, LogicalEdgeType, LogicalGraph, LogicalNode, LogicalProgram, OperatorName,
};
use arroyo_rpc::grpc::api;
use arroyo_rpc::grpc::api::{
    window, KeyPlanOperator, TumblingWindow, ValuePlanOperator, Window, WindowAggregateOperator,
};
use datafusion_common::{DFField, DFSchema, ScalarValue};
use datafusion_expr::{BinaryExpr, Expr, LogicalPlan};
use datafusion_proto::{
    physical_plan::AsExecutionPlan,
    protobuf::{PhysicalExprNode, PhysicalPlanNode},
};
use petgraph::prelude::EdgeRef;
use petgraph::Direction;
use prost::Message;

pub(crate) async fn get_arrow_program(
    rewriter: QueryToGraphVisitor,
    schema_provider: ArroyoSchemaProvider,
) -> Result<CompiledSql> {
    warn!(
        "graph is {:?}",
        petgraph::dot::Dot::with_config(&rewriter.local_logical_plan_graph, &[])
    );
    let mut topo = Topo::new(&rewriter.local_logical_plan_graph);
    let mut program_graph: LogicalGraph = DiGraph::new();

    let planner = DefaultPhysicalPlanner::default();
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    config.options_mut().optimizer.repartition_aggregations = false;
    let session_state = SessionState::new_with_config_rt(config, Arc::new(RuntimeEnv::default()))
        .with_physical_optimizer_rules(vec![]);

    let mut node_mapping = HashMap::new();
    while let Some(node_index) = topo.next(&rewriter.local_logical_plan_graph) {
        let logical_extension = rewriter
            .local_logical_plan_graph
            .node_weight(node_index)
            .unwrap();

        let new_node = match logical_extension {
            crate::LogicalPlanExtension::TableScan(logical_plan) => {
                let LogicalPlan::TableScan(table_scan) = logical_plan else {
                    panic!("expected table scan")
                };

                let table_name = table_scan.table_name.to_string();
                let source = schema_provider
                    .get_table(&table_name)
                    .ok_or_else(|| anyhow!("table {} not found", table_scan.table_name))?;

                let Table::ConnectorTable(cn) = source else {
                    panic!("expect connector table")
                };

                let sql_source = cn.as_sql_source()?;
                let source_index = program_graph.add_node(LogicalNode {
                    operator_id: format!("source_{}", program_graph.node_count()),
                    description: sql_source.source.config.description.clone(),
                    operator_name: OperatorName::ConnectorSource,
                    operator_config: api::ConnectorOp::from(sql_source.source.config)
                        .encode_to_vec(),
                    parallelism: 1,
                });

                let watermark_index = program_graph.add_node(LogicalNode {
                    operator_id: format!("watermark_{}", program_graph.node_count()),
                    description: "watermark".to_string(),
                    operator_name: OperatorName::Watermark,
                    parallelism: 1,
                    operator_config: api::PeriodicWatermark {
                        period_micros: 1_000_000,
                        max_lateness_micros: 0,
                        idle_time_micros: None,
                    }
                    .encode_to_vec(),
                });

                let mut edge: LogicalEdge = (&DataFusionEdge::new(
                    table_scan.projected_schema.clone(),
                    LogicalEdgeType::Forward,
                    vec![],
                )
                .unwrap())
                    .into();

                edge.projection = table_scan.projection.clone();

                program_graph.add_edge(source_index, watermark_index, edge);

                node_mapping.insert(node_index, watermark_index);
                watermark_index
            }
            crate::LogicalPlanExtension::ValueCalculation(logical_plan) => {
                let _inputs = logical_plan.inputs();
                let physical_plan = planner
                    .create_physical_plan(logical_plan, &session_state)
                    .await;

                let physical_plan =
                    physical_plan.context("creating physical plan for value calculation")?;

                let physical_plan_node: PhysicalPlanNode =
                    PhysicalPlanNode::try_from_physical_plan(
                        physical_plan,
                        &ArroyoPhysicalExtensionCodec::default(),
                    )?;

                let config = ValuePlanOperator {
                    name: "tmp".into(),
                    physical_plan: physical_plan_node.encode_to_vec(),
                };

                let new_node_index = program_graph.add_node(LogicalNode {
                    operator_id: format!("value_{}", program_graph.node_count()),
                    description: format!("arrow_value<{}>", config.name),
                    operator_name: OperatorName::ArrowValue,
                    operator_config: config.encode_to_vec(),
                    parallelism: 1,
                });

                node_mapping.insert(node_index, new_node_index);

                new_node_index
            }
            crate::LogicalPlanExtension::KeyCalculation {
                projection: logical_plan,
                key_columns,
            } => {
                info!("logical plan for key calculation:\n{:?}", logical_plan);
                info!("input schema: {:?}", logical_plan.schema());
                let physical_plan = planner
                    .create_physical_plan(logical_plan, &session_state)
                    .await;

                let physical_plan = physical_plan.context("creating physical plan")?;

                println!("physical plan {:#?}", physical_plan);
                let physical_plan_node: PhysicalPlanNode =
                    PhysicalPlanNode::try_from_physical_plan(
                        physical_plan,
                        &ArroyoPhysicalExtensionCodec::default(),
                    )?;
                let config = KeyPlanOperator {
                    name: "tmp".into(),
                    physical_plan: physical_plan_node.encode_to_vec(),
                    key_fields: key_columns.iter().map(|column| (*column) as u64).collect(),
                };

                let new_node_index = program_graph.add_node(LogicalNode {
                    operator_id: format!("key_{}", program_graph.node_count()),
                    operator_name: OperatorName::ArrowKey,
                    operator_config: config.encode_to_vec(),
                    description: format!("ArrowKey<{}>", config.name),
                    parallelism: 1,
                });

                node_mapping.insert(node_index, new_node_index);

                new_node_index
            }
            crate::LogicalPlanExtension::AggregateCalculation(aggregate) => {
                let WindowType::Tumbling { width } = aggregate.window else {
                    bail!("only implemented tumbling windows currently")
                };
                let my_aggregate = aggregate.aggregate.clone();
                let logical_plan = LogicalPlan::Aggregate(my_aggregate);

                let LogicalPlan::TableScan(table_scan) = aggregate.aggregate.input.as_ref() else {
                    bail!("expected logical plan")
                };
                info!(
                    "input arrow schema:{:?}\n input df schema:{:?}",
                    table_scan.source.schema(),
                    table_scan.projected_schema
                );

                info!(
                    "logical plan:{:?}\nlogical plan schema:{:?}\n",
                    logical_plan,
                    logical_plan.schema()
                );

                let physical_plan = planner
                    .create_physical_plan(&logical_plan, &session_state)
                    .await
                    .context("couldn't create physical plan for aggregate")?;

                println!("physical plan for aggregate: {:#?}", physical_plan);

                let physical_plan_node: PhysicalPlanNode =
                    PhysicalPlanNode::try_from_physical_plan(
                        physical_plan,
                        &ArroyoPhysicalExtensionCodec::default(),
                    )?;

                let division = Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(Expr::Column(datafusion_common::Column {
                        relation: None,
                        name: "timestamp_nanos".into(),
                    })),
                    op: datafusion_expr::Operator::Divide,
                    right: Box::new(Expr::Literal(ScalarValue::Int64(Some(
                        width.as_nanos() as i64
                    )))),
                });

                let timestamp_nanos_field =
                    DFField::new_unqualified("timestamp_nanos", DataType::Int64, false);
                let binning_df_schema =
                    DFSchema::new_with_metadata(vec![timestamp_nanos_field], HashMap::new())
                        .context("can't make timestamp nanos schema")?;
                let binning_arrow_schema: Schema = (&binning_df_schema).into();
                let binning_function = planner
                    .create_physical_expr(
                        &division,
                        &binning_df_schema,
                        &binning_arrow_schema,
                        &session_state,
                    )
                    .context("couldn't create binning function")?;

                let binning_function_proto = PhysicalExprNode::try_from(binning_function)
                    .context("couldn't encode binning function")?;
                let input_schema: Schema = aggregate.aggregate.input.schema().as_ref().into();

                let config = WindowAggregateOperator {
                    name: "window_aggregate".into(),
                    physical_plan: physical_plan_node.encode_to_vec(),
                    binning_function: binning_function_proto.encode_to_vec(),
                    binning_schema: serde_json::to_vec(&binning_arrow_schema)?,
                    input_schema: serde_json::to_vec(&input_schema)?,
                    window: Some(Window {
                        window: Some(window::Window::TumblingWindow(TumblingWindow {
                            size_micros: width.as_micros() as u64,
                        })),
                    }),
                    window_field_name: aggregate.window_field.name().to_string(),
                    window_index: aggregate.window_index as u64,
                    key_fields: aggregate
                        .key_fields
                        .iter()
                        .map(|field| (*field) as u64)
                        .collect(),
                };

                let new_node_index = program_graph.add_node(LogicalNode {
                    operator_id: format!("aggregate_{}", program_graph.node_count()),
                    operator_name: OperatorName::ArrowAggregate,
                    operator_config: config.encode_to_vec(),
                    parallelism: 1,
                    description: config.name.clone(),
                });

                node_mapping.insert(node_index, new_node_index);
                new_node_index
            }
            crate::LogicalPlanExtension::Sink { name, connector_op } => {
                let connector_op: api::ConnectorOp = connector_op.clone().into();
                let sink_index = program_graph.add_node(LogicalNode {
                    operator_id: format!("sink_{}", program_graph.node_count()),
                    operator_name: OperatorName::ConnectorSink,
                    operator_config: connector_op.encode_to_vec(),
                    parallelism: 1,
                    description: connector_op.description.clone(),
                });
                node_mapping.insert(node_index, sink_index);
                sink_index
            }
        };

        for edge in rewriter
            .local_logical_plan_graph
            .edges_directed(node_index, Direction::Incoming)
        {
            program_graph.add_edge(
                *node_mapping.get(&edge.source()).unwrap(),
                new_node,
                edge.weight().try_into().unwrap(),
            );
        }
    }

    let program = LogicalProgram {
        graph: program_graph,
    };

    Ok(CompiledSql {
        program,
        connection_ids: vec![],
        schemas: HashMap::new(),
    })
}
