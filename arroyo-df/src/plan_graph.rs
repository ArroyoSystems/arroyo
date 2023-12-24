use std::{
    collections::{HashMap, HashSet},
    io::sink,
    sync::Arc,
    time::Duration,
};

use arrow_schema::{DataType, Schema};
use arroyo_datastream::{ArroyoSchema, ConnectorOp, EdgeType, ExpressionReturnType, NonWindowAggregator, Operator, PeriodicWatermark, Program, ProgramUdf, SlidingAggregatingTopN, SlidingWindowAggregator, Stream, StreamEdge, StreamNode, TIMESTAMP_FIELD, TumblingTopN, TumblingWindowAggregator, WatermarkStrategy, WindowAgg, WindowType};

use datafusion::{
    datasource::MemTable,
    execution::{
        context::{SessionConfig, SessionState},
        runtime_env::RuntimeEnv,
    },
    physical_plan::{memory::MemoryExec, streaming::StreamingTableExec, PhysicalExpr},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use petgraph::{
    data::Build,
    graph::{DiGraph, NodeIndex},
    visit::{IntoNeighborsDirected, Topo},
};
use quote::{quote, ToTokens};
use syn::{parse_quote, parse_str, Type};
use tracing::{info, warn};

use crate::{physical::ArroyoPhysicalExtensionCodec, QueryToGraphVisitor};
use crate::{
    tables::Table,
    types::{StructDef, StructField, StructPair, TypeDef},
    ArroyoSchemaProvider, CompiledSql, SqlConfig,
};
use anyhow::{anyhow, bail, Context, Result};
use arroyo_datastream::EdgeType::Forward;
use arroyo_rpc::grpc::api::{
    window, KeyPlanOperator, MemTableScan, ProjectionOperator, TumblingWindow, ValuePlanOperator,
    Window, WindowAggregateOperator,
};
use datafusion_common::{DFField, DFSchema, DFSchemaRef, DataFusionError, ScalarValue};
use datafusion_expr::{logical_plan, BinaryExpr, Cast, Expr, LogicalPlan};
use datafusion_proto::{
    bytes::{physical_plan_to_bytes, Serializeable},
    physical_plan::{
        to_proto, AsExecutionPlan, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
    },
    protobuf::{PhysicalExprNode, PhysicalPlanNode},
};
use petgraph::Direction;
use prost::Message;
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalGraph, LogicalNode, LogicalProgram, OperatorName};
use arroyo_rpc::grpc::api;

fn to_arroyo_schema(s: &DFSchemaRef, key_cols: Vec<usize>) -> ArroyoSchema {
    println!("Converting schema: #{:?}", s);
    ArroyoSchema {
        schema: Arc::new(Schema::from(&*(*s))),
        timestamp_col: s.index_of_column_by_name(None, TIMESTAMP_FIELD)
            .unwrap()
            .expect("No timestamp field in schema"),
        key_cols,
    }
}

pub(crate) async fn get_arrow_program(
    mut rewriter: QueryToGraphVisitor,
    schema_provider: ArroyoSchemaProvider,
) -> Result<CompiledSql> {
    warn!(
        "graph is {:?}",
        petgraph::dot::Dot::with_config(&rewriter.local_logical_plan_graph, &[])
    );
    let mut topo = Topo::new(&rewriter.local_logical_plan_graph);
    let mut program_graph: LogicalGraph = DiGraph::new();

    let mut planner = DefaultPhysicalPlanner::default();
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    config.options_mut().optimizer.repartition_aggregations = false;
    let mut session_state = SessionState::with_config_rt(config, Arc::new(RuntimeEnv::default()))
        .with_physical_optimizer_rules(vec![]);

    let mut node_mapping = HashMap::new();
    while let Some(node_index) = topo.next(&rewriter.local_logical_plan_graph) {
        let logical_extension = rewriter
            .local_logical_plan_graph
            .node_weight(node_index)
            .unwrap();

        match logical_extension {
            crate::LogicalPlanExtension::TableScan(logical_plan) => {
                let LogicalPlan::TableScan(table_scan) = logical_plan else {
                    bail!("expected table scan")
                };

                let table_name = table_scan.table_name.to_string();
                let source = schema_provider
                    .get_table(&table_name)
                    .ok_or_else(|| anyhow!("table {} not found", table_scan.table_name))?;

                let Table::ConnectorTable(cn) = source else {
                    bail!("expect connector table")
                };

                let sql_source = cn.as_sql_source()?;
                let source_index = program_graph.add_node(LogicalNode {
                    operator_id: format!("source_{}", program_graph.node_count()),
                    description: sql_source.source.config.description.clone(),
                    operator_name: OperatorName::ConnectorSource,
                    operator_config: api::ConnectorOp::from(sql_source.source.config).encode_to_vec(),
                    parallelism: 1,
                });

                let watermark_index = program_graph.add_node(LogicalNode {
                    operator_id: format!("watermark_{}", program_graph.node_count()),
                    description: "watermark".to_string(),
                    operator_name: OperatorName::Watermark,
                    parallelism: 1,
                    operator_config: api::PeriodicWatermark {
                        period_micros: 10_000_000,
                        max_lateness_micros: 0,
                        idle_time_micros: None,
                    }.encode_to_vec()
                });

                program_graph.add_edge(
                    source_index,
                    watermark_index,
                    LogicalEdge {
                        edge_type: LogicalEdgeType::Forward,
                        schema: to_arroyo_schema(&table_scan.projected_schema, vec![]),
                    },
                );
                node_mapping.insert(node_index, watermark_index);
            }
            crate::LogicalPlanExtension::ValueCalculation(logical_plan) => {
                let inputs = logical_plan.inputs();
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

                for upstream in rewriter
                    .local_logical_plan_graph
                    .neighbors_directed(node_index, Direction::Incoming)
                {
                    program_graph.add_edge(
                        *node_mapping.get(&upstream).unwrap(),
                        new_node_index,
                        LogicalEdge {
                            edge_type: LogicalEdgeType::Forward,
                            schema: to_arroyo_schema(logical_plan.schema(), vec![]),
                        },
                    );
                }
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

                for upstream in rewriter
                    .local_logical_plan_graph
                    .neighbors_directed(node_index, Direction::Incoming)
                {
                    program_graph.add_edge(
                        *node_mapping.get(&upstream).unwrap(),
                        new_node_index,
                        LogicalEdge {
                            edge_type: LogicalEdgeType::Forward,
                            schema: to_arroyo_schema(logical_plan.schema(), vec![]),
                        }
                    );
                }
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
                for upstream in rewriter
                    .local_logical_plan_graph
                    .neighbors_directed(node_index, Direction::Incoming)
                {
                    program_graph.add_edge(
                        *node_mapping.get(&upstream).unwrap(),
                        new_node_index,
                        LogicalEdge {
                            edge_type: LogicalEdgeType::Shuffle,
                            schema: to_arroyo_schema(aggregate.aggregate.input.schema(), aggregate.key_fields.clone()),
                        },
                    );
                }
            }
            crate::LogicalPlanExtension::Sink => {
                let sink_index = program_graph.add_node(LogicalNode {
                    operator_id: format!("sink_{}", program_graph.node_count()),
                    operator_name: OperatorName::ConnectorSink,
                    operator_config: api::ConnectorOp::from(ConnectorOp::web_sink()).encode_to_vec(),
                    parallelism: 1,
                    description: "GrpcSink".into(),
                });
                node_mapping.insert(node_index, sink_index);
                for upstream in rewriter
                    .local_logical_plan_graph
                    .neighbors_directed(node_index, Direction::Incoming)
                {
                    let schema = &rewriter.local_logical_plan_graph.node_weight(upstream).unwrap().outgoing_edge().value_schema;

                    program_graph.add_edge(
                        *node_mapping.get(&upstream).unwrap(),
                        sink_index,
                        LogicalEdge {
                            edge_type: LogicalEdgeType::Forward,
                            schema: to_arroyo_schema(schema, vec![]),
                        },
                    );
                }
            }
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
