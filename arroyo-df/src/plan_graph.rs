use std::{collections::HashMap, sync::Arc, time::Duration};

use arrow_array::types::IntervalMonthDayNanoType;
use arrow_schema::Schema;
use arroyo_datastream::WindowType;

use datafusion::{
    execution::{
        context::{SessionConfig, SessionState},
        runtime_env::RuntimeEnv,
    },
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use datafusion_execution::{runtime_env::RuntimeConfig, FunctionRegistry};
use petgraph::{graph::DiGraph, visit::Topo};

use tracing::info;

use crate::{
    create_table_with_timestamp,
    physical::{ArroyoMemExec, ArroyoPhysicalExtensionCodec, DecodingContext, EmptyRegistry},
    schemas::add_timestamp_field_arrow,
    AggregateCalculation, QueryToGraphVisitor, WindowBehavior,
};
use crate::{tables::Table, ArroyoSchemaProvider, CompiledSql};
use anyhow::{anyhow, bail, Context, Result};
use arroyo_datastream::logical::{LogicalGraph, LogicalNode, LogicalProgram, OperatorName};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::api::{self, TumblingWindowAggregateOperator};
use arroyo_rpc::grpc::api::{
    JoinOperator, KeyPlanOperator, SessionWindowAggregateOperator, SlidingWindowAggregateOperator,
    ValuePlanOperator,
};
use datafusion_common::{Column, DFField, DFSchema, DFSchemaRef, ScalarValue};
use datafusion_expr::{
    expr::ScalarFunction, BinaryExpr, BuiltinScalarFunction, Expr, LogicalPlan, Projection,
    ScalarFunctionDefinition, TableScan,
};
use datafusion_proto::{
    physical_plan::AsExecutionPlan,
    protobuf::{
        physical_plan_node::PhysicalPlanType, AggregateMode, PhysicalExprNode, PhysicalPlanNode,
    },
};
use petgraph::prelude::EdgeRef;
use petgraph::Direction;
use prost::Message;

pub(crate) struct Planner {
    schema_provider: ArroyoSchemaProvider,
    planner: DefaultPhysicalPlanner,
    session_state: SessionState,
}

impl Planner {
    pub fn new(schema_provider: ArroyoSchemaProvider) -> Self {
        let planner = DefaultPhysicalPlanner::default();
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
            schema_provider,
            planner,
            session_state,
        }
    }

    pub(crate) async fn get_arrow_program(
        &self,
        rewriter: QueryToGraphVisitor,
    ) -> Result<CompiledSql> {
        let mut topo = Topo::new(&rewriter.local_logical_plan_graph);
        let mut program_graph: LogicalGraph = DiGraph::new();

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
                    let source = self
                        .schema_provider
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

                    node_mapping.insert(node_index, source_index);
                    source_index
                }
                crate::LogicalPlanExtension::ValueCalculation(logical_plan) => {
                    let _inputs = logical_plan.inputs();
                    let physical_plan = self
                        .planner
                        .create_physical_plan(logical_plan, &self.session_state)
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
                    let physical_plan = self
                        .planner
                        .create_physical_plan(logical_plan, &self.session_state)
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
                    let LogicalPlan::TableScan(_table_scan) = aggregate.aggregate.input.as_ref()
                    else {
                        bail!("expected logical plan")
                    };
                    let logical_node = match &aggregate.window_behavior {
                        WindowBehavior::FromOperator {
                            window,
                            window_field: _,
                            window_index: _,
                        } => match window {
                            WindowType::Tumbling { width: _ } => {
                                let mut logical_node =
                                    self.tumbling_window_config(aggregate).await?;
                                logical_node.operator_id = format!(
                                    "{}_{}",
                                    logical_node.operator_id,
                                    program_graph.node_count()
                                );
                                logical_node
                            }
                            WindowType::Sliding { width: _, slide: _ } => {
                                let mut logical_node =
                                    self.sliding_window_config(aggregate).await?;
                                logical_node.operator_id = format!(
                                    "{}_{}",
                                    logical_node.operator_id,
                                    program_graph.node_count()
                                );
                                logical_node
                            }
                            WindowType::Instant => bail!("instant windows not supported yet"),
                            WindowType::Session { gap: _ } => {
                                let mut logical_node =
                                    self.session_window_config(aggregate).await?;
                                logical_node.operator_id = format!(
                                    "{}_{}",
                                    logical_node.operator_id,
                                    program_graph.node_count()
                                );
                                logical_node
                            }
                        },
                        WindowBehavior::InData => {
                            let mut logical_node = self.instant_window_config(aggregate).await?;
                            logical_node.operator_id = format!(
                                "{}_{}",
                                logical_node.operator_id,
                                program_graph.node_count()
                            );
                            logical_node
                        }
                    };

                    let new_node_index = program_graph.add_node(logical_node);
                    node_mapping.insert(node_index, new_node_index);
                    new_node_index
                }
                crate::LogicalPlanExtension::Sink {
                    name: _,
                    connector_op,
                } => {
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
                crate::LogicalPlanExtension::WatermarkNode(watermark_node) => {
                    let expression = self.planner.create_physical_expr(
                        &watermark_node.watermark_expression.as_ref().unwrap(),
                        &watermark_node.schema,
                        &self.session_state,
                    )?;

                    let expression = PhysicalExprNode::try_from(expression)?;

                    let watermark_index = program_graph.add_node(LogicalNode {
                        operator_id: format!("watermark_{}", program_graph.node_count()),
                        description: "watermark".to_string(),
                        operator_name: OperatorName::ExpressionWatermark,
                        parallelism: 1,
                        operator_config: api::ExpressionWatermarkConfig {
                            period_micros: 1_000_000,
                            idle_time_micros: None,
                            expression: expression.encode_to_vec(),
                            input_schema: Some(watermark_node.arroyo_schema().try_into()?),
                        }
                        .encode_to_vec(),
                    });

                    node_mapping.insert(node_index, watermark_index);
                    watermark_index
                }
                crate::LogicalPlanExtension::Join(join) => {
                    let output_schema = ArroyoSchema::new(
                        Arc::new(join.output_schema.as_ref().into()),
                        join.output_schema.fields().len() - 1,
                        vec![],
                    );

                    let join_plan = self
                        .planner
                        .create_physical_plan(&join.rewritten_join, &self.session_state)
                        .await?;
                    let physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
                        join_plan,
                        &ArroyoPhysicalExtensionCodec::default(),
                    )?;
                    let join_plan = physical_plan_node.encode_to_vec();

                    let join_operator = JoinOperator {
                        name: format!("join_{}", program_graph.node_count()),
                        left_schema: Some(join.left_input_schema.clone().try_into()?),
                        right_schema: Some(join.right_input_schema.clone().try_into()?),
                        output_schema: Some(output_schema.try_into()?),
                        join_plan,
                    };
                    let join_index = program_graph.add_node(LogicalNode {
                        operator_id: format!("join_{}", program_graph.node_count()),
                        operator_name: OperatorName::Join,
                        operator_config: join_operator.encode_to_vec(),
                        parallelism: 1,
                        description: "join".to_string(),
                    });
                    node_mapping.insert(node_index, join_index);
                    join_index
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

    fn binning_function_proto(
        &self,
        duration: Duration,
        input_schema: DFSchemaRef,
    ) -> Result<PhysicalExprNode> {
        let date_bin = Expr::ScalarFunction(ScalarFunction {
            func_def: datafusion_expr::ScalarFunctionDefinition::BuiltIn(
                BuiltinScalarFunction::DateBin,
            ),
            args: vec![
                Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(
                    IntervalMonthDayNanoType::make_value(0, 0, duration.as_nanos() as i64),
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

    fn input_schema(&self, aggregate: &AggregateCalculation) -> ArroyoSchema {
        let input_schema: Schema = aggregate.aggregate.input.schema().as_ref().into();

        ArroyoSchema {
            schema: Arc::new(input_schema),
            timestamp_index: aggregate.aggregate.input.schema().fields().len() - 1,
            key_indices: aggregate.key_fields.clone(),
        }
    }

    /* Splits an aggregate into two physical plan nodes, one for the partial and one for the final.
     */
    async fn split_physical_plan(
        &self,
        aggregate: &crate::AggregateCalculation,
    ) -> Result<SplitPlanOutput> {
        let key_indices = aggregate.key_fields.clone();
        let physical_plan = self
            .planner
            .create_physical_plan(
                &LogicalPlan::Aggregate(aggregate.aggregate.clone()),
                &self.session_state,
            )
            .await
            .context("couldn't create physical plan for aggregate")?;

        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::Planning,
        };

        let mut physical_plan_node: PhysicalPlanNode =
            PhysicalPlanNode::try_from_physical_plan(physical_plan.clone(), &codec)?;

        let PhysicalPlanType::Aggregate(mut final_aggregate_proto) = physical_plan_node
            .physical_plan_type
            .take()
            .ok_or_else(|| anyhow!("missing physical plan"))?
        else {
            bail!("expected aggregate physical plan, not {:?}", physical_plan);
        };

        let AggregateMode::Final = final_aggregate_proto.mode() else {
            bail!("expect AggregateMode to be Final so we can decompose it for checkpointing.")
        };

        // pull out the partial aggregation, so we can checkpoint it.
        let partial_aggregation_plan = *final_aggregate_proto
            .input
            .take()
            .expect("should have input");

        // need to convert to ExecutionPlan to get the partial schema.
        let partial_aggregation_exec_plan = partial_aggregation_plan.try_into_physical_plan(
            &EmptyRegistry::new(),
            &RuntimeEnv::new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;

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

    async fn instant_window_config(&self, aggregate: &AggregateCalculation) -> Result<LogicalNode> {
        let WindowBehavior::InData = &aggregate.window_behavior else {
            bail!("expected tumbling window")
        };
        let binning_function = self.planner.create_physical_expr(
            &Expr::Column(Column::new_unqualified("_timestamp".to_string())),
            aggregate.aggregate.input.schema().as_ref(),
            &self.session_state,
        )?;
        let binning_function_proto = PhysicalExprNode::try_from(binning_function)?;

        let input_schema = self.input_schema(aggregate);
        let SplitPlanOutput {
            partial_aggregation_plan,
            partial_schema,
            finish_plan,
        } = self.split_physical_plan(&aggregate).await?;

        let config = TumblingWindowAggregateOperator {
            name: "InstantWindow".to_string(),
            width_micros: 0,
            binning_function: binning_function_proto.encode_to_vec(),
            input_schema: Some(input_schema.try_into()?),
            partial_schema: Some(partial_schema.try_into()?),
            partial_aggregation_plan: partial_aggregation_plan.encode_to_vec(),
            final_aggregation_plan: finish_plan.encode_to_vec(),
            final_projection: None,
        };

        Ok(LogicalNode {
            operator_id: config.name.clone(),
            description: "instant window".to_string(),
            operator_name: OperatorName::TumblingWindowAggregate,
            operator_config: config.encode_to_vec(),
            parallelism: 1,
        })
    }

    async fn tumbling_window_config(
        &self,
        aggregate: &crate::AggregateCalculation,
    ) -> Result<LogicalNode> {
        let WindowBehavior::FromOperator {
            window: WindowType::Tumbling { width },
            window_index,
            window_field,
        } = &aggregate.window_behavior
        else {
            bail!("expected tumbling window")
        };
        let binning_function_proto =
            self.binning_function_proto(*width, aggregate.aggregate.input.schema().clone())?;

        let input_schema = self.input_schema(aggregate);
        let SplitPlanOutput {
            partial_aggregation_plan,
            partial_schema,
            finish_plan,
        } = self.split_physical_plan(&aggregate).await?;

        let aggregate_plan = LogicalPlan::Aggregate(aggregate.aggregate.clone());
        let mut aggregate_fields = aggregate_plan.schema().fields().to_vec();

        // the timestamp will have been set as bin_start, need to calculate the bin from that, and also set _timestamp to bin_end - 1;
        let registry = EmptyRegistry::new();
        let window_expression = Expr::ScalarFunction(ScalarFunction {
            func_def: ScalarFunctionDefinition::UDF(registry.udf("window")?),
            args: vec![
                // copy bin_start as first argument
                Expr::Column(Column::new_unqualified("_timestamp".to_string())),
                // add width interval to _timestamp for bin end
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(Expr::Column(Column::new_unqualified(
                        "_timestamp".to_string(),
                    ))),
                    op: datafusion_expr::Operator::Plus,
                    right: Box::new(Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(
                        IntervalMonthDayNanoType::make_value(0, 0, width.as_nanos() as i64),
                    )))),
                }),
            ],
        });
        let mut aggregate_expressions = aggregate_fields
            .iter()
            .map(|field| Expr::Column(Column::new_unqualified(field.name().to_string())))
            .collect::<Vec<_>>();
        let bin_end_calculation = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified(
                "_timestamp".to_string(),
            ))),
            op: datafusion_expr::Operator::Plus,
            right: Box::new(Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(0, 0, (width.as_nanos() - 1) as i64),
            )))),
        });
        aggregate_fields.push(DFField::new_unqualified(
            "_timestamp",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ));
        aggregate_expressions.push(bin_end_calculation);
        let arrow_input_schema: Schema =
            DFSchema::new_with_metadata(aggregate_fields.clone(), HashMap::new())?.into();
        let aggregate_output_table =
            create_table_with_timestamp("tmp".to_string(), arrow_input_schema.fields().to_vec());
        let table_scan_input = LogicalPlan::TableScan(TableScan::try_new(
            "tmp",
            aggregate_output_table,
            None,
            vec![],
            None,
        )?);
        aggregate_fields.insert(*window_index, window_field.clone());
        aggregate_expressions.insert(*window_index, window_expression);
        let output_schema = DFSchema::new_with_metadata(aggregate_fields, HashMap::new())?;
        let projection = Projection::try_new_with_schema(
            aggregate_expressions,
            Arc::new(table_scan_input),
            Arc::new(output_schema),
        )?;
        let final_plan = LogicalPlan::Projection(projection);

        let final_physical_plan = self
            .planner
            .create_physical_plan(&final_plan, &self.session_state)
            .await?;
        let final_physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            final_physical_plan,
            &ArroyoPhysicalExtensionCodec::default(),
        )?;

        let config = TumblingWindowAggregateOperator {
            name: format!("TumblingWindow<{:?}>", width),
            width_micros: width.as_micros() as u64,
            binning_function: binning_function_proto.encode_to_vec(),
            input_schema: Some(input_schema.try_into()?),
            partial_schema: Some(partial_schema.try_into()?),
            partial_aggregation_plan: partial_aggregation_plan.encode_to_vec(),
            final_aggregation_plan: finish_plan.encode_to_vec(),
            final_projection: Some(final_physical_plan_node.encode_to_vec()),
        };

        Ok(LogicalNode {
            operator_id: config.name.clone(),
            description: "tumbling window".to_string(),
            operator_name: OperatorName::TumblingWindowAggregate,
            operator_config: config.encode_to_vec(),
            parallelism: 1,
        })
    }

    async fn sliding_window_config(
        &self,
        aggregate: &crate::AggregateCalculation,
    ) -> Result<LogicalNode> {
        let WindowBehavior::FromOperator {
            window: WindowType::Sliding { width, slide },
            window_index,
            window_field,
        } = &aggregate.window_behavior
        else {
            bail!("expected sliding window")
        };
        let binning_function_proto =
            self.binning_function_proto(*slide, aggregate.aggregate.input.schema().clone())?;

        let input_schema = self.input_schema(aggregate);
        let SplitPlanOutput {
            partial_aggregation_plan,
            partial_schema,
            finish_plan,
        } = self.split_physical_plan(&aggregate).await?;
        let config = SlidingWindowAggregateOperator {
            name: format!("SlidingWindow<{:?}>", width),
            width_micros: width.as_micros() as u64,
            slide_micros: slide.as_micros() as u64,
            binning_function: binning_function_proto.encode_to_vec(),
            window_field_name: window_field.name().to_string(),
            window_index: *window_index as u64,
            input_schema: Some(input_schema.try_into()?),
            partial_schema: Some(partial_schema.try_into()?),
            partial_aggregation_plan: partial_aggregation_plan.encode_to_vec(),
            final_aggregation_plan: finish_plan.encode_to_vec(),
        };
        Ok(LogicalNode {
            operator_id: config.name.clone(),
            description: "sliding window".to_string(),
            operator_name: OperatorName::SlidingWindowAggregate,
            operator_config: config.encode_to_vec(),
            parallelism: 1,
        })
    }

    async fn session_window_config(&self, aggregate: &AggregateCalculation) -> Result<LogicalNode> {
        let WindowBehavior::FromOperator {
            window: WindowType::Session { gap },
            window_index,
            window_field,
        } = &aggregate.window_behavior
        else {
            bail!("expected sliding window")
        };
        let input_schema = self.input_schema(aggregate);
        let key_count = input_schema.key_indices.len();
        let unkeyed_aggregate_schema = ArroyoSchema::new(
            Arc::new(Schema::new(&input_schema.schema.fields[key_count..])),
            input_schema.timestamp_index - key_count,
            vec![],
        );
        let mut agg = aggregate.aggregate.clone();
        agg.group_expr = vec![];
        let output_schema = agg.schema;
        agg.schema = Arc::new(DFSchema::new_with_metadata(
            output_schema.fields()[key_count..].to_vec(),
            output_schema.metadata().clone(),
        )?);

        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::Planning,
        };

        let physical_plan = self
            .planner
            .create_physical_plan(&LogicalPlan::Aggregate(agg), &self.session_state)
            .await?;

        let mut physical_plan_node: PhysicalPlanNode =
            PhysicalPlanNode::try_from_physical_plan(physical_plan.clone(), &codec)?;

        let PhysicalPlanType::Aggregate(mut aggregate_exec_node) = physical_plan_node
            .physical_plan_type
            .take()
            .ok_or_else(|| anyhow!("missing physical plan"))?
        else {
            bail!("expected aggregate physical plan, not {:?}", physical_plan);
        };
        aggregate_exec_node.mode = AggregateMode::Single.into();
        let table_provider = ArroyoMemExec {
            table_name: "session".into(),
            schema: input_schema.schema.clone(),
        };

        aggregate_exec_node.input = Some(Box::new(PhysicalPlanNode::try_from_physical_plan(
            Arc::new(table_provider),
            &codec,
        )?));

        let finish_plan = PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Aggregate(aggregate_exec_node)),
        };

        let config = SessionWindowAggregateOperator {
            name: "session_window".into(),
            gap_micros: gap.as_micros() as u64,
            window_field_name: window_field.name().to_string(),
            window_index: *window_index as u64,
            input_schema: Some(input_schema.try_into()?),
            unkeyed_aggregate_schema: Some(unkeyed_aggregate_schema.try_into()?),
            partial_aggregation_plan: vec![],
            final_aggregation_plan: finish_plan.encode_to_vec(),
        };

        Ok(LogicalNode {
            operator_id: config.name.clone(),
            description: format!("SessionWindow<{:?}>", gap),
            operator_name: OperatorName::SessionWindowAggregate,
            operator_config: config.encode_to_vec(),
            parallelism: 1,
        })
    }
}

struct SplitPlanOutput {
    partial_aggregation_plan: PhysicalPlanNode,
    partial_schema: ArroyoSchema,
    finish_plan: PhysicalPlanNode,
}
