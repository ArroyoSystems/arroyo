use std::time::Duration;

use anyhow::Result;
use arroyo_datastream::{EdgeType, ExpressionReturnType, ExpressionReturnType::*, WindowType};

use petgraph::data::DataMap;
use petgraph::graph::DiGraph;
use petgraph::prelude::EdgeRef;
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::Dfs;
use petgraph::Direction::{self, Incoming, Outgoing};

use quote::quote;

use crate::operators::{AggregateProjection, GroupByKind, Projection, TwoPhaseAggregateProjection};
use crate::pipeline::RecordTransform;
use crate::plan_graph::{
    FusedRecordTransform, PlanEdge, PlanNode, PlanOperator, PlanType, WindowFunctionOperator,
};

pub fn optimize(graph: &mut DiGraph<PlanNode, PlanEdge>) {
    WindowTopNOptimization::default().optimize(graph);
    ExpressionFusionOptimizer::default().optimize(graph);
    TwoPhaseOptimization {}.optimize(graph);
}

pub trait Optimizer {
    fn add_node(
        &mut self,
        node_index: NodeIndex,
        node: PlanNode,
        outgoing_edge: PlanEdge,
        graph: &mut DiGraph<PlanNode, PlanEdge>,
    ) -> bool;

    fn optimize(&mut self, graph: &mut DiGraph<PlanNode, PlanEdge>) {
        while self.optimize_once(graph) {
            self.clear()
        }
    }

    fn optimize_once(&mut self, graph: &mut DiGraph<PlanNode, PlanEdge>) -> bool {
        let sources: Vec<_> = graph.externals(Direction::Incoming).collect();
        for source in sources {
            let mut dfs = Dfs::new(&(*graph), source);

            while let Some(idx) = dfs.next(&(*graph)) {
                let _node = graph.node_weight(idx).unwrap();
                let ins = graph.edges_directed(idx, Direction::Incoming);

                let in_degree = ins.clone().count();
                if in_degree > 1 && self.try_finish_optimization(graph) {
                    return true;
                }
                // if the out degree is terminal, don't try to optimize it
                if graph.edges_directed(idx, Direction::Outgoing).count() == 0 {
                    self.clear();
                    continue;
                }
                let (node, outgoing_edge) = get_node_edge_pair(graph, idx);
                if self.add_node(idx, node, outgoing_edge, graph) {
                    return true;
                }
            }
            if self.try_finish_optimization(graph) {
                return true;
            }
        }
        false
    }

    fn clear(&mut self);

    fn try_finish_optimization(&mut self, graph: &mut DiGraph<PlanNode, PlanEdge>) -> bool;
}

fn get_node_edge_pair(graph: &DiGraph<PlanNode, PlanEdge>, idx: NodeIndex) -> (PlanNode, PlanEdge) {
    let node = graph.node_weight(idx).unwrap();
    let edge = graph.edges_directed(idx, Outgoing).last().unwrap().weight();
    (node.clone(), edge.clone())
}

fn replace_run(
    graph: &mut DiGraph<PlanNode, PlanEdge>,
    run: &Vec<NodeIndex>,
    new_node: PlanNode,
    additional_nodes: Vec<(PlanEdge, PlanNode)>,
) {
    let node_index = graph.add_node(new_node);
    let upstream_edge = graph.edges_directed(run[0], Incoming).last().unwrap();

    graph.add_edge(
        upstream_edge.source(),
        node_index,
        upstream_edge.weight().clone(),
    );
    let mut last_node_index = node_index;
    for (edge, node) in additional_nodes {
        let new_node_index = graph.add_node(node);
        graph.add_edge(last_node_index, new_node_index, edge);
        last_node_index = new_node_index;
    }
    let downstream_edge = graph
        .edges_directed(*run.last().unwrap(), Outgoing)
        .last()
        .unwrap();
    graph.add_edge(
        last_node_index,
        downstream_edge.target(),
        downstream_edge.weight().clone(),
    );

    let mut nodes_to_remove = vec![];
    for idx in run {
        nodes_to_remove.push(*idx);
    }

    nodes_to_remove.sort();
    nodes_to_remove.reverse();
    nodes_to_remove.iter().for_each(|index| {
        graph.remove_node(*index);
    });
}

#[derive(Debug, Default)]
struct ExpressionFusionOptimizer {
    builder: FusedExpressionOperatorBuilder,
    run: Vec<NodeIndex>,
}

impl Optimizer for ExpressionFusionOptimizer {
    fn clear(&mut self) {
        self.builder = FusedExpressionOperatorBuilder::default();
        self.run.clear();
    }
    fn try_finish_optimization(&mut self, graph: &mut DiGraph<PlanNode, PlanEdge>) -> bool {
        if self.run.is_empty() {
            false
        } else {
            replace_run(graph, &self.run, self.builder.get_node().unwrap(), vec![]);
            true
        }
    }

    fn add_node(
        &mut self,
        _node_index: NodeIndex,
        node: PlanNode,
        outgoing_edge: PlanEdge,
        graph: &mut DiGraph<PlanNode, PlanEdge>,
    ) -> bool {
        if matches!(&node.operator, PlanOperator::RecordTransform { .. }) {
            self.builder.fuse_node(&node, &outgoing_edge);
            false
        } else if !self.run.is_empty() {
            self.try_finish_optimization(graph)
        } else {
            false
        }
    }
}
#[derive(Debug, Default)]
struct FusedExpressionOperatorBuilder {
    sequence: Vec<RecordTransform>,
    output_types: Vec<PlanType>,
    expression_return_type: Option<ExpressionReturnType>,
}

impl FusedExpressionOperatorBuilder {
    fn add_filter(&mut self) {
        match self.expression_return_type {
            Some(ExpressionReturnType::Predicate) | None => {
                self.expression_return_type = Some(Predicate)
            }
            _ => self.expression_return_type = Some(OptionalRecord),
        }
    }
    fn add_projection(&mut self) {
        match self.expression_return_type {
            Some(ExpressionReturnType::Record) | None => self.expression_return_type = Some(Record),
            _ => self.expression_return_type = Some(OptionalRecord),
        }
    }

    fn fuse_node(&mut self, node: &PlanNode, _edge: &PlanEdge) -> bool {
        match &node.operator {
            PlanOperator::RecordTransform(record_transform) => {
                self.sequence.push(record_transform.clone());
                self.output_types.push(node.output_type.clone());
                match record_transform {
                    RecordTransform::ValueProjection(_) | RecordTransform::KeyProjection(_) => {
                        self.add_projection()
                    }
                    RecordTransform::Filter(_) => self.add_filter(),
                }
                true
            }
            _ => false,
        }
    }

    fn get_node(&self) -> Option<PlanNode> {
        self.expression_return_type.as_ref()?;
        let output_type = self.output_types.last().unwrap().clone();
        let expressions = self.sequence.clone();
        let expression_return_type = self.expression_return_type.clone().unwrap();
        let output_types = self.output_types.clone();
        let operator = PlanOperator::FusedRecordTransform(FusedRecordTransform {
            expressions,
            output_types,
            expression_return_type,
        });

        Some(PlanNode {
            operator,
            output_type,
        })
    }
}

struct TwoPhaseOptimization {}

impl Optimizer for TwoPhaseOptimization {
    fn add_node(
        &mut self,
        node_index: NodeIndex,
        node: PlanNode,
        outgoing_edge: PlanEdge,
        graph: &mut DiGraph<PlanNode, PlanEdge>,
    ) -> bool {
        let PlanOperator::WindowAggregate { window, projection } = node.operator else { return false };
        let (width, slide) = match window {
            WindowType::Tumbling { width } => (width, width),
            WindowType::Sliding { width, slide } => (width, slide),
            WindowType::Instant => (Duration::ZERO, Duration::ZERO),
        };
        if !slide.is_zero() && width.as_micros() % slide.as_micros() != 0 {
            return false;
        }
        let Ok(projection) = projection.try_into() else {
            return false;
        };
        let operator = if width == slide {
            PlanOperator::TumblingWindowTwoPhaseAggregator {
                tumble_width: width,
                projection,
            }
        } else {
            PlanOperator::SlidingWindowTwoPhaseAggregator {
                width,
                slide,
                projection,
            }
        };
        let current_weight = graph.node_weight_mut(node_index).unwrap();
        *current_weight = PlanNode {
            operator,
            output_type: outgoing_edge.edge_data_type,
        };
        true
    }

    fn clear(&mut self) {}

    fn try_finish_optimization(&mut self, _graph: &mut DiGraph<PlanNode, PlanEdge>) -> bool {
        false
    }
}

#[derive(Debug, Default)]
struct WindowTopNOptimization {
    aggregate_key: Option<Projection>,
    window_aggregate: Option<(WindowType, AggregateProjection)>,
    merge: Option<GroupByKind>,
    partition_projection: Option<Projection>,
    window_function_operator: Option<WindowFunctionOperator>,
    projection: Option<Projection>,
    nodes: Vec<NodeIndex>,
    outgoing_edges: Vec<PlanEdge>,
    search_target: SearchTarget,
}
#[derive(Default, Debug)]
enum SearchTarget {
    #[default]
    AggregateKey,
    WindowAggregate,
    GroupByKind,
    PartitionProjection,
    PartitionKeyProjection,
    WindowFunctionOperator,
    Unkey,
    Limit,
}

impl Optimizer for WindowTopNOptimization {
    fn add_node(
        &mut self,
        node_index: NodeIndex,
        node: PlanNode,
        outgoing_edge: PlanEdge,
        graph: &mut DiGraph<PlanNode, PlanEdge>,
    ) -> bool {
        match self.search_target {
            SearchTarget::AggregateKey => {
                if let PlanOperator::RecordTransform(RecordTransform::KeyProjection(projection)) =
                    node.operator
                {
                    self.aggregate_key = Some(projection);
                    self.nodes.push(node_index);
                    self.outgoing_edges.push(outgoing_edge);
                    self.search_target = SearchTarget::WindowAggregate;
                }
            }
            SearchTarget::WindowAggregate => {
                if let PlanOperator::WindowAggregate { window, projection } = node.operator {
                    self.window_aggregate = Some((window, projection));
                    self.nodes.push(node_index);
                    self.outgoing_edges.push(outgoing_edge);
                    self.search_target = SearchTarget::GroupByKind;
                } else {
                    self.clear();
                }
            }
            SearchTarget::GroupByKind => {
                if let PlanOperator::WindowMerge { group_by_kind, .. } = node.operator {
                    self.merge = Some(group_by_kind);
                    self.nodes.push(node_index);
                    self.outgoing_edges.push(outgoing_edge);
                    self.search_target = SearchTarget::PartitionProjection;
                } else {
                    self.clear()
                }
            }
            SearchTarget::PartitionProjection => {
                if let PlanOperator::RecordTransform(RecordTransform::ValueProjection(projection)) =
                    node.operator
                {
                    self.projection = Some(projection);
                    self.nodes.push(node_index);
                    self.outgoing_edges.push(outgoing_edge);
                    self.search_target = SearchTarget::PartitionKeyProjection;
                } else {
                    self.clear()
                }
            }
            SearchTarget::PartitionKeyProjection => {
                if let PlanOperator::RecordTransform(RecordTransform::KeyProjection(
                    partition_projection,
                )) = node.operator
                {
                    self.partition_projection = Some(partition_projection);
                    self.nodes.push(node_index);
                    self.outgoing_edges.push(outgoing_edge);
                    self.search_target = SearchTarget::WindowFunctionOperator;
                } else {
                    self.clear()
                }
            }
            SearchTarget::WindowFunctionOperator => {
                if let PlanOperator::WindowFunction(window_function_operator) = node.operator {
                    let _field_name = window_function_operator.field_name.clone();
                    self.window_function_operator = Some(window_function_operator);
                    self.nodes.push(node_index);
                    self.outgoing_edges.push(outgoing_edge);
                    self.search_target = SearchTarget::Unkey;
                } else {
                    self.clear()
                }
            }
            SearchTarget::Unkey => {
                if let PlanOperator::Unkey = node.operator {
                    self.nodes.push(node_index);
                    self.outgoing_edges.push(outgoing_edge);
                    self.search_target = SearchTarget::Limit;
                } else {
                    self.clear()
                }
            }
            SearchTarget::Limit => {
                if let PlanOperator::RecordTransform(RecordTransform::Filter(filter)) =
                    node.operator
                {
                    let  PlanType::Unkeyed(input_type) = outgoing_edge.edge_data_type else {
                        unreachable!("Filter must have unkeyed output type")
                    };
                    let field_name = &self.window_function_operator.as_ref().unwrap().field_name;
                    let field = input_type.get_field(None, field_name).unwrap();
                    if let Some(max_value) = filter.has_max_value(&field) {
                        // set key for tumbling aggregator.
                        let key_projection = self.aggregate_key.take().unwrap();
                        let key_operator = PlanOperator::RecordTransform(
                            RecordTransform::KeyProjection(key_projection.clone()),
                        );
                        let key_node = PlanNode {
                            operator: key_operator,
                            output_type: self.outgoing_edges[0].edge_data_type.clone(),
                        };
                        let new_node = key_node;
                        let mut additional_nodes = vec![];
                        // Non-shuffle slide-width tumbling aggregator.
                        let (window, projection) = self.window_aggregate.take().unwrap();
                        let (width, slide) = match window {
                            WindowType::Tumbling { width } => (width, width),
                            WindowType::Sliding { width, slide } => (width, slide),
                            WindowType::Instant => (Duration::ZERO, Duration::ZERO),
                        };
                        if width.as_micros() % slide.as_micros() != 0 {
                            self.clear();
                            return false;
                        }
                        let Ok(two_phase_projection) : Result<TwoPhaseAggregateProjection> = projection.try_into() else {
                            self.clear();
                            return false;
                        };
                        let tumbling_local_operator = PlanOperator::TumblingLocalAggregator {
                            width: slide,
                            projection: two_phase_projection.clone(),
                        };
                        let bin_type = two_phase_projection.bin_type();
                        let tumbling_local_node = PlanNode {
                            operator: tumbling_local_operator,
                            output_type: PlanType::KeyedLiteralTypeValue {
                                key: key_projection.output_struct(),
                                value: quote!(#bin_type).to_string(),
                            },
                        };
                        let tumbling_incoming_edge = PlanEdge {
                            edge_data_type: new_node.output_type.clone(),
                            edge_type: EdgeType::Forward,
                        };
                        additional_nodes
                            .push((tumbling_incoming_edge, tumbling_local_node.clone()));

                        let window_function_operator =
                            self.window_function_operator.take().unwrap();
                        let partition_projection = self.partition_projection.take().unwrap();
                        let converting_projection = self.projection.take().unwrap();

                        let sliding_incoming_edge = PlanEdge {
                            edge_data_type: tumbling_local_node.output_type,
                            edge_type: EdgeType::Shuffle,
                        };

                        //Sliding Aggregating TopN
                        let sliding_aggregating_top_n_operator =
                            PlanOperator::SlidingAggregatingTopN {
                                width,
                                slide,
                                aggregating_projection: two_phase_projection,
                                order_by: window_function_operator.order_by.clone(),
                                partition_projection: partition_projection.clone(),
                                converting_projection: converting_projection.clone(),
                                max_elements: max_value as usize,
                                group_by_projection: key_projection,
                                group_by_kind: self.merge.take().unwrap(),
                            };

                        let sliding_aggregating_top_n_node = PlanNode {
                            operator: sliding_aggregating_top_n_operator,
                            output_type: PlanType::Keyed {
                                key: partition_projection.output_struct(),
                                value: converting_projection.output_struct(),
                            },
                        };

                        additional_nodes.push((
                            sliding_incoming_edge,
                            sliding_aggregating_top_n_node.clone(),
                        ));

                        let tumbling_top_n_operator = PlanOperator::TumblingTopN {
                            width: Duration::ZERO,
                            max_elements: max_value as usize,
                            window_function: window_function_operator.clone(),
                        };

                        let tumbling_top_n_node = PlanNode {
                            operator: tumbling_top_n_operator,
                            output_type: PlanType::Keyed {
                                key: partition_projection.output_struct(),
                                value: window_function_operator.result_struct.clone(),
                            },
                        };

                        let tumbling_top_n_incoming_edge = PlanEdge {
                            edge_data_type: sliding_aggregating_top_n_node.output_type,
                            edge_type: EdgeType::Shuffle,
                        };

                        additional_nodes
                            .push((tumbling_top_n_incoming_edge, tumbling_top_n_node.clone()));

                        let unkey_operator = PlanOperator::Unkey;
                        let unkey_node = PlanNode {
                            operator: unkey_operator,
                            output_type: PlanType::Unkeyed(window_function_operator.result_struct),
                        };

                        let unkey_incoming_edge = PlanEdge {
                            edge_data_type: tumbling_top_n_node.output_type,
                            edge_type: EdgeType::Forward,
                        };

                        additional_nodes.push((unkey_incoming_edge, unkey_node));

                        replace_run(graph, &self.nodes, new_node, additional_nodes);

                        return true;
                    }
                }
                self.clear();
            }
        }
        false
    }

    fn clear(&mut self) {
        self.nodes.clear();
        self.outgoing_edges.clear();
        self.aggregate_key = None;
        self.window_aggregate = None;
        self.window_function_operator = None;
        self.partition_projection = None;
        self.merge = None;
        self.projection = None;
        self.search_target = SearchTarget::AggregateKey;
    }

    fn try_finish_optimization(&mut self, _graph: &mut DiGraph<PlanNode, PlanEdge>) -> bool {
        false
    }
}
