use petgraph::data::DataMap;
use petgraph::graph::DiGraph;
use petgraph::prelude::EdgeRef;
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::{Dfs, NodeRef};
use petgraph::Direction::{Incoming, Outgoing};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_str, Type};

use arroyo_datastream::{
    AggregateBehavior, EdgeType, ExpressionReturnType, ExpressionReturnType::*, Operator,
    StreamEdge, StreamNode, WindowAgg,
};

pub fn optimize(graph: &mut DiGraph<StreamNode, StreamEdge>) {
    WasmFusionOptimizer {}.optimize(graph);
    fuse_window_aggregation(graph);
    ExpressionFusionOptimizer {}.optimize(graph);
    FlatMapFusionOptimizer {}.optimize(graph);
}

fn remove_in_place(graph: &mut DiGraph<StreamNode, StreamEdge>, node: NodeIndex) {
    let incoming = graph.edges_directed(node, Incoming).next().unwrap();

    let parent = incoming.source().id();
    let incoming = incoming.id();
    graph.remove_edge(incoming);

    let outgoing: Vec<_> = graph
        .edges_directed(node, Outgoing)
        .map(|e| (e.id(), e.target().id()))
        .collect();

    for (edge, target) in outgoing {
        let weight = graph.remove_edge(edge).unwrap();
        graph.add_edge(parent, target, weight);
    }

    graph.remove_node(node);
}

fn fuse_window_aggregation(graph: &mut DiGraph<StreamNode, StreamEdge>) {
    'outer: loop {
        let sources: Vec<_> = graph.externals(Incoming).collect();

        for source in sources {
            let mut dfs = Dfs::new(&(*graph), source);

            while let Some(idx) = dfs.next(&(*graph)) {
                let operator = graph.node_weight(idx).unwrap().operator.clone();
                let mut ins = graph.edges_directed(idx, Incoming);

                let in_degree = ins.clone().count();
                let no_shuffles = ins.all(|e| e.weight().typ == EdgeType::Forward);

                let mut ins = graph.edges_directed(idx, Incoming);
                if !(no_shuffles && in_degree == 1) {
                    continue;
                }

                let source_idx = ins.next().unwrap().source();
                let in_node = graph.node_weight_mut(source_idx).unwrap();
                if let Operator::Window { agg, .. } = &mut in_node.operator {
                    if agg.is_some() {
                        continue;
                    }

                    let new_agg = match &operator {
                        Operator::Count => Some(WindowAgg::Count),
                        Operator::Aggregate(AggregateBehavior::Min) => Some(WindowAgg::Min),
                        Operator::Aggregate(AggregateBehavior::Max) => Some(WindowAgg::Max),
                        Operator::Aggregate(AggregateBehavior::Sum) => Some(WindowAgg::Sum),
                        _ => None,
                    };

                    if let Some(new_agg) = new_agg {
                        *agg = Some(new_agg);
                        remove_in_place(graph, idx);
                        // restart the loop if we change something
                        continue 'outer;
                    }
                }
            }
        }
        break;
    }
}

pub trait Optimizer {
    fn can_optimize(&self, node: &StreamNode, current_chain: &[StreamNode]) -> bool;
    fn try_optimize(&self, chain: Vec<(StreamNode, StreamEdge)>) -> Option<StreamNode>;

    fn optimize(&self, graph: &mut DiGraph<StreamNode, StreamEdge>) {
        while self.optimize_once(graph) {}
    }

    fn optimize_once(&self, graph: &mut DiGraph<StreamNode, StreamEdge>) -> bool {
        let mut to_fuse = vec![vec![]];
        for source in graph.externals(Incoming) {
            let mut dfs = Dfs::new(&(*graph), source);
            let mut current_chain = vec![];

            while let Some(idx) = dfs.next(&(*graph)) {
                let node = graph.node_weight(idx).unwrap();
                let mut ins = graph.edges_directed(idx, Incoming);

                let in_degree = ins.clone().count();
                let no_shuffles = ins.all(|e| e.weight().typ == EdgeType::Forward);
                if no_shuffles && in_degree <= 1 && self.can_optimize(node, &current_chain) {
                    current_chain.push(node.clone());
                    to_fuse.last_mut().unwrap().push(idx);
                } else {
                    current_chain.clear();
                    to_fuse.push(vec![]);
                }
            }
        }
        for nodes in to_fuse.iter().filter(|f| f.len() > 1) {
            if nodes.iter().all(|idx| graph.node_weight(*idx).is_some()) {
                let input: Vec<_> = nodes
                    .iter()
                    .map(|idx| get_node_edge_pair(graph, *idx))
                    .collect();
                if let Some(new_node) = self.try_optimize(input) {
                    replace_run(graph, nodes, new_node);
                    return true;
                };
            }
        }

        false
    }
}

fn get_node_edge_pair(
    graph: &DiGraph<StreamNode, StreamEdge>,
    idx: NodeIndex,
) -> (StreamNode, StreamEdge) {
    let node = graph.node_weight(idx).unwrap();
    let edge = graph.edges_directed(idx, Outgoing).last().unwrap().weight();
    (node.clone(), edge.clone())
}

fn replace_run(
    graph: &mut DiGraph<StreamNode, StreamEdge>,
    run: &Vec<NodeIndex>,
    new_node: StreamNode,
) {
    let node_index = graph.add_node(new_node);
    let upstream_edge = graph.edges_directed(run[0], Incoming).last().unwrap();

    graph.add_edge(
        upstream_edge.source(),
        node_index,
        upstream_edge.weight().clone(),
    );
    let downstream_edge = graph
        .edges_directed(*run.last().unwrap(), Outgoing)
        .last()
        .unwrap();
    graph.add_edge(
        node_index,
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

struct ExpressionFusionOptimizer {}

impl Optimizer for ExpressionFusionOptimizer {
    fn can_optimize(&self, node: &StreamNode, _current_chain: &[StreamNode]) -> bool {
        matches!(&node.operator, Operator::ExpressionOperator { .. })
    }

    fn try_optimize(&self, chain: Vec<(StreamNode, StreamEdge)>) -> Option<StreamNode> {
        let mut operator_builder = FusedExpressionOperatorBuilder::new();
        let first_node = chain[0].clone().0;
        chain.iter().for_each(|(node, edge)| {
            if !operator_builder.fuse_node(node, edge) {
                unreachable!();
            };
        });

        Some(StreamNode {
            operator_id: first_node.operator_id,
            parallelism: first_node.parallelism,
            operator: operator_builder.get_operator(),
        })
    }
}

struct FusedExpressionOperatorBuilder {
    body: Vec<TokenStream>,
    names: Vec<String>,
    current_return_type: Option<ExpressionReturnType>,
}

impl FusedExpressionOperatorBuilder {
    fn new() -> Self {
        FusedExpressionOperatorBuilder {
            body: vec![],
            names: vec![],
            current_return_type: None,
        }
    }
    fn fuse_node(&mut self, node: &StreamNode, edge: &StreamEdge) -> bool {
        match &node.operator {
            Operator::ExpressionOperator {
                name,
                expression,
                return_type,
            } => self.fuse_expression_operator(
                edge,
                name.clone(),
                expression.to_string(),
                return_type.clone(),
            ),
            _ => false,
        }
    }

    fn get_operator(self) -> Operator {
        let name = format!("api_fused<{}>", self.names.join(","));
        let fused_body_tokens = self.body;
        let return_type = self.current_return_type.unwrap();
        let return_value = match return_type {
            Predicate => quote!(predicate),
            Record => quote!(record),
            OptionalRecord => quote!(Some(record)),
        };
        let body = quote!(
        {#(#fused_body_tokens)*
            #return_value
        })
        .to_string();

        let expr: syn::Expr = parse_str(&body).expect(&body);
        let expression = quote!(#expr).to_string();
        Operator::ExpressionOperator {
            name,
            expression,
            return_type,
        }
    }

    fn fuse_expression_operator(
        &mut self,
        edge: &StreamEdge,
        name: String,
        expression: String,
        return_type: ExpressionReturnType,
    ) -> bool {
        let out_type = format!("arroyo_types::Record<{},{}>", edge.key, edge.value);
        match return_type {
            Predicate => self.fuse_predicate(name, expression),
            Record => self.fuse_map(name, expression, out_type),
            OptionalRecord => self.fuse_option_map(name, expression, out_type),
        }
    }

    fn fuse_predicate(&mut self, name: String, expression: String) -> bool {
        self.names.push(name);
        let expression: syn::Expr = parse_str(&expression).unwrap();
        match self.current_return_type {
            None => {
                self.body.push(quote!(
                    let predicate = #expression;));
                self.current_return_type = Some(Predicate);
            }
            Some(Predicate) => {
                self.body.push(quote!(
                    let predicate = if predicate &&(#expression);
                ));
            }
            Some(Record) | Some(OptionalRecord) => {
                self.body.push(quote!(
                    if !(#expression) {return None;};));
                self.current_return_type = Some(OptionalRecord);
            }
        };
        true
    }

    fn fuse_map(&mut self, name: String, expression: String, out_type: String) -> bool {
        self.names.push(name);
        let expression: syn::Expr = parse_str(&expression).unwrap();
        let out_type: Type = parse_str(&out_type).unwrap();
        match self.current_return_type {
            None => {
                self.body.push(quote!(
                    let record:#out_type = #expression;));
                self.current_return_type = Some(Record);
            }
            Some(Predicate) => {
                self.body.push(quote!(
                    if !predicate {return None;};
                    let record:#out_type = #expression;));
                self.current_return_type = Some(OptionalRecord);
            }
            Some(Record) => {
                self.body.push(quote!(
                    let record:#out_type = #expression;));
            }
            Some(OptionalRecord) => {
                self.body.push(quote!(
                    let record:#out_type = #expression;));
                self.current_return_type = Some(OptionalRecord);
            }
        };
        true
    }

    fn fuse_option_map(&mut self, name: String, expression: String, out_type: String) -> bool {
        self.names.push(name);
        let expression: syn::Expr = parse_str(&expression).unwrap();
        let out_type: Type = parse_str(&out_type).unwrap();
        match self.current_return_type {
            None => {
                self.body.push(quote!(
                    let record:#out_type = (#expression)?;));
                self.current_return_type = Some(OptionalRecord);
            }
            Some(Predicate) => {
                self.body.push(quote!(
                    if !predicate {return None;};
                    let record:#out_type = (#expression)?;));
                self.current_return_type = Some(OptionalRecord);
            }
            Some(Record) => {
                self.body.push(quote!(
                    let record:#out_type = #expression?;));
                self.current_return_type = Some(OptionalRecord);
            }
            Some(OptionalRecord) => {
                self.body.push(quote!(
                    let record:#out_type = (#expression)?;));
                self.current_return_type = Some(OptionalRecord);
            }
        };
        true
    }
}

struct FlatMapFusionOptimizer {}

impl Optimizer for FlatMapFusionOptimizer {
    fn can_optimize(&self, node: &StreamNode, current_chain: &[StreamNode]) -> bool {
        match &node.operator {
            Operator::ExpressionOperator { .. } => current_chain.len() == 1,
            Operator::FlattenOperator { .. } => current_chain.is_empty(),
            _ => false,
        }
    }
    fn try_optimize(&self, chain: Vec<(StreamNode, StreamEdge)>) -> Option<StreamNode> {
        if chain.len() != 2 {
            return None;
        }
        let flatten = &chain[0].0;
        let expression = &chain[1].0;
        match (&flatten.operator, &expression.operator) {
            (
                Operator::FlattenOperator { name: flatten_name },
                Operator::ExpressionOperator {
                    name,
                    expression,
                    return_type,
                },
            ) => {
                let name = format!("flat_fused({},{})", flatten_name, name);
                let operator = Operator::FlatMapOperator {
                    name,
                    expression: expression.to_string(),
                    return_type: return_type.clone(),
                };
                Some(StreamNode {
                    operator_id: chain[0].0.operator_id.to_string(),
                    operator,
                    parallelism: chain[0].0.parallelism,
                })
            }
            _ => unreachable!(),
        }
    }
}

struct WasmFusionOptimizer {}

impl Optimizer for WasmFusionOptimizer {
    fn can_optimize(&self, node: &StreamNode, _current_chain: &[StreamNode]) -> bool {
        matches!(&node.operator, Operator::FusedWasmUDFs { .. })
    }
    fn try_optimize(&self, chain: Vec<(StreamNode, StreamEdge)>) -> Option<StreamNode> {
        let first_node = chain[0].clone().0;
        let (names, fused_udfs): (Vec<_>, Vec<_>) = chain
            .iter()
            .map(|(node, _edge)| {
                if let Operator::FusedWasmUDFs { name, udfs } = &node.operator {
                    (name.clone(), udfs.clone())
                } else {
                    unreachable!()
                }
            })
            .unzip();

        let operator = Operator::FusedWasmUDFs {
            name: format!("fused_{}", names.join("_")),
            udfs: fused_udfs.into_iter().flatten().collect(),
        };

        Some(StreamNode {
            operator_id: first_node.operator_id,
            parallelism: first_node.parallelism,
            operator,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use petgraph::prelude::DiGraph;

    use arroyo_datastream::{ConnectorOp, StreamEdge, StreamNode, WindowAgg};

    use super::fuse_window_aggregation;

    #[test]
    fn test_window_agg_fusion() {
        let mut graph = DiGraph::new();
        let source = graph.add_node(StreamNode {
            operator_id: "o1".to_string(),
            operator: arroyo_datastream::Operator::ConnectorSource(ConnectorOp {
                operator: "NullSource".to_string(),
                config: "".to_string(),
                description: "Null".to_string(),
            }),
            parallelism: 5,
        });

        let map1 = graph.add_node(StreamNode {
            operator_id: "o2".to_string(),
            operator: arroyo_datastream::Operator::FusedWasmUDFs {
                name: "map".to_string(),
                udfs: vec![],
            },
            parallelism: 5,
        });

        let map2 = graph.add_node(StreamNode {
            operator_id: "o3".to_string(),
            operator: arroyo_datastream::Operator::FusedWasmUDFs {
                name: "map2".to_string(),
                udfs: vec![],
            },
            parallelism: 5,
        });

        let window = graph.add_node(StreamNode {
            operator_id: "o4".to_string(),
            operator: arroyo_datastream::Operator::Window {
                typ: arroyo_datastream::WindowType::Tumbling {
                    width: Duration::from_secs(10),
                },
                agg: None,
                flatten: false,
            },
            parallelism: 5,
        });

        let count = graph.add_node(StreamNode {
            operator_id: "o5".to_string(),
            operator: arroyo_datastream::Operator::Count {},
            parallelism: 5,
        });

        let sink = graph.add_node(StreamNode {
            operator_id: "o6".to_string(),
            operator: arroyo_datastream::Operator::ConnectorSink(ConnectorOp {
                operator: "ConsoleSink".to_string(),
                config: "".to_string(),
                description: "ConsoleSink".to_string(),
            }),
            parallelism: 5,
        });

        graph.add_edge(
            source,
            map1,
            StreamEdge {
                key: "String".to_string(),
                value: "String".to_string(),
                typ: arroyo_datastream::EdgeType::Forward,
            },
        );
        graph.add_edge(
            source,
            map2,
            StreamEdge {
                key: "String".to_string(),
                value: "String".to_string(),
                typ: arroyo_datastream::EdgeType::Forward,
            },
        );

        graph.add_edge(
            map1,
            sink,
            StreamEdge {
                key: "String".to_string(),
                value: "String".to_string(),
                typ: arroyo_datastream::EdgeType::Forward,
            },
        );

        graph.add_edge(
            map2,
            window,
            StreamEdge {
                key: "String".to_string(),
                value: "String".to_string(),
                typ: arroyo_datastream::EdgeType::Forward,
            },
        );
        graph.add_edge(
            window,
            count,
            StreamEdge {
                key: "String".to_string(),
                value: "Vec<String>".to_string(),
                typ: arroyo_datastream::EdgeType::Forward,
            },
        );
        graph.add_edge(
            count,
            sink,
            StreamEdge {
                key: "String".to_string(),
                value: "usize".to_string(),
                typ: arroyo_datastream::EdgeType::Forward,
            },
        );

        assert_eq!(6, graph.node_count());
        assert_eq!(6, graph.edge_count());
        fuse_window_aggregation(&mut graph);
        assert_eq!(5, graph.node_count());
        assert_eq!(5, graph.edge_count());

        assert_eq!(
            arroyo_datastream::Operator::Window {
                typ: arroyo_datastream::WindowType::Tumbling {
                    width: Duration::from_secs(10)
                },
                agg: Some(WindowAgg::Count),
                flatten: false,
            },
            graph.node_weight(window).unwrap().operator
        );
    }
}
