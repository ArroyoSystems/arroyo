use crate::logical::{LogicalEdgeType, LogicalGraph};
use petgraph::prelude::*;
use petgraph::visit::NodeRef;
use std::mem;

pub trait Optimizer {
    fn optimize_once(&self, plan: &mut LogicalGraph) -> bool;

    fn optimize(&self, plan: &mut LogicalGraph) {
        loop {
            if !self.optimize_once(plan) {
                break;
            }
        }
    }
}

pub struct ChainingOptimizer {}

fn remove_in_place<N, E>(graph: &mut DiGraph<N, E>, node: NodeIndex) {
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

impl Optimizer for ChainingOptimizer {
    fn optimize_once(&self, plan: &mut LogicalGraph) -> bool {
        let node_indices: Vec<NodeIndex> = plan.node_indices().collect();

        for &node_idx in &node_indices {
            let cur = plan.node_weight(node_idx).unwrap();

            // sources can't be chained
            if cur.operator_chain.is_source() {
                continue;
            }

            let mut successors = plan.edges_directed(node_idx, Outgoing).collect::<Vec<_>>();

            if successors.len() != 1 {
                continue;
            }

            let edge = successors.remove(0);
            let edge_type = edge.weight().edge_type;

            if edge_type != LogicalEdgeType::Forward {
                continue;
            }

            let successor_idx = edge.target();

            let successor_node = plan.node_weight(successor_idx).unwrap();

            // skip if parallelism doesn't match or successor is a sink
            if cur.parallelism != successor_node.parallelism
                || successor_node.operator_chain.is_sink()
            {
                continue;
            }

            // skip successors with multiple predecessors
            if plan.edges_directed(successor_idx, Incoming).count() > 1 {
                continue;
            }

            // construct the new node
            let mut new_cur = cur.clone();

            new_cur.description = format!("{} -> {}", cur.description, successor_node.description);

            new_cur
                .operator_chain
                .operators
                .extend(successor_node.operator_chain.operators.clone());

            new_cur
                .operator_chain
                .edges
                .push(edge.weight().schema.clone());

            new_cur
                .operator_chain
                .edges
                .extend(successor_node.operator_chain.edges.clone());

            mem::swap(&mut new_cur, plan.node_weight_mut(node_idx).unwrap());

            // remove the old successor
            remove_in_place(plan, successor_idx);
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::{LogicalEdge, LogicalNode, OperatorName};
    use arrow_schema::{DataType, Field};
    use arroyo_rpc::df::ArroyoSchema;
    use std::sync::Arc;

    fn node(id: u32, operator_id: &str, name: OperatorName) -> LogicalNode {
        LogicalNode::single(
            id,
            operator_id.to_string(),
            name,
            vec![],
            operator_id.to_string(),
            1,
        )
    }

    fn schema(name: &str) -> Arc<ArroyoSchema> {
        Arc::new(ArroyoSchema::from_fields(vec![Field::new(
            name,
            DataType::Int64,
            false,
        )]))
    }

    #[test]
    fn preserves_edges_when_merging_into_chain() {
        let mut graph = LogicalGraph::new();

        // Reverse-topological insertion makes B merge with C before A merges with B.
        let c = graph.add_node(node(2, "c", OperatorName::ArrowValue));
        let b = graph.add_node(node(1, "b", OperatorName::ExpressionWatermark));
        let a = graph.add_node(node(0, "a", OperatorName::ArrowValue));

        let ab_schema = schema("a_to_b");
        let bc_schema = schema("b_to_c");

        graph.add_edge(
            a,
            b,
            LogicalEdge {
                edge_type: LogicalEdgeType::Forward,
                schema: ab_schema.clone(),
            },
        );
        graph.add_edge(
            b,
            c,
            LogicalEdge {
                edge_type: LogicalEdgeType::Forward,
                schema: bc_schema.clone(),
            },
        );

        ChainingOptimizer {}.optimize(&mut graph);

        assert_eq!(graph.node_count(), 1);
        let chain = &graph.node_weights().next().unwrap().operator_chain;
        assert_eq!(
            chain
                .operators
                .iter()
                .map(|operator| operator.operator_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        assert_eq!(chain.edges, vec![ab_schema, bc_schema]);
    }
}
