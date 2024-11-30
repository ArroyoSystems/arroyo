use std::collections::HashSet;
use petgraph::prelude::*;
use crate::logical::{LogicalEdgeType, LogicalGraph};

pub trait Optimizer {
    fn optimize(&self, plan: &mut LogicalGraph);
}

pub struct ChainingOptimizer {
}


impl Optimizer for ChainingOptimizer {
    fn optimize(&self, plan: &mut LogicalGraph) {
        let node_indices: Vec<NodeIndex> = plan.node_indices().collect();
        let mut removed_nodes = HashSet::new();

        for &node_idx in &node_indices {
            if removed_nodes.contains(&node_idx) {
                continue;
            }

            let mut current_node = match plan.node_weight(node_idx) {
                Some(node) => node.clone(),
                None => continue,
            };

            // sources and sinks can't be chained
            if current_node.operator_chain.is_source() || current_node.operator_chain.is_sink() {
                continue;
            }

            let mut chain = vec![node_idx];
            let mut next_node_idx = node_idx;

            loop {
                let mut successors = plan
                    .edges_directed(next_node_idx, Outgoing)
                    .collect::<Vec<_>>();
                
                if successors.len() != 1 {
                    break;
                }

                let edge = successors.remove(0);
                let edge_type = edge.weight().edge_type;
                
                if edge_type != LogicalEdgeType::Forward {
                    break;
                }

                let successor_idx = edge.target();

                if removed_nodes.contains(&successor_idx) {
                    break;
                }

                let successor_node = match plan.node_weight(successor_idx) {
                    Some(node) => node.clone(),
                    None => break,
                };

                // skip if parallelism doesn't match or successor is a sink
                if current_node.parallelism != successor_node.parallelism || successor_node.operator_chain.is_sink()
                {
                    break;
                }

                // skip successors with multiple predecessors
                if plan.edges_directed(successor_idx, Incoming).count() > 1 {
                    break;
                }

                chain.push(successor_idx);
                next_node_idx = successor_idx;
            }

            if chain.len() > 1 {
                for i in 1..chain.len() {
                    let node_to_merge_idx = chain[i];
                    let node_to_merge = plan.node_weight(node_to_merge_idx).unwrap().clone();

                    current_node.description = format!(
                        "{} -> {}",
                        current_node.description, node_to_merge.description
                    );

                    current_node
                        .operator_chain
                        .operators
                        .extend(node_to_merge.operator_chain.operators.clone());

                    if let Some(edge_idx) = plan.find_edge(chain[i - 1], node_to_merge_idx) {
                        let edge = plan.edge_weight(edge_idx).unwrap();
                        current_node
                            .operator_chain
                            .edges
                            .push(edge.schema.clone());
                    }

                    removed_nodes.insert(node_to_merge_idx);
                }

                plan[node_idx] = current_node;

                let last_node_idx = *chain.last().unwrap();
                let outgoing_edges: Vec<_> = plan
                    .edges_directed(last_node_idx, petgraph::Outgoing)
                    .map(|e| (e.id(), e.target(), e.weight().clone()))
                    .collect();

                for (edge_id, target_idx, edge_weight) in outgoing_edges {
                    plan.remove_edge(edge_id);
                    plan.add_edge(node_idx, target_idx, edge_weight);
                }
            }
        }

        for node_idx in removed_nodes {
            plan.remove_node(node_idx);
        }
    }
}