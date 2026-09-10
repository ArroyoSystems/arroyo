use crate::grpc::api::ArrowProgram;
use anyhow::{Result, ensure};
use std::collections::HashMap;

impl ArrowProgram {
    /// Arroyo task parallelism, independent of DataFusion's local partitions.
    pub fn effective_parallelism(
        &self,
        overrides: &HashMap<u32, usize>,
    ) -> Result<HashMap<u32, usize>> {
        let mut parallelism = self.tasks_per_node();
        for (&id, &value) in overrides {
            ensure!(
                value > 0 && u32::try_from(value).is_ok(),
                "invalid parallelism override {value} for node {id}"
            );
            let current = parallelism.get_mut(&id).ok_or_else(|| {
                anyhow::anyhow!("parallelism override references unknown node id {id}")
            })?;
            *current = value;
        }
        Ok(parallelism)
    }

    /// Apply parallelism overrides in place, leaving unspecified nodes unchanged.
    pub fn update_parallelism(&mut self, overrides: &HashMap<u32, usize>) -> Result<()> {
        let parallelism = self.effective_parallelism(overrides)?;
        for node in &mut self.nodes {
            node.parallelism = parallelism[&node.node_id] as u32;
        }
        Ok(())
    }

    pub fn task_count(&self) -> usize {
        self.nodes
            .iter()
            .map(|node| node.parallelism as usize)
            .sum()
    }

    pub fn slots_required(&self) -> usize {
        self.nodes
            .iter()
            .map(|node| node.parallelism as usize)
            .max()
            .unwrap_or(0)
    }

    pub fn tasks_per_node(&self) -> HashMap<u32, usize> {
        self.nodes
            .iter()
            .map(|node| (node.node_id, node.parallelism as usize))
            .collect()
    }
}
