use crate::grpc::api::ArrowProgram;
use anyhow::{Result, ensure};
use std::collections::{HashMap, HashSet};

impl ArrowProgram {
    /// Validate the scheduling metadata without interpreting schemas, operators,
    /// or UDF configuration. Those payloads belong to the worker's runtime.
    pub fn validate_topology(&self) -> Result<()> {
        ensure!(
            !self.nodes.is_empty(),
            "program must contain at least one node"
        );
        let mut ids = HashSet::new();
        let mut indices = HashSet::new();
        for node in &self.nodes {
            ensure!(
                ids.insert(node.node_id),
                "duplicate node id {}",
                node.node_id
            );
            ensure!(
                indices.insert(node.node_index),
                "duplicate node index {}",
                node.node_index
            );
            ensure!(
                node.parallelism > 0,
                "node {} has zero parallelism",
                node.node_id
            );
        }
        for edge in &self.edges {
            for index in [edge.source, edge.target] {
                ensure!(
                    indices.contains(&index),
                    "edge references unknown node index {index}"
                );
            }
        }
        Ok(())
    }

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

    /// Produce a scheduling/legacy-transport copy, leaving persisted defaults intact.
    pub fn with_parallelism_overrides(&self, overrides: &HashMap<u32, usize>) -> Result<Self> {
        let parallelism = self.effective_parallelism(overrides)?;
        let mut program = self.clone();
        for node in &mut program.nodes {
            node.parallelism = parallelism[&node.node_id] as u32;
        }
        Ok(program)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::api::{ArrowProgramConfig, DylibUdfConfig, PythonUdfConfig};
    use prost::Message;

    // Frozen v2 wire bytes, including an intentionally unreadable DF operator
    // payload. Do not regenerate from the current protobuf definitions.
    fn fixture_program() -> ArrowProgram {
        let hex = include_str!("../testdata/v2_program.hex").trim();
        let bytes: Vec<_> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        ArrowProgram::decode(bytes.as_slice()).unwrap()
    }

    #[test]
    fn schedules_without_interpreting_runtime_payloads() {
        let mut program = fixture_program();
        program.nodes[0].operators[0].operator_name = "FutureRuntimeOperator".into();
        program.edges[0].schema.as_mut().unwrap().arrow_schema = "not JSON".into();
        program.program_config = Some(ArrowProgramConfig {
            udf_dylibs: HashMap::from([(
                "native".into(),
                DylibUdfConfig {
                    arg_types: vec![vec![0xff]],
                    return_type: vec![0xff],
                    ..Default::default()
                },
            )]),
            python_udfs: HashMap::from([(
                "python".into(),
                PythonUdfConfig {
                    arg_types: vec![vec![0xff]],
                    return_type: vec![0xff],
                    ..Default::default()
                },
            )]),
        });
        let program = ArrowProgram::decode(program.encode_to_vec().as_slice()).unwrap();
        program.validate_topology().unwrap();
        let scheduled = program
            .with_parallelism_overrides(&HashMap::from([(10, 4)]))
            .unwrap();
        assert_eq!(scheduled.task_count(), 8);
        assert_eq!(scheduled.nodes[0].operators, program.nodes[0].operators);
        assert_eq!(scheduled.program_config, program.program_config);
        assert_eq!(scheduled.edges, program.edges);
    }

    #[test]
    fn effective_parallelism_preserves_defaults() {
        let program = fixture_program();
        assert_eq!(program.task_count(), 6);
        assert_eq!(program.slots_required(), 3);
        let scheduled = program
            .with_parallelism_overrides(&HashMap::from([(10, 4), (30, 2)]))
            .unwrap();
        assert_eq!(scheduled.task_count(), 9);
        assert_eq!(scheduled.slots_required(), 4);
        assert_eq!(
            scheduled.tasks_per_node(),
            HashMap::from([(10, 4), (20, 3), (30, 2)])
        );
        assert_eq!(
            program.with_parallelism_overrides(&HashMap::new()).unwrap(),
            program
        );
        assert_eq!(program.task_count(), 6);
    }

    #[test]
    fn rejects_invalid_topology() {
        let mut program = fixture_program();
        program.nodes[1].node_id = 10;
        assert!(
            program
                .validate_topology()
                .unwrap_err()
                .to_string()
                .contains("duplicate node id")
        );
        let mut program = fixture_program();
        program.nodes[1].node_index = 0;
        assert!(
            program
                .validate_topology()
                .unwrap_err()
                .to_string()
                .contains("duplicate node index")
        );
        let mut program = fixture_program();
        program.nodes[1].parallelism = 0;
        assert!(
            program
                .validate_topology()
                .unwrap_err()
                .to_string()
                .contains("zero parallelism")
        );
        let mut program = fixture_program();
        program.edges[0].source = 999;
        assert!(
            program
                .validate_topology()
                .unwrap_err()
                .to_string()
                .contains("unknown node index")
        );
        assert!(ArrowProgram::default().validate_topology().is_err());
    }

    #[test]
    fn rejects_invalid_overrides() {
        let program = fixture_program();
        for overrides in [
            HashMap::from([(999, 1)]),
            HashMap::from([(10, 0)]),
            HashMap::from([(10, usize::MAX)]),
        ] {
            assert!(program.with_parallelism_overrides(&overrides).is_err());
        }
    }
}
