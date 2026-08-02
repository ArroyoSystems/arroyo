use crate::engine::construct_operator;
use arrow_schema::Schema;
use arroyo_datastream::logical::{ChainedLogicalOperator, LogicalEdgeType, LogicalProgram};
use arroyo_operator::operator::Registry;
use arroyo_planner::physical::new_registry;
use std::fmt::Write;
use std::sync::Arc;
use tracing::warn;

fn format_arrow_schema_fields(schema: &Schema) -> Vec<(String, String)> {
    schema
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().to_string()))
        .collect()
}

fn write_op(d2: &mut String, registry: &Arc<Registry>, idx: usize, el: &ChainedLogicalOperator) {
    let operator = construct_operator(el.operator_name, &el.operator_config, registry.clone());
    let display = operator.display();

    let mut label = format!(
        "### [{}] {} ({})",
        el.operator_id,
        operator.name(),
        &display.name
    );
    for (field, value) in display.fields {
        label.push_str(&format!("\n## {field}\n\n{value}"));
    }

    writeln!(
        d2,
        "{idx}: {{
  label: |markdown
{label}
  |
  shape: rectangle
}}"
    )
    .unwrap();
}

fn write_edge(
    d2: &mut String,
    from: usize,
    to: usize,
    edge_idx: usize,
    edge_type: &LogicalEdgeType,
    schema: &Schema,
) {
    let edge_label = format!("{edge_type}");

    let schema_node_name = format!("schema_{edge_idx}");
    let schema_fields = format_arrow_schema_fields(schema);

    writeln!(d2, "{schema_node_name}: {{").unwrap();
    writeln!(d2, "  shape: sql_table").unwrap();

    for (field_name, field_type) in schema_fields {
        writeln!(
            d2,
            "  \"{}\": \"{}\"",
            field_name.replace("\"", "\\\""),
            field_type.replace("\"", "\\\"")
        )
        .unwrap();
    }

    writeln!(d2, "}}").unwrap();

    writeln!(
        d2,
        "{} -> {}: \"{}\"",
        from,
        schema_node_name,
        edge_label.replace("\"", "\\\"")
    )
    .unwrap();

    writeln!(d2, "{schema_node_name} -> {to}").unwrap();
}

pub async fn to_d2(logical: &LogicalProgram) -> anyhow::Result<String> {
    let mut registry = new_registry();

    for (name, udf) in &logical.program_config.udf_dylibs {
        registry.load_dylib(name, udf).await?;
    }

    for udf in logical.program_config.python_udfs.values() {
        registry.add_python_udf(udf).await?;
    }

    let registry = Arc::new(registry);

    let mut d2 = String::new();

    for idx in logical.graph.node_indices() {
        let node = logical.graph.node_weight(idx).unwrap();

        if node.operator_chain.len() == 1 {
            let el = node.operator_chain.first();
            write_op(&mut d2, &registry, idx.index(), el);
        } else {
            writeln!(d2, "{}: {{", idx.index()).unwrap();
            for (i, (el, edge)) in node.operator_chain.iter().enumerate() {
                write_op(&mut d2, &registry, i, el);
                if let Some(edge) = edge {
                    write_edge(
                        &mut d2,
                        i,
                        i + 1,
                        i,
                        &LogicalEdgeType::Forward,
                        &edge.schema,
                    );
                }
            }
            writeln!(d2, "}}").unwrap();
        }
    }

    for idx in logical.graph.edge_indices() {
        let edge = logical.graph.edge_weight(idx).unwrap();
        let (from, to) = logical.graph.edge_endpoints(idx).unwrap();
        write_edge(
            &mut d2,
            from.index(),
            to.index(),
            idx.index(),
            &edge.edge_type,
            &edge.schema.schema,
        );
    }

    Ok(d2)
}

pub(crate) const MAX_TASK_ERROR_FIELD_BYTES: usize = 64 * 1024;

pub(crate) fn maybe_truncate(mut value: String, max_size_bytes: usize) -> String {
    let original_bytes = value.len();
    if original_bytes <= max_size_bytes {
        return value;
    }

    let suffix = format!(" [truncated; original_bytes={original_bytes}]");
    let mut end = max_size_bytes - suffix.len();
    while !value.is_char_boundary(end) {
        end -= 1;
    }

    value.truncate(end);
    value.push_str(&suffix);

    warn!(
        "Truncated oversized String from {} bytes to {} bytes: {}",
        original_bytes,
        value.len(),
        value
    );
    value
}

#[cfg(test)]
mod tests {
    use super::maybe_truncate;

    #[test]
    fn maybe_truncate_preserves_value_at_limit() {
        let value = "a".repeat(64);

        assert_eq!(maybe_truncate(value.clone(), value.len()), value);
    }

    #[test]
    fn maybe_truncate_respects_limit() {
        let value = "a".repeat(100);
        let max_size_bytes = 64;

        let truncated = maybe_truncate(value, max_size_bytes);

        assert_eq!(truncated.len(), max_size_bytes);
        assert_eq!(
            truncated,
            format!("{} [truncated; original_bytes=100]", "a".repeat(32))
        );
    }
}
