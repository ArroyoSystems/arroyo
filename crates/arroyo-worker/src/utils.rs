use crate::engine::construct_operator;
use arrow_schema::Schema;
use arroyo_datastream::logical::{ChainedLogicalOperator, LogicalEdgeType, LogicalProgram};
use arroyo_df::physical::new_registry;
use arroyo_operator::operator::Registry;
use std::fmt::Write;
use std::sync::Arc;

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

    let mut label = format!("### {} ({})", operator.name(), &display.name);
    for (field, value) in display.fields {
        label.push_str(&format!("\n## {}\n\n{}", field, value));
    }

    writeln!(
        d2,
        "{}: {{
  label: |markdown
{}
  |
  shape: rectangle
}}",
        idx, label
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
    let edge_label = format!("{}", edge_type);

    let schema_node_name = format!("schema_{}", edge_idx);
    let schema_fields = format_arrow_schema_fields(schema);

    writeln!(d2, "{}: {{", schema_node_name).unwrap();
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

    writeln!(d2, "{} -> {}", schema_node_name, to).unwrap();
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
