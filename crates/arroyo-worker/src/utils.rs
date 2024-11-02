use crate::engine::construct_operator;
use arrow_schema::Schema;
use arroyo_datastream::logical::LogicalProgram;
use arroyo_df::physical::new_registry;
use std::fmt::Write;
use std::sync::Arc;

fn format_arrow_schema_fields(schema: &Schema) -> Vec<(String, String)> {
    schema
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().to_string()))
        .collect()
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
        let operator = construct_operator(
            node.operator_name,
            node.operator_config.clone(),
            registry.clone(),
        );
        let display = operator.display();

        let mut label = format!("### {} ({})", operator.name(), &display.name);
        for (field, value) in display.fields {
            label.push_str(&format!("\n- **{}**: {}", field, value));
        }

        writeln!(
            &mut d2,
            "{}: {{
  label: |markdown
{}
  |
  shape: rectangle
}}",
            idx.index(),
            label
        )
        .unwrap();
    }

    for idx in logical.graph.edge_indices() {
        let edge = logical.graph.edge_weight(idx).unwrap();
        let (from, to) = logical.graph.edge_endpoints(idx).unwrap();

        let edge_label = format!("{}", edge.edge_type);

        let schema_node_name = format!("schema_{}", idx.index());
        let schema_fields = format_arrow_schema_fields(&edge.schema.schema);

        writeln!(&mut d2, "{}: {{", schema_node_name).unwrap();
        writeln!(&mut d2, "  shape: sql_table").unwrap();

        for (field_name, field_type) in schema_fields {
            writeln!(
                &mut d2,
                "  \"{}\": \"{}\"",
                field_name.replace("\"", "\\\""),
                field_type.replace("\"", "\\\"")
            )
            .unwrap();
        }

        writeln!(&mut d2, "}}").unwrap();

        writeln!(
            &mut d2,
            "{} -> {}: \"{}\"",
            from.index(),
            schema_node_name,
            edge_label.replace("\"", "\\\"")
        )
        .unwrap();

        writeln!(&mut d2, "{} -> {}", schema_node_name, to.index()).unwrap();
    }

    Ok(d2)
}
