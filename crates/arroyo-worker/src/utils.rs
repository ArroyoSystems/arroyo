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

pub fn to_d2(logical: &LogicalProgram) -> String {
    let registry = Arc::new(new_registry());
    assert!(
        logical.program_config.udf_dylibs.is_empty(),
        "UDFs not supported"
    );
    assert!(
        logical.program_config.python_udfs.is_empty(),
        "UDFs not supported"
    );

    let mut d2 = String::new();

    // Nodes
    for idx in logical.graph.node_indices() {
        let node = logical.graph.node_weight(idx).unwrap();
        let operator = construct_operator(
            node.operator_name,
            node.operator_config.clone(),
            registry.clone(),
        );
        let display = operator.display();

        // Create a Markdown-formatted label with operator details
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

    // Edges and Schema Nodes
    for idx in logical.graph.edge_indices() {
        let edge = logical.graph.edge_weight(idx).unwrap();
        let (from, to) = logical.graph.edge_endpoints(idx).unwrap();

        // Edge label (could be empty or minimal)
        let edge_label = format!("{}", edge.edge_type);

        // Create a schema node using sql_table shape
        let schema_node_name = format!("schema_{}", idx.index());
        let schema_fields = format_arrow_schema_fields(&edge.schema.schema);

        // Begin schema node definition
        writeln!(&mut d2, "{}: {{", schema_node_name).unwrap();
        writeln!(&mut d2, "  shape: sql_table").unwrap();

        // Add fields to the schema node
        for (field_name, field_type) in schema_fields {
            writeln!(
                &mut d2,
                "  \"{}\": \"{}\"", // Field definition
                field_name.replace("\"", "\\\""),
                field_type.replace("\"", "\\\"")
            )
            .unwrap();
        }

        // End schema node definition
        writeln!(&mut d2, "}}").unwrap();

        // Connect source operator to schema node
        writeln!(
            &mut d2,
            "{} -> {}: \"{}\"",
            from.index(),
            schema_node_name,
            edge_label.replace("\"", "\\\"")
        )
        .unwrap();

        // Connect schema node to destination operator
        writeln!(&mut d2, "{} -> {}", schema_node_name, to.index()).unwrap();
    }

    d2
}
