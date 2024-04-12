pub fn cargo_toml(dependencies: &str) -> String {
    format!(
        r#"
[package]
name = "udf"
version = "1.0.0"
edition = "2021"

{}
        "#,
        dependencies
    )
}
