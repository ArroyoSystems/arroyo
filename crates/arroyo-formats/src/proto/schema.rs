use anyhow::{anyhow, bail, Context};
use arrow_schema::{DataType, Field, Schema};
use arroyo_types::ArroyoExtensionType;
use prost_reflect::{Cardinality, DescriptorPool, FieldDescriptor, Kind, MessageDescriptor};
use regex::Regex;
use std::collections::HashMap;
use std::env::temp_dir;
use std::path::Path;
use std::sync::Arc;
use tracing::warn;
use uuid::Uuid;

fn protobuf_to_arrow_datatype(
    field: &FieldDescriptor,
    in_list: bool,
) -> (DataType, Option<ArroyoExtensionType>) {
    if field.is_list() && !in_list {
        let (dt, ext) = protobuf_to_arrow_datatype(field, true);
        return (
            DataType::List(Arc::new(ArroyoExtensionType::add_metadata(
                ext,
                Field::new("item", dt, true),
            ))),
            None,
        );
    }
    (
        match field.kind() {
            Kind::Bool => DataType::Boolean,
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => DataType::Int32,
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => DataType::Int64,
            Kind::Uint32 | Kind::Fixed32 => DataType::UInt32,
            Kind::Uint64 | Kind::Fixed64 => DataType::UInt64,
            Kind::Float => DataType::Float32,
            Kind::Double => DataType::Float64,
            Kind::String | Kind::Bytes => DataType::Utf8,
            Kind::Message(message) => {
                if field.is_map() {
                    // we don't currently support maps so treat maps as raw json
                    return (DataType::Utf8, Some(ArroyoExtensionType::JSON));
                } else {
                    DataType::Struct(fields_for_message(&message).into())
                }
            }
            Kind::Enum(_) => DataType::Utf8,
        },
        None,
    )
}

fn fields_for_message(message: &MessageDescriptor) -> Vec<Arc<Field>> {
    message
        .fields()
        .map(|f| {
            let (t, ext) = protobuf_to_arrow_datatype(&f, false);
            Arc::new(ArroyoExtensionType::add_metadata(
                ext,
                Field::new(f.name(), t, is_nullable(&f)),
            ))
        })
        .collect()
}

pub fn get_pool(encoded: &[u8]) -> anyhow::Result<DescriptorPool> {
    let mut pool = DescriptorPool::global();
    pool.decode_file_descriptor_set(encoded)?;
    Ok(pool)
}

/// Computes an Arrow schema from a protobuf schema
pub fn protobuf_to_arrow(proto_schema: &MessageDescriptor) -> anyhow::Result<Schema> {
    let fields = fields_for_message(proto_schema);
    Ok(Schema::new(fields))
}

fn is_nullable(field: &FieldDescriptor) -> bool {
    field.cardinality() == Cardinality::Optional || field.is_list() || field.is_map()
}

fn is_safe_path(base: &Path, path: &Path) -> bool {
    path.is_relative()
        && path
            .components()
            .all(|c| matches!(c, std::path::Component::Normal(_)))
        && base.join(path).starts_with(base)
}

async fn write_files(base_path: &Path, files: &HashMap<String, String>) -> anyhow::Result<()> {
    for (path_str, content) in files {
        let path = Path::new(&path_str);
        if !is_safe_path(base_path, path) {
            bail!("invalid path '{path_str}' for proto file; must be a normal, relative path");
        }

        let full_path = base_path.join(path);

        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(full_path, content).await?;
    }

    Ok(())
}

#[allow(async_fn_in_trait)]
pub trait ProtoSchemaResolver {
    async fn resolve(&self, path: &str) -> anyhow::Result<Option<String>>;
}

pub struct DefaultProtoSchemaResolver {}

impl ProtoSchemaResolver for DefaultProtoSchemaResolver {
    async fn resolve(&self, _path: &str) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
}

pub async fn schema_file_to_descriptor(
    schema: &str,
    dependencies: &HashMap<String, String>,
) -> anyhow::Result<Vec<u8>> {
    schema_file_to_descriptor_with_resolver(schema, dependencies, DefaultProtoSchemaResolver {})
        .await
}

pub async fn schema_file_to_descriptor_with_resolver<R: ProtoSchemaResolver>(
    schema: &str,
    dependencies: &HashMap<String, String>,
    schema_resolver: R,
) -> anyhow::Result<Vec<u8>> {
    let protoc = prost_build::protoc_from_env();
    let uuid = Uuid::new_v4().to_string();
    let dir = temp_dir().join(uuid);

    tokio::fs::create_dir_all(&dir).await?;

    write_files(&dir, dependencies).await?;
    let input = dir.join("schema.proto");
    tokio::fs::write(&input, schema).await?;

    let output_file = dir.join("schema.bin");

    let import_regex = Regex::new(r"([\w\-_.]+\.proto+): File not found.").unwrap();

    for _ in 0..10 {
        let output = tokio::process::Command::new(&protoc)
            .current_dir(&dir)
            .arg("--descriptor_set_out=schema.bin")
            .arg("--include_imports")
            .arg("-I")
            .arg(".")
            .arg("schema.proto")
            .output()
            .await
            .map_err(|e| anyhow!("unable to compile protobuf; is protoc installed? {e}"))?;

        if !output.status.success() {
            let output = String::from_utf8_lossy(&output.stderr);
            let imports: Vec<_> = output
                .lines()
                .filter_map(|l| {
                    import_regex
                        .captures(l)
                        .map(|c| c.get(1).unwrap().as_str().to_string())
                })
                .collect();

            if imports.is_empty() {
                bail!("failed to compile proto: {}", output);
            }

            let mut extra_deps = HashMap::new();
            for import in imports {
                let schema = schema_resolver
                    .resolve(&import)
                    .await
                    .map_err(|e| anyhow!("Could not resolve protobuf import {import}: {e}"))?
                    .ok_or_else(|| anyhow!("Protobuf import {import} not found"))?;
                extra_deps.insert(import, schema);
            }

            write_files(&dir, &extra_deps).await?;
            continue;
        }

        let bin = tokio::fs::read(&output_file)
            .await
            .context("failed to read protoc output");

        if let Err(e) = tokio::fs::remove_dir_all(&dir).await {
            warn!(
                "Could not clean up temp directory '{:?}' from protobuf compilation: {}",
                dir, e
            );
        }

        return bin;
    }

    bail!("Protobuf imports nested more than 10 layers deep, which is not supported");
}
