use super::FinishedFile;
use anyhow::Result;
use arrow::datatypes::Schema;
use arroyo_storage::{BackendConfig, R2Config, S3Config, StorageProvider};
use arroyo_types::to_millis;
use deltalake::aws::storage::S3StorageBackend;
use deltalake::TableProperty::{MinReaderVersion, MinWriterVersion};
use deltalake::{
    kernel::{Action, Add},
    operations::create::CreateBuilder,
    protocol::SaveMode,
    table::PeekCommit,
    DeltaTable, DeltaTableBuilder,
};
use itertools::Itertools;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};
use tracing::debug;
use url::Url;

pub(crate) async fn commit_files_to_delta(
    finished_files: &[FinishedFile],
    table: &mut DeltaTable,
    last_version: i64,
) -> Result<Option<i64>> {
    if finished_files.is_empty() {
        return Ok(None);
    }

    let add_actions = finished_files.iter().map(create_add_action).collect_vec();

    if let Some(new_version) = check_existing_files(table, last_version, finished_files).await? {
        return Ok(Some(new_version));
    }

    let new_version = commit_to_delta(table, add_actions).await?;
    Ok(Some(new_version))
}

pub(crate) async fn load_or_create_table(
    storage_provider: &StorageProvider,
    schema: &Schema,
) -> Result<DeltaTable> {
    deltalake::aws::register_handlers(None);
    deltalake::gcp::register_handlers(None);

    let empty_path = &Path::parse("").unwrap();

    let (backing_store, url): (Arc<dyn ObjectStore>, _) = match storage_provider.config() {
        BackendConfig::S3(S3Config { bucket, .. }) | BackendConfig::R2(R2Config { bucket, .. }) => {
            (
                Arc::new(S3StorageBackend::try_new(
                    storage_provider.get_backing_store(),
                    true,
                )?),
                format!(
                    "s3://{}/{}",
                    bucket,
                    storage_provider.qualify_path(empty_path)
                ),
            )
        }
        BackendConfig::GCS(gcs) => (
            storage_provider.get_backing_store(),
            format!(
                "gs://{}/{}",
                gcs.bucket,
                storage_provider.qualify_path(empty_path)
            ),
        ),
        BackendConfig::Local(_) => (storage_provider.get_backing_store(), "/".to_string()),
    };

    let mut delta = DeltaTableBuilder::from_uri(&url)
        .with_storage_backend(backing_store, Url::parse(storage_provider.canonical_url())?)
        .build()?;

    if delta.verify_deltatable_existence().await? {
        delta.load().await?;
        Ok(delta)
    } else {
        let delta_schema: deltalake::kernel::Schema = schema.try_into()?;
        Ok(CreateBuilder::new()
            .with_log_store(delta.log_store())
            .with_columns(delta_schema.fields().cloned())
            .with_configuration_property(MinReaderVersion, Some("3"))
            .with_configuration_property(MinWriterVersion, Some("7"))
            .await?)
    }
}

fn create_add_action(file: &FinishedFile) -> Action {
    debug!("creating add action for file {:?}", file);

    let subpath = file.filename.trim_start_matches('/');

    Action::Add(Add {
        path: subpath.to_string(),
        size: file.size as i64,
        partition_values: HashMap::new(),
        modification_time: to_millis(SystemTime::now()) as i64,
        data_change: true,
        ..Default::default()
    })
}

async fn check_existing_files(
    table: &mut DeltaTable,
    last_version: i64,
    finished_files: &[FinishedFile],
) -> Result<Option<i64>> {
    if last_version >= table.version() {
        return Ok(None);
    }

    let files: HashSet<_> = finished_files
        .iter()
        .map(|file| file.filename.to_string())
        .collect();

    let mut version_to_check = last_version;
    while let PeekCommit::New(version, actions) = table.peek_next_commit(version_to_check).await? {
        for action in actions {
            if let Action::Add(add) = action {
                if files.contains(&add.path) {
                    return Ok(Some(version));
                }
            }
        }
        version_to_check = version;
    }
    Ok(None)
}

async fn commit_to_delta(table: &mut DeltaTable, add_actions: Vec<Action>) -> Result<i64> {
    Ok(deltalake::operations::transaction::CommitBuilder::default()
        .with_actions(add_actions)
        .build(
            Some(table.snapshot()?),
            table.log_store(),
            deltalake::protocol::DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
        )
        .await?
        .version)
}
