use super::FinishedFile;
use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use arroyo_storage::{get_current_credentials, StorageProvider};
use arroyo_types::to_millis;
use deltalake::{
    operations::create::CreateBuilder,
    protocol::{Action, Add, SaveMode},
    table::{builder::s3_storage_options::AWS_S3_ALLOW_UNSAFE_RENAME, PeekCommit},
    DeltaTableBuilder,
};
use object_store::{aws::AmazonS3ConfigKey, path::Path};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::SystemTime,
};
use tracing::info;

pub(crate) async fn commit_files_to_delta(
    finished_files: Vec<FinishedFile>,
    relative_table_path: Path,
    storage_provider: Arc<StorageProvider>,
    last_version: i64,
    schema: Schema,
) -> Result<Option<i64>> {
    if finished_files.is_empty() {
        return Ok(None);
    }

    let add_actions = create_add_actions(&finished_files, &relative_table_path)?;
    let table_path = build_table_path(&storage_provider, &relative_table_path);
    let storage_options = configure_storage_options(&table_path, storage_provider.clone()).await?;
    let mut table = load_or_create_table(&table_path, storage_options.clone(), &schema).await?;

    if let Some(new_version) = check_existing_files(
        &mut table,
        last_version,
        &finished_files,
        &relative_table_path,
    )
    .await?
    {
        return Ok(Some(new_version));
    }

    let new_version = commit_to_delta(table, add_actions).await?;
    Ok(Some(new_version))
}

async fn load_or_create_table(
    table_path: &str,
    storage_options: HashMap<String, String>,
    schema: &Schema,
) -> Result<deltalake::DeltaTable> {
    match DeltaTableBuilder::from_uri(table_path)
        .with_storage_options(storage_options.clone())
        .load()
        .await
    {
        Ok(table) => Ok(table),
        Err(deltalake::DeltaTableError::NotATable(_)) => {
            create_new_table(table_path, storage_options, schema).await
        }
        Err(err) => Err(err.into()),
    }
}

async fn create_new_table(
    table_path: &str,
    storage_options: HashMap<String, String>,
    schema: &Schema,
) -> Result<deltalake::DeltaTable> {
    let delta_object_store = DeltaTableBuilder::from_uri(table_path)
        .with_storage_options(storage_options)
        .build_storage()?;
    let delta_schema: deltalake::Schema = (schema).try_into()?;
    CreateBuilder::new()
        .with_object_store(delta_object_store)
        .with_columns(delta_schema.get_fields().clone())
        .await
        .map_err(Into::into)
}

async fn configure_storage_options(
    table_path: &str,
    storage_provider: Arc<StorageProvider>,
) -> Result<HashMap<String, String>> {
    let mut options = storage_provider.storage_options().clone();
    if table_path.starts_with("s3://") {
        update_s3_credentials(&mut options).await?;
    }
    Ok(options)
}

async fn update_s3_credentials(options: &mut HashMap<String, String>) -> Result<()> {
    if !options.contains_key(AmazonS3ConfigKey::SecretAccessKey.as_ref()) {
        let tmp_credentials = get_current_credentials().await?;
        options.insert(
            AmazonS3ConfigKey::AccessKeyId.as_ref().to_string(),
            tmp_credentials.key_id.clone(),
        );
        options.insert(
            AmazonS3ConfigKey::SecretAccessKey.as_ref().to_string(),
            tmp_credentials.secret_key.clone(),
        );
    }
    options.insert(AWS_S3_ALLOW_UNSAFE_RENAME.to_string(), "true".to_string());
    Ok(())
}

fn create_add_actions(
    finished_files: &[FinishedFile],
    relative_table_path: &Path,
) -> Result<Vec<Action>> {
    finished_files
        .iter()
        .map(|file| create_add_action(file, relative_table_path))
        .collect()
}

fn create_add_action(file: &FinishedFile, relative_table_path: &Path) -> Result<Action> {
    info!(
        "creating add action for file {:?}, relative table path {}",
        file, relative_table_path
    );
    let subpath = file
        .filename
        .strip_prefix(&relative_table_path.to_string())
        .context(format!(
            "File {} is not in table {}",
            file.filename,
            relative_table_path.to_string()
        ))?;
    Ok(Action::add(Add {
        path: subpath.to_string(),
        size: file.size as i64,
        partition_values: HashMap::new(),
        modification_time: to_millis(SystemTime::now()) as i64,
        data_change: true,
        ..Default::default()
    }))
}

async fn check_existing_files(
    table: &mut deltalake::DeltaTable,
    last_version: i64,
    finished_files: &[FinishedFile],
    relative_table_path: &Path,
) -> Result<Option<i64>> {
    if last_version >= table.version() {
        return Ok(None);
    }

    let files: HashSet<_> = finished_files
        .iter()
        .map(|file| {
            file.filename
                .strip_prefix(&relative_table_path.to_string())
                .unwrap()
                .to_string()
        })
        .collect();

    let mut version_to_check = last_version;
    while let PeekCommit::New(version, actions) = table.peek_next_commit(version_to_check).await? {
        for action in actions {
            if let Action::add(add) = action {
                if files.contains(&add.path) {
                    return Ok(Some(version));
                }
            }
        }
        version_to_check = version;
    }
    Ok(None)
}

async fn commit_to_delta(table: deltalake::DeltaTable, add_actions: Vec<Action>) -> Result<i64> {
    deltalake::operations::transaction::commit(
        table.object_store().as_ref(),
        &add_actions,
        deltalake::protocol::DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        },
        table.get_state(),
        None,
    )
    .await
    .map_err(Into::into)
}

fn build_table_path(storage_provider: &StorageProvider, relative_table_path: &Path) -> String {
    format!(
        "{}/{}",
        storage_provider.canonical_url(),
        relative_table_path
    )
}
