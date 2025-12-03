pub mod metadata;
pub mod schema;
pub mod transforms;

use crate::filesystem::config::{IcebergCatalog, IcebergPartitioning, IcebergSink};
use crate::filesystem::sink::iceberg::metadata::build_datafile_from_meta;
use crate::filesystem::sink::iceberg::schema::add_parquet_field_ids;
use crate::filesystem::sink::FinishedFile;
use arrow::datatypes::Schema;
use arroyo_storage::StorageProvider;
use arroyo_types::TaskInfo;
use iceberg::spec::ManifestFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use itertools::Itertools;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::iter::once;
use std::sync::Arc;
use tracing::{debug, info};
use uuid::Uuid;

const CONFIG_MAPPINGS: [(&str, &str); 4] = [
    ("s3.region", "aws_region"),
    ("s3.endpoint", "aws_endpoint"),
    ("s3.access-key-id", "aws_access_key_id"),
    ("s3.secret-access-key", "aws_secret_access_key"),
];

const ARROYO_COMMIT_ID: &str = "arroyo.commit-id";

#[derive(Debug)]
pub struct IcebergTable {
    pub task_info: Option<Arc<TaskInfo>>,
    pub catalog: RestCatalog,
    pub storage_options: HashMap<String, String>,
    pub table_ident: TableIdent,
    pub location_path: Option<String>,
    pub table: Option<Table>,
    pub manifest_files: Vec<ManifestFile>,
    pub partitioning: IcebergPartitioning,
}

pub fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

/// Unique transaction ID per epoch so we can always determine if we've already committed for
/// this epoch without needing to scan files
fn transaction_id(task_info: &TaskInfo, epoch: u32, table_uuid: Uuid) -> String {
    let mut h = Sha256::new();
    h.update("arroyo-txid-v1");
    h.update([0]);
    h.update(&task_info.job_id);
    h.update([0]);
    h.update(&task_info.operator_id);
    h.update([0]);
    h.update(epoch.to_be_bytes());
    h.update([0]);
    h.update(table_uuid.as_bytes());

    let digest = h.finalize();
    let short = &digest[..16]; // 16 bytes is enough

    format!("tx-{}", to_hex(short))
}

impl IcebergTable {
    pub fn new(catalog: &IcebergCatalog, sink: &IcebergSink) -> anyhow::Result<Self> {
        match catalog {
            IcebergCatalog::Rest(rest) => {
                let mut props = HashMap::new();
                if let Some(token) = &rest.token {
                    props.insert("token".to_string(), token.sub_env_vars()?);
                }

                for (k, v) in &sink.storage_options {
                    if let Some((mapped, _)) = CONFIG_MAPPINGS.iter().find(|(_, n)| n == k) {
                        props.insert(mapped.to_string(), v.to_string());
                    }
                }

                let config = RestCatalogConfig::builder()
                    .uri(rest.url.clone())
                    .warehouse_opt(rest.warehouse.clone())
                    .props(props)
                    .build();

                let catalog = RestCatalog::new(config);

                let table_ident = TableIdent::from_strs(
                    sink.namespace
                        .as_str()
                        .split(".")
                        .chain(once(sink.table_name.as_str())),
                )?;

                Ok(Self {
                    task_info: None,
                    catalog,
                    location_path: sink.location_path.clone(),
                    storage_options: sink.storage_options.clone(),
                    table_ident,
                    table: None,
                    manifest_files: vec![],
                    partitioning: sink.partitioning.clone(),
                })
            }
        }
    }

    pub async fn load_or_create(
        &mut self,
        task_info: Arc<TaskInfo>,
        schema: &Schema,
    ) -> anyhow::Result<&Table> {
        if self.table.is_some() {
            return Ok(self.table.as_ref().unwrap());
        }

        self.task_info = Some(task_info);

        if !self
            .catalog
            .namespace_exists(self.table_ident.namespace())
            .await?
            && let Err(e) = self
                .catalog
                .create_namespace(self.table_ident.namespace(), HashMap::new())
                .await
                && e.kind() != iceberg::ErrorKind::NamespaceAlreadyExists {
                    return Err(e.into());
                }

        let table = if !self.catalog.table_exists(&self.table_ident).await? {
            let schema_with_ids = add_parquet_field_ids(schema);
            let iceberg_schema = iceberg::arrow::arrow_schema_to_schema(&schema_with_ids)?;

            let partition_spec = self
                .partitioning
                .as_partition_spec(Arc::new(iceberg_schema.clone()))?;

            match self
                .catalog
                .create_table(
                    self.table_ident.namespace(),
                    TableCreation::builder()
                        .location_opt(self.location_path.clone())
                        .schema(iceberg_schema)
                        .partition_spec(partition_spec)
                        .name(self.table_ident.name.clone())
                        .build(),
                )
                .await
            {
                Ok(table) => table,
                Err(e) if e.kind() == iceberg::ErrorKind::TableAlreadyExists => {
                    self.catalog.load_table(&self.table_ident).await?
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        } else {
            self.catalog.load_table(&self.table_ident).await?
        };

        self.table = Some(table);

        Ok(self.table.as_ref().unwrap())
    }

    pub async fn get_storage_provider(
        &mut self,
        task_info: Arc<TaskInfo>,
        schema: &Schema,
    ) -> anyhow::Result<StorageProvider> {
        let storage_options = self.storage_options.clone();
        let table = self.load_or_create(task_info, schema).await?;
        let (_, mut config) = table.file_io().clone().into_builder().into_parts();

        let mut our_config = HashMap::new();
        for (from, to) in CONFIG_MAPPINGS {
            if let Some(v) = config.remove(from) {
                our_config.insert(to.to_string(), v);
            }
        }

        if let Some(path_style) = our_config.remove("s3.path-style-access") {
            let path_style = path_style.to_lowercase();
            let enabled = path_style == "true" || path_style == "t" || path_style == "1";
            our_config.insert(
                "virtual_hosted_style_request".to_string(),
                (!enabled).to_string(),
            );
        }

        storage_options.into_iter().for_each(|(k, v)| {
            our_config.insert(k, v);
        });

        let mut location = table.metadata().location().to_string();
        if !location.ends_with('/') {
            location.push('/');
        }
        location.push_str("data/");

        Ok(StorageProvider::for_url_with_options(&location, our_config).await?)
    }

    pub async fn commit(
        &mut self,
        epoch: u32,
        finished_files: &[FinishedFile],
    ) -> anyhow::Result<()> {
        if finished_files.is_empty() {
            debug!("no new files, skipping commit");
            return Ok(());
        }

        let table = self.table.as_ref().expect("table must have been loaded");

        let tx_id = transaction_id(
            self.task_info.as_ref().unwrap(),
            epoch,
            table.metadata().uuid(),
        );

        if let Some(current_snapshot) = table.metadata().current_snapshot()
            && let Some(existing_tx_id) = current_snapshot
                .summary()
                .additional_properties
                .get(ARROYO_COMMIT_ID)
                && *existing_tx_id == tx_id {
                    // we've already committed this epoch (but crashed before it could be records), so we're good
                    info!("epoch {epoch} already committed to iceberg, skipping");
                    return Ok(());
                }

        let partition_spec_id = table.metadata().default_partition_spec_id();
        let files: Vec<_> = finished_files
            .iter()
            .map(|f| {
                build_datafile_from_meta(
                    table.metadata().current_schema(),
                    f.metadata
                        .as_ref()
                        .expect("metadata not set for Iceberg write"),
                    &self.partitioning,
                    format!(
                        "{}/data/{}",
                        table.metadata().location(),
                        f.filename.clone()
                    ),
                    f.size as u64,
                    partition_spec_id,
                )
            })
            .try_collect()?;

        // commit the transaction in the rest catalog
        debug!(
            message = "starting iceberg commit",
            files = files.len(),
            manifest_files = self.manifest_files.len()
        );
        let tx = Transaction::new(table);
        let tx = tx
            .fast_append()
            .set_snapshot_properties(
                [(ARROYO_COMMIT_ID.to_string(), tx_id)]
                    .into_iter()
                    .collect(),
            )
            .add_data_files(files)
            .with_check_duplicate(false)
            .apply(tx)?;

        tx.commit(&self.catalog).await?;
        debug!("finished iceberg commit");
        // the tx.commit call returns the table but the FileIO somehow ends up misconfigured in such
        // a way that breaks future IO operations
        self.table = Some(self.catalog.load_table(&self.table_ident).await?);

        Ok(())
    }
}
