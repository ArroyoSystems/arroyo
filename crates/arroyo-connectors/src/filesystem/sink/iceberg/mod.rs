pub mod metadata;

use crate::filesystem::config::{IcebergCatalog, IcebergSink};
use crate::filesystem::sink::iceberg::metadata::build_datafile_from_meta;
use crate::filesystem::sink::FinishedFile;
use arrow::datatypes::Schema;
use arrow::datatypes::{DataType, Field, Fields, UnionFields};
use arroyo_storage::StorageProvider;
use arroyo_types::TaskInfo;
use iceberg::spec::ManifestFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use itertools::Itertools;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
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
        {
            if let Err(e) = self
                .catalog
                .create_namespace(self.table_ident.namespace(), HashMap::new())
                .await
            {
                if e.kind() != iceberg::ErrorKind::NamespaceAlreadyExists {
                    return Err(e.into());
                }
            }
        }

        let table = if !self.catalog.table_exists(&self.table_ident).await? {
            let schema_with_ids = add_parquet_field_ids(schema);
            let iceberg_schema = iceberg::arrow::arrow_schema_to_schema(&schema_with_ids)?;

            match self
                .catalog
                .create_table(
                    self.table_ident.namespace(),
                    TableCreation::builder()
                        .location_opt(self.location_path.clone())
                        .schema(iceberg_schema)
                        // TODO: partitioning
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

        if let Some(current_snapshot) = table.metadata().current_snapshot() {
            if let Some(existing_tx_id) = current_snapshot
                .summary()
                .additional_properties
                .get(ARROYO_COMMIT_ID)
            {
                if *existing_tx_id == tx_id {
                    // we've already committed this epoch (but crashed before it could be records), so we're good
                    info!("epoch {epoch} already committed to iceberg, skipping");
                    return Ok(());
                }
            }
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

/// Add metadata "PARQUET:field_id" to every field , which is required by the arrow -> iceberg
/// schema conversion code from iceberg-rust
pub fn add_parquet_field_ids(schema: &Schema) -> Schema {
    let mut next_id: i32 = 1;
    let new_fields: Fields = schema
        .fields()
        .iter()
        .map(|f| annotate_field(f, &mut next_id))
        .collect();

    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

fn annotate_field(field: &Field, next_id: &mut i32) -> Field {
    let new_dt = match field.data_type() {
        DataType::Struct(children) => {
            let ch: Fields = children
                .iter()
                .map(|c| annotate_field(c, next_id))
                .collect();
            DataType::Struct(ch)
        }
        DataType::List(child) => {
            let c = Arc::new(annotate_field(child, next_id));
            DataType::List(c)
        }
        DataType::LargeList(child) => {
            let c = Arc::new(annotate_field(child, next_id));
            DataType::LargeList(c)
        }
        DataType::FixedSizeList(child, len) => {
            let c = Arc::new(annotate_field(child, next_id));
            DataType::FixedSizeList(c, *len)
        }
        DataType::Map(entry_field, keys_sorted) => {
            let e = Arc::new(annotate_field(entry_field, next_id));
            DataType::Map(e, *keys_sorted)
        }
        DataType::Union(fields, mode) => {
            let new_fields_vec: Vec<Field> = fields
                .iter()
                .map(|(_, f)| annotate_field(f, next_id))
                .collect();
            let type_ids: Vec<i8> = fields.iter().map(|(id, _)| id).collect();
            let uf = UnionFields::new(type_ids, new_fields_vec);
            DataType::Union(uf, *mode)
        }
        other => other.clone(),
    };

    let mut md = field.metadata().clone();
    md.insert(PARQUET_FIELD_ID_META_KEY.to_string(), next_id.to_string());
    *next_id += 1;

    Field::new(field.name(), new_dt, field.is_nullable()).with_metadata(md)
}

#[cfg(test)]
mod tests {
    use super::add_parquet_field_ids;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_adds_parquet_field_ids_recursively() {
        let struct_field = Field::new(
            "props",
            DataType::Struct(
                vec![
                    Field::new("k", DataType::Utf8, true),
                    Field::new("v", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        );

        let list_field = Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        );

        let id_field = Field::new("id", DataType::Int64, false);
        let price_field = Field::new("price", DataType::Decimal128(10, 2), true);
        let active_field = Field::new("active", DataType::Boolean, true);
        let ts_field = Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        );

        let schema = Schema::new(vec![
            id_field,
            price_field,
            active_field,
            ts_field,
            list_field,
            struct_field,
        ]);

        let out = add_parquet_field_ids(&schema);

        let has_id = |f: &Field| {
            f.metadata()
                .get("PARQUET:field_id")
                .map(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit()))
                .unwrap_or(false)
        };

        let fields = out.fields();
        assert!(has_id(&fields[0]), "id missing PARQUET:field_id");
        assert!(has_id(&fields[1]), "price missing PARQUET:field_id");
        assert!(has_id(&fields[2]), "active missing PARQUET:field_id");
        assert!(has_id(&fields[3]), "ts missing PARQUET:field_id");
        assert!(has_id(&fields[4]), "tags (list) missing PARQUET:field_id");
        assert!(
            has_id(&fields[5]),
            "props (struct) missing PARQUET:field_id"
        );

        if let DataType::List(inner) = fields[4].data_type() {
            assert!(has_id(inner), "list inner field missing PARQUET:field_id");
        } else {
            panic!("tags not a List");
        }

        if let DataType::Struct(children) = fields[5].data_type() {
            assert!(has_id(&children[0]), "struct.k missing PARQUET:field_id");
            assert!(has_id(&children[1]), "struct.v missing PARQUET:field_id");
        } else {
            panic!("props not a Struct");
        }
    }
}
