use crate::filesystem::config::{IcebergCatalog, IcebergSink};
use arrow::datatypes::Schema;
use arrow::datatypes::{DataType, Field, Fields, UnionFields};
use arroyo_storage::StorageProvider;
use iceberg::table::Table;
use iceberg::{Catalog, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::collections::HashMap;
use std::iter::once;
use std::sync::Arc;
use iceberg::spec::{DataFile, DataFileBuilder, DataFileFormat};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use itertools::Itertools;
use crate::filesystem::sink::FinishedFile;

const CONFIG_MAPPINGS: [(&str, &str); 4] = [
    ("s3.region", "aws_region"),
    ("s3.endpoint", "aws_endpoint"),
    ("s3.access-key-id", "aws_access_key_id"),
    ("s3.secret-access-key", "aws_secret_access_key"),
];

#[derive(Debug)]
pub struct IcebergTable {
    pub catalog: RestCatalog,
    pub storage_options: HashMap<String, String>,
    pub table_ident: TableIdent,
    pub location_path: Option<String>,
    pub table: Option<Table>,
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
                    catalog,
                    location_path: sink.location_path.clone(),
                    storage_options: sink.storage_options.clone(),
                    table_ident,
                    table: None,
                })
            }
        }
    }

    pub async fn load_or_create(&mut self, schema: &Schema) -> anyhow::Result<&Table> {
        if self.table.is_some() {
            return Ok(self.table.as_ref().unwrap());
        }

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
        schema: &Schema,
    ) -> anyhow::Result<StorageProvider> {
        let storage_options = self.storage_options.clone();
        let table = self.load_or_create(schema).await?;
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

    fn data_file_from_file(f: &FinishedFile) -> anyhow::Result<DataFile> {
        let metadata = f.metadata.as_ref()
            // this should never happen, implies a bug somewhere
            .expect("metadata was not recorded in file; cannot create iceberg commit");

        Ok(DataFileBuilder::default()
            .file_path(f.filename.clone())
            .file_format(DataFileFormat::Parquet)
            .record_count(metadata.row_count as u64)
            .file_size_in_bytes(f.size as u64)
            .build()?)
    }

    pub async fn commit(&mut self, storage_provider: &StorageProvider, finished_files: &[FinishedFile]) -> anyhow::Result<()> {
        let table = self.table.as_ref().expect("table must have been initialized");

        //

        // commit the transaction in the rest catalog
        let files: Vec<_> = finished_files.iter().map(|f| Self::data_file_from_file(f))
            .try_collect()?;

        let tx = Transaction::new(table);
        let tx = tx
            .fast_append()
            .add_data_files(files)
            .apply(tx)?;

        self.table = Some(tx.commit(&self.catalog)
            .await?);

        Ok(())
    }
}

/// Add metadata "PARQUET:field_id" to every field, which is required by the arrow -> iceberg
/// schema conversion code from iceberg-rust
fn add_parquet_field_ids(schema: &Schema) -> Schema {
    let mut next_id: i32 = 1;
    let new_fields: Fields = schema
        .fields()
        .iter()
        .map(|f| annotate_field(f, &mut next_id))
        .collect();

    // preserve top-level schema metadata
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
