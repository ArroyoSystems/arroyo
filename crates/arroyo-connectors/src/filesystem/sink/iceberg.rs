use crate::filesystem::config::{IcebergCatalog, IcebergSink};
use arrow::datatypes::Schema;
use iceberg::table::Table;
use iceberg::{Catalog, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use std::collections::HashMap;
use std::iter::once;

#[derive(Debug)]
pub struct IcebergTable {
    catalog: RestCatalog,
    table_ident: TableIdent,
    location_path: Option<String>,
    table: Option<Table>,
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

        match self.catalog.load_table(&self.table_ident).await {
            Ok(table) => {
                self.table = Some(table);
                return Ok(self.table.as_ref().unwrap());
            }
            Err(e) if e.kind() == iceberg::ErrorKind::NamespaceNotFound => {
                self.catalog
                    .create_namespace(self.table_ident.namespace(), HashMap::new())
                    .await?;
                // continue to table creation
            }
            Err(e) if e.kind() == iceberg::ErrorKind::TableNotFound => {
                // continue to table creation
            }
            Err(e) => {
                return Err(e.into());
            }
        }

        let schema = iceberg::arrow::arrow_schema_to_schema(schema)?;

        let table = self
            .catalog
            .create_table(
                self.table_ident.namespace(),
                TableCreation::builder()
                    .location_opt(self.location_path.clone())
                    .schema(schema)
                    // TODO: partitioning
                    .name(self.table_ident.name.clone())
                    .build(),
            )
            .await?;

        self.table = Some(table);

        Ok(self.table.as_ref().unwrap())
    }
}
