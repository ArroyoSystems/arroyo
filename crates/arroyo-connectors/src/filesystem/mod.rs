mod config;
pub mod delta;
pub(crate) mod iceberg;
mod sink;
mod source;

use self::sink::{
    JsonFileSystemSink, LocalJsonFileSystemSink, LocalParquetFileSystemSink, ParquetFileSystemSink,
};
use crate::filesystem::config::*;
use crate::filesystem::source::FileSystemSourceFunc;
use crate::{render_schema, EmptyConfig};
use anyhow::{anyhow, bail, Result};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arroyo_operator::connector::Connection;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::formats::Format;
use arroyo_rpc::{ConnectorOptions, OperatorConfig, TIMESTAMP_FIELD};
use arroyo_storage::{BackendConfig, StorageProvider};
use arroyo_types::TaskInfo;
use std::collections::{HashMap};
use std::sync::Arc;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{col, concat, lit, to_char};
use itertools::Itertools;

const ICON: &str = include_str!("./filesystem.svg");

pub enum TableFormat {
    None {
        partitioning: PartitioningConfig
    },
    Delta {
        partitioning: PartitioningConfig
    },
    Iceberg(Box<sink::iceberg::IcebergTable>),
}

impl TableFormat {
    pub async fn get_storage_provider(
        &mut self,
        task_info: Arc<TaskInfo>,
        config: &FileSystemSink,
        schema: &Schema,
    ) -> anyhow::Result<StorageProvider> {
        Ok(match self {
            TableFormat::None { .. } | TableFormat::Delta { .. } => {
                StorageProvider::for_url_with_options(&config.path, config.storage_options.clone())
                    .await?
            }
            TableFormat::Iceberg(table) => table.get_storage_provider(task_info, schema).await?,
        })
    }
}

fn partitioner_from_config(
    config: &PartitioningConfig,
    schema: SchemaRef,
) -> Result<Option<Expr>> {
    Ok(match (&config.time_pattern, &config.fields) {
        (None, fields) if !fields.is_empty() => {
            Some(field_logical_expression(schema.clone(), fields)?)
        }
        (None, _) => None,
        (Some(pattern), fields) if !fields.is_empty() => {
            Some(partition_string_for_fields_and_time(schema, fields, pattern)?)
        }
        (Some(pattern), _) => Some(timestamp_logical_expression(pattern)),
    })
}

fn partition_string_for_fields_and_time(
    schema: SchemaRef,
    partition_fields: &[String],
    time_partition_pattern: &str,
) -> Result<Expr> {
    let field_function = field_logical_expression(schema.clone(), partition_fields)?;
    let time_function = timestamp_logical_expression(time_partition_pattern);
    let function = concat(vec![
        time_function,
        Expr::Literal(ScalarValue::Utf8(Some("/".to_string())), None),
        field_function,
    ]);
    Ok(function)
}


fn field_logical_expression(schema: SchemaRef, partition_fields: &[String]) -> Result<Expr> {
    let columns_as_string = partition_fields
        .iter()
        .map(|field| {
            let field = schema.field_with_name(field)?;
            let column_expr = col(field.name());
            let expr = match field.data_type() {
                DataType::Utf8 => column_expr,
                _ => Expr::Cast(datafusion::logical_expr::Cast {
                    expr: Box::new(column_expr),
                    data_type: DataType::Utf8,
                }),
            };
            Ok((field.name(), expr))
        })
        .collect::<Result<Vec<_>>>()?;

    let function = concat(
        columns_as_string
            .into_iter()
            .enumerate()
            .flat_map(|(i, (name, expr))| {
                let preamble = if i == 0 {
                    format!("{name}=")
                } else {
                    format!("/{name}=")
                };
                vec![Expr::Literal(ScalarValue::Utf8(Some(preamble)), None), expr]
            })
            .collect(),
    );

    Ok(function)
}

pub(crate) fn timestamp_logical_expression(time_partition_pattern: &str) -> Expr {
    to_char(col(TIMESTAMP_FIELD), lit(time_partition_pattern))
}


pub fn make_sink(
    sink: FileSystemSink,
    config: OperatorConfig,
    table_format: TableFormat,
    connection_id: Option<String>,
) -> Result<ConstructedOperator> {
    let is_local = match table_format {
        TableFormat::None { .. } | TableFormat::Delta { .. } => {
            let backend_config = BackendConfig::parse_url(&sink.path, true)?;
            backend_config.is_local()
        }
        TableFormat::Iceberg { .. } => {
            // for iceberg, there's no way for us to know the path (and whether it's local or not)
            // until we connect to the catalog, which we can't do in a non-async context
            // so for now we'll just support object storage
            // TODO: support local paths for iceberg
            false
        }
    };

    let format = config.format.expect("must have format for FileSystemSink");

    match (&format, is_local) {
        (Format::Parquet { .. }, true) => Ok(ConstructedOperator::from_operator(Box::new(
            LocalParquetFileSystemSink::new(sink, table_format, format),
        ))),
        (Format::Parquet { .. }, false) => Ok(ConstructedOperator::from_operator(Box::new(
            ParquetFileSystemSink::create_and_start(sink, table_format, format, connection_id),
        ))),
        (Format::Json { .. }, true) => Ok(ConstructedOperator::from_operator(Box::new(
            LocalJsonFileSystemSink::new(sink, table_format, format),
        ))),
        (Format::Json { .. }, false) => Ok(ConstructedOperator::from_operator(Box::new(
            JsonFileSystemSink::create_and_start(sink, table_format, format, connection_id),
        ))),
        (f, _) => bail!("unsupported format {f}"),
    }
}

pub struct FileSystemConnector {}

impl Connector for FileSystemConnector {
    type ProfileT = EmptyConfig;

    type TableT = FileSystemTable;

    fn name(&self) -> &'static str {
        "filesystem"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "filesystem".to_string(),
            name: "FileSystem".to_string(),
            icon: ICON.to_string(),
            description: "Read or write to a filesystem or object store like S3".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: None,
            table_config: render_schema::<Self::TableT>(),
        }
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(message).await.unwrap();
        });
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.table_type {
            FileSystemTableType::Source { .. } => ConnectionType::Source,
            FileSystemTableType::Sink { .. } => ConnectionType::Sink,
        }
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for FileSystem connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for FileSystem connection"))?;

        let (description, connection_type, partition_fields) = match &table.table_type {
            FileSystemTableType::Source { .. } => {
                ("FileSystem".to_string(), ConnectionType::Source, None)
            }
            FileSystemTableType::Sink(FileSystemSink {
                path, partitioning, ..
            }) => {
                BackendConfig::parse_url(path, true)?;

                let description = format!("FileSystemSink<{format}, {path}>");

                let schema = Schema::new(schema.fields.iter()
                    .map(|f| Field::from(f.clone()))
                    .collect_vec());

                let partitioner = partitioner_from_config(&partitioning, schema.into())?;

                (description, ConnectionType::Sink, partitioner.map(|p| vec![p]))
            }
        };


        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            connection_type,
            schema,
            &config,
            description,
        )
        .with_partition_exprs(partition_fields))
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _: Option<&ConnectionProfile>,
    ) -> Result<Connection> {
        self.from_config(None, name, EmptyConfig {}, options.pull_struct()?, schema)
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> Result<ConstructedOperator> {
        match table.table_type {
            FileSystemTableType::Source(source) => Ok(ConstructedOperator::from_source(Box::new(
                FileSystemSourceFunc {
                    source,
                    format: config
                        .format
                        .ok_or_else(|| anyhow!("format required for FileSystem source"))?,
                    framing: config.framing,
                    bad_data: config.bad_data,
                    file_states: HashMap::new(),
                },
            ))),
            FileSystemTableType::Sink(sink) => {
                let partitioning = sink.partitioning.clone();
                make_sink(sink, config, TableFormat::None {
                    partitioning,
                }, None)
            },
        }
    }
}
