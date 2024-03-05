use std::str::FromStr;
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use arroyo_connectors::connector_for_type;

use arroyo_datastream::preview_sink;
use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, SourceField,
};
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::api::ConnectorOp;
use arroyo_types::ArroyoExtensionType;
use datafusion::sql::planner::PlannerContext;
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::ast::Query;
use datafusion::{
    optimizer::{analyzer::Analyzer, optimizer::Optimizer, OptimizerContext},
    sql::{
        planner::SqlToRel,
        sqlparser::ast::{ColumnDef, ColumnOption, Statement, Value},
    },
};
use datafusion_common::tree_node::TreeNode;
use datafusion_common::Column;
use datafusion_common::{config::ConfigOptions, DFField, DFSchema};
use datafusion_expr::{
    CreateMemoryTable, CreateView, DdlStatement, DmlStatement, Expr, Extension, LogicalPlan,
    WriteOp,
};
use tracing::info;

use crate::extension::remote_table::RemoteTableExtension;
use crate::plan::ArroyoRewriter;
use crate::types::convert_data_type;
use crate::DEFAULT_IDLE_TIME;
use crate::{
    external::{ProcessingMode, SqlSource},
    ArroyoSchemaProvider,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectorTable {
    pub id: Option<i64>,
    pub connector: String,
    pub name: String,
    pub connection_type: ConnectionType,
    pub fields: Vec<FieldSpec>,
    pub config: String,
    pub description: String,
    pub format: Option<Format>,
    pub event_time_field: Option<String>,
    pub watermark_field: Option<String>,
    pub idle_time: Option<Duration>,

    pub inferred_fields: Option<Vec<DFField>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldSpec {
    StructField(Field),
    VirtualField { field: Field, expression: Expr },
}

impl FieldSpec {
    fn is_virtual(&self) -> bool {
        match self {
            FieldSpec::StructField(_) => false,
            FieldSpec::VirtualField { .. } => true,
        }
    }
    pub fn field(&self) -> &Field {
        match self {
            FieldSpec::StructField(f) => f,
            FieldSpec::VirtualField { field, .. } => field,
        }
    }
}

impl From<Field> for FieldSpec {
    fn from(value: Field) -> Self {
        FieldSpec::StructField(value)
    }
}

fn produce_optimized_plan(
    statement: &Statement,
    schema_provider: &ArroyoSchemaProvider,
) -> Result<LogicalPlan> {
    let sql_to_rel = SqlToRel::new(schema_provider);
    let plan = sql_to_rel
        .sql_statement_to_plan(statement.clone())
        .map_err(|e| anyhow!(e.strip_backtrace()))?;

    let optimizer_config = OptimizerContext::default();
    let analyzer = Analyzer::default();
    let mut optimizer = Optimizer::new();
    optimizer
        .rules
        // This rule can drop event time calculation fields if they aren't used elsewhere.
        .retain(|rule| rule.name() != "optimize_projections");
    let analyzed_plan =
        analyzer.execute_and_check(&plan, &ConfigOptions::default(), |_plan, _rule| {})?;

    let plan = optimizer.optimize(&analyzed_plan, &optimizer_config, |_plan, _rule| {})?;
    info!("optimized plan: {:?}", plan);
    Ok(plan)
}

impl From<Connection> for ConnectorTable {
    fn from(value: Connection) -> Self {
        ConnectorTable {
            id: value.id,
            connector: value.connector.to_string(),
            name: value.name.clone(),
            connection_type: value.connection_type,
            fields: value
                .schema
                .fields
                .iter()
                .map(|f| FieldSpec::StructField(f.clone().into()))
                .collect(),
            config: value.config,
            description: value.description,
            format: value.schema.format.clone(),
            event_time_field: None,
            watermark_field: None,
            idle_time: DEFAULT_IDLE_TIME,
            inferred_fields: None,
        }
    }
}

impl ConnectorTable {
    fn from_options(
        name: &str,
        connector: &str,
        mut fields: Vec<FieldSpec>,
        options: &mut HashMap<String, String>,
        connection_profile: Option<&ConnectionProfile>,
    ) -> Result<Self> {
        // TODO: a more principled way of letting connectors dictate types to use
        if "delta" == connector {
            fields = fields
                .into_iter()
                .map(|field_spec| match &field_spec {
                    FieldSpec::StructField(struct_field) => match struct_field.data_type() {
                        DataType::Timestamp(_, None) => {
                            FieldSpec::StructField(struct_field.clone().with_data_type(
                                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                            ))
                        }
                        _ => field_spec,
                    },
                    FieldSpec::VirtualField { .. } => {
                        unreachable!("delta lake is only a sink, can't have virtual fields")
                    }
                })
                .collect();
        }
        let connector = connector_for_type(connector)
            .ok_or_else(|| anyhow!("Unknown connector '{}'", connector))?;

        let format = Format::from_opts(options).map_err(|e| anyhow!("invalid format: '{e}'"))?;

        let framing = Framing::from_opts(options).map_err(|e| anyhow!("invalid framing: '{e}'"))?;

        let schema_fields: Result<Vec<SourceField>> = fields
            .iter()
            .filter(|f| !f.is_virtual())
            .map(|f| {
                let struct_field = f.field();
                struct_field.clone().try_into().map_err(|_| {
                    anyhow!(
                        "field '{}' has a type '{:?}' that cannot be used in a connection table",
                        struct_field.name(),
                        struct_field.data_type()
                    )
                })
            })
            .collect();
        let bad_data =
            BadData::from_opts(options).map_err(|e| anyhow!("Invalid bad_data: '{e}'"))?;

        let schema = ConnectionSchema::try_new(
            format,
            bad_data,
            framing,
            None,
            schema_fields?,
            None,
            Some(fields.is_empty()),
        )?;

        let connection =
            connector.from_options(name, options, Some(&schema), connection_profile)?;

        let mut table: ConnectorTable = connection.into();
        if !fields.is_empty() {
            table.fields = fields;
        }

        table.event_time_field = options.remove("event_time_field");
        table.watermark_field = options.remove("watermark_field");

        table.idle_time = options
            .remove("idle_micros")
            .map(|t| i64::from_str(&t))
            .transpose()
            .map_err(|_| anyhow!("idle_micros must be set to a number"))?
            .or_else(|| DEFAULT_IDLE_TIME.map(|t| t.as_micros() as i64))
            .filter(|t| *t <= 0)
            .map(|t| Duration::from_micros(t as u64));

        if !options.is_empty() {
            let keys: Vec<String> = options.keys().map(|s| format!("'{}'", s)).collect();
            bail!(
                "unknown options provided in WITH clause: {}",
                keys.join(", ")
            );
        }

        Ok(table)
    }

    fn has_virtual_fields(&self) -> bool {
        self.fields.iter().any(|f| f.is_virtual())
    }

    fn is_update(&self) -> bool {
        self.format
            .as_ref()
            .map(|f| f.is_updating())
            .unwrap_or(false)
    }

    fn timestamp_override(&self) -> Result<Option<Expr>> {
        if let Some(field_name) = &self.event_time_field {
            if self.is_update() {
                bail!("can't use event_time_field with update mode.")
            }

            // check that a column exists and it is a timestamp
            let field = self
                .fields
                .iter()
                .find(|f| {
                    f.field().name() == field_name
                        && matches!(f.field().data_type(), DataType::Timestamp(..))
                })
                .ok_or_else(|| {
                    anyhow!(
                        "event_time_field {} not found or not a timestamp",
                        field_name
                    )
                })?;

            Ok(Some(Expr::Column(Column::from_name(field.field().name()))))
        } else {
            Ok(None)
        }
    }

    fn watermark_column(&self) -> Result<Option<Expr>> {
        if let Some(field_name) = &self.watermark_field {
            // check that a column exists and it is a timestamp
            let field = self
                .fields
                .iter()
                .find(|f| {
                    f.field().name() == field_name
                        && matches!(f.field().data_type(), DataType::Timestamp(..))
                })
                .ok_or_else(|| {
                    anyhow!(
                        "watermark_field {} not found or not a timestamp",
                        field_name
                    )
                })?;

            Ok(Some(Expr::Column(Column::from_name(field.field().name()))))
        } else {
            Ok(None)
        }
    }

    pub fn physical_schema(&self) -> Schema {
        Schema::new(
            self.fields
                .iter()
                .filter(|f| !f.is_virtual())
                .map(|f| f.field().clone())
                .collect::<Vec<_>>(),
        )
    }

    fn connector_op(&self) -> ConnectorOp {
        ConnectorOp {
            connector: self.connector.clone(),
            config: self.config.clone(),
            description: self.description.clone(),
        }
    }

    fn processing_mode(&self) -> ProcessingMode {
        if self.is_update() {
            ProcessingMode::Update
        } else {
            ProcessingMode::Append
        }
    }

    pub fn as_sql_source(&self) -> Result<SourceOperator> {
        match self.connection_type {
            ConnectionType::Source => {}
            ConnectionType::Sink => {
                bail!("cannot read from sink")
            }
        };

        if self.is_update() && self.has_virtual_fields() {
            bail!("can't read from a source with virtual fields and update mode.")
        }

        let timestamp_override = self.timestamp_override()?;
        let watermark_column = self.watermark_column()?;

        let source = SqlSource {
            id: self.id,
            struct_def: self
                .fields
                .iter()
                .filter_map(|field| match field {
                    FieldSpec::StructField(struct_field) => Some(Arc::new(struct_field.clone())),
                    FieldSpec::VirtualField { .. } => None,
                })
                .collect(),
            config: self.connector_op(),
            processing_mode: self.processing_mode(),
            idle_time: self.idle_time,
        };

        Ok(SourceOperator {
            name: self.name.clone(),
            source,
            timestamp_override,
            watermark_column,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SourceOperator {
    pub name: String,
    pub source: SqlSource,
    pub timestamp_override: Option<Expr>,
    pub watermark_column: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Table {
    ConnectorTable(ConnectorTable),
    MemoryTable {
        name: String,
        fields: Vec<FieldRef>,
    },
    TableFromQuery {
        name: String,
        logical_plan: LogicalPlan,
    },
    PreviewSink {
        logical_plan: LogicalPlan,
    },
}

fn value_to_inner_string(value: &Value) -> Result<String> {
    match value {
        Value::SingleQuotedString(inner_string)
        | Value::UnQuotedString(inner_string)
        | Value::DoubleQuotedString(inner_string) => Ok(inner_string.clone()),
        _ => bail!("Expected a string value, found {:?}", value),
    }
}

impl Table {
    fn schema_from_columns(
        columns: &Vec<ColumnDef>,
        schema_provider: &ArroyoSchemaProvider,
    ) -> Result<Vec<FieldSpec>> {
        let struct_field_pairs = columns
            .iter()
            .map(|column| {
                let name = column.name.value.to_string();
                let (data_type, extension) = convert_data_type(&column.data_type)?;
                let nullable = !column
                    .options
                    .iter()
                    .any(|option| matches!(option.option, ColumnOption::NotNull));

                let struct_field = ArroyoExtensionType::add_metadata(
                    extension,
                    Field::new(name, data_type, nullable),
                );

                let generating_expression = column.options.iter().find_map(|option| {
                    if let ColumnOption::Generated {
                        generation_expr, ..
                    } = &option.option
                    {
                        generation_expr.clone()
                    } else {
                        None
                    }
                });
                Ok((struct_field, generating_expression))
            })
            .collect::<Result<Vec<_>>>()?;

        let physical_fields: Vec<_> = struct_field_pairs
            .iter()
            .filter_map(
                |(field, generating_expression)| match generating_expression {
                    Some(_) => None,
                    None => Some(field.clone()),
                },
            )
            .collect();

        let physical_schema = DFSchema::new_with_metadata(
            physical_fields
                .iter()
                .map(|f| {
                    Ok(DFField::new_unqualified(
                        f.name(),
                        f.data_type().clone(),
                        f.is_nullable(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            HashMap::new(),
        )?;

        let sql_to_rel = SqlToRel::new(schema_provider);
        struct_field_pairs
            .into_iter()
            .map(|(struct_field, generating_expression)| {
                if let Some(generating_expression) = generating_expression {
                    // TODO: Implement automatic type coercion here, as we have elsewhere.
                    // It is done by calling the Analyzer which inserts CAST operators where necessary.

                    let df_expr = sql_to_rel.sql_to_expr(
                        generating_expression,
                        &physical_schema,
                        &mut PlannerContext::default(),
                    )?;

                    Ok(FieldSpec::VirtualField {
                        field: struct_field,
                        expression: df_expr,
                    })
                } else {
                    Ok(FieldSpec::StructField(struct_field))
                }
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn try_from_statement(
        statement: &Statement,
        schema_provider: &ArroyoSchemaProvider,
    ) -> Result<Option<Self>> {
        if let Statement::CreateTable {
            name,
            columns,
            with_options,
            query: None,
            ..
        } = statement
        {
            let name: String = name.to_string();
            let mut with_map = HashMap::new();
            for option in with_options {
                let sqlparser::ast::Expr::Value(value) = &option.value else {
                    bail!("Expected a value, found {:?}", option.value)
                };
                with_map.insert(option.name.value.to_string(), value_to_inner_string(value)?);
            }

            let connector = with_map.remove("connector");
            let fields = Self::schema_from_columns(columns, schema_provider)?;

            match connector.as_deref() {
                Some("memory") | None => {
                    if fields.iter().any(|f| f.is_virtual()) {
                        bail!("Virtual fields are not supported in memory tables; instead write a query");
                    }

                    if !with_map.is_empty() {
                        if connector.is_some() {
                            bail!("Memory tables do not allow with options");
                        } else {
                            bail!("Memory tables do not allow with options; to create a connection table set the 'connector' option");
                        }
                    }

                    Ok(Some(Table::MemoryTable {
                        name,
                        fields: fields
                            .into_iter()
                            .map(|f| Arc::new(f.field().clone()))
                            .collect(),
                    }))
                }
                Some(connector) => {
                    let connection_profile = match with_map.remove("connection_profile") {
                        Some(connection_profile_name) => Some(
                            schema_provider
                                .profiles
                                .get(&connection_profile_name)
                                .ok_or_else(|| {
                                    anyhow!(
                                        "connection profile '{}' not found",
                                        connection_profile_name
                                    )
                                })?,
                        ),
                        None => None,
                    };
                    Ok(Some(Table::ConnectorTable(
                        ConnectorTable::from_options(
                            &name,
                            connector,
                            fields,
                            &mut with_map,
                            connection_profile,
                        )
                        .map_err(|e| anyhow!("Failed to construct table '{}': {:?}", name, e))?,
                    )))
                }
            }
        } else {
            match &produce_optimized_plan(statement, schema_provider) {
                // views and memory tables are the same now.
                Ok(LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                    name, input, ..
                })))
                | Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
                    name,
                    input,
                    ..
                }))) => {
                    let rewritten_plan = input
                        .as_ref()
                        .clone()
                        .rewrite(&mut ArroyoRewriter { schema_provider })?;
                    let schema = rewritten_plan.schema().clone();
                    let remote_extension = RemoteTableExtension {
                        input: rewritten_plan,
                        name: name.to_owned(),
                        schema,
                        materialize: true,
                    };
                    // Return a TableFromQuery
                    Ok(Some(Table::TableFromQuery {
                        name: name.to_string(),
                        logical_plan: LogicalPlan::Extension(Extension {
                            node: Arc::new(remote_extension),
                        }),
                    }))
                }
                _ => Ok(None),
            }
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Table::MemoryTable { name, .. } | Table::TableFromQuery { name, .. } => name.as_str(),
            Table::ConnectorTable(c) => c.name.as_str(),
            Table::PreviewSink { .. } => "preview",
        }
    }

    pub fn set_inferred_fields(&mut self, fields: Vec<DFField>) -> Result<()> {
        let Table::ConnectorTable(t) = self else {
            bail!("can only infer schema for connector tables");
        };

        if !t.fields.is_empty() {
            return Ok(());
        }

        if let Some(existing) = &t.inferred_fields {
            let matches = existing.len() == fields.len()
                && existing
                    .iter()
                    .zip(&fields)
                    .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type());

            if !matches {
                bail!("all inserts into a table must share the same schema");
            }
        }

        t.inferred_fields.replace(fields);

        Ok(())
    }

    pub fn get_fields(&self) -> Vec<FieldRef> {
        match self {
            Table::MemoryTable { fields, .. } => fields.clone(),
            Table::ConnectorTable(ConnectorTable {
                fields,
                inferred_fields,
                ..
            }) => inferred_fields
                .as_ref()
                .map(|fs| fs.iter().map(|f| f.field().clone()).collect())
                .unwrap_or_else(|| {
                    fields
                        .iter()
                        .map(|field| field.field().clone().into())
                        .collect()
                }),
            Table::TableFromQuery { logical_plan, .. } => logical_plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.field().clone())
                .collect(),
            Table::PreviewSink { logical_plan } => logical_plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.field().clone())
                .collect(),
        }
    }

    pub fn connector_op(&self) -> Result<ConnectorOp> {
        match self {
            Table::ConnectorTable(c) => Ok(c.connector_op()),
            Table::MemoryTable { .. } => {
                bail!("can't write to a memory table")
            }
            Table::TableFromQuery { .. } => todo!(),
            Table::PreviewSink { logical_plan: _ } => Ok(preview_sink()),
        }
    }
}

#[derive(Debug)]
pub enum Insert {
    InsertQuery {
        sink_name: String,
        logical_plan: LogicalPlan,
    },
    Anonymous {
        logical_plan: LogicalPlan,
    },
}

fn infer_sink_schema(
    source: &Box<Query>,
    table_name: String,
    schema_provider: &mut ArroyoSchemaProvider,
) -> Result<()> {
    let plan = produce_optimized_plan(&Statement::Query(source.clone()), schema_provider)
        .context("failed to produce optimized plan")?;
    let table = schema_provider
        .get_table_mut(&table_name)
        .ok_or_else(|| anyhow!("table {} not found", table_name))?;

    table.set_inferred_fields(plan.schema().fields().to_vec())?;

    Ok(())
}

impl Insert {
    pub fn try_from_statement(
        statement: &Statement,
        schema_provider: &mut ArroyoSchemaProvider,
    ) -> Result<Insert> {
        if let Statement::Insert {
            source,
            into: true,
            table_name,
            ..
        } = statement
        {
            infer_sink_schema(
                source.as_ref().unwrap(),
                table_name.to_string(),
                schema_provider,
            )?;
        }

        let logical_plan = produce_optimized_plan(statement, schema_provider)?;
        info!("logical plan: {:#?}", logical_plan);

        match &logical_plan {
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::InsertInto,
                input,
                ..
            }) => {
                let sink_name = table_name.to_string();
                Ok(Insert::InsertQuery {
                    sink_name,
                    logical_plan: (**input).clone(),
                })
            }
            _ => Ok(Insert::Anonymous { logical_plan }),
        }
    }
}
