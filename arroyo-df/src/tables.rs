use std::str::FromStr;
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, bail, Result};
use arrow_schema::{DataType, Field, FieldRef};
use arroyo_connectors::{connector_for_type, Connection};
use arroyo_datastream::ConnectorOp;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, SchemaDefinition, SourceField,
};
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_types::ArroyoExtensionType;
use datafusion::sql::sqlparser::ast::Query;
use datafusion::{
    optimizer::{analyzer::Analyzer, optimizer::Optimizer, OptimizerContext},
    sql::{
        planner::SqlToRel,
        sqlparser::ast::{ColumnDef, ColumnOption, Statement, Value},
    },
};
use datafusion_common::Column;
use datafusion_common::{config::ConfigOptions, DFField, DFSchema};
use datafusion_expr::{
    CreateMemoryTable, CreateView, DdlStatement, DmlStatement, Expr, LogicalPlan, Projection,
    WriteOp,
};
use tracing::info;

use crate::{avro, DEFAULT_IDLE_TIME};
use crate::{
    external::{ProcessingMode, SqlSource},
    json_schema,
    types::{convert_data_type, StructDef, StructField, TypeDef},
    ArroyoSchemaProvider,
};

#[derive(Debug, Clone)]
pub struct ConnectorTable {
    pub id: Option<i64>,
    pub name: String,
    pub connection_type: ConnectionType,
    pub fields: Vec<FieldSpec>,
    pub operator: String,
    pub config: String,
    pub description: String,
    pub format: Option<Format>,
    pub event_time_field: Option<String>,
    pub watermark_field: Option<String>,
    pub idle_time: Option<Duration>,

    pub inferred_fields: Option<Vec<DFField>>,
}

#[derive(Debug, Clone)]
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
    fn struct_field(&self) -> &Field {
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
    let optimizer = Optimizer::new();
    let analyzed_plan =
        analyzer.execute_and_check(&plan, &ConfigOptions::default(), |_plan, _rule| {})?;
    let plan = optimizer.optimize(&analyzed_plan, &optimizer_config, |_plan, _rule| {})?;
    Ok(plan)
}

impl From<Connection> for ConnectorTable {
    fn from(value: Connection) -> Self {
        ConnectorTable {
            id: value.id,
            name: value.name.clone(),
            connection_type: value.connection_type,
            fields: value
                .schema
                .fields
                .iter()
                .map(|f| {
                    let struct_field: StructField = f.clone().into();
                    let field: Field = struct_field.into();
                    field.into()
                })
                .collect(),
            operator: value.operator,
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
                let struct_field = f.struct_field();
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
            .map_err(|_| anyhow!("idle_micros must be sent to a number"))?
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

    fn virtual_field_projection(&self) -> Result<Option<Projection>> {
        if self.has_virtual_fields() {
            bail!("virtual fields not supported in Arrow");
        } else {
            Ok(None)
        }
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
                    f.struct_field().name() == field_name
                        && matches!(f.struct_field().data_type(), DataType::Timestamp(..))
                })
                .ok_or_else(|| {
                    anyhow!(
                        "event_time_field {} not found or not a timestamp",
                        field_name
                    )
                })?;

            Ok(Some(Expr::Column(Column::from_name(
                field.struct_field().name(),
            ))))
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
                    f.struct_field().name() == field_name
                        && matches!(f.struct_field().data_type(), DataType::Timestamp(..))
                })
                .ok_or_else(|| {
                    anyhow!(
                        "watermark_field {} not found or not a timestamp",
                        field_name
                    )
                })?;

            Ok(Some(Expr::Column(Column::from_name(
                field.struct_field().name(),
            ))))
        } else {
            Ok(None)
        }
    }

    fn connector_op(&self) -> ConnectorOp {
        ConnectorOp {
            operator: self.operator.clone(),
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

        let _virtual_field_projection = self.virtual_field_projection()?;
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

    // TODO: implement
    pub fn as_sql_sink(&self) -> Result<()> {
        todo!();
        /*
        match self.connection_type {
            ConnectionType::Source => {
                bail!("inserting into a source is not allowed")
            }
            ConnectionType::Sink => {}
        }

        if self.has_virtual_fields() {
            // TODO: I think it would be reasonable and possibly useful to support this
            bail!("virtual fields are not currently supported in sinks");
        }

        let updating_type = if self.is_update() {
            SinkUpdateType::Force
        } else {
            SinkUpdateType::Disallow
        };

        if updating_type == SinkUpdateType::Disallow && input.is_updating() {
            bail!("sink does not support update messages, cannot be used with an updating query");
        }

        if let Some(format) = &self.format {
            let output_struct: StructDef = input.return_type();
            // we may need to copy the record into a new struct, that has the appropriate annotations
            // for serializing into our format
            let mut projection = Projection::new(
                output_struct
                    .fields
                    .iter()
                    .map(|t| Column {
                        relation: t.alias.clone(),
                        name: t.name(),
                    })
                    .zip(
                        output_struct
                            .fields
                            .iter()
                            .map(|t| Expression::Column(ColumnExpression::new(t.clone()))),
                    )
                    .collect(),
            );

            projection.format = Some(format.clone());
            projection.struct_name = self.type_name.clone();

            input = SqlOperator::RecordTransform(
                Box::new(input),
                crate::pipeline::RecordTransform::ValueProjection(projection),
            );
        }

        Ok(SqlOperator::Sink(
            self.name.clone(),
            SqlSink {
                id: self.id,
                struct_def: input.return_type(),
                updating_type,
                operator: Operator::ConnectorSink(self.connector_op()),
            },
            Box::new(input),
        ))*/
    }
}

#[derive(Debug, Clone)]
pub struct SourceOperator {
    pub name: String,
    pub source: SqlSource,
    pub timestamp_override: Option<Expr>,
    pub watermark_column: Option<Expr>,
}

#[derive(Debug, Clone)]
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
}

fn value_to_inner_string(value: &Value) -> Result<String> {
    match value {
        Value::SingleQuotedString(inner_string)
        | Value::UnQuotedString(inner_string)
        | Value::DoubleQuotedString(inner_string) => Ok(inner_string.clone()),
        _ => bail!("Expected a string value, found {:?}", value),
    }
}

fn qualified_field(f: &DFField) -> Field {
    Field::new(f.qualified_name(), f.data_type().clone(), f.is_nullable())
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

        let _physical_schema = DFSchema::new_with_metadata(
            physical_fields
                .iter()
                .map(|f| {
                    Ok(DFField::new_unqualified(
                        &f.name(),
                        f.data_type().clone(),
                        f.is_nullable(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            HashMap::new(),
        )?;

        let _sql_to_rel = SqlToRel::new(schema_provider);
        struct_field_pairs
            .into_iter()
            .map(|(struct_field, generating_expression)| {
                if let Some(_generating_expression) = generating_expression {
                    // TODO: Implement automatic type coercion here, as we have elsewhere.
                    // It is done by calling the Analyzer which inserts CAST operators where necessary.
                    todo!("support generating expressions");
                    /*let df_expr = sql_to_rel.sql_to_expr(
                        generating_expression,
                        &physical_schema,
                        &mut PlannerContext::default(),
                    )?;
                    let expression = expression_context.compile_expr(&df_expr)?;
                    Ok(FieldSpec::VirtualField {
                        field: struct_field,
                        expression,
                    })*/
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
                with_map.insert(
                    option.name.value.to_string(),
                    value_to_inner_string(&option.value)?,
                );
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
                            .map(|f| Arc::new(f.struct_field().clone()))
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
                    // Return a TableFromQuery
                    Ok(Some(Table::TableFromQuery {
                        name: name.to_string(),
                        logical_plan: (**input).clone(),
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
                .map(|fs| fs.iter().map(qualified_field).map(Arc::new).collect())
                .unwrap_or_else(|| {
                    fields
                        .iter()
                        .map(|field| field.struct_field().clone().into())
                        .collect()
                }),
            Table::TableFromQuery { logical_plan, .. } => logical_plan
                .schema()
                .fields()
                .iter()
                .map(qualified_field)
                .map(Arc::new)
                .collect(),
        }
    }

    pub fn as_sql_sink(&self, _input: LogicalPlan) -> Result<()> {
        match self {
            Table::ConnectorTable(c) => c.as_sql_sink(),
            Table::MemoryTable { .. } => {
                todo!()
                //Ok(SqlOperator::NamedTable(name.clone(), Box::new(input)))
            }
            Table::TableFromQuery { .. } => todo!(),
        }
    }
}

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
    let plan = produce_optimized_plan(&Statement::Query(source.clone()), schema_provider)?;
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
            into,
            table_name,
            ..
        } = statement
        {
            if *into {
                infer_sink_schema(
                    source.as_ref().unwrap(),
                    table_name.to_string(),
                    schema_provider,
                )?;
            }
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
