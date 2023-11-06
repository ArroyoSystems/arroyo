use std::str::FromStr;
use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, bail, Result};
use arrow_schema::{DataType, Field};
use arroyo_connectors::{connector_for_type, Connection};
use arroyo_datastream::{ConnectorOp, Operator};
use arroyo_rpc::api_types::connections::{
    ConnectionSchema, ConnectionType, SchemaDefinition, SourceField,
};
use arroyo_rpc::formats::{Format, Framing};
use datafusion::{
    optimizer::{analyzer::Analyzer, optimizer::Optimizer, OptimizerContext},
    sql::{
        planner::{PlannerContext, SqlToRel},
        sqlparser::ast::{ColumnDef, ColumnOption, Statement, Value},
    },
};
use datafusion_common::{config::ConfigOptions, DFField, DFSchema};
use datafusion_expr::{
    CreateMemoryTable, CreateView, DdlStatement, DmlStatement, LogicalPlan, WriteOp,
};

use crate::code_gen::{CodeGenerator, ValuePointerContext};
use crate::expressions::CastExpression;
use crate::external::SinkUpdateType;
use crate::{avro, DEFAULT_IDLE_TIME};
use crate::{
    expressions::{Column, ColumnExpression, Expression, ExpressionContext},
    external::{ProcessingMode, SqlSink, SqlSource},
    json_schema,
    operators::Projection,
    pipeline::{SourceOperator, SqlOperator, SqlPipelineBuilder},
    types::{convert_data_type, StructDef, StructField, TypeDef},
    ArroyoSchemaProvider,
};

#[derive(Debug, Clone)]
pub struct ConnectorTable {
    pub id: Option<i64>,
    pub name: String,
    pub connection_type: ConnectionType,
    pub fields: Vec<FieldSpec>,
    pub type_name: Option<String>,
    pub operator: String,
    pub config: String,
    pub description: String,
    pub format: Option<Format>,
    pub event_time_field: Option<String>,
    pub watermark_field: Option<String>,
    pub idle_time: Option<Duration>,
}

#[derive(Debug, Clone)]
pub enum FieldSpec {
    StructField(StructField),
    VirtualField {
        field: StructField,
        expression: Expression,
    },
}

impl FieldSpec {
    fn is_virtual(&self) -> bool {
        match self {
            FieldSpec::StructField(_) => false,
            FieldSpec::VirtualField { .. } => true,
        }
    }
    fn struct_field(&self) -> &StructField {
        match self {
            FieldSpec::StructField(f) => f,
            FieldSpec::VirtualField { field, .. } => field,
        }
    }
}

impl From<StructField> for FieldSpec {
    fn from(value: StructField) -> Self {
        FieldSpec::StructField(value)
    }
}

fn schema_type(name: &str, schema: &ConnectionSchema) -> Option<String> {
    schema.struct_name.as_ref().cloned().or_else(|| {
        let def = &schema.definition.as_ref()?;
        match def {
            SchemaDefinition::JsonSchema(_) => {
                Some(format!("{}::{}", name, json_schema::ROOT_NAME))
            }
            SchemaDefinition::ProtobufSchema(_) => todo!(),
            SchemaDefinition::AvroSchema(_) => Some(format!("{}::{}", name, avro::ROOT_NAME)),
            SchemaDefinition::RawSchema(_) => Some("arroyo_types::RawJson".to_string()),
        }
    })
}

pub fn schema_defs(name: &str, schema: &ConnectionSchema) -> Option<String> {
    let def = &schema.definition.as_ref()?;

    match def {
        SchemaDefinition::JsonSchema(s) => Some(json_schema::get_defs(&name, &s).unwrap()),
        SchemaDefinition::ProtobufSchema(_) => todo!(),
        SchemaDefinition::AvroSchema(s) => Some(avro::get_defs(&name, &s).unwrap()),
        SchemaDefinition::RawSchema(_) => None,
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
                    struct_field.into()
                })
                .collect(),
            type_name: schema_type(&value.name, &value.schema),
            operator: value.operator,
            config: value.config,
            description: value.description,
            format: value.schema.format.clone(),
            event_time_field: None,
            watermark_field: None,
            idle_time: DEFAULT_IDLE_TIME,
        }
    }
}

impl ConnectorTable {
    fn from_options(
        name: &str,
        connector: &str,
        fields: Vec<FieldSpec>,
        options: &mut HashMap<String, String>,
    ) -> Result<Self> {
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
                        struct_field.name,
                        struct_field.data_type
                    )
                })
            })
            .collect();

        let schema = ConnectionSchema::try_new(format, framing, None, schema_fields?, None)?;

        let connection = connector.from_options(name, options, Some(&schema))?;

        let mut table: ConnectorTable = connection.into();
        table.fields = fields;
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
            let fields = self
                .fields
                .iter()
                .map(|field| {
                    match field {
                        FieldSpec::StructField(struct_field) => Ok((Column{relation: None, name: struct_field.name.clone()}, Expression::Column(ColumnExpression::new(struct_field.clone())))),
                        FieldSpec::VirtualField { field, expression } => {
                            let expression_type_def = expression.expression_type(&ValuePointerContext::new());
                            let expression_return_type = expression_type_def.as_datatype().expect("virtual fields shouldn't return structs");
                            let expression_nullability = expression_type_def.is_optional();
                            let field_return_type = field.data_type.as_datatype().expect("virtual fields shouldn't return structs");
                            let field_nullability = field.data_type.is_optional();
                            if !field_nullability && expression_nullability {
                                bail!("virtual field {} is not nullable, but the expression for calculating it is nullable", field.name);
                            }
                            if field_nullability == expression_nullability && expression_return_type == field_return_type {
                                // no need to cast
                                Ok((Column {
                                    relation: None,
                                    name: field.name.clone(),
                                },
                                    expression.clone()
                                ))
                            } else {
                                let force_nullability = field_nullability && !expression_nullability;
                                let cast_expr = CastExpression::new(Box::new(expression.clone()), field_return_type, &crate::code_gen::ValuePointerContext::new(), force_nullability)?;
                                Ok((
                                    Column {
                                        relation: None,
                                        name: field.name.clone(),
                                    },
                                    cast_expr))
                            }
                    }
                }
                }).collect::<Result<Vec<_>>>()?.into_iter().collect();

            Ok(Some(Projection::new(fields)))
        } else {
            Ok(None)
        }
    }

    fn timestamp_override(&self) -> Result<Option<Expression>> {
        if let Some(field_name) = &self.event_time_field {
            if self.is_update() {
                bail!("can't use event_time_field with update mode.")
            }

            // check that a column exists and it is a timestamp
            let field = self
                .fields
                .iter()
                .find(|f| {
                    f.struct_field().name == *field_name
                        && matches!(
                            f.struct_field().data_type,
                            TypeDef::DataType(DataType::Timestamp(..), _)
                        )
                })
                .ok_or_else(|| {
                    anyhow!(
                        "event_time_field {} not found or not a timestamp",
                        field_name
                    )
                })?;

            Ok(Some(Expression::Column(ColumnExpression::new(
                field.struct_field().clone(),
            ))))
        } else {
            Ok(None)
        }
    }

    fn watermark_column(&self) -> Result<Option<Expression>> {
        if let Some(field_name) = &self.watermark_field {
            // check that a column exists and it is a timestamp
            let field = self
                .fields
                .iter()
                .find(|f| {
                    f.struct_field().name == *field_name
                        && matches!(
                            f.struct_field().data_type,
                            TypeDef::DataType(DataType::Timestamp(..), _)
                        )
                })
                .ok_or_else(|| {
                    anyhow!(
                        "watermark_field {} not found or not a timestamp",
                        field_name
                    )
                })?;

            Ok(Some(Expression::Column(ColumnExpression::new(
                field.struct_field().clone(),
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

    pub fn as_sql_source(&self) -> Result<SqlOperator> {
        match self.connection_type {
            ConnectionType::Source => {}
            ConnectionType::Sink => {
                bail!("cannot read from sink")
            }
        };

        if self.is_update() && self.has_virtual_fields() {
            bail!("can't read from a source with virtual fields and update mode.")
        }

        let virtual_field_projection = self.virtual_field_projection()?;
        let timestamp_override = self.timestamp_override()?;
        let watermark_column = self.watermark_column()?;

        let source = SqlSource {
            id: self.id,
            struct_def: StructDef::new(
                self.type_name.clone(),
                self.type_name.is_none(),
                self.fields
                    .iter()
                    .filter_map(|field| match field {
                        FieldSpec::StructField(struct_field) => Some(struct_field.clone()),
                        FieldSpec::VirtualField { .. } => None,
                    })
                    .collect(),
                self.format.clone(),
            ),
            operator: Operator::ConnectorSource(self.connector_op()),
            processing_mode: self.processing_mode(),
            idle_time: self.idle_time,
        };

        Ok(SqlOperator::Source(SourceOperator {
            name: self.name.clone(),
            source,
            virtual_field_projection,
            timestamp_override,
            watermark_column,
        }))
    }

    pub fn as_sql_sink(&self, mut input: SqlOperator) -> Result<SqlOperator> {
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
        ))
    }
}

#[derive(Debug, Clone)]
pub enum Table {
    ConnectorTable(ConnectorTable),
    MemoryTable {
        name: String,
        fields: Vec<StructField>,
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

impl Table {
    fn schema_from_columns(
        columns: &Vec<ColumnDef>,
        schema_provider: &ArroyoSchemaProvider,
    ) -> Result<Vec<FieldSpec>> {
        let struct_field_pairs = columns
            .iter()
            .map(|column| {
                let name = column.name.value.to_string();
                let data_type = convert_data_type(&column.data_type)?;
                let nullable = !column
                    .options
                    .iter()
                    .any(|option| matches!(option.option, ColumnOption::NotNull));

                let struct_field =
                    StructField::new(name, None, TypeDef::DataType(data_type, nullable));

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

        let physical_struct: StructDef = StructDef::for_fields(
            struct_field_pairs
                .iter()
                .filter_map(
                    |(field, generating_expression)| match generating_expression {
                        Some(_) => None,
                        None => Some(field.clone()),
                    },
                )
                .collect(),
        );

        let physical_schema = DFSchema::new_with_metadata(
            physical_struct
                .fields
                .iter()
                .map(|f| {
                    let TypeDef::DataType(data_type, nullable) = f.data_type.clone() else {
                        bail!("expect data type for generated column")
                    };
                    Ok(DFField::new_unqualified(&f.name, data_type, nullable))
                })
                .collect::<Result<Vec<_>>>()?,
            HashMap::new(),
        )?;

        let expression_context = ExpressionContext {
            input_struct: &physical_struct,
            schema_provider,
        };

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
                    let expression = expression_context.compile_expr(&df_expr)?;
                    Ok(FieldSpec::VirtualField {
                        field: struct_field,
                        expression,
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
                with_map.insert(
                    option.name.value.to_string(),
                    value_to_inner_string(&option.value)?,
                );
            }

            let connector = with_map.remove("connector");
            let fields = Self::schema_from_columns(columns, schema_provider)?;

            match connector.as_ref().map(|c| c.as_str()) {
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
                            .map(|f| f.struct_field().clone())
                            .collect(),
                    }))
                }
                Some(connector) => Ok(Some(Table::ConnectorTable(
                    ConnectorTable::from_options(&name, connector, fields, &mut with_map)
                        .map_err(|e| anyhow!("Failed to construct table '{}': {:?}", name, e))?,
                ))),
            }
        } else {
            match &produce_optimized_plan(statement, schema_provider)? {
                // views and memory tables are the same now.
                LogicalPlan::Ddl(DdlStatement::CreateView(CreateView { name, input, .. }))
                | LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
                    name,
                    input,
                    ..
                })) => {
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

    pub fn get_fields(&self) -> Result<Vec<Field>> {
        match self {
            Table::MemoryTable { fields, .. } => fields
                .iter()
                .map(|field| {
                    let field: Field = field.clone().into();
                    Ok(field)
                })
                .collect::<Result<Vec<_>>>(),
            Table::ConnectorTable(ConnectorTable { fields, .. }) => fields
                .iter()
                .map(|field| {
                    let field: Field = field.struct_field().clone().into();
                    Ok(field)
                })
                .collect::<Result<Vec<_>>>(),
            Table::TableFromQuery { logical_plan, .. } => logical_plan
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    let field: Field = (**field.field()).clone();
                    Ok(field)
                })
                .collect::<Result<Vec<_>>>(),
        }
    }

    pub fn as_sql_source(&self, builder: &mut SqlPipelineBuilder) -> Result<SqlOperator> {
        match self {
            Table::ConnectorTable(cn) => cn.as_sql_source(),
            Table::MemoryTable { name, .. } => Ok(builder
                .planned_tables
                .get(name)
                .ok_or_else(|| {
                    anyhow!(
                        "memory table {} not found in planned tables. This is a bug.",
                        name
                    )
                })?
                .clone()),
            Table::TableFromQuery { logical_plan, .. } => {
                builder.insert_sql_plan(&logical_plan.clone())
            }
        }
    }

    pub fn as_sql_sink(&self, input: SqlOperator) -> Result<SqlOperator> {
        match self {
            Table::ConnectorTable(c) => c.as_sql_sink(input),
            Table::MemoryTable { name, .. } => {
                Ok(SqlOperator::NamedTable(name.clone(), Box::new(input)))
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

impl Insert {
    pub fn try_from_statement(
        statement: &Statement,
        schema_provider: &ArroyoSchemaProvider,
    ) -> Result<Insert> {
        let logical_plan = produce_optimized_plan(statement, schema_provider)?;

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
