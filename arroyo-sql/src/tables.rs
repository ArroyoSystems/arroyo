use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use arrow_schema::{DataType, Field};
use arroyo_connectors::{connector_for_type, serialization_mode, Connection, ConnectionType};
use arroyo_datastream::{ConnectorOp, Operator, SerializationMode};
use arroyo_rpc::grpc::{
    self,
    api::{ConnectionSchema, Format, FormatOptions, SourceField},
};
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
    pub fields: Vec<StructField>,
    pub type_name: Option<String>,
    pub operator: String,
    pub config: String,
    pub description: String,
    pub serialization_mode: SerializationMode,
    pub event_time_field: Option<String>,
    pub watermark_field: Option<String>,
}

fn schema_type(name: &str, schema: &ConnectionSchema) -> Option<String> {
    schema.struct_name.as_ref().cloned().or_else(|| {
        let def = schema.definition.as_ref()?;
        match def {
            grpc::api::connection_schema::Definition::JsonSchema(_) => {
                Some(format!("{}::{}", name, json_schema::ROOT_NAME))
            }
            grpc::api::connection_schema::Definition::ProtobufSchema(_) => todo!(),
            grpc::api::connection_schema::Definition::AvroSchema(_) => todo!(),
            grpc::api::connection_schema::Definition::RawSchema(_) => {
                Some("arroyo_types::RawJson".to_string())
            }
        }
    })
}

pub fn schema_defs(name: &str, schema: &ConnectionSchema) -> Option<String> {
    let def = schema.definition.as_ref()?;

    match def {
        grpc::api::connection_schema::Definition::JsonSchema(s) => {
            Some(json_schema::get_defs(&name, &s).unwrap())
        }
        grpc::api::connection_schema::Definition::ProtobufSchema(_) => todo!(),
        grpc::api::connection_schema::Definition::AvroSchema(_) => todo!(),
        grpc::api::connection_schema::Definition::RawSchema(_) => None,
    }
}

fn produce_optimized_plan(
    statement: &Statement,
    schema_provider: &ArroyoSchemaProvider,
) -> Result<LogicalPlan> {
    let sql_to_rel = SqlToRel::new(schema_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone())?;

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
                .map(|f| f.clone().into())
                .collect(),
            type_name: schema_type(&value.name, &value.schema),
            operator: value.operator,
            config: value.config,
            description: value.description,
            serialization_mode: serialization_mode(&value.schema).into(),
            event_time_field: None,
            watermark_field: None,
        }
    }
}

impl ConnectorTable {
    fn from_options(
        name: &str,
        connector: &str,
        fields: Vec<StructField>,
        options: &mut HashMap<String, String>,
    ) -> Result<Self> {
        let connector = connector_for_type(connector)
            .ok_or_else(|| anyhow!("Unknown connector '{}'", connector))?;

        let mut format = None;
        if let Some(f) = options.remove("format") {
            format = Some(match f.as_str() {
                "json" => Format::JsonFormat,
                "debezium_json" => Format::DebeziumJsonFormat,
                "protobuf" => Format::ProtobufFormat,
                "avro" => Format::AvroFormat,
                "raw_string" => Format::RawStringFormat,
                f => bail!("Unknown format '{}'", f),
            });
        }

        let schema_registry = options
            .remove("format_options.confluent_schema_registry")
            .map(|f| f == "true")
            .unwrap_or(false);

        let schema_fields: Result<Vec<SourceField>> = fields
            .iter()
            .map(|f| {
                f.clone().try_into().map_err(|_| {
                    anyhow!(
                        "field '{}' has a type '{:?}' that cannot be used in a connection table",
                        f.name,
                        f.data_type
                    )
                })
            })
            .collect();

        let schema = ConnectionSchema {
            format: format.map(|f| f as i32),
            format_options: Some(FormatOptions {
                confluent_schema_registry: schema_registry,
            }),
            struct_name: None,
            fields: schema_fields?,
            definition: None,
        };

        let connection = connector.from_options(name, options, Some(&schema))?;

        if !options.is_empty() {
            let keys: Vec<String> = options.keys().map(|s| s.to_string()).collect();
            bail!(
                "unknown options provided in WITH clause: {}",
                keys.join(", ")
            );
        }

        let mut table: ConnectorTable = connection.into();
        table.fields = fields;

        Ok(table)
    }

    fn has_virtual_fields(&self) -> bool {
        self.fields.iter().any(|f| f.expression.is_some())
    }

    fn is_update(&self) -> bool {
        self.serialization_mode == SerializationMode::DebeziumJson
    }

    fn virtual_field_projection(&self) -> Option<Projection> {
        if self.has_virtual_fields() {
            let (field_names, field_computations) = self
                .fields
                .iter()
                .map(|field| {
                    (
                        Column {
                            relation: None,
                            name: field.name.clone(),
                        },
                        field
                            .expression
                            .as_ref()
                            .map(|expr| *(expr.clone()))
                            .unwrap_or_else(|| {
                                Expression::Column(ColumnExpression::new(field.clone()))
                            }),
                    )
                })
                .unzip();

            Some(Projection {
                field_names,
                field_computations,
            })
        } else {
            None
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
                .find(|f: &&StructField| {
                    f.name == *field_name
                        && matches!(f.data_type, TypeDef::DataType(DataType::Timestamp(..), _))
                })
                .ok_or_else(|| {
                    anyhow!(
                        "event_time_field {} not found or not a timestamp",
                        field_name
                    )
                })?;

            Ok(Some(Expression::Column(ColumnExpression::new(
                field.clone(),
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
                .find(|f: &&StructField| {
                    f.name == *field_name
                        && matches!(f.data_type, TypeDef::DataType(DataType::Timestamp(..), _))
                })
                .ok_or_else(|| {
                    anyhow!(
                        "watermark_field {} not found or not a timestamp",
                        field_name
                    )
                })?;

            Ok(Some(Expression::Column(ColumnExpression::new(
                field.clone(),
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
        match self.serialization_mode {
            SerializationMode::DebeziumJson => ProcessingMode::Update,
            _ => ProcessingMode::Append,
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

        let virtual_field_projection = self.virtual_field_projection();
        let timestamp_override = self.timestamp_override()?;
        let watermark_column = self.watermark_column()?;

        let source = SqlSource {
            id: self.id,
            struct_def: StructDef {
                name: self.type_name.clone(),
                fields: self.fields.clone(),
            },
            operator: Operator::ConnectorSource(self.connector_op()),
            processing_mode: self.processing_mode(),
        };

        Ok(SqlOperator::Source(SourceOperator {
            name: self.name.clone(),
            source,
            virtual_field_projection,
            timestamp_override,
            watermark_column,
        }))
    }

    pub fn as_sql_sink(&self, input: SqlOperator) -> Result<SqlOperator> {
        match self.connection_type {
            ConnectionType::Source => {
                bail!("Inserting into a source is not allowed")
            }
            ConnectionType::Sink => {}
        }

        if input.is_updating() {
            bail!("inserting updating tables into connection tables is not currently supported");
        }

        if self.has_virtual_fields() {
            // TODO: I think it would be reasonable and possibly useful to support this
            bail!("Virtual fields are not currently supported in sinks");
        }

        Ok(SqlOperator::Sink(
            self.name.clone(),
            SqlSink {
                id: self.id,
                struct_def: input.return_type(),
                updating_type: crate::external::SinkUpdateType::Disallow,
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
    ) -> Result<Vec<StructField>> {
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

        let physical_struct: StructDef = StructDef {
            name: None,
            fields: struct_field_pairs
                .iter()
                .filter_map(
                    |(field, generating_expression)| match generating_expression {
                        Some(_) => None,
                        None => Some(field.clone()),
                    },
                )
                .collect(),
        };

        let physical_schema = DFSchema::new_with_metadata(
            physical_struct
                .fields
                .iter()
                .map(|f| {
                    let TypeDef::DataType(data_type, nullable ) = f.data_type.clone() else {
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
            .map(|(mut struct_field, generating_expression)| {
                if let Some(generating_expression) = generating_expression {
                    // TODO: Implement automatic type coercion here, as we have elsewhere.
                    // It is done by calling the Analyzer which inserts CAST operators where necessary.

                    let df_expr = sql_to_rel.sql_to_expr(
                        generating_expression,
                        &physical_schema,
                        &mut PlannerContext::default(),
                    )?;
                    let expr = expression_context.compile_expr(&df_expr)?;
                    struct_field.expression = Some(Box::new(expr));
                }

                Ok(struct_field)
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

            let fields = Self::schema_from_columns(columns, schema_provider)?;

            let connector = with_map.remove("connector");

            match connector.as_ref().map(|c| c.as_str()) {
                Some("memory") | None => {
                    if fields.iter().any(|f| f.expression.is_some()) {
                        bail!("Virtual fields are not supported in memory tables; instead write a query");
                    }

                    if !with_map.is_empty() {
                        bail!("Memory tables do not allow any with options; to create a connector table set the 'connector' option");
                    }

                    Ok(Some(Table::MemoryTable { name, fields }))
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
            Table::MemoryTable { fields, .. }
            | Table::ConnectorTable(ConnectorTable { fields, .. }) => fields
                .iter()
                .map(|field| {
                    let field: Field = field.clone().into();
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
                op: WriteOp::Insert,
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

    pub fn get_fields(&self) -> Result<Vec<Field>> {
        match self {
            Insert::InsertQuery { logical_plan, .. } | Insert::Anonymous { logical_plan } => {
                logical_plan
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| {
                        let field: Field = (**field.field()).clone();
                        Ok(field)
                    })
                    .collect::<Result<Vec<_>>>()
            }
        }
    }
}
