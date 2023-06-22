use arrow_schema::{Field, DataType};
use arroyo_datastream::{SerializationMode, Operator, ConnectorOp};
use datafusion_expr::LogicalPlan;
use anyhow::{Result, bail, anyhow};

use crate::{types::{StructField, StructDef, TypeDef}, expressions::{Expression, Column, ColumnExpression}, pipeline::{SqlOperator, SourceOperator, SqlPipelineBuilder}, external::{SqlSource, SqlSink}, ConnectorType, operators::Projection};

#[derive(Debug, Clone)]
pub struct ConnectorTable {
        pub id: i64,
        pub name: String,
        pub connector_type: ConnectorType,
        pub fields: Vec<StructField>,
        pub type_name: Option<String>,
        pub operator: String,
        pub config: String,
        pub description: String,
        pub serialization_mode: SerializationMode,
        pub event_time_field: Option<String>,
        pub watermark_field: Option<String>,
}

impl ConnectorTable {
    fn physical_fields(&self) -> Vec<&StructField> {
        self.fields.iter()
            .filter(|f| f.expression.is_none())
            .collect()
    }

    fn has_virtual_fields(&self) -> bool {
        self.fields.iter().any(|f| f.expression.is_some())
    }

    fn is_update(&self) -> bool {
        self.serialization_mode == SerializationMode::DebeziumJson
    }

    fn virtual_field_projection(&self) -> Option<Projection> {
        if self.has_virtual_fields() {
            let (field_names, field_computations) = self.fields
                .iter()
                .map(|field| {
                    (Column {
                        relation: None,
                        name: field.name.clone(),
                    },
                    field.expression.as_ref().map(|expr| *(expr.clone()))
                        .unwrap_or_else(||
                            Expression::Column(ColumnExpression::new(field.clone())))
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
            let field = self.fields.iter().find(|f: &&StructField|
                f.name == *field_name && matches!(f.data_type, TypeDef::DataType(DataType::Timestamp(..), _)))
                .ok_or_else(|| anyhow!("event_time_field {} not found or not a timestamp", field_name))?;

            Ok(Some(Expression::Column(ColumnExpression::new(field.clone()))))
        } else {
            Ok(None)
        }
    }

    fn watermark_column(&self) -> Result<Option<Expression>> {
        if let Some(field_name) = &self.watermark_field {
            // check that a column exists and it is a timestamp
            let field = self.fields.iter().find(|f: &&StructField|
                f.name == *field_name && matches!(f.data_type, TypeDef::DataType(DataType::Timestamp(..), _)))
                .ok_or_else(|| anyhow!("watermark_field {} not found or not a timestamp", field_name))?;

            Ok(Some(Expression::Column(ColumnExpression::new(field.clone()))))
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

    pub fn as_sql_source(&self) -> Result<SqlOperator> {
        match self.connector_type {
            ConnectorType::Source => {}
            ConnectorType::Sink => {
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
            id: Some(self.id),
            struct_def: StructDef {
                name: self.type_name.clone(),
                fields: self.fields.clone(),
            },
            operator: Operator::ConnectorSource(self.connector_op()),
            serialization_mode: self.serialization_mode,
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
        match self.connector_type {
            ConnectorType::Source => {
                bail!("Inserting into a source is not allowed")
            }
            ConnectorType::Sink => {}
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
                id: Some(self.id),
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
    InsertQuery {
        sink_name: String,
        logical_plan: LogicalPlan,
    },
    Anonymous {
        logical_plan: LogicalPlan,
    },
}

impl Table {
    pub fn name(&self) -> Option<String> {
        match self {
            Table::MemoryTable { name, .. }
            | Table::TableFromQuery { name, .. } => Some(name.clone()),
            Table::ConnectorTable(c) => Some(c.name.clone()),
            Table::InsertQuery { .. } | Table::Anonymous { .. } => None,
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
            Table::TableFromQuery { logical_plan, .. }
            | Table::InsertQuery { logical_plan, .. }
            | Table::Anonymous { logical_plan } => logical_plan
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
            Table::ConnectorTable(cn) => {
                cn.as_sql_source()
            }
            Table::MemoryTable { name, .. } => {
                Ok(builder.planned_tables.get(name).ok_or_else(|| {
                    anyhow!(
                        "memory table {} not found in planned tables. This is a bug.",
                        name
                    )
                })?.clone())
            }
            Table::TableFromQuery { logical_plan, .. } => builder.insert_sql_plan(&logical_plan.clone()),
            Table::InsertQuery { .. } => {
                bail!("insert queries can't be a table scan");
            }
            Table::Anonymous { .. } => {
                bail!("anonymous queries can't be table sources.")
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
            Table::InsertQuery { .. } => todo!(),
            Table::Anonymous { .. } => todo!(),
        }
    }
}
