use crate::extension::remote_table::RemoteTableExtension;
use crate::types::convert_data_type;
use crate::{
    external::{ProcessingMode, SqlSource},
    fields_with_qualifiers, multifield_partial_ord, parse_sql, ArroyoSchemaProvider, DFField,
};
use crate::{rewrite_plan, DEFAULT_IDLE_TIME};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use arroyo_connectors::connector_for_type;
use arroyo_datastream::default_sink;
use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, SourceField,
};
use arroyo_rpc::formats::{BadData, Format, Framing, JsonFormat};
use arroyo_rpc::grpc::api::ConnectorOp;
use arroyo_rpc::ConnectorOptions;
use arroyo_types::ArroyoExtensionType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{
    config::ConfigOptions, plan_datafusion_err, DFSchema, Result, ScalarValue,
};
use datafusion::common::{plan_err, Column, DataFusionError};
use datafusion::logical_expr::{
    CreateMemoryTable, CreateView, DdlStatement, DmlStatement, Expr, ExprSchemable, Extension,
    LogicalPlan, WriteOp,
};
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use datafusion::optimizer::eliminate_cross_join::EliminateCrossJoin;
use datafusion::optimizer::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_group_by_constant::EliminateGroupByConstant;
use datafusion::optimizer::eliminate_join::EliminateJoin;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::eliminate_nested_union::EliminateNestedUnion;
use datafusion::optimizer::eliminate_one_union::EliminateOneUnion;
use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::push_down_limit::PushDownLimit;
use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::unwrap_cast_in_comparison::UnwrapCastInComparison;
use datafusion::optimizer::OptimizerRule;
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::ast::{CreateTable, Query};
use datafusion::{
    optimizer::{optimizer::Optimizer, OptimizerContext},
    sql::{
        planner::SqlToRel,
        sqlparser::ast::{ColumnDef, ColumnOption, Statement},
    },
};
use itertools::Itertools;
use sqlparser::ast;
use sqlparser::ast::TableConstraint;
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};
use tracing::warn;

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
    pub primary_keys: Arc<Vec<String>>,
    pub inferred_fields: Option<Vec<FieldRef>>,
    pub partition_fields: Arc<Option<Vec<String>>>,

    // for lookup tables
    pub lookup_cache_max_bytes: Option<u64>,
    pub lookup_cache_ttl: Option<Duration>,
}

multifield_partial_ord!(
    ConnectorTable,
    id,
    connector,
    name,
    connection_type,
    config,
    description,
    format,
    event_time_field,
    watermark_field,
    idle_time,
    primary_keys
);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldSpec {
    Struct(Field),
    Metadata { field: Field, key: String },
    Virtual { field: Field, expression: Box<Expr> },
}

impl FieldSpec {
    fn is_virtual(&self) -> bool {
        match self {
            FieldSpec::Struct(_) | FieldSpec::Metadata { .. } => false,
            FieldSpec::Virtual { .. } => true,
        }
    }
    pub fn field(&self) -> &Field {
        match self {
            FieldSpec::Struct(f) => f,
            FieldSpec::Metadata { field, .. } => field,
            FieldSpec::Virtual { field, .. } => field,
        }
    }

    fn metadata_key(&self) -> Option<&str> {
        match &self {
            FieldSpec::Metadata { key, .. } => Some(key.as_str()),
            _ => None,
        }
    }
}

impl From<Field> for FieldSpec {
    fn from(value: Field) -> Self {
        FieldSpec::Struct(value)
    }
}

fn produce_optimized_plan(
    statement: &Statement,
    schema_provider: &ArroyoSchemaProvider,
) -> Result<LogicalPlan> {
    let sql_to_rel = SqlToRel::new(schema_provider);

    let plan = sql_to_rel.sql_statement_to_plan(statement.clone())?;

    let analyzed_plan = schema_provider.analyzer.execute_and_check(
        plan,
        &ConfigOptions::default(),
        |_plan, _rule| {},
    )?;

    let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
        Arc::new(EliminateNestedUnion::new()),
        Arc::new(SimplifyExpressions::new()),
        Arc::new(UnwrapCastInComparison::new()),
        Arc::new(ReplaceDistinctWithAggregate::new()),
        Arc::new(EliminateJoin::new()),
        Arc::new(DecorrelatePredicateSubquery::new()),
        Arc::new(ScalarSubqueryToJoin::new()),
        Arc::new(ExtractEquijoinPredicate::new()),
        Arc::new(EliminateDuplicatedExpr::new()),
        Arc::new(EliminateFilter::new()),
        Arc::new(EliminateCrossJoin::new()),
        Arc::new(CommonSubexprEliminate::new()),
        Arc::new(EliminateLimit::new()),
        Arc::new(PropagateEmptyRelation::new()),
        // Must be after PropagateEmptyRelation
        Arc::new(EliminateOneUnion::new()),
        Arc::new(FilterNullJoinKeys::default()),
        Arc::new(EliminateOuterJoin::new()),
        // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
        Arc::new(PushDownLimit::new()),
        Arc::new(PushDownFilter::new()),
        // This rule creates nested aggregates off of count(distinct value), which we don't support.
        //Arc::new(SingleDistinctToGroupBy::new()),

        // The previous optimizations added expressions and projections,
        // that might benefit from the following rules
        Arc::new(SimplifyExpressions::new()),
        Arc::new(UnwrapCastInComparison::new()),
        Arc::new(CommonSubexprEliminate::new()),
        Arc::new(EliminateGroupByConstant::new()),
        // This rule can drop event time calculation fields if they aren't used elsewhere.
        //Arc::new(OptimizeProjections::new()),
    ];

    let optimizer = Optimizer::with_rules(rules);
    let plan = optimizer.optimize(
        analyzed_plan,
        &OptimizerContext::default(),
        |_plan, _rule| {},
    )?;
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
                .map(|f| FieldSpec::Struct(f.clone().into()))
                .collect(),
            config: value.config,
            description: value.description,
            format: value.schema.format.clone(),
            event_time_field: None,
            watermark_field: None,
            idle_time: DEFAULT_IDLE_TIME,
            primary_keys: Arc::new(vec![]),
            partition_fields: Arc::new(value.partition_fields),
            inferred_fields: None,
            lookup_cache_max_bytes: None,
            lookup_cache_ttl: None,
        }
    }
}

fn to_debezium_fields(fields: Vec<FieldRef>) -> Vec<Field> {
    let df_struct_type = DataType::Struct(fields.iter().cloned().collect());
    let before_field_spec = Field::new("before", df_struct_type.clone(), true);
    let after_field_spec = Field::new("after", df_struct_type, true);
    let op_field_spec = Field::new("op", DataType::Utf8, false);
    vec![before_field_spec, after_field_spec, op_field_spec]
}

impl ConnectorTable {
    #[allow(clippy::too_many_arguments)]
    fn from_options(
        name: &str,
        connector: &str,
        temporary: bool,
        mut fields: Vec<FieldSpec>,
        primary_keys: Vec<String>,
        watermark: Option<(String, Option<ast::Expr>)>,
        options: &mut ConnectorOptions,
        connection_profile: Option<&ConnectionProfile>,
        schema_provider: &ArroyoSchemaProvider,
    ) -> Result<Self> {
        // TODO: a more principled way of letting connectors dictate types to use
        if "delta" == connector {
            fields = fields
                .into_iter()
                .map(|field_spec| match &field_spec {
                    FieldSpec::Struct(struct_field) => match struct_field.data_type() {
                        DataType::Timestamp(_, None) => {
                            FieldSpec::Struct(struct_field.clone().with_data_type(
                                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                            ))
                        }
                        _ => field_spec,
                    },
                    FieldSpec::Metadata { .. } | FieldSpec::Virtual { .. } => {
                        unreachable!("delta lake is only a sink, can't have virtual fields")
                    }
                })
                .collect();
        }
        let connector = connector_for_type(connector)
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown connector '{}'", connector)))?;

        let format = Format::from_opts(options)
            .map_err(|e| DataFusionError::Plan(format!("invalid format: '{e}'")))?;

        let framing = Framing::from_opts(options)
            .map_err(|e| DataFusionError::Plan(format!("invalid framing: '{e}'")))?;

        if temporary {
            if let Some(t) = options.insert_str("type", "lookup")? {
                if t != "lookup" {
                    return plan_err!("Cannot have a temporary table with type '{}'; temporary tables must be type 'lookup'", t);
                }
            }
        }

        let mut input_to_schema_fields = fields.clone();

        if let Some(Format::Json(JsonFormat { debezium: true, .. })) = &format {
            // check that there are no virtual fields in fields
            if fields.iter().any(|f| f.is_virtual()) {
                return plan_err!("can't use virtual fields with debezium format");
            }

            // if this is inferred, don't wrap the empty schema in Debezium format
            if !fields.is_empty() {
                input_to_schema_fields = to_debezium_fields(
                    input_to_schema_fields
                        .into_iter()
                        .map(|f| Arc::new(f.field().clone()))
                        .collect(),
                )
                .into_iter()
                .map(FieldSpec::Struct)
                .collect();
            }
        }

        let schema_fields: Vec<SourceField> = input_to_schema_fields
            .iter()
            .filter(|f| !f.is_virtual())
            .map(|f| {
                let struct_field = f.field();
                let mut sf: SourceField = struct_field.clone().try_into().map_err(|_| {
                    DataFusionError::Plan(format!(
                        "field '{}' has a type '{:?}' that cannot be used in a connection table",
                        struct_field.name(),
                        struct_field.data_type()
                    ))
                })?;

                if let Some(key) = f.metadata_key() {
                    sf.metadata_key = Some(key.to_string());
                }

                Ok(sf)
            })
            .collect::<Result<_>>()?;
        let bad_data = BadData::from_opts(options)
            .map_err(|e| DataFusionError::Plan(format!("Invalid bad_data: '{e}'")))?;

        let schema = ConnectionSchema::try_new(
            format,
            bad_data,
            framing,
            None,
            schema_fields,
            None,
            Some(fields.is_empty()),
            primary_keys.iter().cloned().collect(),
        )
        .map_err(|e| DataFusionError::Plan(format!("could not create connection schema: {}", e)))?;

        let connection = connector
            .from_options(name, options, Some(&schema), connection_profile)
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;

        let mut table: ConnectorTable = connection.into();
        if !fields.is_empty() {
            table.fields = fields;
        }

        if let Some(event_time_field) = options.pull_opt_field("event_time_field")? {
            warn!("`event_time_field` WITH option is deprecated; use WATERMARK FOR syntax");
            table.event_time_field = Some(event_time_field);
        }

        if let Some(watermark_field) = options.pull_opt_field("watermark_field")? {
            warn!("`watermark_field` WITH option is deprecated; use WATERMARK FOR syntax");
            table.watermark_field = Some(watermark_field);
        }

        if let Some((time_field, watermark_expr)) = watermark {
            let schema =
                DFSchema::try_from_qualified_schema(&table.name, &table.physical_schema())?;

            let field = table
                .fields
                .iter()
                .find(|f| f.field().name().as_str() == time_field)
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "WATERMARK FOR field `{}` does not exist in table",
                        time_field
                    )
                })?;

            if !matches!(field.field().data_type(), DataType::Timestamp(_, None)) {
                return plan_err!(
                    "WATERMARK FOR field `{:?}` has type {}, but expected TIMESTAMP",
                    time_field,
                    field.field().data_type()
                );
            }

            table.event_time_field = Some(time_field.clone());

            if let Some(expr) = watermark_expr {
                let logical_expr =
                    plan_generating_expr(&expr, &table.name, &schema, schema_provider)
                        .map_err(|e| e.context("could not plan watermark expression"))?;

                let (data_type, nullable) = logical_expr.data_type_and_nullable(&schema)?;
                if !matches!(data_type, DataType::Timestamp(_, _)) {
                    return plan_err!(
                        "the type of the WATERMARK FOR expression must be TIMESTAMP, but was {}",
                        data_type
                    );
                }
                if nullable {
                    return plan_err!("the type of the WATERMARK FOR expression must be NOT NULL");
                }

                table.fields.push(FieldSpec::Virtual {
                    field: Field::new("__watermark", logical_expr.get_type(&schema)?, false),
                    expression: Box::new(logical_expr),
                });
                table.watermark_field = Some("__watermark".to_string());
            } else {
                table.watermark_field = Some(time_field);
            }
        }

        table.idle_time = options
            .pull_opt_i64("idle_micros")?
            .or_else(|| DEFAULT_IDLE_TIME.map(|t| t.as_micros() as i64))
            .filter(|t| *t <= 0)
            .map(|t| Duration::from_micros(t as u64));

        table.lookup_cache_max_bytes = options.pull_opt_u64("lookup.cache.max_bytes")?;

        table.lookup_cache_ttl = options.pull_opt_duration("lookup.cache.ttl")?;

        if !options.is_empty() {
            let keys: Vec<String> = options.keys().map(|s| format!("'{}'", s)).collect();
            return plan_err!(
                "unknown options provided in WITH clause: {}",
                keys.join(", ")
            );
        }

        if table.connection_type == ConnectionType::Source
            && table.is_updating()
            && primary_keys.is_empty()
        {
            return plan_err!("Debezium source must have at least one PRIMARY KEY field");
        }

        table.primary_keys = Arc::new(primary_keys);

        Ok(table)
    }

    fn has_virtual_fields(&self) -> bool {
        self.fields.iter().any(|f| f.is_virtual())
    }

    pub(crate) fn is_updating(&self) -> bool {
        self.format
            .as_ref()
            .map(|f| f.is_updating())
            .unwrap_or(false)
    }

    fn timestamp_override(&self) -> Result<Option<Expr>> {
        if let Some(field_name) = &self.event_time_field {
            if self.is_updating() {
                return plan_err!("can't use event_time_field with update mode.");
            }

            // check that a column exists and it is a timestamp
            let field = self.get_time_field(field_name)?;

            Ok(Some(Expr::Column(Column::from_name(field.field().name()))))
        } else {
            Ok(None)
        }
    }

    fn get_time_field(&self, field_name: &String) -> Result<&FieldSpec, DataFusionError> {
        let field = self
            .fields
            .iter()
            .find(|f| {
                f.field().name() == field_name
                    && matches!(f.field().data_type(), DataType::Timestamp(..))
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!("field {} not found or not a timestamp", field_name))
            })?;
        Ok(field)
    }

    fn watermark_column(&self) -> Result<Option<Expr>> {
        if let Some(field_name) = &self.watermark_field {
            // check that a column exists and it is a timestamp
            let field = self.get_time_field(field_name)?;

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
        if self.is_updating() {
            ProcessingMode::Update
        } else {
            ProcessingMode::Append
        }
    }

    pub fn as_sql_source(&self) -> Result<SourceOperator> {
        match self.connection_type {
            ConnectionType::Source => {}
            ConnectionType::Sink | ConnectionType::Lookup => {
                return plan_err!("cannot read from sink");
            }
        };

        if self.is_updating() && self.has_virtual_fields() {
            return plan_err!("can't read from a source with virtual fields and update mode.");
        }

        let timestamp_override = self.timestamp_override()?;
        let watermark_column = self.watermark_column()?;

        let source = SqlSource {
            id: self.id,
            struct_def: self
                .fields
                .iter()
                .filter(|f| !f.is_virtual())
                .map(|f| Arc::new(f.field().clone()))
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

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Table {
    LookupTable(ConnectorTable),
    ConnectorTable(ConnectorTable),
    MemoryTable {
        name: String,
        fields: Vec<FieldRef>,
        logical_plan: Option<LogicalPlan>,
    },
    TableFromQuery {
        name: String,
        logical_plan: LogicalPlan,
    },
    PreviewSink {
        logical_plan: LogicalPlan,
    },
}

fn plan_generating_expr(
    expr: &sqlparser::ast::Expr,
    name: &str,
    schema: &DFSchema,
    schema_provider: &ArroyoSchemaProvider,
) -> Result<Expr, DataFusionError> {
    let sql = format!("SELECT {} from {}", expr, name);
    let statement = parse_sql(&sql)
        .expect("generating expression should be valid")
        .into_iter()
        .next()
        .expect("generating expression should produce one statement");

    let mut schema_provider = schema_provider.clone();
    schema_provider.insert_table(Table::MemoryTable {
        name: name.to_string(),
        fields: schema.fields().to_vec(),
        logical_plan: None,
    });

    let plan = produce_optimized_plan(&statement, &schema_provider)?;

    match plan {
        LogicalPlan::Projection(p) => Ok(p.expr.into_iter().next().unwrap()),
        p => {
            unreachable!(
                "top-level plan from generating expression should be a projection, but is {:?}",
                p
            );
        }
    }
}

#[derive(Default)]
struct MetadataFinder {
    key: Option<String>,
    depth: usize,
}

impl<'a> TreeNodeVisitor<'a> for MetadataFinder {
    type Node = Expr;

    fn f_down(&mut self, node: &'a Self::Node) -> Result<TreeNodeRecursion> {
        if let Expr::ScalarFunction(func) = node {
            if func.name() == "metadata" {
                if self.depth > 0 {
                    return plan_err!(
                        "Metadata columns must have only a single call to 'metadata'"
                    );
                }

                return if let &[arg] = &func.args.as_slice() {
                    if let Expr::Literal(ScalarValue::Utf8(Some(key))) = &arg {
                        self.key = Some(key.clone());
                        Ok(TreeNodeRecursion::Stop)
                    } else {
                        plan_err!("For metadata columns, metadata call must have a literal string argument")
                    }
                } else {
                    plan_err!("For metadata columns, metadata call must have a single argument")
                };
            }
        }
        self.depth += 1;
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, _node: &'a Self::Node) -> Result<TreeNodeRecursion> {
        self.depth -= 1;
        Ok(TreeNodeRecursion::Continue)
    }
}

impl Table {
    fn schema_from_columns(
        table_name: &str,
        columns: &[ColumnDef],
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
                    Field::new(name, data_type.clone(), nullable),
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

                let metadata = column.options.iter().find_map(|option| {
                    if let ColumnOption::MetadataField(field) = &option.option {
                        Some(field.clone())
                    } else {
                        None
                    }
                });

                Ok((struct_field, generating_expression, metadata))
            })
            .collect::<Result<Vec<_>>>()?;

        let physical_fields: Vec<_> = struct_field_pairs
            .iter()
            .filter_map(
                |(field, generating_expression, _)| match generating_expression {
                    Some(_) => None,
                    None => Some(field.clone()),
                },
            )
            .collect();

        let physical_schema = DFSchema::new_with_metadata(
            physical_fields
                .iter()
                .map(|f| {
                    Ok(
                        DFField::new_unqualified(f.name(), f.data_type().clone(), f.is_nullable())
                            .into(),
                    )
                })
                .collect::<Result<Vec<_>>>()?,
            HashMap::new(),
        )?;

        struct_field_pairs
            .into_iter()
            .map(|(struct_field, generating_expression, metadata)| {
                if let Some(generating_expression) = generating_expression {
                    let df_expr = plan_generating_expr(
                        &generating_expression,
                        table_name,
                        &physical_schema,
                        schema_provider,
                    )?;

                    let mut metadata_finder = MetadataFinder::default();
                    df_expr.visit(&mut metadata_finder)?;

                    if let Some(key) = metadata_finder.key {
                        warn!("metadata('key') function expressions are deprecated; use the METADATA FROM syntax");
                        Ok(FieldSpec::Metadata {
                            field: struct_field,
                            key,
                        })
                    } else {
                        Ok(FieldSpec::Virtual {
                            field: struct_field,
                            expression: Box::new(df_expr),
                        })
                    }
                } else {
                    Ok(if let Some(key) = metadata {
                        FieldSpec::Metadata {
                            field: struct_field,
                            key,
                        }
                    } else {
                        FieldSpec::Struct(struct_field)
                    })
                }
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn try_from_statement(
        statement: &Statement,
        schema_provider: &ArroyoSchemaProvider,
    ) -> Result<Option<Self>> {
        if let Statement::CreateTable(CreateTable {
            name,
            columns,
            with_options,
            query: None,
            temporary,
            constraints,
            ..
        }) = statement
        {
            let name: String = name.to_string();
            let mut connector_opts: ConnectorOptions = with_options.try_into()?;

            let connector = connector_opts.pull_opt_str("connector")?;
            let fields = Self::schema_from_columns(&name, columns, schema_provider)?;

            let primary_keys = columns
                .iter()
                .filter(|c| {
                    c.options.iter().any(|opt| {
                        matches!(
                            opt.option,
                            ColumnOption::Unique {
                                is_primary: true,
                                ..
                            }
                        )
                    })
                })
                .map(|c| c.name.value.clone())
                .collect();

            match connector.as_deref() {
                Some("memory") | None => {
                    if fields.iter().any(|f| f.is_virtual()) {
                        return plan_err!("Virtual fields are not supported in memory tables; instead write a query");
                    }

                    if !connector_opts.is_empty() {
                        if connector.is_some() {
                            return plan_err!("Memory tables do not allow with options");
                        } else {
                            return plan_err!("Memory tables do not allow with options; to create a connection table set the 'connector' option");
                        }
                    }

                    Ok(Some(Table::MemoryTable {
                        name,
                        fields: fields
                            .into_iter()
                            .map(|f| Arc::new(f.field().clone()))
                            .collect(),
                        logical_plan: None,
                    }))
                }
                Some(connector) => {
                    let connection_profile =
                        match connector_opts.pull_opt_str("connection_profile")? {
                            Some(connection_profile_name) => Some(
                                schema_provider
                                    .profiles
                                    .get(&connection_profile_name)
                                    .ok_or_else(|| {
                                        DataFusionError::Plan(format!(
                                            "connection profile '{}' not found",
                                            connection_profile_name
                                        ))
                                    })?,
                            ),
                            None => None,
                        };

                    let watermark_constraints: Vec<_> = constraints
                        .iter()
                        .map(|c| match c {
                            TableConstraint::Watermark {
                                column_name,
                                watermark_expr,
                            } => Ok((column_name.to_string(), watermark_expr.clone())),
                            c => Err(plan_datafusion_err!("Unsupported table constraint `{}`", c)),
                        })
                        .try_collect()?;

                    if watermark_constraints.len() > 1 {
                        return plan_err!("Only one WATERMARK FOR constraint is allowed per table");
                    }

                    let table = ConnectorTable::from_options(
                        &name,
                        connector,
                        *temporary,
                        fields,
                        primary_keys,
                        watermark_constraints.into_iter().next(),
                        &mut connector_opts,
                        connection_profile,
                        schema_provider,
                    )
                    .map_err(|e| e.context(format!("Failed to create table {}", name)))?;

                    Ok(Some(match table.connection_type {
                        ConnectionType::Source | ConnectionType::Sink => {
                            Table::ConnectorTable(table)
                        }
                        ConnectionType::Lookup => Table::LookupTable(table),
                    }))
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
                    let rewritten_plan = rewrite_plan(input.as_ref().clone(), schema_provider)?;
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
            Table::ConnectorTable(c) | Table::LookupTable(c) => c.name.as_str(),
            Table::PreviewSink { .. } => "preview",
        }
    }

    pub fn set_inferred_fields(&mut self, fields: Vec<DFField>) -> Result<()> {
        let Table::ConnectorTable(t) = self else {
            return Ok(());
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
                return plan_err!("all inserts into a table must share the same schema");
            }
        }

        let fields: Vec<_> = fields.into_iter().map(|f| f.field).collect();
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
            })
            | Table::LookupTable(ConnectorTable {
                fields,
                inferred_fields,
                ..
            }) => inferred_fields.clone().unwrap_or_else(|| {
                fields
                    .iter()
                    .map(|field| field.field().clone().into())
                    .collect()
            }),
            Table::TableFromQuery { logical_plan, .. } => {
                logical_plan.schema().fields().iter().cloned().collect()
            }
            Table::PreviewSink { logical_plan } => {
                logical_plan.schema().fields().iter().cloned().collect()
            }
        }
    }

    pub fn connector_op(&self) -> Result<ConnectorOp> {
        match self {
            Table::ConnectorTable(c) | Table::LookupTable(c) => Ok(c.connector_op()),
            Table::MemoryTable { .. } => plan_err!("can't write to a memory table"),
            Table::TableFromQuery { .. } => todo!(),
            Table::PreviewSink { logical_plan: _ } => Ok(default_sink()),
        }
    }

    pub fn partition_fields(&self) -> Option<&Vec<String>> {
        match self {
            Table::ConnectorTable(c) => (*c.partition_fields).as_ref(),
            _ => None,
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
    source: &Query,
    table_name: String,
    schema_provider: &mut ArroyoSchemaProvider,
) -> Result<()> {
    let plan =
        produce_optimized_plan(&Statement::Query(Box::new(source.clone())), schema_provider)?;
    let table = schema_provider
        .get_table_mut(&table_name)
        .ok_or_else(|| DataFusionError::Plan(format!("table {} not found", table_name)))?;

    table.set_inferred_fields(fields_with_qualifiers(plan.schema()))?;

    Ok(())
}

impl Insert {
    pub fn try_from_statement(
        statement: &Statement,
        schema_provider: &mut ArroyoSchemaProvider,
    ) -> Result<Insert> {
        if let Statement::Insert(insert) = statement {
            infer_sink_schema(
                insert.source.as_ref().unwrap(),
                insert.table_name.to_string(),
                schema_provider,
            )?;
        }

        let logical_plan = produce_optimized_plan(statement, schema_provider)?;

        match &logical_plan {
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::Insert(_),
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
