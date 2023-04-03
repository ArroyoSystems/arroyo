#![allow(clippy::new_without_default)]
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{self, DataType, Field};
use arroyo_datastream::{Operator, Program};

use datafusion::optimizer::optimizer::Optimizer;
use datafusion::optimizer::OptimizerContext;
use datafusion::physical_plan::functions::make_scalar_function;

mod expressions;
mod operators;
mod pipeline;
pub mod schemas;
pub mod types;

use datafusion::prelude::create_udf;
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::{planner::ContextProvider, TableReference};
use datafusion_common::DataFusionError;
use datafusion_expr::{
    logical_plan::builder::LogicalTableSource, AggregateUDF, ScalarUDF, TableSource,
};
use datafusion_expr::{
    AccumulatorFunctionImplementation, ReturnTypeFunction, Signature, StateTypeFunction,
    TypeSignature, Volatility,
};
use pipeline::get_program_from_plan;
use schemas::window_arrow_struct;
use types::{StructDef, StructField};

use std::{collections::HashMap, sync::Arc};

#[cfg(test)]
mod test;

pub struct ArroyoSchemaProvider {
    pub source_defs: HashMap<String, String>,
    pub tables: HashMap<String, Arc<dyn TableSource>>,
    pub functions: HashMap<String, Arc<ScalarUDF>>,
    pub sources: HashMap<String, SqlSource>,
    config_options: datafusion::config::ConfigOptions,
}

impl ArroyoSchemaProvider {
    pub fn new() -> Self {
        let tables = HashMap::new();
        let mut functions = HashMap::new();

        let fn_impl = |args: &[ArrayRef]| Ok(Arc::new(args[0].clone()) as ArrayRef);

        let window_return_type = Arc::new(window_arrow_struct());
        functions.insert(
            "hop".to_string(),
            Arc::new(create_udf(
                "hop",
                vec![],
                window_return_type.clone(),
                Volatility::Stable,
                make_scalar_function(fn_impl),
            )),
        );
        functions.insert(
            "tumble".to_string(),
            Arc::new(create_udf(
                "tumble",
                vec![],
                window_return_type,
                Volatility::Stable,
                make_scalar_function(fn_impl),
            )),
        );
        Self {
            tables,
            functions,
            sources: HashMap::new(),
            source_defs: HashMap::new(),
            config_options: datafusion::config::ConfigOptions::new(),
        }
    }

    pub fn add_source(
        &mut self,
        id: i64,
        name: impl Into<String>,
        fields: Vec<StructField>,
        operator: Operator,
    ) {
        self.add_source_with_type(id, name, fields, operator, None);
    }

    pub fn add_source_with_type(
        &mut self,
        id: i64,
        name: impl Into<String>,
        fields: Vec<StructField>,
        operator: Operator,
        type_name: Option<String>,
    ) {
        let name: String = name.into();
        self.sources.insert(
            name.clone(),
            SqlSource {
                id,
                struct_def: StructDef {
                    name: type_name,
                    fields: fields.clone(),
                },
                operator,
            },
        );

        let arrow_fields = fields.into_iter().map(|f| f.into()).collect();
        self.tables.insert(name, create_table_source(arrow_fields));
    }

    pub fn add_defs(&mut self, source: impl Into<String>, defs: impl Into<String>) {
        self.source_defs.insert(source.into(), defs.into());
    }
}

fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(Arc::new(
        datatypes::Schema::new_with_metadata(fields, HashMap::new()),
    )))
}

impl ContextProvider for ArroyoSchemaProvider {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
            Some(table) => Ok(table.clone()),
            _ => Err(DataFusionError::Plan(format!(
                "Table not found: {}",
                name.table()
            ))),
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        match name {
            "lexographic_max" => {
                let return_type: ReturnTypeFunction = Arc::new(|input_types| {
                    let struct_fields = input_types
                        .iter()
                        .enumerate()
                        .map(|(i, data_type)| {
                            Field::new(format!("_{}", i).as_str(), data_type.clone(), false)
                        })
                        .collect();
                    let result_type: DataType = DataType::Struct(struct_fields);
                    Ok(Arc::new(result_type))
                });
                let accumulator: AccumulatorFunctionImplementation = Arc::new(|_| todo!());
                let state_type: StateTypeFunction = Arc::new(|_| todo!());
                Some(Arc::new(AggregateUDF::new(
                    "lexographic_max",
                    &Signature::one_of(
                        vec![
                            TypeSignature::Any(1),
                            TypeSignature::Any(2),
                            TypeSignature::Any(3),
                            TypeSignature::Any(4),
                        ],
                        Volatility::Immutable,
                    ),
                    &return_type,
                    &accumulator,
                    &state_type,
                )))
            }
            _ => None,
        }
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &datafusion::config::ConfigOptions {
        &self.config_options
    }
}

#[derive(Clone, Debug)]
pub struct SqlSource {
    pub id: i64,
    pub struct_def: StructDef,
    pub operator: Operator,
}

#[derive(Clone, Debug)]
pub struct SqlConfig {
    pub default_parallelism: usize,
    pub sink: Operator,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self {
            default_parallelism: 4,
            sink: Operator::ConsoleSink,
        }
    }
}

pub async fn parse_and_get_program(
    query: &str,
    schema_provider: ArroyoSchemaProvider,
    config: SqlConfig,
) -> Result<(Program, Vec<SqlSource>)> {
    let query = query.to_string();

    if query.trim().is_empty() {
        bail!("Query is empty");
    }

    tokio::spawn(async move {
        // parse the SQL
        let dialect = PostgreSqlDialect {};
        let ast = Parser::parse_sql(&dialect, &query)
            .map_err(|e| match e {
                ParserError::TokenizerError(s) | ParserError::ParserError(s) => anyhow!(s),
                ParserError::RecursionLimitExceeded => anyhow!("recursion limit"),
            })
            .with_context(|| "parse_failure")?;
        let statement = &ast[0];
        let sql_to_rel = SqlToRel::new(&schema_provider);

        let plan = sql_to_rel.sql_statement_to_plan(statement.clone())?;
        let optimizer_config = OptimizerContext::default();
        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(&plan, &optimizer_config, |_plan, _rule| {})?;
        get_program_from_plan(config, schema_provider, &optimized_plan)
    })
    .await
    .map_err(|_| anyhow!("Something went wrong"))?
}
