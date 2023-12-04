use anyhow::bail;
use arroyo_openapi::types::{QueryValidationResult, ValidateQueryPost};
use arroyo_openapi::Client;
use sqlparser::ast::{ObjectName, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use thiserror::Error;
use tracing::warn;

pub struct QueryModel {
    ddl: HashMap<ObjectName, Statement>,
    client: Client,
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("error communicating with server")]
    ClientError(#[from] arroyo_openapi::Error),

    #[error("Query error")]
    InvalidQuery(Vec<String>),
}

impl QueryModel {
    pub fn new(client: Client) -> Self {
        Self {
            ddl: HashMap::new(),
            client,
        }
    }

    fn build_query(queries: &Vec<String>, ddl: &HashMap<ObjectName, Statement>) -> String {
        let mut statements: Vec<_> = ddl.iter().map(|(_, t)| t.to_string()).collect();

        statements.extend(queries.iter().cloned());

        statements.join(";\n")
    }

    async fn validate_query(
        &self,
        query: &str,
    ) -> Result<QueryValidationResult, arroyo_openapi::Error> {
        Ok(self
            .client
            .validate_query()
            .body(ValidateQueryPost::builder().query(query.to_string()))
            .send()
            .await?
            .into_inner())
    }

    fn parse_query(
        &self,
        query: &str,
    ) -> anyhow::Result<(Vec<String>, HashMap<ObjectName, Statement>)> {
        let dialect = PostgreSqlDialect {};
        let ast = Parser::parse_sql(&dialect, query.to_string())?;

        let mut queries = vec![];

        let mut new_ddl = self.ddl.clone();

        for statement in ast {
            match &statement {
                Statement::Query(_) | Statement::Insert { .. } => {
                    queries.push(statement.to_string())
                }
                Statement::CreateView { name, .. } | Statement::CreateTable { name, .. } => {
                    new_ddl.insert(name.to_owned(), statement.clone());
                }
                Statement::Drop {
                    if_exists, names, ..
                } => {
                    for name in names {
                        if new_ddl.remove(&name).is_none() {
                            if *if_exists {
                                warn!("Not dropping {}, which does not exist", name);
                            } else {
                                bail!("Failed to drop {}; table does not exist", name);
                            }
                        }
                    }
                }
                Statement::Copy { .. } => {
                    bail!("COPY is not supported");
                }
                Statement::Update { .. } => {
                    bail!("UPDATE is not supported");
                }
                Statement::Delete { .. } => {
                    bail!("DELETE is not supported");
                }
                Statement::AlterTable { .. } => {
                    bail!("ALTER is not supported; DROP and recreate instead")
                }
                Statement::StartTransaction { .. }
                | Statement::SetTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. } => {
                    bail!("Transactions are not supported");
                }
            }
        }

        Ok((queries, new_ddl))
    }

    pub async fn process_buffer(&mut self, query: String) -> Result<Option<String>, QueryError> {
        let (queries, new_ddl) = self
            .parse_query(&query)
            .map_err(|e| QueryError::InvalidQuery(vec![e.to_string()]))?;

        let query = Self::build_query(&queries, &new_ddl);

        let result = self.validate_query(&query).await?;

        if result.missing_query
            || result.errors.len() == 1
                && (&result.errors[0] == "The provided SQL does not contain a query"
                    || &result.errors[0] == "Query is empty")
        {
            self.ddl = new_ddl;
            Ok(None)
        } else if !result.errors.is_empty() {
            Err(QueryError::InvalidQuery(result.errors))
        } else {
            Ok(Some(query))
        }
    }
}
