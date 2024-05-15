use std::sync::Arc;
use crate::types::public::PipelineType;
use crate::{AuthData, handle_db_error, handle_sqlite_error};
use deadpool_postgres::{Object};
use serde_json::Value;
use tokio::sync::{Mutex, MutexGuard};
use deadpool_postgres::{Transaction as PGTransaction};
use rusqlite::Params;
use crate::queries::api_queries;
use crate::queries::api_queries::DbUdf;
use crate::rest_utils::{ErrorResp, log_and_map};

#[derive(Clone)]
pub enum DatabaseSource {
    Postgres(deadpool_postgres::Pool),
    Sqlite(Arc<Mutex<rusqlite::Connection>>),
}

impl DatabaseSource {
    pub async fn client(&self) -> Result<Database, ErrorResp> {
        Ok(match self {
            DatabaseSource::Postgres(p) => {
                Database::Postgres(p.get().await.map_err(log_and_map)?)
            }
            DatabaseSource::Sqlite(p) => {
                Database::Sqlite(SqliteWrapper::Connection(p.lock().await))
            }
        })
    }
}

pub enum SqliteWrapper<'a> {
    Connection(MutexGuard<'a, rusqlite::Connection>),
    Transaction(rusqlite::Transaction<'a>),
}

impl <'a> SqliteWrapper<'a> {
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        match self {
            SqliteWrapper::Connection(c) => {
                c.execute(sql, params)
            }
            SqliteWrapper::Transaction(t) => {
                t.execute(sql, params)
            }
        }
    }

    pub fn prepare(&self, sql: &str) -> rusqlite::Result<rusqlite::Statement<'_>> {
        match self {
            SqliteWrapper::Connection(c) => {
                c.prepare(sql)
            }
            SqliteWrapper::Transaction(t) => {
                t.prepare(sql)
            }
        }
    }
}

pub enum Database<'a> {
    Postgres(Object),
    PostgresTx(PGTransaction<'a>),
    Sqlite(SqliteWrapper<'a>),
}

impl <'a> Database<'a> {
    pub async fn transaction<'b>(&'a mut self) -> Result<Database<'b>, ErrorResp> where 'a: 'b {
        Ok(match self {
            Database::Postgres(p) => {
                Self::PostgresTx(p.transaction().await.map_err(log_and_map)?)
            }
            Database::PostgresTx(tx) => {
                Self::PostgresTx(tx.transaction().await.map_err(log_and_map)?)
            }
            Database::Sqlite(p) => {
                match p {
                    SqliteWrapper::Connection(c) => {
                        Self::Sqlite(SqliteWrapper::Transaction(c.transaction().map_err(log_and_map)?))                        
                    }
                    SqliteWrapper::Transaction(_) => {
                        panic!("Cannot duplicate an existing sqlite transaction");                        
                    }
                }
            }
        })
    }
    
    pub async fn commit(self) -> Result<(), ErrorResp> {
        match self {
            Database::Postgres(_) => {
                // no op
            }
            Database::PostgresTx(tx) => {
                tx.commit().await.map_err(|e| handle_db_error("record", e))?;
            }
            Database::Sqlite(s) => {
                match s {
                    SqliteWrapper::Connection(_) => {}
                    SqliteWrapper::Transaction(tx) => {
                        tx.commit().map_err(|e| handle_sqlite_error("record", e))?;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    pub async fn create_pipeline(
        &self,
        pub_id: &str,
        auth: &AuthData,
        name: &str,
        pipeline_type: PipelineType,
        query: &str,
        udfs: &Value,
        program: &[u8],
        proto_version: i32,
    ) -> Result<(), ErrorResp> {
        match self {
            Database::Postgres(p) => {
                api_queries::create_pipeline()
                    .bind(
                        p,
                        &pub_id,
                        &auth.organization_id,
                        &auth.user_id,
                        &name,
                        &pipeline_type,
                        &Some(query),
                        udfs,
                        &program,
                        &proto_version,
                    )
                    .await
                    .map_err(|e| handle_db_error("pipeline", e))?;
            }
            Database::PostgresTx(p) => {
                todo!()
            }
            Database::Sqlite(c) => {
                c.execute(
                    "INSERT INTO pipelines (pub_id, organization_id, created_by, name, type, textual_repr, udfs, program, proto_version)
VALUES (:?1, :?2, :?3, :?4, :?5, :?6, :?7, :?8, :?9)",
                    (&pub_id,
                     &auth.organization_id,
                     &auth.user_id,
                     &name,
                     &format!("{:?}", pipeline_type),
                     &Some(query),
                     udfs,
                     &program,
                     &proto_version,)
                ).map_err(|e| handle_sqlite_error("pipeline", e))?;
            }
        }

        Ok(())
    }

    pub async fn get_udfs(&self, auth: &AuthData) -> Result<Vec<DbUdf>, ErrorResp> {
        Ok(match self {
            Database::Postgres(p) => {
                api_queries::get_udfs()
                    .bind(p, &auth.organization_id)
                    .all()
                    .await
                    .map_err(log_and_map)?
                    .into_iter()
                    .collect()
            }
            Database::PostgresTx(_) => {
                todo!()
            }
            Database::Sqlite(c) => {
                let mut stmt = c.prepare("SELECT pub_id, prefix, name, definition, created_at, updated_at, description, dylib_url
                FROM udfs WHERE organization_id = :?1;").unwrap();
                let results = stmt.query([&auth.organization_id]).map_err(log_and_map)?;
                results.mapped(|r| {
                        Ok(DbUdf {
                            pub_id: r.get_unwrap(0),
                            prefix: r.get_unwrap(1),
                            name: r.get_unwrap(2),
                            definition: r.get_unwrap(3),
                            created_at: r.get_unwrap(4),
                            updated_at: r.get_unwrap(5),
                            description: r.get_unwrap(6),
                            dylib_url: r.get_unwrap(7),
                        })
                    })
                    .map(|t| t.unwrap())
                    .collect()
            }
        })
    }
}
