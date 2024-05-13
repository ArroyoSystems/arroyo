use crate::types::public::PipelineType;
use crate::{AuthData, handle_db_error, handle_sqlite_error};
use deadpool_postgres::{Object};
use rusqlite::Connection;
use serde_json::Value;
use tokio_postgres::{Transaction as PGTransaction};
use crate::queries::api_queries;
use crate::queries::api_queries::DbUdf;
use crate::rest_utils::{ErrorResp, log_and_map};

#[derive(Clone)]
pub enum DatabaseSource {
    Postgres(deadpool_postgres::Pool),
    Sqlite(deadpool_sqlite::Pool),
}

pub enum Database<'a> {
    Postgres(&'a Object),
    PostgresTx(&'a PGTransaction<'a>),
    Sqlite(&'a Connection),
}

impl <'a> Database<'a> {
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
                        *p,
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
                    .bind(*p, &auth.organization_id)
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
