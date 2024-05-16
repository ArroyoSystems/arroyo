use crate::rest_utils::{log_and_map, ErrorResp};
use deadpool_postgres::Object;
use deadpool_postgres::Transaction as PGTransaction;
use rusqlite::{Error, Params};
use std::sync::{Arc, Mutex};
use tokio_postgres::error::SqlState;

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum DbError {
    DuplicateViolation,
    ForeignKeyViolation,
    Other(String),
}

impl From<rusqlite::Error> for DbError {
    fn from(err: Error) -> Self {
        if let Some(sqlite) = err.sqlite_error() {
            match (sqlite.code, sqlite.extended_code) {
                (rusqlite::ErrorCode::ConstraintViolation, 2067 /* UNIQUE */) => {
                    return DbError::DuplicateViolation;
                }
                (rusqlite::ErrorCode::ConstraintViolation, 787 /* FOREIGN KEY */) => {
                    return DbError::ForeignKeyViolation;
                }
                _ => {}
            }
        }

        DbError::Other(err.to_string())
    }
}

impl From<tokio_postgres::Error> for DbError {
    fn from(err: tokio_postgres::Error) -> Self {
        if let Some(db) = &err.as_db_error() {
            if *db.code() == SqlState::UNIQUE_VIOLATION {
                return DbError::DuplicateViolation;
            } else if *db.code() == SqlState::FOREIGN_KEY_VIOLATION {
                return DbError::ForeignKeyViolation;
            }
        }

        DbError::Other(err.to_string())
    }
}

#[derive(Debug, Clone)]
pub enum DatabaseSource {
    Postgres(deadpool_postgres::Pool),
    Sqlite(Arc<Mutex<rusqlite::Connection>>),
}

impl DatabaseSource {
    pub async fn client(&self) -> Result<Database, ErrorResp> {
        Ok(match self {
            DatabaseSource::Postgres(p) => Database::Postgres(p.get().await.map_err(log_and_map)?),
            DatabaseSource::Sqlite(p) => Database::Sqlite(SqliteWrapper::Connection(p.clone())),
        })
    }
}

pub enum SqliteWrapper {
    Connection(Arc<Mutex<rusqlite::Connection>>),
    //Transaction(rusqlite::Transaction<'a>),
}

impl SqliteWrapper {
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        match self {
            SqliteWrapper::Connection(c) => {
                let c = c.lock().unwrap();
                c.execute(sql, params)
            } // SqliteWrapper::Transaction(t) => {
              //     t.execute(sql, params)
              // }
        }
    }

    pub fn query_rows<P: Params, T, F: Fn(&rusqlite::Row) -> T>(
        &self,
        sql: &str,
        params: P,
        map: F,
    ) -> rusqlite::Result<Vec<T>> {
        match self {
            SqliteWrapper::Connection(c) => {
                let c = c.lock().unwrap();
                let mut statement = c.prepare(sql).unwrap();
                let results = statement.query(params)?;
                Ok(results.mapped(|r| Ok(map(r))).map(|r| r.unwrap()).collect())
            } // SqliteWrapper::Transaction(t) => {
              //     todo!()
              //     //t.prepare(sql)
              // }
        }
    }
}

pub enum Database<'a> {
    Postgres(Object),
    PostgresTx(PGTransaction<'a>),
    Sqlite(SqliteWrapper),
}

impl<'a> Database<'a> {
    pub async fn transaction<'b>(&'a mut self) -> Result<Database<'b>, ErrorResp>
    where
        'a: 'b,
    {
        Ok(match self {
            Database::Postgres(p) => Self::PostgresTx(p.transaction().await.map_err(log_and_map)?),
            Database::PostgresTx(tx) => {
                Self::PostgresTx(tx.transaction().await.map_err(log_and_map)?)
            }
            Database::Sqlite(p) => {
                match p {
                    SqliteWrapper::Connection(c) => {
                        //Self::Sqlite(SqliteWrapper::Transaction(c.transaction().map_err(log_and_map)?))
                        Self::Sqlite(SqliteWrapper::Connection(Arc::clone(c)))
                    } // SqliteWrapper::Transaction(_) => {
                      //     panic!("Cannot duplicate an existing sqlite transaction");
                      // }
                }
            }
        })
    }

    pub async fn commit(self) -> Result<(), ErrorResp> {
        match self {
            Database::Postgres(_) => {
                // no op
            }
            Database::PostgresTx(_) => {
                todo!()
            }
            Database::Sqlite(s) => {
                match s {
                    SqliteWrapper::Connection(_) => {} // SqliteWrapper::Transaction(tx) => {
                                                       //     tx.commit().map_err(|e| handle_sqlite_error("record", e))?;
                                                       // }
                }
            }
        }

        Ok(())
    }
}
