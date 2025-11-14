use datafusion::error::DataFusionError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataflowError {
    #[error("SQL processing error: {:0?}", .0)]
    DataFusionError(#[from] DataFusionError),
    #[error(transparent)]
    SourceError(#[from] SourceError),
    #[error("operator error: {error} {message}")]
    InternalOperatorError { error: String, message: String },
    #[error(transparent)]
    StateError(#[from] StateError),
}

pub type DataflowResult<T> = Result<T, DataflowError>;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("the provided URL is not a valid object store")]
    InvalidUrl,

    #[error("could not instantiate storage from path: {0}")]
    PathError(String),

    #[error("URL does not contain a key")]
    NoKeyInUrl,

    #[error("object store error: {0:?}")]
    ObjectStore(#[from] object_store::Error),

    #[error("failed to load credentials: {0}")]
    CredentialsError(String),
}

#[derive(Error, Debug)]
pub enum StateError {
    #[error("no registered table with name {table}")]
    NoRegisteredTable { table: String },
    #[error("trying to fetch {table} with wrong table type (expected {expected})")]
    WrongTableKind {
        table: String,
        expected: &'static str,
    },
    #[error("trying to fetch {table} with value table type (expected {expected})")]
    WrongValueType {
        table: String,
        expected: &'static str,
    },
    #[error("unexpected state error: [{table}] {error}")]
    Other { table: String, error: String },
    #[error("storage error: {0}")]
    StorageError(#[from] StorageError),
    #[error("checkpoint error: {0}")]
    CheckpointError(String),
    #[error("serialization error: {0}")]
    SerializationError(String),
    #[error("arrow error: {0}")]
    ArrowError(String),
}

#[derive(Debug, Clone)]
pub struct UserError {
    pub name: String,
    pub details: String,
}

impl UserError {
    pub fn new(name: impl Into<String>, details: impl Into<String>) -> UserError {
        UserError {
            name: name.into(),
            details: details.into(),
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SourceError {
    #[error("failed to deserialize {count} events: {details} ")]
    BadData { details: String, count: usize },
    #[error("error in source: {name}: {details} ")]
    Other { name: String, details: String },
}

impl SourceError {
    pub fn bad_data(details: impl Into<String>) -> SourceError {
        SourceError::BadData {
            details: details.into(),
            count: 1,
        }
    }

    pub fn bad_data_count(details: impl Into<String>, count: usize) -> SourceError {
        SourceError::BadData {
            details: details.into(),
            count,
        }
    }

    pub fn other(name: impl Into<String>, details: impl Into<String>) -> SourceError {
        SourceError::Other {
            name: name.into(),
            details: details.into(),
        }
    }

    pub fn details(&self) -> &String {
        match self {
            SourceError::BadData { details, .. } | SourceError::Other { details, .. } => details,
        }
    }
}
