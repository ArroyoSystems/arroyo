use datafusion::error::DataFusionError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataflowError {
    #[error("SQL processing error: {:0?}", .0)]
    DataFusionError(#[from] DataFusionError),
    #[error(transparent)]
    SourceError(#[from] SourceError),
    #[error("operator error: {error} {message}")]
    InternalOperatorError{
        error: String,
        message: String,
    }
}

pub type DataflowResult<T> = Result<T, DataflowError>;

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