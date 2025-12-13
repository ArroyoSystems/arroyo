use arrow_schema::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use std::time::Duration;
use thiserror::Error;

/// Creates a `DataflowError::ConnectorError` with format string support.
///
/// # Basic usage
///
/// ```ignore
/// // External error with backoff retry
/// connector_err!(External, WithBackoff, "connection failed: {}", e)
///
/// // Full form with retry and source
/// connector_err!(External, WithBackoff, source: io_err, "network error: {}", msg)
/// ```
#[macro_export]
macro_rules! connector_err {
    // Domain + retry hint + source error
    ($domain:ident, $retry:ident, source: $source:expr, $($arg:tt)+) => {
        $crate::errors::DataflowError::ConnectorError {
            domain: $crate::errors::ConnectorErrorDomain::$domain,
            retry: $crate::errors::RetryHint::$retry,
            error: format!($($arg)+),
            source: Some($source),
        }
    };

    ($domain:ident, $retry:ident, $($arg:tt)+) => {
        $crate::errors::DataflowError::ConnectorError {
            domain: $crate::errors::ConnectorErrorDomain::$domain,
            retry: $crate::errors::RetryHint::$retry,
            error: format!($($arg)+),
            source: None,
        }
    };
}

#[derive(Error, Debug)]
#[must_use]
pub enum DataflowError {
    #[error("Arrow error: {:0?}", .0)]
    ArrowError(#[from] ArrowError),
    #[error("SQL processing error: {:0?}", .0)]
    DataFusionError(#[from] DataFusionError),
    #[error("operator error: {error} {message}")]
    InternalOperatorError {
        error: &'static str,
        message: String,
    },
    #[error(transparent)]
    StateError(#[from] StateError),
    #[error("the arguments for this operator are invalid: {0}")]
    ArgumentError(String),
    #[error("error with external system: {0}")]
    ExternalError(String),
    #[error("error deserializing data: {details} ({count} times)")]
    DataError { details: String, count: usize },
    #[error("error in connector: {error}")]
    ConnectorError {
        domain: ConnectorErrorDomain,
        retry: RetryHint,
        error: String,
        source: Option<anyhow::Error>,
    },
    // to ease the migration, we'll start with the ability to wrap anyhows; these will
    // be removed in later stages of the migration
    #[error("unknown error: {0}")]
    UnknownError(#[from] anyhow::Error),
}

#[derive(Debug)]
pub enum ConnectorErrorDomain {
    User,
    External,
    Internal,
}

#[derive(Debug)]
pub enum RetryHint {
    NoRetry,
    WithBackoff,
    After(Duration),
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
    #[error("arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    ParquetError(#[from] ParquetError),
    #[error("bincode decode error: {0}")]
    BincodeDecodeError(#[from] bincode::error::DecodeError),
    #[error("bincode encode error: {0}")]
    BincodeEncodeError(#[from] bincode::error::EncodeError),
    #[error("protobuf decode error: {0}")]
    ProtoDecodeError(#[from] prost::DecodeError),
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

pub struct SourceError {}

impl SourceError {
    pub fn bad_data(details: impl Into<String>) -> DataflowError {
        DataflowError::DataError {
            details: details.into(),
            count: 1,
        }
    }

    pub fn bad_data_count(details: impl Into<String>, count: usize) -> DataflowError {
        DataflowError::DataError {
            details: details.into(),
            count,
        }
    }
}
