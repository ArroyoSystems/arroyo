use arrow_schema::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use std::fmt::{Display, Formatter};
use thiserror::Error;

use crate::grpc::rpc;

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
            domain: $crate::errors::ErrorDomain::$domain,
            retry: $crate::errors::RetryHint::$retry,
            error: format!($($arg)+),
            source: Some($source),
        }
    };

    ($domain:ident, $retry:ident, $($arg:tt)+) => {
        $crate::errors::DataflowError::ConnectorError {
            domain: $crate::errors::ErrorDomain::$domain,
            retry: $crate::errors::RetryHint::$retry,
            error: format!($($arg)+),
            source: None,
        }
    };
}

#[derive(Error, Debug)]
#[must_use]
pub enum DataflowError {
    #[error("Arrow error: {}", .0)]
    ArrowError(#[from] ArrowError),
    #[error("SQL processing error: {}", .0)]
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
        domain: ErrorDomain,
        retry: RetryHint,
        error: String,
        source: Option<anyhow::Error>,
    },
    // to ease the migration, we'll start with the ability to wrap anyhows; these will
    // be removed in later stages of the migration
    #[error("unknown error: {0}")]
    UnknownError(#[from] anyhow::Error),
    #[error("{error} in {operator_id}")]
    WithOperator {
        error: Box<DataflowError>,
        operator_id: String,
    },
}

impl DataflowError {
    pub fn with_operator(self, operator_id: String) -> Self {
        match self {
            DataflowError::WithOperator { error, .. } => {
                DataflowError::WithOperator { error, operator_id }
            }
            e => DataflowError::WithOperator {
                error: Box::new(e),
                operator_id,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ErrorDomain {
    User,
    External,
    Internal,
}

impl ErrorDomain {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::User => "user",
            Self::External => "external",
            Self::Internal => "internal",
        }
    }
}

impl From<ErrorDomain> for rpc::ErrorDomain {
    fn from(value: ErrorDomain) -> Self {
        match value {
            ErrorDomain::User => Self::User,
            ErrorDomain::External => Self::External,
            ErrorDomain::Internal => Self::Internal,
        }
    }
}

impl From<rpc::ErrorDomain> for ErrorDomain {
    fn from(value: rpc::ErrorDomain) -> Self {
        match value {
            rpc::ErrorDomain::User => Self::User,
            rpc::ErrorDomain::External => Self::External,
            rpc::ErrorDomain::Internal => Self::Internal,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RetryHint {
    NoRetry,
    WithBackoff,
}

impl From<RetryHint> for rpc::RetryHint {
    fn from(value: RetryHint) -> Self {
        match value {
            RetryHint::NoRetry => rpc::RetryHint::NoRetry,
            RetryHint::WithBackoff => rpc::RetryHint::WithBackoff,
        }
    }
}

impl From<rpc::RetryHint> for RetryHint {
    fn from(value: rpc::RetryHint) -> Self {
        match value {
            rpc::RetryHint::NoRetry => RetryHint::NoRetry,
            rpc::RetryHint::WithBackoff => RetryHint::WithBackoff,
        }
    }
}

impl RetryHint {
    pub fn as_str(&self) -> &'static str {
        match self {
            RetryHint::NoRetry => "no_retry",
            RetryHint::WithBackoff => "with_backoff",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskError {
    pub message: String,
    pub domain: ErrorDomain,
    pub retry_hint: RetryHint,
    pub operator_id: Option<String>,
    pub details: Option<String>,
}

fn classify_datafusion_error(err: &DataFusionError) -> (ErrorDomain, RetryHint) {
    match err {
        DataFusionError::SQL(_, _)
        | DataFusionError::Plan(_)
        | DataFusionError::Configuration(_)
        | DataFusionError::SchemaError(_, _)
        | DataFusionError::Execution(_)
        | DataFusionError::ArrowError(_, _) => (ErrorDomain::User, RetryHint::NoRetry),

        DataFusionError::IoError(_)
        | DataFusionError::ObjectStore(_)
        | DataFusionError::ResourcesExhausted(_)
        | DataFusionError::External(_)
        | DataFusionError::ParquetError(_) => (ErrorDomain::External, RetryHint::WithBackoff),

        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            classify_datafusion_error(inner)
        }

        // Internal or unknown: default
        _ => (ErrorDomain::Internal, RetryHint::WithBackoff),
    }
}

impl From<DataflowError> for TaskError {
    fn from(value: DataflowError) -> Self {
        let message = value.to_string();
        let (domain, retry_hint, details) = match value {
            DataflowError::ConnectorError {
                domain,
                retry,
                source,
                ..
            } => (
                domain,
                retry.clone(),
                source.as_ref().map(|s| format!("{:?}", s)),
            ),
            DataflowError::ArrowError(_) => (ErrorDomain::Internal, RetryHint::NoRetry, None),
            DataflowError::DataFusionError(df_err) => {
                let (domain, retry) = classify_datafusion_error(&df_err);
                (domain, retry, None)
            }
            DataflowError::InternalOperatorError { .. } => {
                (ErrorDomain::Internal, RetryHint::NoRetry, None)
            }
            DataflowError::StateError(_) => (ErrorDomain::Internal, RetryHint::WithBackoff, None),
            DataflowError::ArgumentError(_) => (ErrorDomain::User, RetryHint::NoRetry, None),
            DataflowError::ExternalError(_) => {
                (ErrorDomain::External, RetryHint::WithBackoff, None)
            }
            DataflowError::DataError { details, count } => (
                ErrorDomain::External,
                RetryHint::WithBackoff,
                Some(format!("count: {}, details: {}", count, details)),
            ),
            DataflowError::UnknownError(_) => (ErrorDomain::Internal, RetryHint::WithBackoff, None),
            DataflowError::WithOperator { error, operator_id } => {
                let mut inner: TaskError = (*error).into();
                inner.operator_id = Some(operator_id);
                return inner;
            }
        };

        Self {
            message,
            domain,
            retry_hint,
            operator_id: None,
            details,
        }
    }
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
