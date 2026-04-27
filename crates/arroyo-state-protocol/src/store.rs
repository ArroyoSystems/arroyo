use std::sync::Arc;

use arroyo_storage::{StorageProviderRef};
use async_trait::async_trait;
use prost::Message;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use arroyo_rpc::errors::StorageError;
use crate::types::{CheckpointRef, ProtocolError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateResult<T> {
    Created,
    AlreadyExists(T),
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("failed to encode protocol object for {path}: {source}")]
    EncodeJson {
        path: CheckpointRef,
        source: serde_json::Error,
    },
    #[error("failed to decode protocol object at {path}: {source}")]
    DecodeJson {
        path: CheckpointRef,
        source: serde_json::Error,
    },
    #[error("failed to decode protobuf object at {path}: {source}")]
    DecodeProtobuf {
        path: CheckpointRef,
        source: prost::DecodeError,
    },
    #[error("conditional create for {path} reported an existing object, but it could not be read")]
    ExistingObjectMissing { path: CheckpointRef },
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
}

#[async_trait]
pub trait ProtocolStore: Send + Sync {
    async fn read_bytes(&self, path: &CheckpointRef) -> Result<Option<Vec<u8>>, StoreError>;

    async fn put_bytes(&self, path: &CheckpointRef, bytes: Vec<u8>) -> Result<(), StoreError>;

    async fn create_bytes(
        &self,
        path: &CheckpointRef,
        bytes: Vec<u8>,
    ) -> Result<CreateResult<Vec<u8>>, StoreError>;
}

pub async fn read_json<S, T>(store: &S, path: &CheckpointRef) -> Result<Option<T>, StoreError>
where
    S: ProtocolStore + ?Sized,
    T: DeserializeOwned,
{
    let Some(bytes) = store.read_bytes(path).await? else {
        return Ok(None);
    };

    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|source| StoreError::DecodeJson {
            path: path.clone(),
            source,
        })
}

pub async fn put_json<S, T>(store: &S, path: &CheckpointRef, value: &T) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
    T: Serialize,
{
    let bytes = encode_json(path, value)?;
    store.put_bytes(path, bytes).await
}

pub async fn create_json<S, T>(
    store: &S,
    path: &CheckpointRef,
    value: &T,
) -> Result<CreateResult<T>, StoreError>
where
    S: ProtocolStore + ?Sized,
    T: DeserializeOwned + Serialize,
{
    let bytes = encode_json(path, value)?;

    match store.create_bytes(path, bytes).await? {
        CreateResult::Created => Ok(CreateResult::Created),
        CreateResult::AlreadyExists(existing) => serde_json::from_slice(&existing)
            .map(CreateResult::AlreadyExists)
            .map_err(|source| StoreError::DecodeJson {
                path: path.clone(),
                source,
            }),
    }
}

pub async fn read_protobuf<S, T>(store: &S, path: &CheckpointRef) -> Result<Option<T>, StoreError>
where
    S: ProtocolStore + ?Sized,
    T: Message + Default,
{
    let Some(bytes) = store.read_bytes(path).await? else {
        return Ok(None);
    };

    T::decode(bytes.as_slice())
        .map(Some)
        .map_err(|source| StoreError::DecodeProtobuf {
            path: path.clone(),
            source,
        })
}

pub async fn put_protobuf<S, T>(
    store: &S,
    path: &CheckpointRef,
    value: &T,
) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
    T: Message,
{
    store.put_bytes(path, value.encode_to_vec()).await
}

pub async fn create_protobuf<S, T>(
    store: &S,
    path: &CheckpointRef,
    value: &T,
) -> Result<CreateResult<T>, StoreError>
where
    S: ProtocolStore + ?Sized,
    T: Message + Default,
{
    match store.create_bytes(path, value.encode_to_vec()).await? {
        CreateResult::Created => Ok(CreateResult::Created),
        CreateResult::AlreadyExists(existing) => T::decode(existing.as_slice())
            .map(CreateResult::AlreadyExists)
            .map_err(|source| StoreError::DecodeProtobuf {
                path: path.clone(),
                source,
            }),
    }
}

fn encode_json<T>(path: &CheckpointRef, value: &T) -> Result<Vec<u8>, StoreError>
where
    T: Serialize,
{
    serde_json::to_vec(value).map_err(|source| StoreError::EncodeJson {
        path: path.clone(),
        source,
    })
}

#[derive(Clone)]
pub struct StorageProviderProtocolStore {
    storage: StorageProviderRef,
}

impl StorageProviderProtocolStore {
    pub fn new(storage: StorageProviderRef) -> Self {
        Self { storage }
    }
}

impl From<StorageProviderRef> for StorageProviderProtocolStore {
    fn from(storage: StorageProviderRef) -> Self {
        Self::new(storage)
    }
}

impl From<arroyo_storage::StorageProvider> for StorageProviderProtocolStore {
    fn from(storage: arroyo_storage::StorageProvider) -> Self {
        Self::new(Arc::new(storage))
    }
}

#[async_trait]
impl ProtocolStore for StorageProviderProtocolStore {
    async fn read_bytes(&self, path: &CheckpointRef) -> Result<Option<Vec<u8>>, StoreError> {
        self.storage
            .get_if_present(path.as_str())
            .await
            .map(|bytes| bytes.map(|bytes| bytes.to_vec()))
            .map_err(StoreError::from)
    }

    async fn put_bytes(&self, path: &CheckpointRef, bytes: Vec<u8>) -> Result<(), StoreError> {
        self.storage
            .put(path.as_str(), bytes)
            .await
            .map_err(StoreError::from)
    }

    async fn create_bytes(
        &self,
        path: &CheckpointRef,
        bytes: Vec<u8>,
    ) -> Result<CreateResult<Vec<u8>>, StoreError> {
        match self.storage.put_if_not_exists(path.as_str(), bytes).await {
            Ok(()) => Ok(CreateResult::Created),
            Err(StorageError::AlreadyExists { .. }) => {
                let existing = self
                    .read_bytes(path)
                    .await?
                    .ok_or_else(|| StoreError::ExistingObjectMissing { path: path.clone() })?;

                Ok(CreateResult::AlreadyExists(existing))
            }
            Err(error) => Err(StoreError::Storage(error)),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Debug, Default, Clone)]
    pub(crate) struct MemoryProtocolStore {
        objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    }

    #[async_trait]
    impl ProtocolStore for MemoryProtocolStore {
        async fn read_bytes(&self, path: &CheckpointRef) -> Result<Option<Vec<u8>>, StoreError> {
            Ok(self.objects.lock().unwrap().get(path.as_str()).cloned())
        }

        async fn put_bytes(&self, path: &CheckpointRef, bytes: Vec<u8>) -> Result<(), StoreError> {
            self.objects
                .lock()
                .unwrap()
                .insert(path.as_str().to_string(), bytes);
            Ok(())
        }

        async fn create_bytes(
            &self,
            path: &CheckpointRef,
            bytes: Vec<u8>,
        ) -> Result<CreateResult<Vec<u8>>, StoreError> {
            let mut objects = self.objects.lock().unwrap();
            if let Some(existing) = objects.get(path.as_str()) {
                return Ok(CreateResult::AlreadyExists(existing.clone()));
            }

            objects.insert(path.as_str().to_string(), bytes);
            Ok(CreateResult::Created)
        }
    }
}
