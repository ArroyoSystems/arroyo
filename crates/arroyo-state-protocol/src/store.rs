use crate::types::{CheckpointRef, ProtocolError};
use arroyo_rpc::errors::StorageError;
use arroyo_storage::StorageProvider;
use async_trait::async_trait;
use prost::Message;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Result of a conditional-create operation.
pub enum CreateResult<T> {
    /// The object did not exist and was created by this call.
    Created,
    /// The object already existed; the decoded existing value is returned.
    AlreadyExists(T),
}

/// Error type for protocol storage and serialization operations.
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

/// Minimal storage interface required by the protocol workflows.
///
/// Implementors must provide read-after-write visibility and linearizable
/// conditional create for [`ProtocolStore::create_bytes`]. Safety decisions are
/// not made in this trait; workflows read objects through it and then call the
/// pure protocol logic.
#[async_trait]
pub trait ProtocolStore: Send + Sync {
    /// Reads raw object bytes, returning `None` if the object is absent.
    async fn read_bytes(&self, path: &CheckpointRef) -> Result<Option<Vec<u8>>, StoreError>;

    /// Writes or overwrites raw object bytes.
    ///
    /// Use this only for mutable protocol objects such as generation manifests.
    /// Immutable objects such as epoch records and checkpoint manifests should
    /// use [`ProtocolStore::create_bytes`] through the typed helpers.
    async fn put_bytes(&self, path: &CheckpointRef, bytes: Vec<u8>) -> Result<(), StoreError>;

    /// Creates raw object bytes only if the object does not already exist.
    ///
    /// On conflict, implementations should return the existing object bytes so
    /// callers can classify idempotent success versus a real ownership conflict.
    async fn create_bytes(
        &self,
        path: &CheckpointRef,
        bytes: Vec<u8>,
    ) -> Result<CreateResult<Vec<u8>>, StoreError>;
}

/// Reads and JSON-decodes an optional protocol object.
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

/// JSON-encodes and writes a protocol object, overwriting any existing object.
///
/// This is appropriate for `current-generation.json` and
/// `generation-manifest.json`. Do not use it for epoch records or commit
/// markers.
pub async fn put_json<S, T>(store: &S, path: &CheckpointRef, value: &T) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
    T: Serialize,
{
    let bytes = encode_json(path, value)?;
    store.put_bytes(path, bytes).await
}

/// JSON-encodes and conditionally creates a protocol object.
///
/// Use this for immutable JSON protocol objects such as epoch records and
/// commit markers.
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

/// Reads and protobuf-decodes an optional protocol object.
///
/// This is used for `checkpoint-manifest.pb`.
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

/// Protobuf-encodes and writes an object, overwriting any existing object.
///
/// Most callers should prefer [`create_protobuf`] for checkpoint manifests so
/// checkpoint contents remain immutable.
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

/// Protobuf-encodes and conditionally creates an immutable object.
///
/// Use this for checkpoint manifests. On conflict, callers receive the existing
/// decoded value and should compare it with the intended manifest for
/// idempotent retry handling.
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

#[async_trait]
impl ProtocolStore for StorageProvider {
    async fn read_bytes(&self, path: &CheckpointRef) -> Result<Option<Vec<u8>>, StoreError> {
        self.get_if_present(path.as_str())
            .await
            .map(|bytes| bytes.map(|bytes| bytes.to_vec()))
            .map_err(StoreError::from)
    }

    async fn put_bytes(&self, path: &CheckpointRef, bytes: Vec<u8>) -> Result<(), StoreError> {
        self.put(path.as_str(), bytes)
            .await
            .map_err(StoreError::from)
    }

    async fn create_bytes(
        &self,
        path: &CheckpointRef,
        bytes: Vec<u8>,
    ) -> Result<CreateResult<Vec<u8>>, StoreError> {
        match self.put_if_not_exists(path.as_str(), bytes).await {
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
