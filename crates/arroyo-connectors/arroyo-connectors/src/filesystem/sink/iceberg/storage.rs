use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use iceberg::Result;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg_storage_opendal::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ArroyoStorageFactory;

#[typetag::serde(name = "arroyo-iceberg")]
impl StorageFactory for ArroyoStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let mut props = config.props().clone();
        // Set the fallback only after the catalog and user properties have been
        // merged. Supplying it as a user property would override catalog config.
        props
            .entry("s3.path-style-access".into())
            .or_insert_with(|| "true".into());
        let config = StorageConfig::from_props(props);
        Ok(Arc::new(ArroyoStorage {
            local: OpenDalStorageFactory::Fs.build(&config)?,
            remote: OpenDalResolvingStorageFactory::new().build(&config)?,
        }))
    }
}

#[derive(Serialize, Deserialize)]
struct ArroyoStorage {
    local: Arc<dyn Storage>,
    remote: Arc<dyn Storage>,
}

impl std::fmt::Debug for ArroyoStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArroyoStorage").finish_non_exhaustive()
    }
}

impl ArroyoStorage {
    fn for_path(&self, path: &str) -> &dyn Storage {
        // The upstream resolving factory only accepts URLs; old local tables
        // can contain absolute filesystem paths in metadata and manifests.
        if std::path::Path::new(path).is_absolute() || path.starts_with("file:") {
            self.local.as_ref()
        } else {
            self.remote.as_ref()
        }
    }
}

#[async_trait]
#[typetag::serde(name = "arroyo-iceberg")]
impl Storage for ArroyoStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.for_path(path).exists(path).await
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.for_path(path).metadata(path).await
    }
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.for_path(path).read(path).await
    }
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.for_path(path).reader(path).await
    }
    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.for_path(path).write(path, bs).await
    }
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.for_path(path).writer(path).await
    }
    async fn delete(&self, path: &str) -> Result<()> {
        self.for_path(path).delete(path).await
    }
    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.for_path(path).delete_prefix(path).await
    }
    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        while let Some(path) = paths.next().await {
            self.delete(&path).await?;
        }
        Ok(())
    }
    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.for_path(path).new_input(path)
    }
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.for_path(path).new_output(path)
    }
}
