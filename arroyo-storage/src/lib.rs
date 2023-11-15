use std::path::PathBuf;
use std::str::FromStr;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use arroyo_types::{S3_ENDPOINT_ENV, S3_REGION_ENV};
use aws::ArroyoCredentialProvider;
use bytes::Bytes;
use object_store::aws::{AmazonS3ConfigKey, AwsCredential};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::multipart::PartId;
use object_store::path::Path;
use object_store::{aws::AmazonS3Builder, local::LocalFileSystem, ObjectStore};
use object_store::{CredentialProvider, MultipartId};
use regex::{Captures, Regex};
use thiserror::Error;

mod aws;

#[derive(Clone, Debug)]
pub struct StorageProvider {
    config: BackendConfig,
    object_store: Arc<dyn ObjectStore>,
    canonical_url: String,
    // A URL that object_store can parse.
    // May require storage_options to properly instantiate
    object_store_base_url: String,
    storage_options: HashMap<String, String>,
}

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

// https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/puppy.jpg
const S3_PATH: &str =
    r"^https://s3\.(?P<region>[\w\-]+)\.amazonaws\.com/(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";
// https://DOC-EXAMPLE-BUCKET1.s3.us-west-2.amazonaws.com/puppy.png
const S3_VIRTUAL: &str =
    r"^https://(?P<bucket>[a-z0-9\-\.]+)\.s3\.(?P<region>[\w\-]+)\.amazonaws\.com(/(?P<key>.+))?$";
// S3://mybucket/puppy.jpg
const S3_URL: &str = r"^[sS]3[aA]?://(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";
// unofficial, but convenient -- s3::https://my-endpoint.com:1234/mybucket/puppy.jpg
const S3_ENDPOINT_URL: &str = r"^[sS]3[aA]?::(?<protocol>https?)://(?P<endpoint>[^:/]+):(?<port>\d+)/(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";

// file:///my/path/directory
const FILE_URI: &str = r"^file://(?P<path>.*)$";
// file:/my/path/directory
const FILE_URL: &str = r"^file:(?P<path>.*)$";
// /my/path/directory
const FILE_PATH: &str = r"^/(?P<path>.*)$";

// https://BUCKET_NAME.storage.googleapis.com/OBJECT_NAME
const GCS_VIRTUAL: &str =
    r"^https://(?P<bucket>[a-z\d\-_\.]+)\.storage\.googleapis\.com(/(?P<key>.+))?$";
// https://storage.googleapis.com/BUCKET_NAME/OBJECT_NAME
const GCS_PATH: &str =
    r"^https://storage\.googleapis\.com/(?P<bucket>[a-z\d\-_\.]+)(/(?P<key>.+))?$";
const GCS_URL: &str = r"^[gG][sS]://(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";

#[derive(Debug, Clone, Hash, PartialEq, Eq, Copy)]
enum Backend {
    S3,
    GCS,
    Local,
}

fn matchers() -> &'static HashMap<Backend, Vec<Regex>> {
    static MATCHERS: OnceLock<HashMap<Backend, Vec<Regex>>> = OnceLock::new();
    MATCHERS.get_or_init(|| {
        let mut m = HashMap::new();

        m.insert(
            Backend::S3,
            vec![
                Regex::new(S3_PATH).unwrap(),
                Regex::new(S3_VIRTUAL).unwrap(),
                Regex::new(S3_ENDPOINT_URL).unwrap(),
                Regex::new(S3_URL).unwrap(),
            ],
        );

        m.insert(
            Backend::GCS,
            vec![
                Regex::new(GCS_PATH).unwrap(),
                Regex::new(GCS_VIRTUAL).unwrap(),
                Regex::new(GCS_URL).unwrap(),
            ],
        );

        m.insert(
            Backend::Local,
            vec![
                Regex::new(FILE_URI).unwrap(),
                Regex::new(FILE_URL).unwrap(),
                Regex::new(FILE_PATH).unwrap(),
            ],
        );

        m
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    endpoint: Option<String>,
    region: Option<String>,
    bucket: String,
    key: Option<String>,
}

impl S3Config {
    pub fn new(bucket: String, region: String, key: String) -> Self {
        Self {
            endpoint: None,
            region: Some(region),
            bucket,
            key: Some(key),
        }
    }

    pub fn canonical_url(&self) -> String {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GCSConfig {
    bucket: String,
    key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalConfig {
    pub path: String,
    pub key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendConfig {
    S3(S3Config),
    GCS(GCSConfig),
    Local(LocalConfig),
}

impl BackendConfig {
    pub fn parse_url(url: &str, with_key: bool) -> Result<Self, StorageError> {
        for (k, v) in matchers() {
            if let Some(matches) = v.iter().filter_map(|r| r.captures(url)).next() {
                return match k {
                    Backend::S3 => Self::parse_s3(matches),
                    Backend::GCS => Self::parse_gcs(matches),
                    Backend::Local => Self::parse_local(matches, with_key),
                };
            }
        }

        return Err(StorageError::InvalidUrl);
    }

    fn parse_s3(matches: Captures) -> Result<Self, StorageError> {
        // fill in env vars
        let bucket = matches
            .name("bucket")
            .expect("bucket should always be available")
            .as_str()
            .to_string();

        let region = last([
            std::env::var("AWS_DEFAULT_REGION").ok(),
            std::env::var(S3_REGION_ENV).ok(),
            matches.name("region").map(|m| m.as_str().to_string()),
        ]);

        let endpoint = last([
            std::env::var("AWS_ENDPOINT").ok(),
            std::env::var(S3_ENDPOINT_ENV).ok(),
            matches
                .name("endpoint")
                .map(|endpoint| -> Result<String, StorageError> {
                    let port = if let Some(port) = matches.name("port") {
                        u16::from_str(port.as_str()).map_err(|_| {
                            StorageError::PathError(format!("invalid port: {}", port.as_str()))
                        })?
                    } else {
                        443
                    };

                    let protocol = if let Some(protocol) = matches.name("protocol") {
                        protocol.as_str().to_string()
                    } else {
                        "https".to_string()
                    };

                    Ok(format!("{}://{}:{}", protocol, endpoint.as_str(), port))
                })
                .transpose()?,
        ]);

        let key = matches.name("key").map(|m| m.as_str().to_string());

        Ok(BackendConfig::S3(S3Config {
            endpoint,
            region,
            bucket,
            key,
        }))
    }

    fn parse_gcs(matches: Captures) -> Result<Self, StorageError> {
        let bucket = matches
            .name("bucket")
            .expect("bucket should always be available")
            .as_str()
            .to_string();

        let key = matches.name("key").map(|r| r.as_str().to_string());

        Ok(BackendConfig::GCS(GCSConfig { bucket, key }))
    }

    fn parse_local(matches: Captures, with_key: bool) -> Result<Self, StorageError> {
        let path = matches
            .name("path")
            .expect("path regex must contain a path group")
            .as_str();

        let mut path = if !path.starts_with("/") {
            PathBuf::from(format!("/{}", path))
        } else {
            PathBuf::from(path)
        };

        let key = if with_key {
            let key = path.file_name().map(|k| k.to_str().unwrap().to_string());
            path.pop();
            key
        } else {
            None
        };

        Ok(BackendConfig::Local(LocalConfig {
            path: path.to_str().unwrap().to_string(),
            key,
        }))
    }

    fn key(&self) -> Option<&String> {
        match self {
            BackendConfig::S3(s3) => s3.key.as_ref(),
            BackendConfig::GCS(gcs) => gcs.key.as_ref(),
            BackendConfig::Local(local) => local.key.as_ref(),
        }
    }
}

fn last<I: Sized, const COUNT: usize>(opts: [Option<I>; COUNT]) -> Option<I> {
    opts.into_iter().flatten().last()
}

pub async fn get_current_credentials() -> Result<Arc<AwsCredential>, StorageError> {
    let provider = ArroyoCredentialProvider::try_new()?;
    let credentials = provider.get_credential().await?;
    Ok(credentials)
}

impl StorageProvider {
    pub async fn for_url(url: &str) -> Result<Self, StorageError> {
        Self::for_url_with_options(url, HashMap::new()).await
    }
    pub async fn for_url_with_options(
        url: &str,
        options: HashMap<String, String>,
    ) -> Result<Self, StorageError> {
        let config: BackendConfig = BackendConfig::parse_url(url, false)?;

        match config {
            BackendConfig::S3(config) => Self::construct_s3(config, options).await,
            BackendConfig::GCS(config) => Self::construct_gcs(config),
            BackendConfig::Local(config) => Self::construct_local(config).await,
        }
    }

    pub async fn get_url(url: &str) -> Result<Bytes, StorageError> {
        Self::get_url_with_options(url, HashMap::new()).await
    }

    pub async fn get_url_with_options(
        url: &str,
        options: HashMap<String, String>,
    ) -> Result<Bytes, StorageError> {
        let config: BackendConfig = BackendConfig::parse_url(url, true)?;

        let provider = match config {
            BackendConfig::S3(config) => Self::construct_s3(config, options).await,
            BackendConfig::GCS(config) => Self::construct_gcs(config),
            BackendConfig::Local(config) => Self::construct_local(config).await,
        }?;

        let path = match &provider.config {
            BackendConfig::S3(s3) => s3.key.as_ref(),
            BackendConfig::GCS(gcs) => gcs.key.as_ref(),
            BackendConfig::Local(local) => local.key.as_ref(),
        }
        .ok_or_else(|| StorageError::NoKeyInUrl)?;

        let result = provider.get("").await;

        result
    }

    pub fn get_key(url: &str) -> Result<String, StorageError> {
        let config = BackendConfig::parse_url(url, true)?;
        let key = match &config {
            BackendConfig::S3(s3) => s3.key.as_ref(),
            BackendConfig::GCS(gcs) => gcs.key.as_ref(),
            BackendConfig::Local(local) => local.key.as_ref(),
        }
        .ok_or_else(|| StorageError::NoKeyInUrl)?;
        Ok(key.clone())
    }

    async fn construct_s3(
        mut config: S3Config,
        options: HashMap<String, String>,
    ) -> Result<Self, StorageError> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);
        let mut aws_key_manually_set = false;
        let mut s3_options = HashMap::new();
        for (key, value) in options {
            let s3_config_key = key.parse().map_err(|_| {
                StorageError::CredentialsError(format!("invalid S3 config key: {}", key))
            })?;
            if AmazonS3ConfigKey::AccessKeyId == s3_config_key {
                aws_key_manually_set = true;
            }
            s3_options.insert(s3_config_key.clone(), value.clone());
            builder = builder.with_config(s3_config_key, value);
        }

        if !aws_key_manually_set {
            let credentials: Arc<ArroyoCredentialProvider> =
                Arc::new(ArroyoCredentialProvider::try_new()?);
            builder = builder.with_credentials(credentials);
        }

        let default_region = ArroyoCredentialProvider::default_region().await;
        config.region = config.region.or(default_region);
        if let Some(region) = &config.region {
            builder = builder.with_region(region);
            s3_options.insert(AmazonS3ConfigKey::Region, region.clone());
        }

        if let Some(endpoint) = &config.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_virtual_hosted_style_request(false)
                .with_allow_http(true);
            s3_options.insert(AmazonS3ConfigKey::Endpoint, endpoint.clone());
            s3_options.insert(
                AmazonS3ConfigKey::VirtualHostedStyleRequest,
                "false".to_string(),
            );
            s3_options.insert(
                AmazonS3ConfigKey::Client(object_store::ClientConfigKey::AllowHttp),
                "true".to_string(),
            );
        }
        let mut canonical_url = match (&config.region, &config.endpoint) {
            (_, Some(endpoint)) => {
                format!("s3::{}/{}", endpoint, config.bucket)
            }
            (Some(region), _) => {
                format!("https://s3.{}.amazonaws.com/{}", region, config.bucket)
            }
            _ => {
                format!("https://s3.amazonaws.com/{}", config.bucket)
            }
        };
        if let Some(key) = &config.key {
            canonical_url = format!("{}/{}", canonical_url, key);
        }
        let object_store_base_url = format!("s3://{}", config.bucket);
        Ok(Self {
            config: BackendConfig::S3(config),
            object_store: Arc::new(builder.build().map_err(|e| Into::<StorageError>::into(e))?),
            canonical_url,
            object_store_base_url,
            storage_options: s3_options
                .into_iter()
                .map(|(k, v)| (k.as_ref().to_string(), v))
                .collect(),
        })
    }

    fn construct_gcs(config: GCSConfig) -> Result<Self, StorageError> {
        let gcs = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(&config.bucket)
            .build()?;

        let mut canonical_url = format!("https://{}.storage.googleapis.com", config.bucket);
        if let Some(key) = &config.key {
            canonical_url = format!("{}/{}", canonical_url, key);
        }

        let object_store_base_url = format!("https://{}.storage.googleapis.com", config.bucket);

        Ok(Self {
            config: BackendConfig::GCS(config),
            object_store: Arc::new(gcs),
            object_store_base_url,
            canonical_url,
            storage_options: HashMap::new(),
        })
    }

    async fn construct_local(config: LocalConfig) -> Result<Self, StorageError> {
        tokio::fs::create_dir_all(&config.path).await.map_err(|e| {
            StorageError::PathError(format!(
                "failed to create directory {}: {:?}",
                config.path, e
            ))
        })?;

        let object_store = Arc::new(
            LocalFileSystem::new_with_prefix(&config.path)
                .map_err(|e| Into::<StorageError>::into(e))?,
        );

        let canonical_url = format!("file://{}", config.path);
        let object_store_base_url = canonical_url.clone();
        Ok(Self {
            config: BackendConfig::Local(config),
            object_store,
            canonical_url,
            object_store_base_url,
            storage_options: HashMap::new(),
        })
    }

    pub async fn get<P: Into<String>>(&self, path: P) -> Result<Bytes, StorageError> {
        let path: String = path.into();
        let bytes = self
            .object_store
            .get(&self.qualify_path(&path.into()))
            .await
            .map_err(|e| Into::<StorageError>::into(e))?
            .bytes()
            .await?;

        Ok(bytes)
    }

    pub async fn put<P: Into<String>>(
        &self,
        path: P,
        bytes: Vec<u8>,
    ) -> Result<String, StorageError> {
        let path = path.into().into();
        self.object_store
            .put(&self.qualify_path(&path), bytes.into())
            .await?;

        Ok(format!("{}/{}", self.canonical_url, path))
    }

    fn qualify_path(&self, path: &Path) -> Path {
        match self.config.key() {
            Some(prefix) => {
                let prefix_path: Path = prefix.to_string().into();
                prefix_path.parts().chain(path.parts()).collect()
            }
            None => path.clone(),
        }
    }

    pub async fn delete_if_present<P: Into<String>>(&self, path: P) -> Result<(), StorageError> {
        let path = path.into();
        return match self.object_store.delete(&path.into()).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        };
    }

    pub async fn start_multipart(&self, path: &Path) -> Result<MultipartId, StorageError> {
        Ok(self
            .object_store
            .initiate_multipart_upload(path)
            .await
            .map_err(|e| Into::<StorageError>::into(e))?
            .0)
    }

    pub async fn add_multipart(
        &self,
        path: &Path,
        multipart_id: &MultipartId,
        part_number: usize,
        bytes: Bytes,
    ) -> Result<PartId, StorageError> {
        Ok(self
            .object_store
            .get_put_part(path, multipart_id)
            .await?
            .put_part(bytes, part_number)
            .await
            .map_err(|e| Into::<StorageError>::into(e))?)
    }

    pub async fn close_multipart(
        &self,
        path: &Path,
        multipart_id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<(), StorageError> {
        Ok(self
            .object_store
            .get_put_part(path, multipart_id)
            .await?
            .complete(parts)
            .await
            .map_err(|e| Into::<StorageError>::into(e))?)
    }

    /// Produces a URL representation of this path that can be read by other systems,
    /// in particular Nomad's artifact fetcher and Arroyo's artifact fetcher.
    pub fn canonical_url(&self) -> &str {
        &self.canonical_url
    }

    // Returns a url that will, combined with storage_options, parse to
    // the same ObjectStore as self.object_store.
    // Needed for systems that build their own ObjectStore, such as delta-rs
    pub fn object_store_base_url(&self) -> &str {
        &self.object_store_base_url
    }

    pub fn storage_options(&self) -> &HashMap<String, String> {
        &self.storage_options
    }

    pub fn config(&self) -> &BackendConfig {
        &self.config
    }

    pub fn get_backing_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use arroyo_types::to_nanos;

    use crate::{matchers, BackendConfig, StorageProvider};

    #[test]
    fn test_regex_compilation() {
        matchers();
    }

    #[test]
    fn test_s3_configs() {
        assert_eq!(
            BackendConfig::parse_url("s3://mybucket/puppy.jpg", false).unwrap(),
            BackendConfig::S3(crate::S3Config {
                endpoint: None,
                region: None,
                bucket: "mybucket".to_string(),
                key: Some("puppy.jpg".to_string()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url(
                "https://s3.us-west-2.amazonaws.com/my-bucket1/puppy.jpg",
                false
            )
            .unwrap(),
            BackendConfig::S3(crate::S3Config {
                endpoint: None,
                region: Some("us-west-2".to_string()),
                bucket: "my-bucket1".to_string(),
                key: Some("puppy.jpg".to_string()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url("https://s3.us-east-1.amazonaws.com/my-bucket", false)
                .unwrap(),
            BackendConfig::S3(crate::S3Config {
                endpoint: None,
                region: Some("us-east-1".to_string()),
                bucket: "my-bucket".to_string(),
                key: None,
            })
        );

        assert_eq!(
            BackendConfig::parse_url(
                "https://my-bucket.s3.us-west-2.amazonaws.com/my/path/test.pdf",
                false
            )
            .unwrap(),
            BackendConfig::S3(crate::S3Config {
                endpoint: None,
                region: Some("us-west-2".to_string()),
                bucket: "my-bucket".to_string(),
                key: Some("my/path/test.pdf".to_string()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url(
                "s3::https://my-custom-endpoint.com:1234/my-bucket/path/test.pdf",
                false
            )
            .unwrap(),
            BackendConfig::S3(crate::S3Config {
                endpoint: Some("https://my-custom-endpoint.com:1234".to_string()),
                region: None,
                bucket: "my-bucket".to_string(),
                key: Some("path/test.pdf".to_string()),
            })
        );
    }

    #[test]
    fn test_local_configs() {
        assert_eq!(
            BackendConfig::parse_url("file:///my/path/directory", false).unwrap(),
            BackendConfig::Local(crate::LocalConfig {
                path: "/my/path/directory".to_string(),
                key: None,
            })
        );

        assert_eq!(
            BackendConfig::parse_url("file:/my/path/directory", false).unwrap(),
            BackendConfig::Local(crate::LocalConfig {
                path: "/my/path/directory".to_string(),
                key: None,
            })
        );

        assert_eq!(
            BackendConfig::parse_url("/my/path/directory", false).unwrap(),
            BackendConfig::Local(crate::LocalConfig {
                path: "/my/path/directory".to_string(),
                key: None,
            })
        );

        assert_eq!(
            BackendConfig::parse_url("/my/path/directory/my-file.pdf", true).unwrap(),
            BackendConfig::Local(crate::LocalConfig {
                path: "/my/path/directory".to_string(),
                key: Some("my-file.pdf".to_string()),
            })
        );
    }

    #[tokio::test]
    async fn test_local_fs() {
        let storage = StorageProvider::for_url("file:///tmp/arroyo-testing/storage-tests")
            .await
            .unwrap();

        let now = to_nanos(SystemTime::now());
        let data = now.to_le_bytes().to_vec();
        let key = format!("my-test/{}", now);

        let full_url = storage.put(&key, data.clone()).await;
        assert!(full_url.is_ok());

        assert_eq!(storage.get(&key).await.unwrap(), data.clone());

        assert_eq!(
            StorageProvider::get_url(&full_url.unwrap()).await.unwrap(),
            data.clone()
        );

        storage.delete_if_present(&key).await.unwrap();

        assert!(
            !tokio::fs::try_exists(format!("/tmp/arroyo-testing/storage-tests/{}", key))
                .await
                .unwrap()
        );
    }
}
