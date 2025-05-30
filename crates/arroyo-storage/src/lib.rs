use arroyo_rpc::retry;
use aws::ArroyoCredentialProvider;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use object_store::aws::AmazonS3ConfigKey;
use object_store::buffered::BufWriter;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    aws::AmazonS3Builder, local::LocalFileSystem, MultipartId, ObjectStore, PutPayload,
};
use object_store::{Error, ObjectMeta};
use regex::{Captures, Regex};
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::future::ready;
use std::path::PathBuf;
use std::str::FromStr;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use thiserror::Error;
use tracing::{debug, error};

mod aws;

/// A reference-counted reference to a [StorageProvider].
pub type StorageProviderRef = Arc<StorageProvider>;

#[derive(Clone)]
pub struct StorageProvider {
    config: BackendConfig,
    object_store: Arc<dyn ObjectStore>,
    multipart_store: Option<Arc<dyn MultipartStore>>,
    canonical_url: String,
    storage_options: HashMap<String, String>,
}

impl Debug for StorageProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageProvider<{}>", self.canonical_url)
    }
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

// Cloudflare R2
const R2_URL: &str =
    r"^[rR]2://((?P<account_id>[a-zA-Z0-9]+)@)?(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";
const R2_ENDPOINT: &str = r"^https://(?P<account_id>[a-zA-Z0-9]+)(\.(?P<jurisdiction>\w+))?\.[rR]2.cloudflarestorage.com/(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";
const R2_VIRTUAL: &str = r"^https://(?P<bucket>[a-z0-9\-]+).(?P<account_id>[a-zA-Z0-9]+)(\.(?P<jurisdiction>\w+))?\.[rR]2.cloudflarestorage.com(/(?P<key>.+))?$";

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
    R2,
    #[allow(clippy::upper_case_acronyms)]
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
            Backend::R2,
            vec![
                Regex::new(R2_URL).unwrap(),
                Regex::new(R2_ENDPOINT).unwrap(),
                Regex::new(R2_VIRTUAL).unwrap(),
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

fn should_retry(e: &object_store::Error) -> bool {
    match e {
        Error::Generic { source, .. } => {
            // some operations (like CompleteMultipartUpload)
            !source.to_string().contains("status 404 Not Found")
        }
        _ => false,
    }
}

macro_rules! storage_retry {
    ($e: expr) => {
        retry!(
            $e,
            10,
            Duration::from_millis(100),
            Duration::from_secs(10),
            |e| error!("Error: {}. Retrying...", e),
            should_retry
        )
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    endpoint: Option<String>,
    region: Option<String>,
    pub bucket: String,
    key: Option<Path>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct R2Config {
    account_id: String,
    pub bucket: String,
    jurisdiction: Option<String>, // e.g., "eu"
    key: Option<Path>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GCSConfig {
    pub bucket: String,
    key: Option<Path>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalConfig {
    pub path: String,
    pub key: Option<Path>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendConfig {
    S3(S3Config),
    R2(R2Config),
    GCS(GCSConfig),
    Local(LocalConfig),
}

impl BackendConfig {
    pub fn parse_url(url: &str, with_key: bool) -> Result<Self, StorageError> {
        for (k, v) in matchers() {
            if let Some(matches) = v.iter().filter_map(|r| r.captures(url)).next() {
                return match k {
                    Backend::S3 => Self::parse_s3(matches),
                    Backend::R2 => Self::parse_r2(url, matches),
                    Backend::GCS => Self::parse_gcs(matches),
                    Backend::Local => Self::parse_local(matches, with_key),
                };
            }
        }

        Err(StorageError::InvalidUrl)
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
            matches.name("region").map(|m| m.as_str().to_string()),
        ]);

        let endpoint = last([
            std::env::var("AWS_ENDPOINT").ok(),
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

        let key = matches.name("key").map(|m| m.as_str().into());

        Ok(BackendConfig::S3(S3Config {
            endpoint,
            region,
            bucket,
            key,
        }))
    }

    fn parse_r2(url: &str, matches: Captures) -> Result<Self, StorageError> {
        let account_id = last([
            std::env::var("CLOUDFLARE_ACCOUNT_ID").ok(),
            matches
                .name("account_id")
                .map(|m| m.as_str().to_lowercase()),
        ])
        .ok_or_else(|| {
            StorageError::PathError(format!(
                "Could not determine Cloudflare Account ID from '{url}'; \
                    must be specified either as part of the URL or via the \
                    CLOUDFLARE_ACCOUNT_ID environment variable"
            ))
        })?;

        let jurisdiction = matches
            .name("jurisdiction")
            .map(|s| s.as_str().to_lowercase());

        let bucket = matches
            .name("bucket")
            .expect("bucket should always be available")
            .as_str()
            .to_string();

        let key = matches.name("key").map(|m| m.as_str().into());

        Ok(Self::R2(R2Config {
            account_id,
            bucket,
            jurisdiction,
            key,
        }))
    }

    fn parse_gcs(matches: Captures) -> Result<Self, StorageError> {
        let bucket = matches
            .name("bucket")
            .expect("bucket should always be available")
            .as_str()
            .to_string();

        let key = matches.name("key").map(|r| r.as_str().into());

        Ok(BackendConfig::GCS(GCSConfig { bucket, key }))
    }

    fn parse_local(matches: Captures, with_key: bool) -> Result<Self, StorageError> {
        let path = matches
            .name("path")
            .expect("path regex must contain a path group")
            .as_str();

        let mut path = if !path.starts_with('/') {
            PathBuf::from(format!("/{}", path))
        } else {
            PathBuf::from(path)
        };

        let key = if with_key {
            let key = path
                .file_name()
                .map(|k| k.to_str().unwrap().to_string().into());
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

    fn key(&self) -> Option<&Path> {
        match self {
            BackendConfig::S3(s3) => s3.key.as_ref(),
            BackendConfig::R2(r2) => r2.key.as_ref(),
            BackendConfig::GCS(gcs) => gcs.key.as_ref(),
            BackendConfig::Local(local) => local.key.as_ref(),
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, BackendConfig::Local { .. })
    }
}

fn last<I: Sized, const COUNT: usize>(opts: [Option<I>; COUNT]) -> Option<I> {
    opts.into_iter().flatten().last()
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
            BackendConfig::R2(config) => Self::construct_r2(config, options).await,
            BackendConfig::S3(config) => Self::construct_s3(config, options).await,
            BackendConfig::GCS(config) => Self::construct_gcs(config).await,
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
            BackendConfig::R2(config) => Self::construct_r2(config, options).await,
            BackendConfig::GCS(config) => Self::construct_gcs(config).await,
            BackendConfig::Local(config) => Self::construct_local(config).await,
        }?;

        provider.get("").await
    }

    pub fn get_key(url: &str) -> Result<Path, StorageError> {
        let config = BackendConfig::parse_url(url, true)?;
        let key = match &config {
            BackendConfig::S3(s3) => s3.key.as_ref(),
            BackendConfig::R2(r2) => r2.key.as_ref(),
            BackendConfig::GCS(gcs) => gcs.key.as_ref(),
            BackendConfig::Local(local) => local.key.as_ref(),
        }
        .ok_or_else(|| StorageError::NoKeyInUrl)?;
        Ok(key.to_owned())
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
            s3_options.insert(s3_config_key, value.clone());
            builder = builder.with_config(s3_config_key, value);
        }

        if !aws_key_manually_set {
            let credentials: Arc<ArroyoCredentialProvider> =
                Arc::new(ArroyoCredentialProvider::try_new().await?);
            builder = builder.with_credentials(credentials);
        }

        let default_region = ArroyoCredentialProvider::default_region().await;
        config.region = config.region.or(default_region);
        if let Some(region) = &config.region {
            builder = builder.with_region(region);
            s3_options.insert(AmazonS3ConfigKey::Region, region.clone());
        }

        if let Some(endpoint) = config
            .endpoint
            .as_ref()
            .or(s3_options.get(&AmazonS3ConfigKey::Endpoint))
        {
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

        let object_store = Arc::new(builder.build()?);

        Ok(Self {
            config: BackendConfig::S3(config),
            object_store: object_store.clone(),
            multipart_store: Some(object_store),
            canonical_url,
            storage_options: s3_options
                .into_iter()
                .map(|(k, v)| (k.as_ref().to_string(), v))
                .collect(),
        })
    }

    async fn construct_r2(
        config: R2Config,
        options: HashMap<String, String>,
    ) -> Result<Self, StorageError> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);

        builder = builder.with_access_key_id(
            last([
                std::env::var("AWS_ACCESS_KEY_ID").ok(),
                std::env::var("R2_ACCESS_KEY_ID").ok(),
                options.get("aws_access_key_id").cloned(),
                options.get("r2_access_key_id").cloned(),
            ])
            .ok_or_else(|| {
                StorageError::CredentialsError(
                    "access_key_id not provided for R2 storage backend".to_string(),
                )
            })?,
        );

        builder = builder.with_secret_access_key(
            last([
                std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
                std::env::var("R2_SECRET_ACCESS_KEY").ok(),
                options.get("aws_secret_access_key").cloned(),
                options.get("r2_secret_access_key").cloned(),
            ])
            .ok_or_else(|| {
                StorageError::CredentialsError(
                    "secret_access_key not provided for R2 storage backend".to_string(),
                )
            })?,
        );

        let mut endpoint = "https://".to_string();
        endpoint.push_str(&config.account_id);
        if let Some(jurisdiction) = config.jurisdiction.as_ref() {
            endpoint.push('.');
            endpoint.push_str(jurisdiction);
        }
        endpoint.push_str(".r2.cloudflarestorage.com");

        let mut canonical_url = format!("{endpoint}/{}", config.bucket);
        if let Some(key) = &config.key {
            canonical_url.push('/');
            canonical_url.push_str(key.as_ref());
        }

        builder = builder
            .with_endpoint(endpoint)
            .with_virtual_hosted_style_request(false);

        let object_store = Arc::new(builder.build()?);

        Ok(Self {
            config: BackendConfig::R2(config),
            object_store: object_store.clone(),
            multipart_store: Some(object_store),
            canonical_url,
            storage_options: HashMap::new(),
        })
    }

    async fn construct_gcs(config: GCSConfig) -> Result<Self, StorageError> {
        let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(&config.bucket);

        if let Ok(service_account_key) = std::env::var("GOOGLE_SERVICE_ACCOUNT_KEY") {
            debug!("Constructing GCS builder with service account key");
            builder = builder.with_service_account_key(&service_account_key);
        }

        let mut canonical_url = format!("https://{}.storage.googleapis.com", config.bucket);
        if let Some(key) = &config.key {
            canonical_url = format!("{}/{}", canonical_url, key);
        }

        let object_store = Arc::new(builder.build()?);

        Ok(Self {
            config: BackendConfig::GCS(config),
            object_store: object_store.clone(),
            multipart_store: Some(object_store),
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
            LocalFileSystem::new_with_prefix(&config.path).map_err(Into::<StorageError>::into)?,
        );

        let canonical_url = format!("file://{}", config.path);
        Ok(Self {
            config: BackendConfig::Local(config),
            object_store,
            multipart_store: None,
            canonical_url,
            storage_options: HashMap::new(),
        })
    }

    pub fn requires_same_part_sizes(&self) -> bool {
        matches!(self.config, BackendConfig::R2(_))
    }

    pub async fn list(
        &self,
        include_subdirectories: bool,
    ) -> Result<impl Stream<Item = Result<Path, object_store::Error>> + '_, StorageError> {
        let key_path: Option<Path> = self.config.key().map(|key| key.to_string().into());
        let key_part_count = key_path
            .as_ref()
            .map(|key| key.parts().count())
            .unwrap_or_default();
        let list = self
            .object_store
            .list(key_path.as_ref())
            .filter_map(move |meta| {
                let result = {
                    match meta {
                        Ok(metadata) => {
                            let path = metadata.location;
                            if !include_subdirectories && path.parts().count() != key_part_count + 1
                            {
                                None
                            } else {
                                Some(Ok(path))
                            }
                        }
                        Err(err) => Some(Err(err)),
                    }
                };
                ready(result)
            });

        Ok(list)
    }

    pub async fn get(&self, path: impl Into<Path>) -> Result<Bytes, StorageError> {
        let path = path.into();
        let bytes = self
            .object_store
            .get(&self.qualify_path(&path))
            .await
            .map_err(Into::<StorageError>::into)?
            .bytes()
            .await?;

        Ok(bytes)
    }

    pub async fn get_if_present(
        &self,
        path: impl Into<Path>,
    ) -> Result<Option<Bytes>, StorageError> {
        let path: Path = path.into();
        match self.object_store.get(&self.qualify_path(&path)).await {
            Ok(obj) => {
                let bytes = obj.bytes().await?;
                Ok(Some(bytes))
            }
            Err(err) => {
                if let object_store::Error::NotFound { .. } = &err {
                    return Ok(None);
                }
                Err(err.into())
            }
        }
    }

    pub async fn exists<P: Into<Path>>(&self, path: P) -> Result<bool, StorageError> {
        let path: Path = path.into();
        let exists = self.object_store.head(&self.qualify_path(&path)).await;

        match exists {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_as_stream(
        &self,
        path: impl Into<Path>,
    ) -> Result<impl tokio::io::AsyncRead, StorageError> {
        let path = path.into();
        let path = self.qualify_path(&path);
        let bytes = storage_retry!(self.object_store.get(&path).await)
            .map_err(Into::<StorageError>::into)?
            .into_stream();

        Ok(tokio_util::io::StreamReader::new(bytes))
    }

    pub async fn put(&self, path: impl Into<Path>, bytes: Vec<u8>) -> Result<(), StorageError> {
        let bytes = PutPayload::from(Bytes::from(bytes));
        let path = path.into();
        let path = self.qualify_path(&path);
        storage_retry!(self.object_store.put(&path, bytes.clone()).await)?;

        Ok(())
    }

    pub fn qualify_path<'a>(&self, path: &'a Path) -> Cow<'a, Path> {
        match self.config.key() {
            Some(prefix) => Cow::Owned(prefix.parts().chain(path.parts()).collect()),
            None => Cow::Borrowed(path),
        }
    }

    pub async fn delete_if_present(&self, path: impl Into<Path>) -> Result<(), StorageError> {
        let path = path.into();
        let path = self.qualify_path(&path);
        match self.object_store.delete(&path).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn as_multipart(&self) -> Option<Arc<dyn MultipartStore>> {
        self.multipart_store.clone()
    }

    fn get_multipart(&self) -> &Arc<dyn MultipartStore> {
        self.multipart_store
            .as_ref()
            .unwrap_or_else(|| panic!("Not a multipart store: {:?}", self))
    }

    /// Produces a URL representation of this path that can be read by other systems,
    /// in particular Nomad's artifact fetcher and Arroyo's artifact fetcher.
    pub fn canonical_url(&self) -> &str {
        &self.canonical_url
    }

    pub fn canonical_url_for(&self, path: &str) -> String {
        format!("{}/{}", self.canonical_url, path)
    }

    pub fn storage_options(&self) -> &HashMap<String, String> {
        &self.storage_options
    }

    pub fn config(&self) -> &BackendConfig {
        &self.config
    }

    /// Provides an Arc'd copy of the underlying ObjectStore implementation. Be *very* careful
    /// using this. An objectstore URL is made up of two components: a bucket and a key, like
    /// s3://my-bucket/my/key.
    ///
    /// The backing store only knows about buckets, so it is *your* responsibility when using
    /// raw object store methods to prepend the key as well (for example, by using
    /// `ArroyoStorage::qualify_path`).
    pub fn get_backing_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    pub async fn head(&self, path: impl Into<Path>) -> Result<ObjectMeta, StorageError> {
        let path = path.into();
        self.object_store
            .head(&self.qualify_path(&path))
            .await
            .map_err(Into::<StorageError>::into)
    }

    pub fn buf_writer(&self, path: impl Into<Path>) -> BufWriter {
        let path = path.into();
        BufWriter::new(
            self.object_store.clone(),
            self.qualify_path(&path).into_owned(),
        )
    }

    pub async fn start_multipart(&self, path: &Path) -> Result<MultipartId, StorageError> {
        let id = storage_retry!(
            self.get_multipart()
                .create_multipart(&self.qualify_path(path))
                .await
        )
        .map_err(Into::<StorageError>::into)?;

        debug!(
            message = "started multipart upload",
            path = path.as_ref(),
            id = id.as_str()
        );

        Ok(id)
    }

    pub async fn add_multipart(
        &self,
        path: &Path,
        multipart_id: &MultipartId,
        part_number: usize,
        bytes: Bytes,
    ) -> Result<PartId, StorageError> {
        let part_id = storage_retry!(
            self.get_multipart()
                .put_part(
                    &self.qualify_path(path),
                    multipart_id,
                    part_number,
                    bytes.clone().into()
                )
                .await
        )
        .map_err(Into::<StorageError>::into)?;

        debug!(
            message = "added part",
            path = path.as_ref(),
            id = multipart_id.as_str(),
            part_number,
            size = bytes.len()
        );

        Ok(part_id)
    }

    pub async fn close_multipart(
        &self,
        path: &Path,
        multipart_id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<(), StorageError> {
        debug!(
            message = "closing multipart",
            path = path.as_ref(),
            id = multipart_id.as_str(),
            parts = parts.len()
        );

        storage_retry!(
            self.get_multipart()
                .complete_multipart(&self.qualify_path(path), multipart_id, parts.clone())
                .await
        )
        .map_err(Into::<StorageError>::into)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arroyo_types::to_nanos;
    use object_store::path::Path;
    use std::time::SystemTime;

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
                key: Some("puppy.jpg".into()),
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
                key: Some("puppy.jpg".into()),
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
                key: Some("my/path/test.pdf".into()),
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
                key: Some("path/test.pdf".into()),
            })
        );
    }

    #[test]
    fn test_r2_configs() {
        assert_eq!(
            BackendConfig::parse_url(
                "r2://97493190cfb48832a99ee97a7637ff6a@my-bucket1/puppy.jpg",
                false
            )
            .unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "97493190cfb48832a99ee97a7637ff6a".to_string(),
                bucket: "my-bucket1".to_string(),
                jurisdiction: None,
                key: Some("puppy.jpg".into()),
            })
        );

        unsafe {
            std::env::set_var("CLOUDFLARE_ACCOUNT_ID", "4bed8261ff878a81208da2fface71221");
        }

        assert_eq!(
            BackendConfig::parse_url("r2://mybucket/puppy.jpg", false).unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "4bed8261ff878a81208da2fface71221".to_string(),
                bucket: "mybucket".to_string(),
                jurisdiction: None,
                key: Some("puppy.jpg".into()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url("https://266035a37ceb5d774c51af1272485f1f.r2.cloudflarestorage.com/mybucket/my/key/path", false).unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "266035a37ceb5d774c51af1272485f1f".to_string(),
                bucket: "mybucket".to_string(),
                jurisdiction: None,
                key: Some("my/key/path".into()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url("https://266035a37ceb5d774c51af1272485f1f.eu.r2.cloudflarestorage.com/mybucket/my/key/path", false).unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "266035a37ceb5d774c51af1272485f1f".to_string(),
                bucket: "mybucket".to_string(),
                jurisdiction: Some("eu".to_string()),
                key: Some("my/key/path".into()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url("https://my-bucket.266035a37ceb5d774c51af1272485f1f.eu.r2.cloudflarestorage.com/my/key/path", false).unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "266035a37ceb5d774c51af1272485f1f".to_string(),
                bucket: "my-bucket".to_string(),
                jurisdiction: Some("eu".to_string()),
                key: Some("my/key/path".into()),
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
                key: Some("my-file.pdf".into()),
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
        let key = Path::parse(format!("my-test/{}", now)).unwrap();

        assert!(storage.put(key.clone(), data.clone()).await.is_ok());
        assert_eq!(storage.get(key.clone()).await.unwrap(), data.clone());

        let full_url = storage.canonical_url_for(key.as_ref());

        assert_eq!(
            StorageProvider::get_url(&full_url).await.unwrap(),
            data.clone()
        );

        storage.delete_if_present(key.clone()).await.unwrap();

        assert!(
            !tokio::fs::try_exists(format!("/tmp/arroyo-testing/storage-tests/{}", key))
                .await
                .unwrap()
        );
    }
}
