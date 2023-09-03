use std::str::FromStr;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use arroyo_types::{S3_BUCKET_ENV, S3_ENDPOINT_ENV, S3_REGION_ENV};
use bytes::Bytes;
use object_store::{aws::AmazonS3Builder, local::LocalFileSystem, ObjectStore};
use regex::{Captures, Regex};
use thiserror::Error;

pub struct StorageProvider {
    backend: Backend,
    object_store: Arc<dyn ObjectStore>,
    canonical_url: String,
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("the provided URL is not a valid object store")]
    InvalidUrl,

    #[error("could not instantiate storage from path: {0}")]
    PathError(String),

    #[error("object store error: {0:?}")]
    ObjectStore(#[from] object_store::Error),
}

// https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/puppy.jpg
const S3_PATH: &str =
    r"https://s3\.(?P<region>[\w-]+)\.amazonaws\.com/(?P<bucket>[a-z0-9-\.]+)/(?P<key>.+)";
// https://DOC-EXAMPLE-BUCKET1.s3.us-west-2.amazonaws.com/puppy.png
const S3_VIRTUAL: &str =
    r"https://(?P<bucket>[a-z0-9-\.]+)\.s3\.(?P<region>[\w-]+)\.amazonaws\.com/(?P<key>.+)";
// S3://mybucket/puppy.jpg
const S3_URL: &str = r"[sS]3a?://(?P<bucket>[a-z0-9-\.]+)/(?P<key>.+)";
// unofficial, but convenient -- s3:https://my-endpoint.com:1234/mybucket/puppy.jpg
const S3_ENDPOINT_URL: &str =
    r"[sS]3a?://((?<protocol>https?)://)?(?P<endpoint>[^:/]+):(?<port>\d+)/(?P<bucket>[a-z0-9-\.]+)/(?P<key>.+)";

// file:///my/path/directory
const FILE_URI: &str = r"file://(?P<path>.*)";
// file:/my/path/directory
const FILE_URL: &str = r"file:(?P<path>.*)";
// /my/path/directory
const FILE_PATH: &str = r"/(?P<path>.*)";

// https://BUCKET_NAME.storage.googleapis.com/OBJECT_NAME
const GCS_VIRTUAL: &str = r"https://(?P<bucket>[a-z\d-_\.]+)\.storage\.googleapis\.com/(?P<key>.+)";
// https://storage.googleapis.com/BUCKET_NAME/OBJECT_NAME
const GCS_PATH: &str = r"https://storage\.googleapis\.com/(?P<bucket>[a-z\d-_\.]+)/(?P<key>.+)";
const GCS_URL: &str = r"[gG][sS]://(?P<bucket>[a-z0-9-\.]+)/(?P<key>.+)";

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
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

fn last<I: Sized, const COUNT: usize>(opts: [Option<I>; COUNT]) -> Option<I> {
    opts.into_iter().flatten().last()
}

impl StorageProvider {
    pub fn for_url(url: &str) -> Result<Self, StorageError> {
        for (k, v) in matchers() {
            if let Some(matches) = v.iter().filter_map(|r| r.captures(url)).next() {
                return match k {
                    Backend::S3 => Self::construct_s3(matches),
                    Backend::GCS => todo!(),
                    Backend::Local => Self::construct_local(matches),
                };
            }
        }

        return Err(StorageError::InvalidUrl);
    }

    fn construct_s3(matches: Captures) -> Result<Self, StorageError> {
        // fill in env vars
        let bucket = last([
            std::env::var(S3_BUCKET_ENV).ok(),
            matches.name("bucket").map(|m| m.as_str().to_string())
        ]).expect("bucket should always be available");

        let region = last([
            std::env::var("AWS_DEFAULT_REGION").ok(),
            std::env::var(S3_REGION_ENV).ok(),
            matches.name("region").map(|m| m.as_str().to_string()),
        ]);

        let endpoint = last([
            std::env::var("AWS_ENDPOINT").ok(),
            std::env::var(S3_ENDPOINT_ENV).ok(),
            matches.name("endpoint").map(|endpoint| -> Result<String, StorageError> {
                let port = if let Some(port) = matches.name("port") {
                    u16::from_str(port.as_str())
                        .map_err(|_| StorageError::PathError(format!("invalid port: {}", port.as_str())))?
                } else {
                    443
                };

                Ok(format!("{}:{}", endpoint.as_str(), port))
            }).transpose()?,
        ]);

        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&bucket);

        if let Some(region) = &region {
            builder = builder.with_region(region);
        }

        if let Some(endpoint) = &endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        let canonical_url = match (region, endpoint) {
            (_, Some(endpoint)) => {
                format!("s3::{}/{}", endpoint, bucket)
            }
            (Some(region), _) => {
                format!("s3::https://s3-{}.amazonaws.com/{}", region, bucket)
            }
            _ => {
                format!("s3::https://s3.amazonaws.com/{}", bucket)
            }
        };

        Ok(Self {
            backend: Backend::S3,
            object_store: Arc::new(builder.build().map_err(|e| Into::<StorageError>::into(e))?),
            canonical_url,
        })
    }

    fn construct_local(matches: Captures) -> Result<Self, StorageError> {
        let path = matches
            .name("path")
            .expect("path regex must contain a path group")
            .as_str();

        let path = if !path.starts_with("/") {
            format!("/{}", path)
        } else {
            path.to_string()
        };

        Ok(Self {
            backend: Backend::Local,
            object_store: Arc::new(
                LocalFileSystem::new_with_prefix(&path)
                    .map_err(|e| Into::<StorageError>::into(e))?,
            ),
            canonical_url: format!("file://{}", path),
        })
    }

    pub async fn get<P: Into<String>>(&self, path: P) -> Result<Bytes, StorageError> {
        let path: String = path.into();
        let bytes = self
            .object_store
            .get(&path.into())
            .await
            .map_err(|e| Into::<StorageError>::into(e))?
            .bytes()
            .await
            .map_err(|e| Into::<StorageError>::into(e))?;

        Ok(bytes)
    }

    pub async fn put<P: Into<String>>(&self, path: P, bytes: Vec<u8>) -> Result<(), StorageError> {
        let path = path.into();
        self.object_store
            .put(&path.into(), bytes.into())
            .await
            .map_err(|e| Into::<StorageError>::into(e))
    }

    /// Produces a URL representation of this path that can be read by other systems,
    /// in particular Nomad's artifact fetcher and Arroyo's artifact fetcher.
    pub fn canonical_url(&self) -> &str {
        &self.canonical_url
    }
}
