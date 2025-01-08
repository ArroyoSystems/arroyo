use crate::StorageError;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use object_store::{aws::AwsCredential, CredentialProvider, TemporaryToken, TokenCache};
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;
use tracing::info;

pub struct ArroyoCredentialProvider {
    cache: TokenCache<Arc<AwsCredential>>,
    provider: SharedCredentialsProvider,
}

impl std::fmt::Debug for ArroyoCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArroyoCredentialProvider").finish()
    }
}

static AWS_CONFIG: OnceCell<Arc<SdkConfig>> = OnceCell::const_new();

async fn get_config<'a>() -> &'a SdkConfig {
    &*AWS_CONFIG
        .get_or_init(|| async {
            Arc::new(
                aws_config::defaults(BehaviorVersion::latest())
                    .timeout_config(
                        TimeoutConfig::builder()
                            .operation_timeout(Duration::from_secs(60))
                            .operation_attempt_timeout(Duration::from_secs(5))
                            .build(),
                    )
                    .load()
                    .await,
            )
        })
        .await
}

impl ArroyoCredentialProvider {
    pub async fn try_new() -> Result<Self, StorageError> {
        let config = get_config().await;

        let credentials = config
            .credentials_provider()
            .ok_or_else(|| {
                StorageError::CredentialsError(
                    "Unable to load S3 credentials from environment".to_string(),
                )
            })?
            .clone();

        Ok(Self {
            cache: Default::default(),
            provider: credentials,
        })
    }

    pub async fn default_region() -> Option<String> {
        get_config().await.region().map(|r| r.to_string())
    }
}

async fn get_token(
    provider: &SharedCredentialsProvider,
) -> Result<TemporaryToken<Arc<AwsCredential>>, Box<dyn Error + Send + Sync>> {
    info!("Getting credentials");
    let creds = provider
        .provide_credentials()
        .await
        .map_err(|e| object_store::Error::Generic {
            store: "S3",
            source: Box::new(e),
        })?;
    info!("Got credentials = {:?}", creds);
    let expiry = creds
        .expiry()
        .map(|exp| Instant::now() + exp.elapsed().unwrap_or_default());
    Ok(TemporaryToken {
        token: Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(ToString::to_string),
        }),
        expiry,
    })
}

#[async_trait::async_trait]
impl CredentialProvider for ArroyoCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        self.cache
            .get_or_insert_with(|| get_token(&self.provider))
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "S3",
                source: e,
            })
    }
}
