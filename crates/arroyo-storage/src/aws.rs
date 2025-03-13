use crate::StorageError;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use object_store::{aws::AwsCredential, CredentialProvider};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tracing::info;

const EXPIRATION_BUFFER: Duration = Duration::from_secs(5 * 60);

type TemporaryToken = (Arc<AwsCredential>, Option<SystemTime>, Instant);

#[derive(Clone)]
pub struct ArroyoCredentialProvider {
    cache: Arc<Mutex<TemporaryToken>>,
    provider: SharedCredentialsProvider,
    refresh_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl std::fmt::Debug for ArroyoCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArroyoCredentialProvider").finish()
    }
}

static AWS_CONFIG: OnceCell<Arc<SdkConfig>> = OnceCell::const_new();
static CREDENTIAL_PROVIDER: OnceCell<ArroyoCredentialProvider> = OnceCell::const_new();

async fn get_config<'a>() -> &'a SdkConfig {
    AWS_CONFIG
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
        Ok(CREDENTIAL_PROVIDER
            .get_or_try_init(|| async {
                let config = get_config().await;

                let credentials = config
                    .credentials_provider()
                    .ok_or_else(|| {
                        StorageError::CredentialsError(
                            "Unable to load S3 credentials from environment".to_string(),
                        )
                    })?
                    .clone();

                info!("Creating credential provider");

                let initial_token = get_token(&credentials).await?;
                let cache = Arc::new(Mutex::new(initial_token));

                Ok::<Self, StorageError>(Self {
                    cache,
                    refresh_task: Default::default(),
                    provider: credentials,
                })
            })
            .await?
            .clone())
    }

    pub async fn default_region() -> Option<String> {
        get_config().await.region().map(|r| r.to_string())
    }
}

async fn get_token(
    provider: &SharedCredentialsProvider,
) -> Result<(Arc<AwsCredential>, Option<SystemTime>, Instant), object_store::Error> {
    info!("fetching new AWS token");
    let creds = provider
        .provide_credentials()
        .await
        .map_err(|e| object_store::Error::Generic {
            store: "S3",
            source: Box::new(e),
        })?;
    Ok((
        Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(ToString::to_string),
        }),
        creds.expiry(),
        Instant::now(),
    ))
}

#[async_trait::async_trait]
impl CredentialProvider for ArroyoCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let token = { self.cache.lock().unwrap().clone() };

        match token {
            (token, Some(expiration), last_refreshed) => {
                let expires_in = expiration
                    .duration_since(SystemTime::now())
                    .unwrap_or_default();
                if expires_in < Duration::from_millis(100) {
                    info!("AWS token has expired, immediately refreshing");

                    let token = get_token(&self.provider).await?;

                    let lock = self.cache.try_lock();
                    if let Ok(mut lock) = lock {
                        *lock = token.clone();
                    }
                    return Ok(token.0);
                }

                if expires_in < EXPIRATION_BUFFER
                    && last_refreshed.elapsed() > Duration::from_millis(100)
                {
                    let refresh_lock = self.refresh_task.try_lock();
                    if let Ok(mut task) = refresh_lock {
                        if task.is_some() && !task.as_ref().unwrap().is_finished() {
                            // the task is working on refreshing, let it do its job
                            return Ok(token);
                        }

                        // else we need to start a refresh task
                        let our_provider = self.provider.clone();
                        let our_lock = self.cache.clone();
                        *task = Some(tokio::spawn(async move {
                            let token = get_token(&our_provider)
                                .await
                                .unwrap_or_else(|e| panic!("Failed to refresh AWS token: {:?}", e));

                            let mut lock = our_lock.lock().unwrap();
                            *lock = token;
                        }));
                    }
                }

                Ok(token)
            }
            (token, None, _) => Ok(token),
        }
    }
}
