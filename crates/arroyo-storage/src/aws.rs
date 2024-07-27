use crate::StorageError;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use object_store::{aws::AwsCredential, CredentialProvider};
use std::sync::Arc;

pub struct ArroyoCredentialProvider {
    provider: aws_credential_types::provider::SharedCredentialsProvider,
}

impl std::fmt::Debug for ArroyoCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArroyoCredentialProvider").finish()
    }
}

impl ArroyoCredentialProvider {
    pub async fn try_new() -> Result<Self, StorageError> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

        let credentials = config
            .credentials_provider()
            .ok_or_else(|| {
                StorageError::CredentialsError(
                    "Unable to load S3 credentials from environment".to_string(),
                )
            })?
            .clone();

        Ok(Self {
            provider: credentials,
        })
    }

    pub async fn default_region() -> Option<String> {
        aws_config::defaults(BehaviorVersion::latest())
            .load()
            .await
            .region()
            .map(|r| r.to_string())
    }
}

#[async_trait::async_trait]
impl CredentialProvider for ArroyoCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let creds = self.provider.provide_credentials().await.map_err(|e| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(e),
            }
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(ToString::to_string),
        }))
    }
}
