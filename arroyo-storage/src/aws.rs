use std::sync::Arc;

use object_store::{aws::AwsCredential, CredentialProvider};
use rusoto_core::credential::{
    AutoRefreshingProvider, ChainProvider, ProfileProvider, ProvideAwsCredentials,
};

use crate::StorageError;

pub struct ArroyoCredentialProvider {
    provider: AutoRefreshingProvider<ChainProvider>,
}

impl std::fmt::Debug for ArroyoCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArroyoCredentialProvider").finish()
    }
}

impl ArroyoCredentialProvider {
    pub fn try_new() -> Result<Self, StorageError> {
        let inner: AutoRefreshingProvider<ChainProvider> =
            AutoRefreshingProvider::new(ChainProvider::new())
                .map_err(|e| StorageError::CredentialsError(e.to_string()))?;

        Ok(Self { provider: inner })
    }

    pub async fn default_region() -> Option<String> {
        ProfileProvider::region().ok()?
    }
}

#[async_trait::async_trait]
impl CredentialProvider for ArroyoCredentialProvider {
    #[doc = " The type of credential returned by this provider"]
    type Credential = AwsCredential;

    /// Return a credential
    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let credentials =
            self.provider
                .credentials()
                .await
                .map_err(|err| object_store::Error::Generic {
                    store: "s3",
                    source: Box::new(err),
                })?;
        Ok(Arc::new(AwsCredential {
            key_id: credentials.aws_access_key_id().to_string(),
            secret_key: credentials.aws_secret_access_key().to_string(),
            token: credentials.token().clone(),
        }))
    }
}
