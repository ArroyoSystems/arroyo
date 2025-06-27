use anyhow::{anyhow, bail, Result};
use arroyo_rpc::config::{config, ApiAuthMode, TlsConfig};
use arroyo_rpc::native_cert_store;
use axum_server::tls_rustls::RustlsConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::sync::Arc;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};

/// Create TLS configuration for Axum HTTP/HTTPS servers
pub async fn create_http_tls_config(
    auth_mode: &ApiAuthMode,
    tls_config: &Option<TlsConfig>,
) -> Result<Option<RustlsConfig>> {
    let config = config();
    let mut tls_config = config.get_tls_config(tls_config).cloned();
    let mtls_enabled = match (&mut tls_config, &auth_mode) {
        (_, ApiAuthMode::None) => false,
        (Some(tls_config), ApiAuthMode::Mtls { ca_cert_file }) => {
            tls_config.mtls_ca_file = tls_config
                .mtls_ca_file
                .as_ref()
                .or(ca_cert_file.as_ref())
                .cloned();
            true
        }
        (None, ApiAuthMode::Mtls { .. }) => {
            bail!("api.auth_mode set to MTLS, but no TLS config was provided");
        }
        (_, ApiAuthMode::StaticApiKey { .. }) => false,
    };

    let Some(tls_config) = tls_config else {
        return Ok(None);
    };

    let tls_config = tls_config.load().await?;

    let cert = CertificateDer::from_pem_slice(&tls_config.cert)?;
    let key = PrivateKeyDer::from_pem_reader(&tls_config.key[..])?;

    let mtls_ca = if mtls_enabled {
        tls_config.mtls_ca
    } else {
        None
    };

    let cfg = if let Some(mtls_ca) = mtls_ca {
        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates(CertificateDer::from_pem_slice(&mtls_ca[..]));

        let verifier = WebPkiClientVerifier::builder(roots.into()).build()?;

        ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![cert], key)?
    } else {
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)?
    };

    Ok(Some(RustlsConfig::from_config(Arc::new(cfg))))
}

/// Create TLS configuration for gRPC servers
pub async fn create_grpc_server_tls_config(tls_config: &TlsConfig) -> Result<ServerTlsConfig> {
    let tls_config = tls_config.load().await?;

    let mut tls =
        ServerTlsConfig::new().identity(Identity::from_pem(tls_config.cert, tls_config.key));

    if let Some(cert) = tls_config.mtls_ca {
        tls = tls
            .client_auth_optional(false)
            .client_ca_root(Certificate::from_pem(&cert[..]));
    }

    Ok(tls)
}

/// Create TLS configuration for raw TCP connections
pub async fn create_tcp_server_tls_config(tls_config: &TlsConfig) -> Result<ServerConfig> {
    use rustls::crypto::aws_lc_rs::default_provider;

    let tls_config = tls_config.load().await?;

    let cert_chain =
        rustls_pemfile::certs(&mut &tls_config.cert[..]).collect::<Result<Vec<_>, _>>()?;

    let key = rustls_pemfile::private_key(&mut &tls_config.key[..])
        .map_err(|e| anyhow!("Failed to parse private key: {e}"))?
        .ok_or_else(|| anyhow!("No private key found"))?;

    let config_builder = ServerConfig::builder_with_provider(default_provider().into())
        .with_safe_default_protocol_versions()?;

    let config = if let Some(mtls_cert) = tls_config.mtls_ca {
        let mut roots = RootCertStore::empty();
        roots.add(
            CertificateDer::from_pem_slice(&mtls_cert)
                .map_err(|e| anyhow!("invalid mtls ca certificate: {:?}", e))?,
        )?;

        config_builder
            .with_client_cert_verifier(WebPkiClientVerifier::builder(roots.into()).build()?)
            .with_single_cert(cert_chain, key)?
    } else {
        config_builder
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)?
    };

    Ok(config)
}

/// Create TLS configuration for raw TCP clients (workers)
pub async fn create_tcp_client_tls_config(tls_config: &TlsConfig) -> Result<Arc<ClientConfig>> {
    use rustls::crypto::aws_lc_rs::default_provider;

    let mut root_store = RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    root_store.extend((*native_cert_store()).clone().roots);

    let config_builder = ClientConfig::builder_with_provider(default_provider().into())
        .with_safe_default_protocol_versions()?
        .with_root_certificates(root_store);

    let config = if tls_config.mtls_ca_file.is_some() {
        let tls_config = tls_config.load().await?;

        config_builder
            .with_client_auth_cert(
                vec![CertificateDer::from_pem_slice(&tls_config.cert)
                    .map_err(|e| anyhow!("invalid worker TLS cert: {:?}", e))?],
                PrivateKeyDer::from_pem_slice(&tls_config.key)
                    .map_err(|e| anyhow!("invalid worker TLS key: {:?}", e))?,
            )
            .map_err(|e| anyhow!("failed to construct worker TLS configuration: {:?}", e))?
    } else {
        config_builder.with_no_client_auth()
    };

    Ok(Arc::new(config))
}
