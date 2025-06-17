use anyhow::{anyhow, Result};
use arroyo_rpc::config::TlsConfig;
use arroyo_rpc::native_cert_store;
use axum_server::tls_rustls::RustlsConfig;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::sync::Arc;
use tonic::transport::{Identity, ServerTlsConfig};

/// Create TLS configuration for Axum HTTP/HTTPS servers
pub async fn create_http_tls_config(tls_config: &TlsConfig) -> Result<RustlsConfig> {
    let tls_config = tls_config.load().await?;

    RustlsConfig::from_pem(tls_config.cert, tls_config.key)
        .await
        .map_err(|e| anyhow!("Failed to load TLS configuration: {}", e))
}

/// Create TLS configuration for gRPC servers
pub async fn create_grpc_server_tls_config(tls_config: &TlsConfig) -> Result<ServerTlsConfig> {
    let tls_config = tls_config.load().await?;

    let tls = ServerTlsConfig::new().identity(Identity::from_pem(tls_config.cert, tls_config.key));

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

    let config = ServerConfig::builder_with_provider(default_provider().into())
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)?;

    Ok(config)
}

/// Create TLS configuration for raw TCP clients (workers)
pub async fn create_tcp_client_tls_config() -> Result<Arc<ClientConfig>> {
    use rustls::crypto::aws_lc_rs::default_provider;

    let mut root_store = RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    root_store.extend((*native_cert_store()).clone().roots);

    let config = ClientConfig::builder_with_provider(default_provider().into())
        .with_safe_default_protocol_versions()?
        .with_root_certificates(root_store)
        .with_no_client_auth();

    // TODO: Add support for custom CA certificates and client certificates

    Ok(Arc::new(config))
}
