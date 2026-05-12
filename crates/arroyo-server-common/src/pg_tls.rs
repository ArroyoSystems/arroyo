//! TLS connector construction for Postgres connections.
//!
//! Three TLS-enabled modes are supported, mirroring [`PostgresTlsMode`]:
//!
//! * [`SystemRoots`](PostgresTlsMode::SystemRoots) — verify against the
//!   OS trust store only. Suitable for managed Postgres providers with
//!   publicly-signed server certs.
//! * [`CaCert`](PostgresTlsMode::CaCert) — verify against the OS trust
//!   store plus an additional CA cert from disk. Recommended for
//!   cluster-internal connections where the CA is mounted as a secret.
//! * [`SkipVerification`](PostgresTlsMode::SkipVerification) — encrypts
//!   but does not validate the server certificate.
//!
//! [`NoTls`](PostgresTlsMode::NoTls) is handled by the caller (no
//! connector needed).

use anyhow::{Context, bail};
use arroyo_rpc::config::PostgresTlsMode;
use arroyo_rpc::native_cert_store;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use std::sync::Arc;
use tracing::info;

/// Build a rustls TLS connector for the given [`PostgresTlsMode`].
///
/// Returns `Ok(None)` for [`PostgresTlsMode::NoTls`] — callers should fall
/// back to `tokio_postgres::NoTls` in that case.
pub fn build_pg_tls_connector(
    mode: &PostgresTlsMode,
) -> anyhow::Result<Option<tokio_postgres_rustls::MakeRustlsConnect>> {
    let client_config = match mode {
        PostgresTlsMode::NoTls => return Ok(None),
        PostgresTlsMode::SkipVerification => build_skip_verification_config(),
        PostgresTlsMode::SystemRoots => build_system_roots_config(),
        PostgresTlsMode::CaCert { path } => build_ca_cert_config(path)?,
    };
    Ok(Some(tokio_postgres_rustls::MakeRustlsConnect::new(
        client_config,
    )))
}

/// Build a [`ClientConfig`] that verifies against the system trust store
/// plus an additional CA cert at `path`.
fn build_ca_cert_config(path: &str) -> anyhow::Result<ClientConfig> {
    let mut root_store = (*native_cert_store()).clone();

    let pem =
        std::fs::read(path).with_context(|| format!("failed to read CA cert file at {path}"))?;

    let mut added = 0;
    for cert in rustls_pemfile::certs(&mut &pem[..]) {
        let cert = cert.with_context(|| format!("failed to parse PEM cert in {path}"))?;
        root_store
            .add(cert)
            .with_context(|| format!("failed to add CA cert from {path}"))?;
        added += 1;
    }

    if added == 0 {
        bail!("CA cert file at {path} contained no valid certificates");
    }
    info!(path, count = added, "loaded extra CA certs");

    Ok(ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth())
}

/// Build a [`ClientConfig`] that verifies against the system trust store
/// only, with no additional CA cert.
fn build_system_roots_config() -> ClientConfig {
    let root_store = (*native_cert_store()).clone();
    ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// Build a [`ClientConfig`] that performs TLS but skips server certificate
/// verification.
fn build_skip_verification_config() -> ClientConfig {
    // A [`ServerCertVerifier`] that accepts every server certificate.
    #[derive(Debug)]
    struct NoVerification;

    impl ServerCertVerifier for NoVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::RSA_PSS_SHA256,
            ]
        }
    }

    ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerification))
        .with_no_client_auth()
}
