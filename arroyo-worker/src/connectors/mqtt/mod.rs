use std::{sync::Arc, time::Duration};

use anyhow::anyhow;
use arroyo_rpc::var_str::VarStr;
use rumqttc::v5::{AsyncClient, EventLoop, MqttOptions};
use rustls_native_certs::load_native_certs;
use serde::{Deserialize, Serialize};
use tokio_rustls::rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore};
use typify::import_types;

pub mod sink;
pub mod source;

import_types!(schema = "../connector-schemas/mqtt/connection.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);
import_types!(schema = "../connector-schemas/mqtt/table.json");

fn load_certs(certificates: &str) -> anyhow::Result<Vec<Certificate>> {
    let cert_bytes = std::fs::read_to_string(certificates).map_or_else(
        |_| certificates.as_bytes().to_owned(),
        |certs| certs.as_bytes().to_owned(),
    );

    let certs = rustls_pemfile::certs(&mut cert_bytes.as_slice()).map_err(|err| anyhow!(err))?;

    Ok(certs.into_iter().map(Certificate).collect())
}

fn load_private_key(certificate: &str) -> anyhow::Result<PrivateKey> {
    let cert_bytes = std::fs::read_to_string(certificate).map_or_else(
        |_| certificate.as_bytes().to_owned(),
        |cert| cert.as_bytes().to_owned(),
    );

    let certs = rustls_pemfile::pkcs8_private_keys(&mut cert_bytes.as_slice())
        .map_err(|err| anyhow!(err))?;
    let cert = certs
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("No private key found"))?;
    Ok(PrivateKey(cert))
}

pub(crate) fn create_connection(
    c: MqttConfig,
    task_id: usize,
) -> anyhow::Result<(AsyncClient, EventLoop)> {
    // It creates a client id with the format: <client_prefix>_<task_id><current_time_in_millis>
    // because the client id must be unique for each connection. Otherwise, the broker will only keep one active connection
    // per client id
    let client_id = format!(
        "{}_{}{}",
        c.client_prefix
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or_else(|| "arroyo-mqtt"),
        task_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            % 100000,
    );

    let mut url = url::Url::parse(&c.url)?;
    let ssl = matches!(url.scheme(), "mqtts" | "ssl");
    url.query_pairs_mut().append_pair("client_id", &client_id);

    let mut options = MqttOptions::try_from(url)?;

    options.set_keep_alive(Duration::from_secs(10));
    if ssl {
        let mut root_cert_store = RootCertStore::empty();

        if let Some(ca) = c.tls.as_ref().and_then(|tls| tls.ca.as_ref()) {
            let ca = ca.sub_env_vars().map_err(|e| anyhow!("{}", e))?;
            let certificates = load_certs(&ca)?;
            for cert in certificates {
                root_cert_store.add(&cert).unwrap();
            }
        } else {
            for cert in load_native_certs().expect("could not load platform certs") {
                root_cert_store.add(&Certificate(cert.0)).unwrap();
            }
        }

        let builder = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_cert_store);

        let tls_config = if let Some((Some(client_cert), Some(client_key))) = c
            .tls
            .as_ref()
            .and_then(|tls| Some((tls.cert.as_ref(), tls.key.as_ref())))
        {
            let client_cert = client_cert.sub_env_vars().map_err(|e| anyhow!("{}", e))?;
            let client_key = client_key.sub_env_vars().map_err(|e| anyhow!("{}", e))?;
            let certs = load_certs(&client_cert)?;
            let key = load_private_key(&client_key)?;

            builder.with_client_auth_cert(certs, key)?
        } else {
            builder.with_no_client_auth()
        };

        options.set_transport(rumqttc::Transport::tls_with_config(
            rumqttc::TlsConfiguration::Rustls(Arc::new(tls_config)),
        ));
    }

    let password = if let Some(password) = c.password {
        password.sub_env_vars().map_err(|e| anyhow!("{}", e))?
    } else {
        "".to_string()
    };

    if let Some(username) = c.username {
        options.set_credentials(
            username.sub_env_vars().map_err(|e| anyhow!("{}", e))?,
            password,
        );
    }

    Ok(AsyncClient::new(options, 100))
}
