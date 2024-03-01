use std::collections::HashMap;
use std::convert::Infallible;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail};
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, TestSourceMessage};
use arroyo_rpc::{var_str::VarStr, OperatorConfig};
use axum::response::sse::Event;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event as MqttEvent, Incoming, MqttOptions};
use rumqttc::Outgoing;
use rustls_native_certs::load_native_certs;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Receiver;
use tokio_rustls::rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore};
use typify::import_types;

use super::Connector;
use crate::{pull_opt, Connection, ConnectionType};

const CONFIG_SCHEMA: &str = include_str!("../../connector-schemas/mqtt/connection.json");
const TABLE_SCHEMA: &str = include_str!("../../connector-schemas/mqtt/table.json");
const ICON: &str = include_str!("../resources/mqtt.svg");

import_types!(
    schema = "../connector-schemas/mqtt/connection.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);
import_types!(schema = "../connector-schemas/mqtt/table.json");

pub struct MqttConnector {}

impl MqttConnector {
    pub fn connection_from_options(
        options: &mut HashMap<String, String>,
    ) -> anyhow::Result<MqttConfig> {
        let host = match options.remove("host") {
            Some(host) => host,
            None => bail!("host is required for mqtt connection"),
        };
        let username = options.remove("username").map(VarStr::new);
        let password = options.remove("password").map(VarStr::new);
        let protocol = match options.remove("protocol") {
            Some(type_) => {
                Protocol::from_str(&type_).map_err(|err| anyhow!("invalid protocol: {}", err))?
            }
            None => Protocol::Tcp,
        };
        let port = match options.remove("port") {
            Some(port) => port.parse::<u16>()?,
            None => match protocol {
                Protocol::Tcp => 1883,
                Protocol::Tls => 8883,
            },
        };

        let ca = options.remove("tls.ca").map(VarStr::new);
        let cert = options.remove("tls.cert").map(VarStr::new);
        let key = options.remove("tls.key").map(VarStr::new);

        let tls = if protocol == Protocol::Tls && (ca.is_none() || cert.is_none() || key.is_none())
        {
            None
        } else {
            Some(Tls { ca, cert, key })
        };

        Ok(MqttConfig {
            host,
            username,
            password,
            port: Some(port as i64),
            protocol,
            tls,
            client_prefix: options.remove("client_prefix"),
        })
    }

    pub fn table_from_options(options: &mut HashMap<String, String>) -> anyhow::Result<MqttTable> {
        let typ = pull_opt("type", options)?;
        let qos = options
            .remove("qos")
            .and_then(|s| QualityOfService::try_from(s).ok());

        let table_type = match typ.as_str() {
            "source" => TableType::Source {
                type_: Some(Source::Source),
            },
            "sink" => TableType::Sink {
                retain: match options.remove("sink.retain") {
                    Some(retain) => Some(Retain::from_str(&retain).map_err(|err| anyhow!(err))?),
                    None => None,
                },
            },
            _ => {
                bail!("type must be one of 'source' or 'sink")
            }
        };

        Ok(MqttTable {
            topic: pull_opt("topic", options)?,
            type_: table_type,
            qos,
        })
    }
}

impl Connector for MqttConnector {
    type ProfileT = MqttConfig;
    type TableT = MqttTable;

    fn name(&self) -> &'static str {
        "mqtt"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "mqtt".to_string(),
            name: "Mqtt".to_string(),
            icon: ICON.to_string(),
            description: "Read and write from a mqtt cluster".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn config_description(&self, config: Self::ProfileT) -> String {
        format!(
            "{}://{}:{}",
            config.protocol.to_string(),
            config.host,
            config.port.unwrap_or(1883)
        )
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: MqttConfig,
        table: MqttTable,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let (typ, operator, desc) = match table.type_ {
            TableType::Source { .. } => (
                ConnectionType::Source,
                "connectors::mqtt::source::MqttSourceFunc",
                format!("MqttSource<{}>", table.topic),
            ),
            TableType::Sink { .. } => (
                ConnectionType::Sink,
                "connectors::mqtt::sink::MqttSinkFunc::<#in_k, #in_t>",
                format!("MqttSink<{}>", table.topic),
            ),
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("No schema defined for Mqtt connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Mqtt connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
        };

        Ok(Connection {
            id,
            name: name.to_string(),
            connection_type: typ,
            schema,
            operator: operator.to_string(),
            config: serde_json::to_string(&config).unwrap(),
            description: desc,
        })
    }

    fn test_profile(&self, profile: Self::ProfileT) -> Option<Receiver<TestSourceMessage>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let (itx, _rx) = tokio::sync::mpsc::channel(8);
            let message = match test_inner(profile, None, itx).await {
                Ok(_) => TestSourceMessage::done("Successfully connected to Mqtt"),
                Err(e) => TestSourceMessage::fail(format!("Failed to connect to Mqtt: {:?}", e)),
            };

            tx.send(message).unwrap();
        });

        Some(rx)
    }

    fn test(
        &self,
        _: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        _schema: Option<&ConnectionSchema>,
        tx: Sender<Result<Event, Infallible>>,
    ) {
        tokio::task::spawn(async move {
            let resp = match test_inner(config, Some(table), tx.clone()).await {
                Ok(c) => TestSourceMessage::done(c),
                Err(e) => TestSourceMessage::fail(e.to_string()),
            };

            tx.send(Ok(Event::default().json_data(resp).unwrap()))
                .await
                .unwrap();
        });
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.type_ {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink { .. } => ConnectionType::Sink,
        }
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let connection = profile
            .map(|p| {
                serde_json::from_value(p.config.clone()).map_err(|e| {
                    anyhow!("invalid config for profile '{}' in database: {}", p.id, e)
                })
            })
            .unwrap_or_else(|| Self::connection_from_options(options))?;

        let table = Self::table_from_options(options)?;

        Self::from_config(&self, None, name, connection, table, schema)
    }
}

async fn test_inner(
    c: MqttConfig,
    t: Option<MqttTable>,
    tx: tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
) -> anyhow::Result<String> {
    tx.send(Ok(Event::default()
        .json_data(TestSourceMessage::info("Connecting to Mqtt"))
        .unwrap()))
        .await
        .unwrap();

    let port = match c.port.map(|p| p as u16) {
        Some(port) => port,
        None => match c.protocol {
            Protocol::Tcp => 1883,
            Protocol::Tls => 8883,
        },
    };

    let mut options = MqttOptions::new("test-arroyo", c.host, port);
    options.set_keep_alive(Duration::from_secs(10));
    match c.protocol {
        Protocol::Tcp => (),
        Protocol::Tls => {
            let mut root_cert_store = RootCertStore::empty();
            for cert in load_native_certs().expect("could not load platform certs") {
                root_cert_store.add(&Certificate(cert.0)).unwrap();
            }

            if let Some(ca) = c.tls.as_ref().and_then(|tls| tls.ca.as_ref()) {
                let ca = ca.sub_env_vars().map_err(|e| anyhow!("{}", e))?;
                let certificates = load_certs(&ca)?;
                for cert in certificates {
                    root_cert_store.add(&cert).unwrap();
                }
            }

            let tls_config = if let Some((Some(client_cert), Some(client_key))) = c
                .tls
                .as_ref()
                .and_then(|tls| Some((tls.cert.as_ref(), tls.key.as_ref())))
            {
                let client_cert = client_cert.sub_env_vars().map_err(|e| anyhow!("{}", e))?;
                let client_key = client_key.sub_env_vars().map_err(|e| anyhow!("{}", e))?;
                let certs = load_certs(&client_cert)?;
                let key = load_private_key(&client_key)?;
                ClientConfig::builder()
                    .with_safe_defaults()
                    .with_root_certificates(root_cert_store)
                    .with_client_auth_cert(certs, key)?
            } else {
                ClientConfig::builder()
                    .with_safe_defaults()
                    .with_root_certificates(root_cert_store)
                    .with_no_client_auth()
            };

            options.set_transport(rumqttc::Transport::tls_with_config(
                rumqttc::TlsConfiguration::Rustls(Arc::new(tls_config)),
            ));
        }
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

    let (client, mut eventloop) = AsyncClient::new(options, 10);

    let wait_for_incomming = match t {
        Some(t) => {
            let topic = t.topic;
            let qos = t
                .qos
                .and_then(|qos| match qos {
                    QualityOfService::AtMostOnce => Some(QoS::AtMostOnce),
                    QualityOfService::AtLeastOnce => Some(QoS::AtLeastOnce),
                    QualityOfService::ExactlyOnce => Some(QoS::ExactlyOnce),
                })
                .unwrap_or(QoS::AtMostOnce);
            if let TableType::Sink { retain, .. } = t.type_ {
                client
                    .publish(
                        topic,
                        qos,
                        retain
                            .and_then(|r| match r {
                                Retain::Yes => Some(true),
                                Retain::No => Some(false),
                            })
                            .unwrap_or_default(),
                        "test".as_bytes(),
                    )
                    .await?;
                false
            } else {
                client.subscribe(&topic, qos).await?;
                client.publish(topic, qos, false, "test".as_bytes()).await?;
                true
            }
        }
        None => {
            client
                .publish("test-arroyo", QoS::AtMostOnce, false, "test".as_bytes())
                .await?;
            false
        }
    };

    while let Ok(notification) = eventloop.poll().await {
        match notification {
            MqttEvent::Incoming(Incoming::Publish(p)) => {
                let _payload = String::from_utf8(p.payload.to_vec())?;
                return Ok("Successfully subscribed".to_string());
            }
            MqttEvent::Outgoing(Outgoing::Publish(_p)) => {
                if !wait_for_incomming {
                    return Ok("Successfully published".to_string());
                }
            }
            MqttEvent::Incoming(Incoming::Disconnect { .. })
            | MqttEvent::Outgoing(Outgoing::Disconnect) => {
                bail!("Disconnected from Mqtt");
            }
            _ => (),
        }
    }

    bail!("Failed to connect to Mqtt")
}

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
