use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use crate::mqtt::sink::MqttSinkFunc;
use crate::mqtt::source::MqttSourceFunc;
use crate::pull_opt;
use anyhow::{anyhow, bail};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::OperatorNode;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::{var_str::VarStr, OperatorConfig};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event as MqttEvent, EventLoop, Incoming, MqttOptions};
use rumqttc::Outgoing;
use rustls_native_certs::load_native_certs;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Receiver;
use tokio_rustls::rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore};
use typify::import_types;

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./mqtt.svg");

pub mod sink;
pub mod source;

import_types!(
    schema = "src/mqtt/profile.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

import_types!(schema = "src/mqtt/table.json");
pub struct MqttConnector {}

impl MqttTable {
    pub fn qos(&self) -> QoS {
        self.qos
            .map(|qos| match qos {
                QualityOfService::AtMostOnce => QoS::AtMostOnce,
                QualityOfService::AtLeastOnce => QoS::AtLeastOnce,
                QualityOfService::ExactlyOnce => QoS::ExactlyOnce,
            })
            .unwrap_or(QoS::AtMostOnce)
    }
}

impl MqttConnector {
    pub fn connection_from_options(
        options: &mut HashMap<String, String>,
    ) -> anyhow::Result<MqttConfig> {
        let url = match options.remove("url") {
            Some(host) => host,
            None => bail!("url is required for mqtt connection"),
        };
        let username = options.remove("username").map(VarStr::new);
        let password = options.remove("password").map(VarStr::new);

        let ca = options.remove("tls.ca").map(VarStr::new);
        let cert = options.remove("tls.cert").map(VarStr::new);
        let key = options.remove("tls.key").map(VarStr::new);

        let parsed_url = url::Url::parse(&url)?;

        let tls = if matches!(parsed_url.scheme(), "mqtts" | "ssl") {
            Some(Tls { ca, cert, key })
        } else {
            None
        };

        Ok(MqttConfig {
            url,
            username,
            password,
            tls,
            client_prefix: options.remove("client_prefix"),
        })
    }

    pub fn table_from_options(options: &mut HashMap<String, String>) -> anyhow::Result<MqttTable> {
        let typ = pull_opt("type", options)?;
        let qos = options
            .remove("qos")
            .map(|s| {
                QualityOfService::try_from(s).map_err(|s| anyhow!("invalid value for 'qos': {s}"))
            })
            .transpose()?;

        let table_type = match typ.as_str() {
            "source" => TableType::Source {},
            "sink" => TableType::Sink {
                retain: options
                    .remove("sink.retain")
                    .map(|s| {
                        s.parse::<bool>()
                            .map_err(|_| anyhow!("'sink.retail' must be either 'true' or 'false'"))
                    })
                    .transpose()?
                    .unwrap_or(false),
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
        config.url.clone()
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: MqttConfig,
        table: MqttTable,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let (typ, desc) = match table.type_ {
            TableType::Source { .. } => (
                ConnectionType::Source,
                format!("MqttSource<{}>", table.topic),
            ),
            TableType::Sink { .. } => (ConnectionType::Sink, format!("MqttSink<{}>", table.topic)),
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
            connector: self.name(),
            name: name.to_string(),
            connection_type: typ,
            schema,
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
        tx: Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let resp = match test_inner(config, Some(table), tx.clone()).await {
                Ok(c) => TestSourceMessage::done(c),
                Err(e) => TestSourceMessage::fail(e.to_string()),
            };

            tx.send(resp).await.unwrap();
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

        Self::from_config(self, None, name, connection, table, schema)
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<OperatorNode> {
        let qos = table.qos();
        Ok(match table.type_ {
            TableType::Source {} => OperatorNode::from_source(Box::new(MqttSourceFunc {
                config: profile,
                topic: table.topic,
                qos,
                format: config
                    .format
                    .ok_or_else(|| anyhow!("format is required for mqtt source"))?,
                framing: config.framing,
                bad_data: config.bad_data,
                messages_per_second: NonZeroU32::new(
                    config
                        .rate_limit
                        .map(|l| l.messages_per_second)
                        .unwrap_or(u32::MAX),
                )
                .unwrap(),
                subscribed: Arc::new(AtomicBool::new(false)),
            })),
            TableType::Sink { retain } => OperatorNode::from_operator(Box::new(MqttSinkFunc {
                config: profile,
                qos,
                topic: table.topic,
                retain,
                serializer: ArrowSerializer::new(
                    config
                        .format
                        .ok_or_else(|| anyhow!("format is required for mqtt sink"))?,
                ),
                stopped: Arc::new(AtomicBool::new(false)),
                client: None,
            })),
        })
    }
}

async fn test_inner(
    c: MqttConfig,
    t: Option<MqttTable>,
    tx: Sender<TestSourceMessage>,
) -> anyhow::Result<String> {
    tx.send(TestSourceMessage::info("Connecting to Mqtt"))
        .await
        .unwrap();

    let (client, mut eventloop) = create_connection(&c, 0)?;

    let wait_for_incomming = match t {
        Some(t) => {
            let topic = t.topic;
            let qos = t
                .qos
                .map(|qos| match qos {
                    QualityOfService::AtMostOnce => QoS::AtMostOnce,
                    QualityOfService::AtLeastOnce => QoS::AtLeastOnce,
                    QualityOfService::ExactlyOnce => QoS::ExactlyOnce,
                })
                .unwrap_or(QoS::AtMostOnce);
            if let TableType::Sink { retain, .. } = t.type_ {
                client
                    .publish(topic, qos, retain, "test".as_bytes())
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

    loop {
        match eventloop.poll().await {
            Ok(notification) => match notification {
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
            },
            Err(e) => bail!("Error while reading from Mqtt: {:?}", e),
        }
    }
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

pub(crate) fn create_connection(
    c: &MqttConfig,
    task_id: usize,
) -> anyhow::Result<(AsyncClient, EventLoop)> {
    // It creates a client id with the format: <client_prefix>_<task_id><current_time_in_millis>
    // because the client id must be unique for each connection. Otherwise, the broker will only keep one active connection
    // per client id
    let client_id = format!(
        "{}_{}{}",
        c.client_prefix.as_deref().unwrap_or("arroyo-mqtt"),
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
            .map(|tls| (tls.cert.as_ref(), tls.key.as_ref()))
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

    let password = if let Some(password) = &c.password {
        password.sub_env_vars().map_err(|e| anyhow!("{}", e))?
    } else {
        "".to_string()
    };

    if let Some(username) = &c.username {
        options.set_credentials(
            username.sub_env_vars().map_err(|e| anyhow!("{}", e))?,
            password,
        );
    }

    Ok(AsyncClient::new(options, 100))
}
