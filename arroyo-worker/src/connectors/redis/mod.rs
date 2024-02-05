use crate::connectors::redis::sink::GeneralConnection;
use anyhow::anyhow;
use arroyo_rpc::var_str::VarStr;
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::{Client, ConnectionInfo, IntoConnectionInfo};
use serde::{Deserialize, Serialize};
use typify::import_types;

pub mod sink;

import_types!(schema = "../connector-schemas/redis/connection.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);
import_types!(schema = "../connector-schemas/redis/table.json");

pub(crate) enum RedisClient {
    Standard(Client),
    Clustered(ClusterClient),
}

impl RedisClient {
    pub fn new(config: &RedisConfig) -> anyhow::Result<Self> {
        Ok(match &config.connection {
            RedisConfigConnection::Address(address) => {
                let info = from_address(&config, &address.0)?;

                RedisClient::Standard(Client::open(info).map_err(|e| {
                    anyhow!(
                        "Failed to construct Redis client for {}: {:?}",
                        address.0,
                        e
                    )
                })?)
            }
            RedisConfigConnection::Addresses(addresses) => {
                let infos: anyhow::Result<Vec<ConnectionInfo>> = addresses
                    .iter()
                    .map(|address| from_address(&config, address))
                    .collect();

                RedisClient::Clustered(
                    ClusterClient::new(infos?).map_err(|e| {
                        anyhow!("Failed to construct Redis Cluster client: {:?}", e)
                    })?,
                )
            }
        })
    }

    async fn get_connection(&self) -> Result<GeneralConnection, redis::RedisError> {
        Ok(match self {
            RedisClient::Standard(c) => {
                GeneralConnection::Standard(ConnectionManager::new(c.clone()).await?)
            }
            RedisClient::Clustered(c) => {
                GeneralConnection::Clustered(c.get_async_connection().await?)
            }
        })
    }
}

fn from_address(config: &RedisConfig, address: &str) -> anyhow::Result<ConnectionInfo> {
    let mut info: ConnectionInfo = address
        .to_string()
        .into_connection_info()
        .map_err(|e| anyhow!("invalid redis address: {:?}", e))?;

    if let Some(username) = &config.username {
        info.redis.username = Some(username.sub_env_vars().map_err(|e| anyhow!("{}", e))?);
    }

    if let Some(password) = &config.password {
        info.redis.password = Some(password.sub_env_vars().map_err(|e| anyhow!("{}", e))?);
    }

    Ok(info)
}
