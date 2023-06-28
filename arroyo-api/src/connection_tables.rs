use anyhow::anyhow;
use arrow_schema::DataType;
use arroyo_connectors::{connector_for_type, ErasedConnector};
use arroyo_rpc::grpc::api::{
    connection_schema::Definition, ConfluentSchemaReq, ConfluentSchemaResp, Connection,
    ConnectionSchema, ConnectionTable, CreateConnectionTableReq, DeleteConnectionTableReq,
    TableType, TestSchemaReq, TestSourceMessage,
};
use arroyo_sql::{
    json_schema::{self, convert_json_schema},
    types::{StructField, TypeDef},
};
use cornucopia_async::GenericClient;
use deadpool_postgres::Pool;
use http::StatusCode;
use tokio::sync::mpsc::{channel, Receiver};
use tonic::Status;
use tracing::warn;

use crate::{
    handle_db_error, handle_delete, log_and_map,
    queries::api_queries::{self, GetConnectionTables},
    required_field, AuthData,
};

async fn get_and_validate_connector<E: GenericClient>(
    req: &CreateConnectionTableReq,
    auth: &AuthData,
    c: &E,
) -> Result<
    (
        Box<dyn ErasedConnector>,
        Option<i64>,
        String,
        Option<ConnectionSchema>,
    ),
    Status,
> {
    let connector = connector_for_type(&req.connector)
        .ok_or_else(|| anyhow!("Unknown connector '{}'", req.connector,))
        .map_err(log_and_map)?;

    let (connection_id, config) = if let Some(connection_id) = &req.connection_id {
        let connection_id: i64 = connection_id.parse().map_err(|_| {
            Status::failed_precondition(format!("No connection with id '{}'", connection_id))
        })?;

        let connection = api_queries::get_connection_by_id()
            .bind(c, &auth.organization_id, &connection_id)
            .opt()
            .await
            .map_err(log_and_map)?
            .ok_or_else(|| {
                Status::failed_precondition(format!("No connection with id '{}'", connection_id))
            })?;

        if connection.r#type != req.connector {
            return Err(Status::failed_precondition(format!(
                "Stored connection has a different connector than requested (found {}, expected {})",
                    connection.r#type, req.connector)));
        }

        (
            Some(connection_id),
            serde_json::to_string(&connection.config).unwrap(),
        )
    } else {
        (None, "".to_string())
    };

    connector
        .validate_table(&req.config)
        .map_err(|e| Status::invalid_argument(&format!("Failed to parse config: {:?}", e)))?;

    let schema = if let Some(schema) = &req.schema {
        Some(expand_schema(&req.name, schema)?)
    } else {
        None
    };

    Ok((connector, connection_id, config, schema))
}

pub(crate) async fn create(
    req: CreateConnectionTableReq,
    auth: AuthData,
    pool: &Pool,
) -> Result<(), Status> {
    let mut c = pool.get().await.map_err(log_and_map)?;

    let transaction = c.transaction().await.map_err(log_and_map)?;
    transaction
        .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
        .await
        .map_err(log_and_map)?;

    let (connector, connection_id, config, schema) =
        get_and_validate_connector(&req, &auth, &transaction).await?;

    let table_type = connector.table_type(&config, &req.config).unwrap();

    let table_config: serde_json::Value = serde_json::from_str(&req.config).unwrap();

    if let Some(schema) = &req.schema {
        if schema.definition.is_none() {
            return Err(Status::failed_precondition("schema.definition must be set"));
        }
    }

    let schema: Option<serde_json::Value> = schema.map(|s| serde_json::to_value(s).unwrap());

    api_queries::create_connection_table()
        .bind(
            &transaction,
            &auth.organization_id,
            &auth.user_id,
            &req.name,
            &table_type.as_str_name(),
            &req.connector,
            &connection_id,
            &table_config,
            &schema,
        )
        .await
        .map_err(|err| handle_db_error("connection_table", err))?;

    transaction.commit().await.map_err(log_and_map)?;
    Ok(())
}

pub(crate) async fn delete(
    req: DeleteConnectionTableReq,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<(), Status> {
    let deleted = api_queries::delete_connection_table()
        .bind(client, &auth.organization_id, &req.id)
        .await
        .map_err(|e| handle_delete("connection_table", "pipelines", e))?;

    if deleted == 0 {
        return Err(Status::not_found(format!(
            "No connection table with id {}",
            req.id
        )));
    }

    Ok(())
}

pub(crate) async fn test(
    req: CreateConnectionTableReq,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<Receiver<Result<TestSourceMessage, Status>>, Status> {
    let (connector, _, config, schema) = get_and_validate_connector(&req, &auth, client).await?;

    let (tx, rx) = channel(8);

    connector
        .test(&req.name, &config, &req.config, schema.as_ref(), tx)
        .map_err(|e| {
            Status::invalid_argument(format!("Failed to parse config or schema: {:?}", e))
        })?;

    Ok(rx)
}

fn get_connection(c: &GetConnectionTables) -> Option<Connection> {
    Some(Connection {
        id: format!("{}", c.connection_id?),
        name: c.connection_name.as_ref()?.clone(),
        connector: c.connection_type.as_ref()?.clone(),
        config: serde_json::to_string(&c.connection_config.as_ref()?).unwrap(),
    })
}

pub(crate) async fn get<C: GenericClient>(
    auth: &AuthData,
    client: &C,
) -> Result<Vec<ConnectionTable>, Status> {
    let tables = api_queries::get_connection_tables()
        .bind(client, &auth.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    Ok(tables
        .into_iter()
        .map(|t| {
            let connection = get_connection(&t);
            let connector = connector_for_type(&t.connector).unwrap_or_else(|| {
                panic!(
                    "invalid connector in saved ConnectionTable: {}",
                    t.connector
                )
            });

            let table = serde_json::to_string(&t.config).unwrap();
            let schema = t.schema.map(|s| serde_json::from_value(s).unwrap());
            let schema = connector
                .get_schema(
                    &connection
                        .as_ref()
                        .map(|t| t.config.as_str())
                        .unwrap_or("{}"),
                    &table,
                    schema.as_ref(),
                )
                .unwrap_or_else(|e| panic!("Invalid connector config for {}: {:?}", t.id, e));

            ConnectionTable {
                id: t.id,
                name: t.name,
                connection,
                connector: t.connector,
                table_type: TableType::from_str_name(&t.table_type).unwrap() as i32,
                config: table,
                schema,
                consumers: t.consumer_count as i32,
            }
        })
        .collect())
}

// attempts to fill in the SQL schema from a schema object that may just have a json-schema or
// other source schema. schemas stored in the database should always be expanded first.
pub(crate) fn expand_schema(
    name: &str,
    schema: &ConnectionSchema,
) -> Result<ConnectionSchema, Status> {
    let mut schema = schema.clone();

    if let Some(d) = &schema.definition {
        let fields = match d {
            Definition::JsonSchema(json) => json_schema::convert_json_schema(name, &json)
                .map_err(|e| Status::invalid_argument(format!("Invalid json-schema: {}", e)))?,
            Definition::ProtobufSchema(_) => {
                return Err(Status::failed_precondition(
                    "Protobuf schemas are not yet supported",
                ))
            }
            Definition::AvroSchema(_) => {
                return Err(Status::failed_precondition(
                    "Avro schemas are not yet supported",
                ))
            }
            Definition::RawSchema(_) => vec![StructField::new(
                "value".to_string(),
                None,
                TypeDef::DataType(DataType::Utf8, false),
            )],
        };

        let fields: Result<_, String> = fields.into_iter().map(|f| f.try_into()).collect();

        schema.fields = fields
            .map_err(|e| Status::failed_precondition(format!("Failed to convert schema: {}", e)))?;
    }

    Ok(schema)
}

pub(crate) async fn test_schema(req: TestSchemaReq) -> Result<Vec<String>, Status> {
    let Some(schema_def) = req
        .schema
        .ok_or_else(|| required_field("schema"))?
        .definition else {
            return Ok(vec![]);
        };

    match schema_def {
        Definition::JsonSchema(schema) => {
            if let Err(e) = convert_json_schema(&"test", &schema) {
                Ok(vec![e])
            } else {
                Ok(vec![])
            }
        }
        _ => {
            // TODO: add testing for other schema types
            Ok(vec![])
        }
    }
}

pub(crate) async fn get_confluent_schema(
    req: ConfluentSchemaReq,
) -> Result<ConfluentSchemaResp, Status> {
    // TODO: ensure only external URLs can be hit
    let url = format!(
        "{}/subjects/{}-value/versions/latest",
        req.endpoint, req.topic
    );
    let resp = reqwest::get(url).await.map_err(|e| {
        warn!("Got error response from schema registry: {:?}", e);
        match e.status() {
            Some(StatusCode::NOT_FOUND) => Status::failed_precondition(format!(
                "Could not find value schema for topic '{}'",
                req.topic
            )),
            Some(code) => {
                Status::failed_precondition(format!("Schema registry returned error: {}", code))
            }
            None => {
                warn!(
                    "Unknown error connecting to schema registry {}: {:?}",
                    req.endpoint, e
                );
                Status::failed_precondition(format!(
                    "Could not connect to Schema Registry at {}: unknown error",
                    req.endpoint
                ))
            }
        }
    })?;

    if !resp.status().is_success() {
        return Err(Status::failed_precondition(format!(
            "Received an error status code from the provided endpoint: {} {}",
            resp.status().as_u16(),
            resp.bytes()
                .await
                .map(|bs| String::from_utf8_lossy(&bs).to_string())
                .unwrap_or_else(|_| "<failed to read body>".to_string())
        )));
    }

    let value: serde_json::Value = resp.json().await.map_err(|e| {
        warn!("Invalid json from schema registry: {:?}", e);
        Status::failed_precondition("Schema registry returned invalid JSON".to_string())
    })?;

    let schema_type = value
        .get("schemaType")
        .ok_or_else(|| {
            Status::failed_precondition(
                "The JSON returned from this endpoint was \
            unexpected. Please confirm that the URL is correct.",
            )
        })?
        .as_str();

    if schema_type != Some("JSON") {
        return Err(Status::failed_precondition(
            "Only JSON is supported currently",
        ));
    }

    let schema = value
        .get("schema")
        .ok_or_else(|| {
            Status::failed_precondition("Missing 'schema' field in schema registry response")
        })?
        .as_str()
        .ok_or_else(|| {
            Status::failed_precondition(
                "'schema' field in schema registry response is not a string",
            )
        })?;

    if let Err(e) = convert_json_schema(&req.topic, schema) {
        warn!(
            "Schema from schema registry is not valid: '{}': {}",
            schema, e
        );
        return Err(Status::failed_precondition(format!(
            "Schema is not a valid json schema: {}",
            e
        )));
    }

    Ok(ConfluentSchemaResp {
        schema: schema.to_string(),
    })
}
