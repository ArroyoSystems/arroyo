use std::convert::Infallible;

use anyhow::anyhow;
use arrow_schema::DataType;
use axum::extract::{Path, Query, State};
use axum::response::sse::Event;
use axum::response::Sse;
use axum::Json;
use axum_extra::extract::WithRejection;
use cornucopia_async::GenericClient;
use futures_util::stream::Stream;
use http::StatusCode;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tracing::{error, warn};

use arroyo_connectors::{connector_for_type, ErasedConnector};
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_rpc::types::{
    ConfluentSchema, ConfluentSchemaQueryParams, ConnectionProfile, ConnectionSchema,
    ConnectionTable, ConnectionTableCollection, ConnectionTablePost, SchemaDefinition,
};
use arroyo_sql::json_schema::convert_json_schema;
use arroyo_sql::{
    json_schema::{self},
    types::{StructField, TypeDef},
};

use crate::rest::AppState;
use crate::rest_utils::{authenticate, client, ApiError, BearerAuth, ErrorResp};
use crate::{
    handle_db_error, handle_delete, log_and_map,
    queries::api_queries::{self, DbConnectionTable},
    AuthData,
};

async fn get_and_validate_connector<E: GenericClient>(
    req: &ConnectionTablePost,
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

    let (connection_id, config) = if let Some(connection_profile_id) = &req.connection_profile_id {
        // let connection_id: i64 = connection_id.parse().map_err(|_| {
        //     Status::failed_precondition(format!("No connection with id '{}'", connection_id))
        // })?;

        let connection = api_queries::get_connection_by_pub_id()
            .bind(c, &auth.organization_id, &connection_profile_id)
            .opt()
            .await
            .map_err(log_and_map)?
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "No connection with id '{}'",
                    connection_profile_id
                ))
            })?;

        if connection.r#type != req.connector {
            return Err(Status::failed_precondition(format!(
                "Stored connection has a different connector than requested (found {}, expected {})",
                    connection.r#type, req.connector)));
        }

        (
            Some(connection.id),
            serde_json::to_string(&connection.config).unwrap(),
        )
    } else {
        (None, "".to_string())
    };

    connector
        .validate_table(&req.config)
        .map_err(|e| Status::invalid_argument(&format!("Failed to parse config: {:?}", e)))?;

    let schema: Option<ConnectionSchema> = req
        .schema
        .as_ref()
        .map(|t| t.clone().try_into())
        .transpose()
        .map_err(|e| Status::failed_precondition(format!("Invalid schema: {}", e)))?;

    let schema = if let Some(schema) = &schema {
        Some(expand_schema(&req.name, schema)?)
    } else {
        None
    };

    Ok((connector, connection_id, config, schema))
}

/// Delete a Connection Table
#[utoipa::path(
    delete,
    path = "/v1/connection_tables/{id}",
    tag = "connection_tables",
    params(
        ("id" = String, Path, description = "Connection Table id")
    ),
    responses(
        (status = 200, description = "Deleted connection table"),
    ),
)]
pub(crate) async fn delete_connection_table(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path(pub_id): Path<String>,
) -> Result<(), ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let deleted = api_queries::delete_connection_table()
        .bind(&client, &auth_data.organization_id, &pub_id)
        .await
        .map_err(|e| handle_delete("connection_table", "pipelines", e))?;

    if deleted == 0 {
        return Err(ErrorResp {
            status_code: StatusCode::NOT_FOUND,
            message: "Connection table not found".to_string(),
        });
    }

    Ok(())
}

/// Test a Connection Table
#[utoipa::path(
    post,
    path = "/v1/connection_tables/test",
    tag = "connection_tables",
    request_body = ConnectionTablePost,
    responses(
        (status = 200, description = "Job output as 'text/event-stream'"),
    ),
)]
pub(crate) async fn test_connection_table(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    WithRejection(Json(req), _): WithRejection<Json<ConnectionTablePost>, ApiError>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let (connector, _, config, schema) =
        get_and_validate_connector(&req, &auth_data, &client).await?;

    let (tx, mut rx) = channel(8);

    let (tx2, rx2) = channel(8);

    connector
        .test(&req.name, &config, &req.config, schema.as_ref(), tx)
        .map_err(|e| {
            Status::invalid_argument(format!("Failed to parse config or schema: {:?}", e))
        })?;

    tokio::spawn(async move {
        loop {
            let m = match rx.recv().await {
                Some(d) => d,
                None => {
                    warn!("Stream closed");
                    break;
                }
            };

            let message = match m {
                Ok(msg) => msg,
                Err(_) => {
                    error!("Stream error");
                    break;
                }
            };

            let e: Result<Event, Infallible> =
                Ok(Event::default().data(serde_json::to_string(&message).unwrap()));
            if tx2.send(e).await.is_err() {
                break;
            }

            if message.done {
                break;
            }
        }
    });

    Ok(Sse::new(ReceiverStream::new(rx2)))
}

fn get_connection(
    c: &DbConnectionTable,
    connector: &dyn ErasedConnector,
) -> Option<ConnectionProfile> {
    let config = serde_json::to_string(&c.connection_config.as_ref()?).unwrap();
    Some(ConnectionProfile {
        id: c.pub_id.clone(),
        name: c.connection_name.as_ref()?.clone(),
        connector: c.connection_type.as_ref()?.clone(),
        description: connector.config_description(&config).ok()?,
        config,
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

    let vec: Vec<ConnectionTable> = tables
        .into_iter()
        .filter_map(|t| {
            // return None;
            let Some(connector) = connector_for_type(&t.connector) else {
                warn!("invalid connector {} in saved ConnectionTable {}", t.connector, t.id);
                return None;
            };

            let connection = get_connection(&t, &*connector);

            let table = serde_json::to_string(&t.config).unwrap();
            let schema = t.schema.map(|s| serde_json::from_value(s).unwrap());
            let schema: Option<ConnectionSchema> = match connector.get_schema(
                &connection
                    .as_ref()
                    .map(|t| t.config.as_str())
                    .unwrap_or("{}"),
                &table,
                schema.as_ref(),
            ) {
                Ok(schema) => schema,
                Err(e) => {
                    warn!("Invalid connector config for {}: {:?}", t.id, e);
                    return None;
                }
            };

            let schema = match schema {
                Some(schema) => schema,
                None => {
                    warn!("No schema found for table {}", t.id);
                    return None;
                }
            };

            Some(ConnectionTable {
                id: t.id,
                pub_id: t.pub_id,
                name: t.name,
                connection,
                connector: t.connector,
                table_type: t.table_type.into(),
                config: table,
                schema,
                consumers: t.consumer_count as u32,
            })
        })
        .collect();
    Ok(vec)
}

/// Create a new connection table
#[utoipa::path(
    post,
    path = "/v1/connection_tables",
    tag = "connection_tables",
    request_body = ConnectionTablePost,
    responses(
        (status = 200, description = "Created connection table", body = ConnectionTable),
    ),
)]
pub async fn create_connection_table(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    WithRejection(Json(req), _): WithRejection<Json<ConnectionTablePost>, ApiError>,
) -> Result<Json<ConnectionTable>, ErrorResp> {
    let mut client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;
    let transaction = client.transaction().await.map_err(log_and_map)?;
    transaction
        .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
        .await
        .map_err(log_and_map)?;

    let (connector, connection_id, config, schema) =
        get_and_validate_connector(&req, &auth_data, &transaction).await?;

    let table_type: String = connector.table_type(&config, &req.config).unwrap().into();

    let table_config: serde_json::Value = serde_json::from_str(&req.config).unwrap();

    if let Some(schema) = &req.schema {
        if schema.definition.is_none() {
            return Err(ErrorResp {
                status_code: StatusCode::BAD_REQUEST,
                message: "schema.definition must be set".to_string(),
            });
        }
    }

    let schema: Option<serde_json::Value> = schema.map(|s| serde_json::to_value(s).unwrap());

    let pub_id = generate_id(IdTypes::ConnectionTable);

    api_queries::create_connection_table()
        .bind(
            &transaction,
            &pub_id,
            &auth_data.organization_id,
            &auth_data.user_id,
            &req.name,
            &table_type,
            &req.connector,
            &connection_id,
            &table_config,
            &schema,
        )
        .await
        .map_err(|err| handle_db_error("connection_table", err))?;

    transaction.commit().await.map_err(log_and_map)?;

    let table = api_queries::get_connection_table()
        .bind(&client, &auth_data.organization_id, &pub_id)
        .one()
        .await
        .map_err(log_and_map)?
        .try_into();

    match table {
        Ok(table) => Ok(Json(table)),
        Err(_) => Err(ErrorResp {
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
            message: "Failed to create connection table".to_string(),
        }),
    }
}

impl TryInto<ConnectionTable> for DbConnectionTable {
    type Error = ();
    fn try_into(self) -> Result<ConnectionTable, Self::Error> {
        let Some(connector) = connector_for_type(&self.connector) else {
                warn!("invalid connector {} in saved ConnectionTable {}", self.connector, self.id);
                return Err(());
            };

        let connection = get_connection(&self, &*connector);

        let table = serde_json::to_string(&self.config).unwrap();
        let schema: Option<ConnectionSchema> = self.schema.and_then(|s| {
            serde_json::from_value(s)
                .map_err(|e| {
                    warn!("Error during connection schema deserialization: {:?}", e);
                })
                .ok()
        });
        let schema = match connector.get_schema(
            &connection
                .as_ref()
                .map(|t| t.config.as_str())
                .unwrap_or("{}"),
            &table,
            schema.as_ref(),
        ) {
            Ok(schema) => schema,
            Err(e) => {
                warn!("Invalid connector config for {}: {:?}", self.id, e);
                return Err(());
            }
        };

        let schema = match schema {
            Some(schema) => schema,
            None => {
                warn!("No schema found for table {}", self.id);
                return Err(());
            }
        };

        Ok(ConnectionTable {
            id: self.id,
            pub_id: self.pub_id,
            name: self.name,
            connection: None,
            connector: self.connector,
            table_type: self.table_type.into(),
            config: serde_json::to_string(&self.config).unwrap(),
            schema,
            consumers: self.consumer_count as u32,
        })
    }
}

/// List all connection tables
#[utoipa::path(
    get,
    path = "/v1/connection_tables",
    tag = "connection_tables",
    responses(
        (status = 200, description = "Got connection table collection", body = ConnectionTableCollection),
    ),
)]
pub(crate) async fn get_connection_tables(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
) -> Result<Json<ConnectionTableCollection>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let tables = api_queries::get_connection_tables()
        .bind(&client, &auth_data.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    let data: Vec<ConnectionTable> = tables
        .into_iter()
        .filter_map(|t| {
            let Some(connector) = connector_for_type(&t.connector) else {
                warn!("invalid connector {} in saved ConnectionTable {}", t.connector, t.id);
                return None;
            };

            let connection = get_connection(&t, &*connector);

            let table = serde_json::to_string(&t.config).unwrap();

            let schema: Option<ConnectionSchema> =
                t.schema
                    .and_then(|value| match serde_json::from_value(value) {
                        Ok(connection_schema) => Some(connection_schema),
                        Err(err) => {
                            warn!("Connection schema deserialization failed: {:?}", err);
                            None
                        }
                    });

            let schema = match connector.get_schema(
                &connection
                    .as_ref()
                    .map(|t| t.config.as_str())
                    .unwrap_or("{}"),
                &table,
                schema.as_ref(),
            ) {
                Ok(schema) => schema,
                Err(e) => {
                    warn!("Invalid connector config for {}: {:?}", t.id, e);
                    return None;
                }
            };

            let schema = match schema {
                Some(schema) => schema,
                None => {
                    warn!("No schema found for table {}", t.id);
                    return None;
                }
            };

            Some(ConnectionTable {
                id: t.id,
                pub_id: t.pub_id,
                name: t.name,
                connector: t.connector,
                connection,
                table_type: t.table_type.into(),
                config: table,
                schema,
                consumers: t.consumer_count as u32,
            })
        })
        .collect();

    Ok(Json(ConnectionTableCollection {
        data,
        has_more: false,
    }))
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
            SchemaDefinition::Json(json) => json_schema::convert_json_schema(name, &json)
                .map_err(|e| Status::invalid_argument(format!("Invalid json-schema: {}", e)))?,
            SchemaDefinition::Protobuf(_) => {
                return Err(Status::failed_precondition(
                    "Protobuf schemas are not yet supported",
                ))
            }
            SchemaDefinition::Avro(_) => {
                return Err(Status::failed_precondition(
                    "Avro schemas are not yet supported",
                ))
            }
            SchemaDefinition::Raw(_) => vec![StructField::new(
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

/// Test a Connection Schema
#[utoipa::path(
    post,
    path = "/v1/connection_tables/schemas/test",
    tag = "connection_tables",
    request_body = ConnectionSchema,
    responses(
        (status = 200, description = "Schema is valid"),
    ),
)]
pub(crate) async fn test_schema(
    WithRejection(Json(req), _): WithRejection<Json<ConnectionSchema>, ApiError>,
) -> Result<(), ErrorResp> {
    let Some(schema_def) = req
        .definition else {
            return Ok(());
        };

    match schema_def {
        SchemaDefinition::Json(schema) => {
            if let Err(e) = convert_json_schema(&"test", &schema) {
                Err(ErrorResp {
                    status_code: StatusCode::BAD_REQUEST,
                    message: e,
                })
            } else {
                Ok(())
            }
        }
        _ => {
            // TODO: add testing for other schema types
            Ok(())
        }
    }
}

/// Get a Confluent Schema
#[utoipa::path(
    get,
    path = "/v1/connection_tables/schemas/confluent",
    tag = "connection_tables",
    params(
        ("topic" = String, Query, description = "Confluent topic name"),
        ("endpoint" = String, Query, description = "Confluent schema registry endpoint"),
    ),
    responses(
        (status = 200, description = "Got Confluent Schema", body = ConfluentSchema),
    ),
)]
pub(crate) async fn get_confluent_schema(
    query_params: Query<ConfluentSchemaQueryParams>,
) -> Result<Json<ConfluentSchema>, ErrorResp> {
    // TODO: ensure only external URLs can be hit
    let url = format!(
        "{}/subjects/{}-value/versions/latest",
        query_params.endpoint, query_params.topic
    );
    let resp = reqwest::get(url).await.map_err(|e| {
        warn!("Got error response from schema registry: {:?}", e);
        match e.status() {
            Some(StatusCode::NOT_FOUND) => Status::failed_precondition(format!(
                "Could not find value schema for topic '{}'",
                query_params.topic
            )),
            Some(code) => {
                Status::failed_precondition(format!("Schema registry returned error: {}", code))
            }
            None => {
                warn!(
                    "Unknown error connecting to schema registry {}: {:?}",
                    query_params.endpoint, e
                );
                Status::failed_precondition(format!(
                    "Could not connect to Schema Registry at {}: unknown error",
                    query_params.endpoint
                ))
            }
        }
    })?;

    if !resp.status().is_success() {
        return Err(ErrorResp {
            status_code: StatusCode::BAD_REQUEST,
            message: format!(
                "Received an error status code from the provided endpoint: {} {}",
                resp.status().as_u16(),
                resp.bytes()
                    .await
                    .map(|bs| String::from_utf8_lossy(&bs).to_string())
                    .unwrap_or_else(|_| "<failed to read body>".to_string())
            ),
        });
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
        return Err(ErrorResp {
            status_code: StatusCode::BAD_REQUEST,
            message: "Only JSON is supported currently".to_string(),
        });
    }

    let schema = value
        .get("schema")
        .ok_or_else(|| {
            return ErrorResp {
                status_code: StatusCode::BAD_REQUEST,
                message: "Missing 'schema' field in schema registry response".to_string(),
            };
        })?
        .as_str()
        .ok_or_else(|| {
            return ErrorResp {
                status_code: StatusCode::BAD_REQUEST,
                message: "'schema' field in schema registry response is not a string".to_string(),
            };
        })?;

    if let Err(e) = convert_json_schema(&query_params.topic, schema) {
        warn!(
            "Schema from schema registry is not valid: '{}': {}",
            schema, e
        );
        return Err(ErrorResp {
            status_code: StatusCode::BAD_REQUEST,
            message: format!("Schema is not a valid json schema: {}", e),
        });
    }

    Ok(Json(ConfluentSchema {
        schema: schema.to_string(),
    }))
}
