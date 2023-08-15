use crate::queries::api_queries::GetConnectionTablesParams;
use anyhow::anyhow;
use arrow_schema::DataType;
use axum::extract::{Path, Query, State};
use axum::response::sse::Event;
use axum::response::Sse;
use axum::Json;
use axum_extra::extract::WithRejection;
use cornucopia_async::GenericClient;
use cornucopia_async::Params;
use futures_util::stream::Stream;
use http::StatusCode;
use serde_json::json;
use std::convert::Infallible;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

use arroyo_connectors::{connector_for_type, ErasedConnector};
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_rpc::types::{
    ConfluentSchema, ConfluentSchemaQueryParams, ConnectionProfile, ConnectionSchema,
    ConnectionTable, ConnectionTableCollection, ConnectionTablePost, PaginationQueryParams,
    SchemaDefinition,
};
use arroyo_sql::json_schema::convert_json_schema;
use arroyo_sql::types::{StructField, TypeDef};

use crate::rest::AppState;
use crate::rest_utils::{
    authenticate, bad_request, client, log_and_map_rest, not_found, paginate_results,
    required_field, validate_pagination_params, ApiError, BearerAuth, ErrorResp,
};
use crate::{
    handle_db_error, handle_delete, log_and_map,
    queries::api_queries::{self, DbConnectionTable},
    to_micros, AuthData,
};

async fn get_and_validate_connector<E: GenericClient>(
    req: &ConnectionTablePost,
    auth: &AuthData,
    c: &E,
) -> Result<
    (
        Box<dyn ErasedConnector>,
        Option<i64>,
        serde_json::Value,
        Option<ConnectionSchema>,
    ),
    ErrorResp,
> {
    let connector = connector_for_type(&req.connector)
        .ok_or_else(|| anyhow!("Unknown connector '{}'", req.connector,))
        .map_err(log_and_map)?;

    let (connection_profile_id, config) = if let Some(connection_profile_id) =
        &req.connection_profile_id
    {
        let connection_profile = api_queries::get_connection_profile_by_pub_id()
            .bind(c, &auth.organization_id, &connection_profile_id)
            .opt()
            .await
            .map_err(log_and_map)?
            .ok_or_else(|| {
                bad_request(format!(
                    "No connection profile with id '{}'",
                    connection_profile_id
                ))
            })?;

        if connection_profile.r#type != req.connector {
            return Err(
                bad_request(format!("Stored connection has a different connector than requested (found {}, expected {})", connection_profile.r#type, req.connector))

            );
        }

        (
            Some(connection_profile.id),
            connection_profile.config.clone(),
        )
    } else {
        (None, json! {{}})
    };

    connector
        .validate_table(&req.config)
        .map_err(|e| bad_request(format!("Failed to parse config: {:?}", e)))?;

    let schema: Option<ConnectionSchema> = req
        .schema
        .as_ref()
        .map(|t| t.clone().try_into())
        .transpose()
        .map_err(|e| bad_request(format!("Invalid schema: {}", e)))?;

    let schema = if let Some(schema) = &schema {
        Some(expand_schema(&req.name, schema)?)
    } else {
        None
    };

    Ok((connector, connection_profile_id, config, schema))
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
        return Err(not_found("Connection table".to_string()));
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

    let (tx, rx) = channel(8);

    connector
        .test(&req.name, &config, &req.config, schema.as_ref(), tx)
        .map_err(|e| bad_request(format!("Failed to parse config or schema: {:?}", e)))?;

    Ok(Sse::new(ReceiverStream::new(rx)))
}

fn get_connection_profile(
    c: &DbConnectionTable,
    connector: &dyn ErasedConnector,
) -> Option<ConnectionProfile> {
    let config = c.connection_config.clone().unwrap_or(json!({}));
    Some(ConnectionProfile {
        id: c.pub_id.clone(),
        name: c.connection_name.as_ref()?.clone(),
        connector: c.connection_type.as_ref()?.clone(),
        description: connector.config_description(&config).ok()?,
        config,
    })
}

pub(crate) async fn get_all_connection_tables<C: GenericClient>(
    auth: &AuthData,
    client: &C,
) -> Result<Vec<ConnectionTable>, ErrorResp> {
    let tables = api_queries::get_all_connection_tables()
        .bind(client, &auth.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    let vec: Vec<ConnectionTable> = tables
        .into_iter()
        .map(|t| t.try_into())
        .map(|result| {
            if let Err(err) = &result {
                warn!("Error building connection table: {}", err);
            }
            result
        })
        .filter_map(Result::ok)
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
    let transaction = client.transaction().await.map_err(log_and_map_rest)?;
    transaction
        .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
        .await
        .map_err(log_and_map_rest)?;

    let (connector, connection_id, config, schema) =
        get_and_validate_connector(&req, &auth_data, &transaction).await?;

    let table_type: String = connector
        .table_type(&config, &req.config)
        .unwrap()
        .to_string();

    if let Some(schema) = &req.schema {
        if schema.definition.is_none() {
            return Err(required_field("schema.definition"));
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
            &table_type.to_string(),
            &req.connector,
            &connection_id,
            &req.config,
            &schema,
        )
        .await
        .map_err(|err| handle_db_error("connection_table", err))?;

    transaction.commit().await.map_err(log_and_map_rest)?;

    let table = api_queries::get_connection_table()
        .bind(&client, &auth_data.organization_id, &pub_id)
        .one()
        .await
        .map_err(log_and_map_rest)?
        .try_into()
        .map_err(log_and_map_rest)?;

    Ok(Json(table))
}

impl TryInto<ConnectionTable> for DbConnectionTable {
    type Error = String;
    fn try_into(self) -> Result<ConnectionTable, Self::Error> {
        let Some(connector) = connector_for_type(&self.connector) else {
                return Err(format!("invalid connector {} in saved ConnectionTable {}", self.connector, self.id));
            };

        let connection = get_connection_profile(&self, &*connector);

        let schema = self.schema.map(|schema| serde_json::from_value(schema));
        let schema = match schema {
            Some(Ok(schema)) => Some(schema),
            Some(Err(err)) => {
                return Err(format!(
                    "Error during connection schema deserialization: {:?}",
                    err
                ));
            }
            None => None,
        };

        let schema = match connector.get_schema(
            &connection
                .as_ref()
                .map(|t| t.config.clone())
                .unwrap_or(json!({})),
            &self.config,
            schema.as_ref(),
        ) {
            Ok(schema) => schema,
            Err(e) => {
                return Err(format!("Invalid connector config for {}: {:?}", self.id, e));
            }
        };

        let schema = match schema {
            Some(schema) => schema,
            None => {
                return Err(format!("No schema found for table {}", self.id));
            }
        };

        let connection_type = self.table_type.try_into()?;

        Ok(ConnectionTable {
            id: self.id,
            pub_id: self.pub_id,
            name: self.name,
            created_at: to_micros(self.created_at),
            connection_profile: None,
            connector: self.connector,
            table_type: connection_type,
            config: self.config,
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
    params(
        PaginationQueryParams
    ),
    responses(
        (status = 200, description = "Got connection table collection", body = ConnectionTableCollection),
    ),
)]
pub(crate) async fn get_connection_tables(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    query_params: Query<PaginationQueryParams>,
) -> Result<Json<ConnectionTableCollection>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let (starting_after, limit) =
        validate_pagination_params(query_params.starting_after.clone(), query_params.limit)?;

    let tables = api_queries::get_connection_tables()
        .params(
            &client,
            &GetConnectionTablesParams {
                organization_id: &auth_data.organization_id,
                starting_after: starting_after.unwrap_or("".to_string()),
                limit: limit as i32, // is 1 more than the requested limit
            },
        )
        .all()
        .await
        .map_err(log_and_map)?;

    let (tables, has_more) = paginate_results(tables, limit);

    let tables: Vec<ConnectionTable> = tables
        .into_iter()
        .map(|t| t.try_into())
        .map(|result| {
            if let Err(err) = &result {
                warn!("Error building connection table: {}", err);
            }
            result
        })
        .filter_map(Result::ok)
        .collect();

    Ok(Json(ConnectionTableCollection {
        data: tables,
        has_more,
    }))
}

// attempts to fill in the SQL schema from a schema object that may just have a json-schema or
// other source schema. schemas stored in the database should always be expanded first.
pub(crate) fn expand_schema(
    name: &str,
    schema: &ConnectionSchema,
) -> Result<ConnectionSchema, ErrorResp> {
    let mut schema = schema.clone();

    if let Some(d) = &schema.definition {
        let fields = match d {
            SchemaDefinition::JsonSchema(json) => convert_json_schema(name, &json)
                .map_err(|e| bad_request(format!("Invalid json-schema: {}", e)))?,
            SchemaDefinition::ProtobufSchema(_) => {
                return Err(bad_request(
                    "Protobuf schemas are not yet supported".to_string(),
                ))
            }
            SchemaDefinition::AvroSchema(_) => {
                return Err(bad_request(
                    "Avro schemas are not yet supported".to_string(),
                ))
            }
            SchemaDefinition::RawSchema(_) => vec![StructField::new(
                "value".to_string(),
                None,
                TypeDef::DataType(DataType::Utf8, false),
            )],
        };

        let fields: Result<_, String> = fields.into_iter().map(|f| f.try_into()).collect();

        schema.fields =
            fields.map_err(|e| bad_request(format!("Failed to convert schema: {}", e)))?;
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
        SchemaDefinition::JsonSchema(schema) => {
            if let Err(e) = convert_json_schema(&"test", &schema) {
                Err(bad_request(e))
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
            Some(StatusCode::NOT_FOUND) => bad_request(format!(
                "Could not find value schema for topic '{}'",
                query_params.topic
            )),

            Some(code) => bad_request(format!("Schema registry returned error: {}", code)),
            None => {
                warn!(
                    "Unknown error connecting to schema registry {}: {:?}",
                    query_params.endpoint, e
                );
                bad_request(format!(
                    "Could not connect to Schema Registry at {}: unknown error",
                    query_params.endpoint
                ))
            }
        }
    })?;

    if !resp.status().is_success() {
        let message = format!(
            "Received an error status code from the provided endpoint: {} {}",
            resp.status().as_u16(),
            resp.bytes()
                .await
                .map(|bs| String::from_utf8_lossy(&bs).to_string())
                .unwrap_or_else(|_| "<failed to read body>".to_string())
        );
        return Err(bad_request(message));
    }

    let value: serde_json::Value = resp.json().await.map_err(|e| {
        warn!("Invalid json from schema registry: {:?}", e);
        bad_request(format!(
            "Schema registry returned invalid JSON: {}",
            e.to_string()
        ))
    })?;

    let schema_type = value
        .get("schemaType")
        .ok_or_else(|| {
            bad_request(
                "The JSON returned from this endpoint was unexpected. Please confirm that the URL is correct."
                    .to_string(),
            )
        })?
        .as_str();

    if schema_type != Some("JSON") {
        return Err(bad_request(
            "Only JSON schema types are supported currently".to_string(),
        ));
    }

    let schema = value
        .get("schema")
        .ok_or_else(|| {
            return bad_request("Missing 'schema' field in schema registry response".to_string());
        })?
        .as_str()
        .ok_or_else(|| {
            return bad_request(
                "The 'schema' field in the schema registry response is not a string".to_string(),
            );
        })?;

    if let Err(e) = convert_json_schema(&query_params.topic, schema) {
        warn!(
            "Schema from schema registry is not valid: '{}': {}",
            schema, e
        );
        return Err(bad_request(format!(
            "Schema from schema registry is not valid: {}",
            e
        )));
    }

    Ok(Json(ConfluentSchema {
        schema: schema.to_string(),
    }))
}
