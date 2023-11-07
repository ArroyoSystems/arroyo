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
use serde_json::{json, Value};
use std::convert::Infallible;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

use arroyo_connectors::kafka::{KafkaConfig, KafkaTable};
use arroyo_connectors::{connector_for_type, ErasedConnector};
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionTable, ConnectionTablePost, SchemaDefinition,
};
use arroyo_rpc::api_types::{ConnectionTableCollection, PaginationQueryParams};
use arroyo_rpc::formats::{AvroFormat, Format, JsonFormat};
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_rpc::schema_resolver::{
    ConfluentSchemaResolver, ConfluentSchemaResponse, ConfluentSchemaType,
};
use arroyo_sql::avro;
use arroyo_sql::json_schema::convert_json_schema;
use arroyo_sql::types::{StructField, TypeDef};

use crate::rest::AppState;
use crate::rest_utils::{
    authenticate, bad_request, client, log_and_map, not_found, paginate_results, required_field,
    validate_pagination_params, ApiError, BearerAuth, ErrorResp,
};
use crate::{
    handle_db_error, handle_delete,
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

    let (connection_profile_id, profile_config) = if let Some(connection_profile_id) =
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
        .clone()
        .map(|t| t.validate())
        .transpose()
        .map_err(|e| bad_request(format!("Invalid schema: {}", e)))?;

    let schema = if let Some(schema) = schema {
        let name = connector.name();
        Some(expand_schema(&req.name, name, schema, &req.config, &profile_config).await?)
    } else {
        None
    };

    Ok((connector, connection_profile_id, profile_config, schema))
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
        return Err(not_found("Connection table"));
    }

    Ok(())
}

/// Test a Connection Table
#[axum::debug_handler]
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

    let (connector, _, profile, schema) =
        get_and_validate_connector(&req, &auth_data, &client).await?;

    let (tx, rx) = channel(8);

    connector
        .test(&req.name, &profile, &req.config, schema.as_ref(), tx)
        .map_err(|e| bad_request(format!("Failed to parse config or schema: {:?}", e)))?;

    Ok(Sse::new(ReceiverStream::new(rx)))
}

fn get_connection_profile(
    c: &DbConnectionTable,
    connector: &dyn ErasedConnector,
) -> anyhow::Result<Option<ConnectionProfile>> {
    if let Some(id) = &c.profile_id {
        let config = c
            .profile_config
            .as_ref()
            .ok_or_else(|| anyhow!("connection name not found"))?
            .clone();
        Ok(Some(ConnectionProfile {
            id: id.clone(),
            name: c
                .profile_name
                .as_ref()
                .ok_or_else(|| anyhow!("connection name not found"))?
                .clone(),
            connector: c
                .profile_type
                .as_ref()
                .ok_or_else(|| anyhow!("connection type not found"))?
                .clone(),
            description: connector
                .config_description(&config)
                .map_err(|e| anyhow!("invalid config for connector {}: {:?}", id, e))?,
            config,
        }))
    } else {
        Ok(None)
    }
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
    let transaction = client.transaction().await.map_err(log_and_map)?;
    transaction
        .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
        .await
        .map_err(log_and_map)?;

    let (connector, connection_id, profile, schema) =
        get_and_validate_connector(&req, &auth_data, &transaction).await?;

    let table_type: String = connector
        .table_type(&profile, &req.config)
        .unwrap()
        .to_string();

    if let Some(schema) = &schema {
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

    transaction.commit().await.map_err(log_and_map)?;

    let table = api_queries::get_connection_table()
        .bind(&client, &auth_data.organization_id, &pub_id)
        .one()
        .await
        .map_err(log_and_map)?
        .try_into()
        .map_err(log_and_map)?;

    Ok(Json(table))
}

impl TryInto<ConnectionTable> for DbConnectionTable {
    type Error = String;
    fn try_into(self) -> Result<ConnectionTable, Self::Error> {
        let Some(connector) = connector_for_type(&self.connector) else {
            return Err(format!(
                "invalid connector {} in saved ConnectionTable {}",
                self.connector, self.id
            ));
        };

        let profile = get_connection_profile(&self, &*connector).map_err(|e| e.to_string())?;

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
            &profile
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
            connection_profile: profile,
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
pub(crate) async fn expand_schema(
    name: &str,
    connector: &str,
    schema: ConnectionSchema,
    table_config: &Value,
    profile_config: &Value,
) -> Result<ConnectionSchema, ErrorResp> {
    let Some(format) = schema.format.as_ref() else {
        return Ok(schema);
    };

    match format {
        Format::Json(_) => {
            expand_json_schema(name, connector, schema, table_config, profile_config).await
        }
        Format::Avro(_) => {
            expand_avro_schema(name, connector, schema, table_config, profile_config).await
        }
        Format::Parquet(_) => Ok(schema),
        Format::RawString(_) => Ok(schema),
    }
}

async fn expand_avro_schema(
    name: &str,
    connector: &str,
    mut schema: ConnectionSchema,
    table_config: &Value,
    profile_config: &Value,
) -> Result<ConnectionSchema, ErrorResp> {
    if let Some(Format::Avro(AvroFormat {
        confluent_schema_registry: true,
        ..
    })) = &schema.format
    {
        let schema_response = get_schema(connector, table_config, profile_config).await?;

        if schema_response.schema_type != ConfluentSchemaType::Avro {
            return Err(bad_request(format!(
                "Format configured is avro, but confluent schema repository returned a {:?} schema",
                schema_response.schema_type
            )));
        }

        schema.definition = Some(SchemaDefinition::AvroSchema(schema_response.schema));
    }

    let Some(SchemaDefinition::AvroSchema(definition)) = schema.definition.as_ref() else {
        return Err(bad_request("avro format requires an avro schema be set"));
    };

    if let Some(Format::Avro(format)) = &mut schema.format {
        format.add_reader_schema(
            apache_avro::Schema::parse_str(&definition)
                .map_err(|e| bad_request(format!("Avro schema is invalid: {:?}", e)))?,
        );
    }

    let fields: Result<_, String> = avro::convert_avro_schema(&name, &definition)
        .map_err(|e| bad_request(format!("Invalid avro schema: {}", e)))?
        .into_iter()
        .map(|f| f.try_into())
        .collect();

    schema.fields = fields.map_err(|e| bad_request(format!("Failed to convert schema: {}", e)))?;

    Ok(schema)
}

async fn expand_json_schema(
    name: &str,
    connector: &str,
    mut schema: ConnectionSchema,
    table_config: &Value,
    profile_config: &Value,
) -> Result<ConnectionSchema, ErrorResp> {
    if let Some(Format::Json(JsonFormat {
        confluent_schema_registry: true,
        ..
    })) = &schema.format
    {
        let schema_response = get_schema(connector, table_config, profile_config).await?;

        if schema_response.schema_type != ConfluentSchemaType::Json {
            return Err(bad_request(format!(
                "Format configured is json, but confluent schema repository returned a {:?} schema",
                schema_response.schema_type
            )));
        }

        schema.definition = Some(SchemaDefinition::JsonSchema(schema_response.schema));
    }

    if let Some(d) = &schema.definition {
        let fields = match d {
            SchemaDefinition::JsonSchema(json) => convert_json_schema(&name, &json)
                .map_err(|e| bad_request(format!("Invalid json-schema: {}", e)))?,
            SchemaDefinition::RawSchema(_) => vec![StructField::new(
                "value".to_string(),
                None,
                TypeDef::DataType(DataType::Utf8, false),
            )],
            _ => return Err(bad_request("Invalid schema type for json format")),
        };

        let fields: Result<_, String> = fields.into_iter().map(|f| f.try_into()).collect();

        schema.fields =
            fields.map_err(|e| bad_request(format!("Failed to convert schema: {}", e)))?;
    }

    Ok(schema)
}

async fn get_schema(
    connector: &str,
    table_config: &Value,
    profile_config: &Value,
) -> Result<ConfluentSchemaResponse, ErrorResp> {
    if connector != "kafka" {
        return Err(bad_request(
            "confluent schema registry can only be used for Kafka connections",
        ));
    }

    // we unwrap here because this should already have been validated
    let profile: KafkaConfig =
        serde_json::from_value(profile_config.clone()).expect("invalid kafka config");

    let table: KafkaTable =
        serde_json::from_value(table_config.clone()).expect("invalid kafka table");

    let schema_registry = profile.schema_registry.as_ref().ok_or_else(|| {
        bad_request("schema registry must be configured on the Kafka connection profile")
    })?;

    let resolver =
        ConfluentSchemaResolver::new(&schema_registry.endpoint, &table.topic).map_err(|e| {
            bad_request(format!(
                "failed to fetch schemas from schema repository: {}",
                e
            ))
        })?;

    resolver.get_schema(None).await.map_err(|e| {
        bad_request(format!(
            "failed to fetch schemas from schema repository: {}",
            e
        ))
    })
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
    let Some(schema_def) = req.definition else {
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
