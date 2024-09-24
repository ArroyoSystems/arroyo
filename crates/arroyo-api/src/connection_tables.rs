use anyhow::anyhow;
use axum::extract::{Path, Query, State};
use axum::response::sse::Event;
use axum::response::Sse;
use axum::Json;
use axum_extra::extract::WithRejection;
use futures_util::stream::Stream;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::debug;

use arroyo_connectors::confluent::ConfluentProfile;
use arroyo_connectors::connector_for_type;
use arroyo_connectors::kafka::{KafkaConfig, KafkaTable, SchemaRegistry};
use arroyo_formats::{avro, json, proto};
use arroyo_operator::connector::ErasedConnector;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionTable, ConnectionTablePost, ConnectionType,
    SchemaDefinition, SourceField,
};
use arroyo_rpc::api_types::{ConnectionTableCollection, PaginationQueryParams};
use arroyo_rpc::formats::{AvroFormat, Format, JsonFormat, ProtobufFormat};
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_rpc::schema_resolver::{
    ConfluentSchemaRegistry, ConfluentSchemaSubjectResponse, ConfluentSchemaType,
};
use arroyo_types::raw_schema;

use crate::rest::AppState;
use crate::rest_utils::{
    authenticate, bad_request, internal_server_error, log_and_map, map_delete_err, map_insert_err,
    not_found, paginate_results, required_field, validate_pagination_params, ApiError, BearerAuth,
    ErrorResp,
};
use crate::{
    queries::api_queries::{self, DbConnectionTable},
    to_micros, AuthData,
};
use arroyo_formats::proto::schema::{protobuf_to_arrow, schema_file_to_descriptor};
use cornucopia_async::{Database, DatabaseSource};

async fn get_and_validate_connector(
    req: &ConnectionTablePost,
    auth: &AuthData,
    db: &DatabaseSource,
) -> Result<
    (
        Box<dyn ErasedConnector>,
        Option<i64>,
        serde_json::Value,
        Option<ConnectionSchema>,
    ),
    ErrorResp,
> {
    let client = db.client().await?;
    let connector = connector_for_type(&req.connector)
        .ok_or_else(|| anyhow!("Unknown connector '{}'", req.connector,))
        .map_err(log_and_map)?;

    let (connection_profile_id, profile_config) = if let Some(connection_profile_id) =
        &req.connection_profile_id
    {
        let connection_profile = api_queries::fetch_get_connection_profile_by_pub_id(
            &client,
            &auth.organization_id,
            &connection_profile_id,
        )
        .await?
        .into_iter()
        .next()
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

    let schema: Option<ConnectionSchema> = req.schema.clone();

    let schema = if let Some(schema) = schema {
        let name = connector.name();
        Some(
            expand_schema(
                &req.name,
                name,
                connector.table_type(&profile_config, &req.config).unwrap(),
                schema,
                &profile_config,
                &req.config,
            )
            .await?
            .validate()
            .map_err(|e| bad_request(format!("Invalid schema: {}", e)))?,
        )
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
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    let deleted = api_queries::execute_delete_connection_table(
        &state.database.client().await?,
        &auth_data.organization_id,
        &pub_id,
    )
    .await
    .map_err(|e| map_delete_err("connection_table", "pipelines", e))?;

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
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    let (connector, _, profile, schema) =
        get_and_validate_connector(&req, &auth_data, &state.database).await?;

    let (tx, rx) = channel(8);

    connector
        .test(&req.name, &profile, &req.config, schema.as_ref(), tx)
        .map_err(|e| bad_request(format!("Failed to parse config or schema: {:?}", e)))?;

    let stream = ReceiverStream::new(rx);

    Ok(Sse::new(
        stream.map(|msg| Ok(Event::default().json_data(msg).unwrap())),
    ))
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

pub(crate) async fn get_all_connection_tables(
    auth: &AuthData,
    db: &Database<'_>,
) -> Result<Vec<ConnectionTable>, ErrorResp> {
    let tables = api_queries::fetch_get_all_connection_tables(db, &auth.organization_id)
        .await
        .map_err(log_and_map)?;

    let vec: Vec<ConnectionTable> = tables
        .into_iter()
        .map(|t| t.try_into())
        .inspect(|result| {
            if let Err(err) = result {
                debug!("Error building connection table: {}", err);
            }
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
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    // let transaction = client.transaction().await.map_err(log_and_map)?;
    // transaction
    //     .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
    //     .await
    //     .map_err(log_and_map)?;

    let (connector, connection_id, profile, schema) =
        get_and_validate_connector(&req, &auth_data, &state.database).await?;

    let table_type = connector.table_type(&profile, &req.config).unwrap();

    if let Some(schema) = &schema {
        if schema.definition.is_none() && schema.inferred != Some(true) {
            return Err(required_field("schema.definition"));
        }
    }

    let schema: Option<serde_json::Value> = schema.map(|s| serde_json::to_value(s).unwrap());

    let pub_id = generate_id(IdTypes::ConnectionTable);

    let client = state.database.client().await?;

    api_queries::execute_create_connection_table(
        &client,
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
    .map_err(|err| map_insert_err("connection_table", err))?;

    // transaction.commit().await.map_err(log_and_map)?;

    let table =
        api_queries::fetch_get_connection_table(&client, &auth_data.organization_id, &pub_id)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| internal_server_error("Could not create connection table"))?
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

        let schema = self.schema.map(serde_json::from_value);
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
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    let (starting_after, limit) =
        validate_pagination_params(query_params.starting_after.clone(), query_params.limit)?;

    let tables = api_queries::fetch_get_connection_tables(
        &state.database.client().await?,
        &auth_data.organization_id,
        &starting_after.unwrap_or("".to_string()),
        &(limit as i32), // is 1 more than the requested limit
    )
    .await?;

    let (tables, has_more) = paginate_results(tables, limit);

    let tables: Vec<ConnectionTable> = tables
        .into_iter()
        .map(|t| t.try_into())
        .inspect(|result| {
            if let Err(err) = result {
                debug!("Error building connection table: {}", err);
            }
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
    connection_type: ConnectionType,
    schema: ConnectionSchema,
    profile_config: &Value,
    table_config: &Value,
) -> Result<ConnectionSchema, ErrorResp> {
    let Some(format) = schema.format.as_ref() else {
        return Ok(schema);
    };

    match format {
        Format::Json(_) => {
            expand_json_schema(
                name,
                connector,
                connection_type,
                schema,
                profile_config,
                table_config,
            )
            .await
        }
        Format::Avro(_) => {
            expand_avro_schema(
                connector,
                connection_type,
                schema,
                profile_config,
                table_config,
            )
            .await
        }
        Format::Parquet(_) => Ok(schema),
        Format::RawString(_) => Ok(schema),
        Format::RawBytes(_) => Ok(schema),
        Format::Protobuf(_) => {
            expand_proto_schema(
                connector,
                connection_type,
                schema,
                profile_config,
                table_config,
            )
            .await
        }
    }
}

async fn expand_avro_schema(
    connector: &str,
    connection_type: ConnectionType,
    mut schema: ConnectionSchema,
    profile_config: &Value,
    table_config: &Value,
) -> Result<ConnectionSchema, ErrorResp> {
    if let Some(Format::Avro(AvroFormat {
        confluent_schema_registry: true,
        ..
    })) = &mut schema.format
    {
        let schema_response = get_schema(connector, table_config, profile_config).await?;
        match connection_type {
            ConnectionType::Source => {
                let (schema_response, _) = schema_response.ok_or_else(|| bad_request(
                        "No schema was found; ensure that the topic exists and has a value schema configured in the schema registry".to_string()))?;

                if schema_response.schema_type != ConfluentSchemaType::Avro {
                    return Err(bad_request(format!(
                        "Format configured is avro, but confluent schema repository returned a {:?} schema",
                        schema_response.schema_type
                    )));
                }

                schema.definition = Some(SchemaDefinition::AvroSchema(schema_response.schema));
            }
            ConnectionType::Sink => {
                // don't fetch schemas for sinks for now
            }
        }
    }

    let Some(SchemaDefinition::AvroSchema(definition)) = schema.definition.as_ref() else {
        return match connection_type {
            ConnectionType::Source => Err(bad_request(
                "avro format requires an avro schema be set for sources",
            )),
            ConnectionType::Sink => {
                schema.inferred = Some(true);
                Ok(schema)
            }
        };
    };

    if let Some(Format::Avro(format)) = &mut schema.format {
        format.add_reader_schema(
            apache_avro::Schema::parse_str(definition)
                .map_err(|e| bad_request(format!("Avro schema is invalid: {:?}", e)))?,
        );
    }

    let fields: Result<_, String> = avro::schema::to_arrow(definition)
        .map_err(|e| bad_request(format!("Invalid avro schema: {}", e)))?
        .fields
        .into_iter()
        .map(|f| (**f).clone().try_into())
        .collect();

    schema.fields = fields.map_err(|e| bad_request(format!("Failed to convert schema: {}", e)))?;

    Ok(schema)
}

async fn expand_proto_schema(
    connector: &str,
    connection_type: ConnectionType,
    mut schema: ConnectionSchema,
    profile_config: &Value,
    table_config: &Value,
) -> Result<ConnectionSchema, ErrorResp> {
    let Some(Format::Protobuf(ProtobufFormat {
        message_name,
        compiled_schema,
        confluent_schema_registry,
        ..
    })) = &mut schema.format
    else {
        panic!("not proto");
    };

    if *confluent_schema_registry {
        let schema_response = get_schema(connector, table_config, profile_config).await?;
        match connection_type {
            ConnectionType::Source => {
                let (schema_response, dependencies) = schema_response.ok_or_else(|| bad_request(
                    "No schema was found; ensure that the topic exists and has a value schema configured in the schema registry".to_string()))?;

                if schema_response.schema_type != ConfluentSchemaType::Protobuf {
                    return Err(bad_request(format!(
                        "Format configured is protobuf, but confluent schema repository returned a {:?} schema",
                        schema_response.schema_type
                    )));
                }

                let dependencies: Result<HashMap<_, _>, ErrorResp> = dependencies
                    .into_iter()
                    .map(|(name, s)| {
                        if s.schema_type != ConfluentSchemaType::Protobuf {
                            Err(bad_request(format!(
                                "Schema reference {} has type {:?}, but must be protobuf",
                                name, s.schema_type
                            )))
                        } else {
                            Ok((name, s.schema))
                        }
                    })
                    .collect();

                schema.definition = Some(SchemaDefinition::ProtobufSchema {
                    schema: schema_response.schema,
                    dependencies: dependencies?,
                });
            }
            ConnectionType::Sink => {
                // don't fetch schemas for sinks for now
            }
        }
    }

    let Some(definition) = &schema.definition else {
        return Err(bad_request("No definition for protobuf schema"));
    };

    let SchemaDefinition::ProtobufSchema {
        schema: protobuf_schema,
        dependencies,
    } = &definition
    else {
        return Err(bad_request("Schema is not a protobuf schema"));
    };

    let (compiled, fields) =
        expand_local_proto_schema(protobuf_schema, message_name, dependencies).await?;
    *compiled_schema = Some(compiled);
    schema.fields = fields;

    Ok(schema)
}

async fn expand_local_proto_schema(
    schema_def: &str,
    message_name: &Option<String>,
    dependencies: &HashMap<String, String>,
) -> Result<(Vec<u8>, Vec<SourceField>), ErrorResp> {
    let message_name = message_name
        .as_ref()
        .filter(|m| !m.is_empty())
        .ok_or_else(|| bad_request("message name must be provided for protobuf schemas"))?;

    let encoded = schema_file_to_descriptor(schema_def, dependencies)
        .await
        .map_err(|e| bad_request(e.to_string()))?;

    let pool = proto::schema::get_pool(&encoded)
        .map_err(|e| bad_request(format!("error handling protobuf: {}", e)))?;

    let descriptor = pool.get_message_by_name(message_name).ok_or_else(|| {
        bad_request(format!(
            "Message '{}' not found in proto definition; messages are {}",
            message_name,
            pool.all_messages()
                .map(|m| m.full_name().to_string())
                .filter(|m| !m.starts_with("google.protobuf."))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    })?;

    let arrow = protobuf_to_arrow(&descriptor)
        .map_err(|e| bad_request(format!("Failed to convert schema: {}", e)))?;

    let fields: Result<_, String> = arrow
        .fields
        .into_iter()
        .map(|f| (**f).clone().try_into())
        .collect();

    let fields = fields.map_err(|e| bad_request(format!("failed to convert schema: {}", e)))?;

    Ok((encoded, fields))
}

async fn expand_json_schema(
    name: &str,
    connector: &str,
    connection_type: ConnectionType,
    mut schema: ConnectionSchema,
    profile_config: &Value,
    table_config: &Value,
) -> Result<ConnectionSchema, ErrorResp> {
    if let Some(Format::Json(JsonFormat {
        confluent_schema_registry: true,
        schema_id: confluent_schema_id,
        ..
    })) = schema.format.as_mut()
    {
        let schema_response = get_schema(connector, table_config, profile_config).await?;

        match connection_type {
            ConnectionType::Source => {
                let schema_response = schema_response.ok_or_else(|| bad_request(
                    "No schema was found; ensure that the topic exists and has a value schema configured in the schema registry".to_string()))?;

                if schema_response.0.schema_type != ConfluentSchemaType::Json {
                    return Err(bad_request(format!(
                        "Format configured is json, but confluent schema repository returned a {:?} schema",
                        schema_response.0.schema_type
                    )));
                }
                confluent_schema_id.replace(schema_response.0.version);

                schema.definition = Some(SchemaDefinition::JsonSchema(schema_response.0.schema));
            }
            ConnectionType::Sink => {
                // don't fetch schemas for sinks for now until we're better able to conform our output to the schema
                schema.inferred = Some(true);
            }
        }
    }

    if let Some(d) = &schema.definition {
        let arrow = match d {
            SchemaDefinition::JsonSchema(json) => json::schema::to_arrow(name, json)
                .map_err(|e| bad_request(format!("Invalid json-schema: {}", e)))?,
            SchemaDefinition::RawSchema(_) => raw_schema(),
            _ => return Err(bad_request("Invalid schema type for json format")),
        };

        let fields: Result<_, String> = arrow
            .fields
            .into_iter()
            .map(|f| (**f).clone().try_into())
            .collect();

        schema.fields =
            fields.map_err(|e| bad_request(format!("Failed to convert schema: {}", e)))?;
    }

    Ok(schema)
}

async fn get_schema(
    connector: &str,
    table_config: &Value,
    profile_config: &Value,
) -> Result<
    Option<(
        ConfluentSchemaSubjectResponse,
        Vec<(String, ConfluentSchemaSubjectResponse)>,
    )>,
    ErrorResp,
> {
    let profile: KafkaConfig = match connector {
        "kafka" => {
            // we unwrap here because this should already have been validated
            serde_json::from_value(profile_config.clone()).expect("invalid kafka config")
        }
        "confluent" => {
            let c: ConfluentProfile =
                serde_json::from_value(profile_config.clone()).expect("invalid confluent config");
            c.into()
        }
        _ => {
            return Err(bad_request(
                "confluent schema registry can only be used for Kafka or Confluent connections",
            ));
        }
    };

    let table: KafkaTable =
        serde_json::from_value(table_config.clone()).expect("invalid kafka table");

    let Some(SchemaRegistry::ConfluentSchemaRegistry {
        endpoint,
        api_key,
        api_secret,
    }) = profile.schema_registry_enum
    else {
        return Err(bad_request(
            "schema registry must be configured on the Kafka connection profile",
        ));
    };

    let resolver = ConfluentSchemaRegistry::new(
        &endpoint,
        &table.subject(),
        api_key.clone(),
        api_secret.clone(),
    )
    .map_err(|e| {
        bad_request(format!(
            "failed to fetch schemas from schema registry: {}",
            e
        ))
    })?;

    let Some(resp) = resolver.get_schema_for_version(None).await.map_err(|e| {
        bad_request(format!(
            "failed to fetch schemas from schema registry: {}",
            e.chain()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(": ")
        ))
    })?
    else {
        return Ok(None);
    };

    let references = resolver
        .resolve_references(&resp.references)
        .await
        .map_err(|e| bad_request(e.to_string()))?;

    Ok(Some((resp, references)))
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
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    WithRejection(Json(req), _): WithRejection<Json<ConnectionSchema>, ApiError>,
) -> Result<(), ErrorResp> {
    let _ = authenticate(&state.database, bearer_auth).await?;
    let Some(schema_def) = &req.definition else {
        return Ok(());
    };

    match schema_def {
        SchemaDefinition::JsonSchema(schema) => {
            if let Err(e) = json::schema::to_arrow("test", schema) {
                Err(bad_request(e.to_string()))
            } else {
                Ok(())
            }
        }
        SchemaDefinition::ProtobufSchema {
            schema,
            dependencies,
        } => {
            let Some(Format::Protobuf(ProtobufFormat { message_name, .. })) = &req.format else {
                return Err(bad_request(
                    "Schema has a protobuf definition but is not protobuf format",
                ));
            };

            let _ = expand_local_proto_schema(schema, message_name, dependencies).await?;
            Ok(())
        }
        _ => {
            // TODO: add testing for other schema types
            Ok(())
        }
    }
}
