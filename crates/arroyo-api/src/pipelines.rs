use anyhow::{anyhow, bail, Context};
use arrow_schema::SchemaRef;
use arroyo_connectors::connector_for_type;
use axum::extract::{Path, Query, State};
use axum::Json;
use axum_extra::extract::WithRejection;
use cornucopia_async::{GenericClient, Params};
use deadpool_postgres::{Object, Transaction};
use http::StatusCode;

use petgraph::{Direction, EdgeDirection};
use std::collections::HashMap;
use std::env;

use petgraph::visit::NodeRef;
use std::time::Duration;

use crate::{compiler_service, connection_profiles, jobs, types};
use arroyo_datastream::preview_sink;
use arroyo_rpc::api_types::pipelines::{
    Job, Pipeline, PipelinePatch, PipelinePost, PipelineRestart, QueryValidationResult, StopType,
    ValidateQueryPost,
};
use arroyo_rpc::api_types::udfs::{GlobalUdf, Udf};
use arroyo_rpc::api_types::{JobCollection, PaginationQueryParams, PipelineCollection};
use arroyo_rpc::grpc::api::{ArrowProgram, ConnectorOp};

use arroyo_connectors::kafka::{KafkaConfig, KafkaTable, SchemaRegistry};
use arroyo_datastream::logical::{LogicalProgram, OperatorName};
use arroyo_df::{has_duplicate_udf_names, ArroyoSchemaProvider, CompiledSql, SqlConfig};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_rpc::formats::Format;
use arroyo_rpc::grpc::compiler_grpc_client::CompilerGrpcClient;
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_rpc::schema_resolver::{ConfluentSchemaRegistry, ConfluentSchemaType};
use arroyo_rpc::{error_chain, OperatorConfig};
use arroyo_server_common::log_event;
use arroyo_udf_host::ParsedUdfFile;
use prost::Message;
use serde_json::json;
use time::OffsetDateTime;
use tracing::warn;

use crate::jobs::get_action;
use crate::queries::api_queries;
use crate::queries::api_queries::{DbPipeline, DbPipelineJob, GetPipelinesParams};
use crate::rest::AppState;
use crate::rest_utils::{
    authenticate, bad_request, client, log_and_map, not_found, paginate_results, required_field,
    validate_pagination_params, ApiError, BearerAuth, ErrorResp,
};
use crate::types::public::{PipelineType, RestartMode, StopMode};
use crate::udfs::build_udf;
use crate::{connection_tables, to_micros};
use crate::{handle_db_error, AuthData};

const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(10);

async fn compile_sql<'e, E>(
    query: String,
    local_udfs: &Vec<Udf>,
    parallelism: usize,
    auth_data: &AuthData,
    validate_only: bool,
    tx: &E,
) -> anyhow::Result<CompiledSql>
where
    E: GenericClient,
{
    let mut schema_provider = ArroyoSchemaProvider::new();

    let global_udfs = api_queries::get_udfs()
        .bind(tx, &auth_data.organization_id)
        .all()
        .await
        .map_err(|e| anyhow!("Error global global UDFs: {}", e))?
        .into_iter()
        .map(|u| u.into())
        .collect::<Vec<GlobalUdf>>();

    // error if there are duplicate local or duplicate global UDF names,
    // but allow  global UDFs to override local ones

    if has_duplicate_udf_names(global_udfs.iter().map(|u| &u.definition)) {
        bail!("Global UDFs have duplicate function names");
    }

    if has_duplicate_udf_names(local_udfs.iter().map(|u| &u.definition)) {
        bail!("Local UDFs have duplicate function names");
    }

    for udf in global_udfs {
        if let Err(e) = schema_provider.add_rust_udf(&udf.definition, &udf.dylib_url) {
            warn!("Invalid global UDF {}: {}", udf.name, e);
        }
    }

    if !local_udfs.is_empty() {
        let mut compiler_service: CompilerGrpcClient<_> = compiler_service()
            .await
            .map_err(|e| anyhow!("{}", e.message))?;

        for udf in local_udfs {
            let parsed = ParsedUdfFile::try_parse(&udf.definition)
                .map_err(|e| anyhow!("invalid UDF: {e}"))?;

            let url = if !validate_only {
                let res = build_udf(&mut compiler_service, &udf.definition, true)
                    .await
                    .map_err(|e| anyhow!("Failed to reach compiler service: {}", e.message))?;

                if !res.errors.is_empty() {
                    bail!("Failed to build UDF: {}", res.errors.join("\n"));
                }

                res.url.expect("valid UDF does not have a URL in response")
            } else {
                "".to_string()
            };

            schema_provider
                .add_rust_udf(&parsed.definition, &url)
                .map_err(|e| anyhow!("Invalid UDF {}: {}", parsed.udf.name, e))?;
        }
    }

    let tables = connection_tables::get_all_connection_tables(auth_data, tx)
        .await
        .map_err(|e| anyhow!(e.message))?;

    for table in tables {
        let Some(connector) = connector_for_type(&table.connector) else {
            warn!(
                "Saved table found with unknown connector {}",
                table.connector
            );
            continue;
        };

        let connection = connector.from_config(
            Some(table.id),
            &table.name,
            &table
                .connection_profile
                .map(|c| c.config.clone())
                .unwrap_or(json!({})),
            &table.config,
            Some(&table.schema),
        )?;

        schema_provider.add_connector_table(connection);
    }
    let profiles = connection_profiles::get_all_connection_profiles(auth_data, tx)
        .await
        .map_err(|e| anyhow!(e.message))?;

    for profile in profiles {
        schema_provider.add_connection_profile(profile);
    }

    arroyo_df::parse_and_get_program(
        &query,
        schema_provider,
        SqlConfig {
            default_parallelism: parallelism,
        },
    )
    .await
    .with_context(|| "failed to generate SQL program")
    .map_err(|err| {
        warn!("{:?}", err);
        anyhow!(format!("{}", err.root_cause()))
    })
}

fn set_parallelism(program: &mut LogicalProgram, parallelism: usize) {
    for node in program.graph.node_weights_mut() {
        node.parallelism = parallelism;
    }
}

#[allow(unused)]
async fn try_register_confluent_schema(
    sink: &mut ConnectorOp,
    schema: &SchemaRef,
) -> anyhow::Result<()> {
    let mut config: OperatorConfig = serde_json::from_str(&sink.config).unwrap();

    let Ok(profile) = serde_json::from_value::<KafkaConfig>(config.connection.clone()) else {
        return Ok(());
    };

    let Ok(table) = serde_json::from_value::<KafkaTable>(config.table.clone()) else {
        return Ok(());
    };

    let Some(SchemaRegistry::ConfluentSchemaRegistry {
        endpoint,
        api_key,
        api_secret,
    }) = profile.schema_registry_enum
    else {
        return Ok(());
    };

    let schema_registry =
        ConfluentSchemaRegistry::new(&endpoint, &table.subject(), api_key, api_secret)?;

    match config.format.clone() {
        Some(Format::Avro(mut avro)) => {
            if avro.confluent_schema_registry && avro.schema_id.is_none() {
                let avro_schema = ArrowSerializer::avro_schema(schema);

                let id = schema_registry
                    .write_schema(avro_schema.canonical_form(), ConfluentSchemaType::Avro)
                    .await?;

                avro.schema_id = Some(id as u32);
                config.format = Some(Format::Avro(avro))
            }
        }
        Some(Format::Json(mut json)) => {
            if json.confluent_schema_registry && json.schema_id.is_none() {
                let json_schema = ArrowSerializer::json_schema(schema);

                let id = schema_registry
                    .write_schema(json_schema.to_string(), ConfluentSchemaType::Json)
                    .await?;

                json.schema_id = Some(id as u32);
                config.format = Some(Format::Json(json))
            }
        }
        _ => {
            // unsupported for schema registry
        }
    }

    sink.config = serde_json::to_string(&config).unwrap();

    Ok(())
}

async fn register_schemas(compiled_sql: &mut CompiledSql) -> anyhow::Result<()> {
    // register schemas for sinks
    for idx in compiled_sql
        .program
        .graph
        .externals(Direction::Outgoing)
        .collect::<Vec<_>>()
    {
        let edge = compiled_sql
            .program
            .graph
            .edges_directed(idx, EdgeDirection::Incoming)
            .next()
            .ok_or_else(|| anyhow!("no incoming edges for sink node: {:?}", idx.weight()))?;

        let schema = edge.weight().schema.schema.clone();

        let node = compiled_sql.program.graph.node_weight_mut(idx).unwrap();
        if node.operator_name == OperatorName::ConnectorSink {
            let mut op = ConnectorOp::decode(&node.operator_config[..]).map_err(|_| {
                anyhow!(
                    "failed to decode configuration for connector node {:?}",
                    node
                )
            })?;

            try_register_confluent_schema(&mut op, &schema).await?;

            node.operator_config = op.encode_to_vec();
        }
    }

    Ok(())
}

pub(crate) async fn create_pipeline_int<'a>(
    req: &PipelinePost,
    pub_id: &str,
    auth: AuthData,
    tx: &Transaction<'a>,
) -> Result<(i64, LogicalProgram), ErrorResp> {
    let is_preview = req.preview.unwrap_or(false);

    if req.parallelism > auth.org_metadata.max_parallelism as u64 {
        return Err(bad_request(format!(
            "Your plan allows you to run pipelines up to parallelism {};
            contact support@arroyo.systems for an increase",
            auth.org_metadata.max_parallelism
        )));
    }

    let mut compiled = compile_sql(
        req.query.clone(),
        req.udfs.as_ref().unwrap_or(&vec![]),
        req.parallelism as usize,
        &auth,
        false,
        tx,
    )
    .await
    .map_err(|e| bad_request(e.to_string()))?;

    if compiled.program.graph.node_count() > auth.org_metadata.max_operators as usize {
        return Err(bad_request(
            format!("This pipeline is too large to create under your plan, which only allows pipelines up to {} nodes;
                contact support@arroyo.systems for an increase", auth.org_metadata.max_operators)));
    }

    set_parallelism(&mut compiled.program, 1);

    if is_preview && !env::var("PREVIEW_SINKS").is_ok_and(|s| s == "true") {
        for node in compiled.program.graph.node_weights_mut() {
            // replace all sink connectors with websink for preview
            if node.operator_name == OperatorName::ConnectorSink {
                node.operator_config = preview_sink().encode_to_vec();
            }
        }
    }

    register_schemas(&mut compiled)
        .await
        .map_err(|e| ErrorResp {
            status_code: StatusCode::BAD_REQUEST,
            message: format!(
                "Failed to register schemas with the schema registry. Make sure \
            that the schema_registry is configured correctly and running.\nDetails: {}",
                error_chain(e)
            ),
        })?;

    let proto_program: ArrowProgram = compiled.program.clone().into();

    let program_bytes = proto_program.encode_to_vec();

    if req.name.is_empty() {
        return Err(required_field("name"));
    }

    let udfs = serde_json::to_value(req.udfs.as_ref().unwrap_or(&vec![])).unwrap();

    let pipeline_id = api_queries::create_pipeline()
        .bind(
            tx,
            &pub_id,
            &auth.organization_id,
            &auth.user_id,
            &req.name,
            &PipelineType::sql,
            &Some(req.query.clone()),
            &udfs,
            &program_bytes,
            &2,
        )
        .one()
        .await
        .map_err(|e| handle_db_error("pipeline", e))?;

    if !is_preview {
        for connection in compiled.connection_ids {
            api_queries::add_pipeline_connection_table()
                .bind(
                    tx,
                    &generate_id(IdTypes::ConnectionTablePipeline),
                    &pipeline_id,
                    &connection,
                )
                .await
                .map_err(log_and_map)?;
        }
    }

    Ok((pipeline_id, compiled.program))
}

impl TryInto<Pipeline> for DbPipeline {
    type Error = ErrorResp;

    fn try_into(self) -> Result<Pipeline, ErrorResp> {
        let running_desired = self.stop == StopMode::none;
        let state = self.state.unwrap_or_else(|| "Created".to_string());
        let (action_text, action, action_in_progress) = get_action(&state, &running_desired);

        let mut program: LogicalProgram = ArrowProgram::decode(&self.program[..])
            .map_err(log_and_map)?
            .try_into()
            .map_err(log_and_map)?;

        program.update_parallelism(
            &self
                .parallelism_overrides
                .as_object()
                .unwrap()
                .into_iter()
                .map(|(k, v)| (k.clone(), v.as_u64().unwrap() as usize))
                .collect(),
        );

        let stop = match self.stop {
            StopMode::none => StopType::None,
            StopMode::checkpoint => StopType::Checkpoint,
            StopMode::graceful => StopType::Graceful,
            StopMode::immediate => StopType::Immediate,
            StopMode::force => StopType::Force,
        };

        Ok(Pipeline {
            id: self.pub_id,
            name: self.name,
            query: self.textual_repr,
            udfs: serde_json::from_value(self.udfs).map_err(log_and_map)?,
            checkpoint_interval_micros: self.checkpoint_interval_micros as u64,
            stop,
            created_at: to_micros(self.created_at),
            graph: program.try_into().map_err(log_and_map)?,
            action,
            action_text,
            action_in_progress,
            preview: self.ttl_micros.is_some(),
        })
    }
}

impl From<DbPipelineJob> for Job {
    fn from(val: DbPipelineJob) -> Self {
        Job {
            id: val.id,
            running_desired: val.stop == StopMode::none,
            state: val.state.unwrap_or_else(|| "Created".to_string()),
            run_id: val.run_id.unwrap_or(0) as u64,
            start_time: val.start_time.map(to_micros),
            finish_time: val.finish_time.map(to_micros),
            tasks: val.tasks.map(|t| t as u64),
            failure_message: val.failure_message,
            created_at: to_micros(val.created_at),
        }
    }
}

/// Validate a query and return pipeline graph
#[utoipa::path(
    post,
    path = "/v1/pipelines/validate_query",
    tag = "pipelines",
    request_body = ValidateQueryPost,
    responses(
        (status = 200, description = "Validated query", body = QueryValidationResult),
    ),
)]
pub async fn validate_query(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    WithRejection(Json(validate_query_post), _): WithRejection<Json<ValidateQueryPost>, ApiError>,
) -> Result<Json<QueryValidationResult>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let udfs = validate_query_post.udfs.unwrap_or(vec![]);

    let pipeline_graph_validation_result = match compile_sql(
        validate_query_post.query,
        &udfs,
        1,
        &auth_data,
        true,
        &client,
    )
    .await
    {
        Ok(CompiledSql { program, .. }) => QueryValidationResult {
            graph: Some(program.try_into().map_err(log_and_map)?),
            errors: vec![],
        },
        Err(e) => QueryValidationResult {
            graph: None,
            errors: vec![e.to_string()],
        },
    };

    Ok(Json(pipeline_graph_validation_result))
}

/// Create a new pipeline
///
/// The API will create a single job for the pipeline.
#[utoipa::path(
    post,
    path = "/v1/pipelines",
    tag = "pipelines",
    request_body = PipelinePost,
    responses(
        (status = 200, description = "Created pipeline and job", body = Pipeline),
        (status = 400, description = "Bad request", body = ErrorResp),
    ),
)]
pub async fn create_pipeline(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    WithRejection(Json(pipeline_post), _): WithRejection<Json<PipelinePost>, ApiError>,
) -> Result<Json<Pipeline>, ErrorResp> {
    let mut client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let pipeline_pub_id = generate_id(IdTypes::Pipeline);

    let transaction = client.transaction().await.map_err(log_and_map)?;

    let (pipeline_id, program) = create_pipeline_int(
        &pipeline_post,
        &pipeline_pub_id,
        auth_data.clone(),
        &transaction,
    )
    .await?;

    let preview = pipeline_post.preview.unwrap_or(false);

    let checkpoint_interval = pipeline_post
        .checkpoint_interval_micros
        .map(Duration::from_micros)
        .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL);

    let job_id = jobs::create_job(
        &pipeline_post.name,
        pipeline_id,
        checkpoint_interval,
        preview,
        &auth_data,
        &transaction,
    )
    .await?;

    transaction.commit().await.map_err(log_and_map)?;

    log_event(
        "job_created",
        json!({
            "service": "api",
            "is_preview": preview,
            "job_id": job_id,
            "parallelism": pipeline_post.parallelism,
            "has_udfs": pipeline_post.udfs.map(|e| !e.is_empty() && !e[0].definition.trim().is_empty())
              .unwrap_or(false),
            // TODO: program features
            "features": program.features(),
        }),
    );

    let pipeline = query_pipeline_by_pub_id(&pipeline_pub_id, &client, &auth_data).await?;

    Ok(Json(pipeline))
}

/// Update a pipeline
#[utoipa::path(
    patch,
    path = "/v1/pipelines/{id}",
    tag = "pipelines",
    params(
        ("id" = String, Path, description = "Pipeline id")
    ),
    request_body = PipelinePatch,
    responses(
        (status = 200, description = "Updated pipeline", body = Pipeline),
    ),
)]
pub async fn patch_pipeline(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path(pipeline_pub_id): Path<String>,
    WithRejection(Json(pipeline_patch), _): WithRejection<Json<PipelinePatch>, ApiError>,
) -> Result<Json<Pipeline>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    // this assumes there is just one job for the pipeline
    let job_id = api_queries::get_pipeline_jobs()
        .bind(&client, &auth_data.organization_id, &pipeline_pub_id)
        .one()
        .await
        .map_err(log_and_map)?
        .id;

    let interval = pipeline_patch
        .checkpoint_interval_micros
        .map(Duration::from_micros);

    let stop = &pipeline_patch.stop.map(|s| match s {
        StopType::None => types::public::StopMode::none,
        StopType::Graceful => types::public::StopMode::graceful,
        StopType::Immediate => types::public::StopMode::immediate,
        StopType::Checkpoint => types::public::StopMode::checkpoint,
        StopType::Force => types::public::StopMode::force,
    });

    if let Some(interval) = interval {
        if interval < Duration::from_secs(1) || interval > Duration::from_secs(24 * 60 * 60) {
            return Err(bad_request(
                "checkpoint_interval_micros must be between 1 second and 1 day".to_string(),
            ));
        }
    }

    let parallelism_overrides = if let Some(parallelism) = pipeline_patch.parallelism {
        let res = api_queries::get_job_details()
            .bind(&client, &auth_data.organization_id, &job_id)
            .opt()
            .await
            .map_err(log_and_map)?
            .ok_or_else(|| not_found("Job"))?;

        let program = ArrowProgram::decode(&res.program[..]).map_err(log_and_map)?;
        let map: HashMap<String, u32> = program
            .nodes
            .into_iter()
            .map(|node| (node.node_id, parallelism as u32))
            .collect();

        Some(serde_json::to_value(map).map_err(log_and_map)?)
    } else {
        None
    };

    let res = api_queries::update_job()
        .bind(
            &client,
            &OffsetDateTime::now_utc(),
            &auth_data.user_id,
            stop,
            &interval.map(|i| i.as_micros() as i64),
            &parallelism_overrides,
            &job_id,
            &auth_data.organization_id,
        )
        .await
        .map_err(log_and_map)?;

    if res == 0 {
        return Err(not_found("Job"));
    }

    let pipeline = query_pipeline_by_pub_id(&pipeline_pub_id, &client, &auth_data).await?;
    Ok(Json(pipeline))
}

/// Restart a pipeline
#[utoipa::path(
    post,
    path = "/v1/pipelines/{id}/restart",
    tag = "pipelines",
    params(
        ("id" = String, Path, description = "Pipeline id")
    ),
    request_body = PipelineRestart,
    responses(
      (status = 200, description = "Updated pipeline", body = Pipeline)),
)]
pub async fn restart_pipeline(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path(id): Path<String>,
    WithRejection(Json(req), _): WithRejection<Json<PipelineRestart>, ApiError>,
) -> Result<Json<Pipeline>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let job_id = api_queries::get_pipeline_jobs()
        .bind(&client, &auth_data.organization_id, &id)
        .one()
        .await
        .map_err(log_and_map)?
        .id;

    let mode = if req.force == Some(true) {
        RestartMode::force
    } else {
        RestartMode::safe
    };

    let res = api_queries::restart_job()
        .bind(
            &client,
            &OffsetDateTime::now_utc(),
            &auth_data.user_id,
            &mode,
            &job_id,
            &auth_data.organization_id,
        )
        .await
        .map_err(log_and_map)?;

    if res == 0 {
        return Err(not_found("Pipeline"));
    }

    let pipeline = query_pipeline_by_pub_id(&id, &client, &auth_data).await?;
    Ok(Json(pipeline))
}

/// List all pipelines
#[utoipa::path(
    get,
    path = "/v1/pipelines",
    tag = "pipelines",
    params(
        PaginationQueryParams
    ),
    responses(
        (status = 200, description = "Got pipelines collection", body = PipelineCollection),
    ),
)]
pub async fn get_pipelines(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    query_params: Query<PaginationQueryParams>,
) -> Result<Json<PipelineCollection>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let (starting_after, limit) =
        validate_pagination_params(query_params.starting_after.clone(), query_params.limit)?;

    let pipelines: Vec<DbPipeline> = api_queries::get_pipelines()
        .params(
            &client,
            &GetPipelinesParams {
                organization_id: &auth_data.organization_id,
                starting_after: starting_after.unwrap_or_default(),
                limit: limit as i32, // is 1 more than the requested limit
            },
        )
        .all()
        .await
        .map_err(log_and_map)?;

    let (pipelines, has_more) = paginate_results(pipelines, limit);

    Ok(Json(PipelineCollection {
        has_more,
        data: pipelines
            .into_iter()
            .filter_map(|p| {
                let id = p.pub_id.clone();
                match p.try_into() {
                    Ok(p) => Some(p),
                    Err(e) => {
                        warn!("Failed to map pipeline {} from database: {:?}", id, e);
                        None
                    }
                }
            })
            .collect(),
    }))
}

/// Get a single pipeline
#[utoipa::path(
    get,
    path = "/v1/pipelines/{id}",
    tag = "pipelines",
    params(
        ("id" = String, Path, description = "Pipeline id")
    ),
    responses(
        (status = 200, description = "Got pipeline", body = Pipeline),
    ),
)]
pub async fn get_pipeline(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path(pipeline_pub_id): Path<String>,
) -> Result<Json<Pipeline>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let pipeline = query_pipeline_by_pub_id(&pipeline_pub_id, &client, &auth_data).await?;
    Ok(Json(pipeline))
}

/// Delete a pipeline
#[utoipa::path(
    delete,
    path = "/v1/pipelines/{id}",
    tag = "pipelines",
    params(
        ("id" = String, Path, description = "Pipeline id")
    ),
    responses(
        (status = 200, description = "Deleted pipeline"),
    ),
)]
pub async fn delete_pipeline(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path(pipeline_pub_id): Path<String>,
) -> Result<(), ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let jobs: Vec<Job> = api_queries::get_pipeline_jobs()
        .bind(&client, &auth_data.organization_id, &pipeline_pub_id)
        .all()
        .await
        .map_err(log_and_map)?
        .into_iter()
        .map(|j| j.into())
        .collect();

    if jobs
        .iter()
        .any(|job| job.state != "Stopped" && job.state != "Finished" && job.state != "Failed")
    {
        return Err(bad_request("Pipeline's jobs must be in a terminal state (stopped, finished, or failed) before it can be deleted"
                .to_string()
        ));
    }

    let count = api_queries::delete_pipeline()
        .bind(&client, &pipeline_pub_id, &auth_data.organization_id)
        .await
        .map_err(log_and_map)?;

    if count != 1 {
        return Err(not_found("Pipeline"));
    }

    Ok(())
}

/// List a pipeline's jobs
#[utoipa::path(
    get,
    path = "/v1/pipelines/{id}/jobs",
    tag = "pipelines",
    params(
        ("id" = String, Path, description = "Pipeline id")
    ),
    responses(
        (status = 200, description = "Got jobs collection", body = JobCollection),
    ),
)]
pub async fn get_pipeline_jobs(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path(pipeline_pub_id): Path<String>,
) -> Result<Json<JobCollection>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    query_pipeline_by_pub_id(&pipeline_pub_id, &client, &auth_data).await?;

    let jobs: Vec<DbPipelineJob> = api_queries::get_pipeline_jobs()
        .bind(&client, &auth_data.organization_id, &pipeline_pub_id)
        .all()
        .await
        .map_err(log_and_map)?;

    Ok(Json(JobCollection {
        data: jobs.into_iter().map(|p| p.into()).collect(),
    }))
}

pub async fn query_pipeline_by_pub_id(
    pipeline_pub_id: &String,
    client: &impl GenericClient,
    auth_data: &AuthData,
) -> Result<Pipeline, ErrorResp> {
    let pipeline = api_queries::get_pipeline()
        .bind(client, pipeline_pub_id, &auth_data.organization_id)
        .opt()
        .await
        .map_err(log_and_map)?;

    let res = pipeline.ok_or_else(|| not_found("Pipeline"))?;

    res.try_into()
}

pub async fn query_job_by_pub_id(
    pipeline_pub_id: &String,
    job_pub_id: &String,
    client: &Object,
    auth_data: &AuthData,
) -> Result<Job, ErrorResp> {
    // make sure pipeline exists
    query_pipeline_by_pub_id(pipeline_pub_id, client, auth_data).await?;

    let job = api_queries::get_pipeline_job()
        .bind(client, &auth_data.organization_id, &job_pub_id)
        .opt()
        .await
        .map_err(log_and_map)?;

    let res: DbPipelineJob = job.ok_or_else(|| not_found("Job"))?;

    Ok(res.into())
}
