use crate::queries::api_queries;
use crate::queries::api_queries::DbUdf;
use crate::rest::AppState;
use crate::rest_utils::{
    authenticate, bad_request, internal_server_error, map_insert_err, not_found, ApiError,
    BearerAuth, ErrorResp,
};
use crate::{compiler_service, to_micros};
use arroyo_rpc::api_types::udfs::{
    GlobalUdf, UdfLanguage, UdfPost, UdfValidationResult, ValidateUdfPost,
};
use arroyo_rpc::api_types::GlobalUdfCollection;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc::compiler_grpc_client::CompilerGrpcClient;
use arroyo_rpc::grpc::rpc::{BuildUdfReq, UdfCrate};
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_udf_host::ParsedUdfFile;
use arroyo_udf_python::PythonUDF;
use axum::extract::{Path, State};
use axum::Json;
use axum_extra::extract::WithRejection;
use std::str::FromStr;
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::error;

const PLUGIN_VERSION: &str = "=0.2.0";

const LOCAL_UDF_LIB_CRATE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../arroyo-udf/arroyo-udf-plugin"
);

impl From<DbUdf> for GlobalUdf {
    fn from(val: DbUdf) -> Self {
        GlobalUdf {
            id: val.pub_id,
            prefix: val.prefix,
            name: val.name,
            created_at: to_micros(val.created_at),
            definition: val.definition,
            updated_at: to_micros(val.updated_at),
            description: val.description,
            dylib_url: val.dylib_url,
            language: UdfLanguage::from_str(&val.language).unwrap_or_default(),
        }
    }
}

/// Create a global UDF
#[utoipa::path(
    post,
    path = "/v1/udfs",
    tag = "udfs",
    request_body = UdfPost,
    responses(
        (status = 200, description = "Created UDF", body = Udf),
    ),
)]
pub async fn create_udf(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    WithRejection(Json(req), _): WithRejection<Json<UdfPost>, ApiError>,
) -> Result<Json<GlobalUdf>, ErrorResp> {
    let auth_data = authenticate(&state.database, bearer_auth).await.unwrap();

    // let transaction = client.transaction().await.map_err(log_and_map)?;
    // transaction
    //     .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
    //     .await
    //     .map_err(log_and_map)?;

    // build udf
    let build_udf_resp = build_udf(
        &mut compiler_service().await?,
        &req.definition,
        req.language,
        true,
    )
    .await?;

    if !build_udf_resp.errors.is_empty() {
        return Err(bad_request("UDF is invalid"));
    }

    let client = state.database.client().await?;

    let udf_name = build_udf_resp.name.expect("udf name not set for valid UDF");

    // check for duplicates
    let pub_id = generate_id(IdTypes::Udf);
    api_queries::execute_create_udf(
        &client,
        &pub_id,
        &auth_data.organization_id,
        &auth_data.user_id,
        &req.prefix,
        &udf_name,
        &req.language.to_string(),
        &req.definition,
        &req.description.unwrap_or_default(),
        &build_udf_resp.url,
    )
    .await
    .map_err(|e| map_insert_err("udf", e))?;

    let created_udf = api_queries::fetch_get_udf(&client, &auth_data.organization_id, &pub_id)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| internal_server_error("Failed to fetch created UDF"))?
        .into();

    // transaction.commit().await.map_err(log_and_map)?;

    Ok(Json(created_udf))
}

/// Get Global UDFs
#[utoipa::path(
    get,
    path = "/v1/udfs",
    tag = "udfs",
    responses(
        (status = 200, description = "List of UDFs", body = GlobalUdfCollection),
    ),
)]
pub async fn get_udfs(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
) -> Result<Json<GlobalUdfCollection>, ErrorResp> {
    let auth_data = authenticate(&state.database, bearer_auth).await.unwrap();

    let udfs =
        api_queries::fetch_get_udfs(&state.database.client().await?, &auth_data.organization_id)
            .await?;

    Ok(Json(GlobalUdfCollection {
        data: udfs.into_iter().map(|u| u.into()).collect(),
    }))
}

/// Delete UDF
#[utoipa::path(
    delete,
    path = "/v1/udfs/{id}",
    tag = "udfs",
    params(
        ("id" = String, Path, description = "UDF id")
    ),
    responses(
        (status = 200, description = "Deleted UDF"),
    ),
)]
pub async fn delete_udf(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path(udf_pub_id): Path<String>,
) -> Result<(), ErrorResp> {
    let auth_data = authenticate(&state.database, bearer_auth).await.unwrap();

    let count = api_queries::execute_delete_udf(
        &state.database.client().await?,
        &auth_data.organization_id,
        &udf_pub_id,
    )
    .await?;

    if count != 1 {
        return Err(not_found("UDF"));
    }

    Ok(())
}

pub struct UdfResp {
    pub errors: Vec<String>,
    pub name: Option<String>,
    pub url: Option<String>,
}

impl From<anyhow::Error> for UdfResp {
    fn from(value: anyhow::Error) -> Self {
        Self {
            errors: vec![value.to_string()],
            name: None,
            url: None,
        }
    }
}

pub async fn build_udf(
    compiler_service: &mut CompilerGrpcClient<Channel>,
    udf_definition: &str,
    language: UdfLanguage,
    save: bool,
) -> Result<UdfResp, ErrorResp> {
    match language {
        UdfLanguage::Python => match PythonUDF::parse(udf_definition).await {
            Ok(udf) => Ok(UdfResp {
                errors: vec![],
                name: Some(Arc::unwrap_or_clone(udf.name)),
                url: None,
            }),
            Err(e) => Ok(UdfResp {
                errors: vec![e.to_string()],
                name: None,
                url: None,
            }),
        },
        UdfLanguage::Rust => {
            // use the arroyo-udf lib to do some validation and to get the function name
            let file = match ParsedUdfFile::try_parse(udf_definition) {
                Ok(p) => p,
                Err(e) => return Ok(e.into()),
            };

            let mut dependencies = file.dependencies;
            let plugin_dep = if config().compiler.use_local_udf_crate {
                toml::Value::Table(
                    [(
                        "path".to_string(),
                        toml::Value::String(LOCAL_UDF_LIB_CRATE.to_string()),
                    )]
                    .into_iter()
                    .collect(),
                )
            } else {
                toml::Value::String(PLUGIN_VERSION.to_string())
            };

            dependencies.insert("arroyo-udf-plugin".to_string(), plugin_dep);

            let check_udfs_resp = match compiler_service
                .build_udf(BuildUdfReq {
                    udf_crate: Some(UdfCrate {
                        name: file.udf.name.clone(),
                        definition: udf_definition.to_string(),
                        dependencies: dependencies.to_string(),
                    }),
                    save,
                })
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    error!("compiler service failed to validate UDF: {}", e.message());
                    return Err(internal_server_error(format!(
                        "Failed to validate UDF: {}",
                        e.message()
                    )));
                }
            };

            Ok(UdfResp {
                errors: check_udfs_resp.errors,
                name: Some(file.udf.name),
                url: check_udfs_resp.udf_path,
            })
        }
    }
}

/// Validate UDFs
#[utoipa::path(
    post,
    path = "/v1/udfs/validate",
    tag = "udfs",
    request_body = ValidateUdfPost,
    responses(
        (status = 200, description = "Validated query", body = UdfValidationResult),
    ),
)]
pub async fn validate_udf(
    WithRejection(Json(req), _): WithRejection<Json<ValidateUdfPost>, ApiError>,
) -> Result<Json<UdfValidationResult>, ErrorResp> {
    let check_udfs_resp = build_udf(
        &mut compiler_service().await?,
        &req.definition,
        req.language,
        false,
    )
    .await?;

    Ok(Json(UdfValidationResult {
        udf_name: check_udfs_resp.name,
        errors: check_udfs_resp.errors,
    }))
}
