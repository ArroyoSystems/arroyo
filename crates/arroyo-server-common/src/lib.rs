#![allow(clippy::type_complexity)]

pub mod shutdown;

use anyhow::anyhow;
use arroyo_types::POSTHOG_KEY;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use hyper::Body;
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use prometheus::{register_int_counter, Encoder, IntCounter, ProtobufEncoder, TextEncoder};
use reqwest::Client;
use serde_json::{json, Value};
use std::error::Error;
use std::fs;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::BoxBody;
use tonic::transport::Server;
use tower::layer::util::Stack;
use tower::{Layer, Service};
use tower_http::classify::{GrpcCode, GrpcErrorsAsFailures, SharedClassifier};
use tower_http::trace::{DefaultOnFailure, TraceLayer};

use tracing::metadata::LevelFilter;
use tracing::{debug, info, span, Level};
use tracing_subscriber::fmt::format::{FmtSpan, Format};
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

use arroyo_rpc::config::{config, LogFormat};
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_log::LogTracer;
use uuid::Uuid;

pub const BUILD_TIMESTAMP: &str = env!("VERGEN_BUILD_TIMESTAMP");
pub const GIT_SHA: &str = env!("VERGEN_GIT_SHA");
pub const VERSION: &str = "0.13.0-dev";

static CLUSTER_ID: OnceCell<String> = OnceCell::new();

pub fn init_logging(name: &str) -> Option<WorkerGuard> {
    init_logging_with_filter(
        name,
        EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy(),
    )
}

macro_rules! register_log {
    ($e: expr, $nonblocking: expr, $filter: expr) => {{
        let layer = $e;
        if let Some(nonblocking) = $nonblocking {
            tracing::subscriber::set_global_default(
                Registry::default().with(layer.with_writer(nonblocking).with_filter($filter)),
            )
            .expect("Unable to set global log subscriber")
        } else {
            tracing::subscriber::set_global_default(
                Registry::default().with(layer.with_writer(std::io::stderr).with_filter($filter)),
            )
            .expect("Unable to set global log subscriber")
        }
    }};
}

pub fn init_logging_with_filter(_name: &str, filter: EnvFilter) -> Option<WorkerGuard> {
    if let Err(e) = LogTracer::init() {
        eprintln!("Failed to initialize log tracer {:?}", e);
    }

    let filter = filter
        .add_directive("refinery_core=warn".parse().unwrap())
        .add_directive("aws_config::profile::credentials=warn".parse().unwrap());

    let (nonblocking, guard) = if config().logging.nonblocking {
        let (nonblocking, guard) = NonBlockingBuilder::default()
            .buffered_lines_limit(config().logging.buffered_lines_limit)
            .finish(std::io::stderr());
        (Some(nonblocking), Some(guard))
    } else {
        (None, None)
    };

    match config().logging.format {
        LogFormat::Plaintext => {
            register_log!(
                tracing_subscriber::fmt::layer()
                    .with_line_number(config().logging.enable_file_line)
                    .with_file(config().logging.enable_file_name)
                    .with_span_events(FmtSpan::NONE),
                nonblocking,
                filter
            )
        }
        LogFormat::Logfmt => {
            register_log!(
                tracing_subscriber::fmt::layer()
                    .with_line_number(config().logging.enable_file_line)
                    .with_file(config().logging.enable_file_name)
                    .event_format(tracing_logfmt::EventsFormatter)
                    .fmt_fields(tracing_logfmt::FieldsFormatter),
                nonblocking,
                filter
            )
        }
        LogFormat::Json => {
            register_log!(
                tracing_subscriber::fmt::layer()
                    .with_line_number(config().logging.enable_file_line)
                    .with_file(config().logging.enable_file_name)
                    .event_format(Format::default().json()),
                nonblocking,
                filter
            )
        }
    }

    std::panic::set_hook(Box::new(|panic| {
        if let Some(location) = panic.location() {
            tracing::error!(
                message = %panic,
                panic.file = location.file(),
                panic.line = location.line(),
                panic.column = location.column(),
            );
        } else {
            tracing::error!(message = %panic);
        }
    }));

    guard
}

fn existing_cluster_id(path: Option<&PathBuf>) -> Option<String> {
    let path = path?;
    if path.exists() {
        let s = fs::read_to_string(path).ok()?.trim().to_string();
        Uuid::parse_str(&s).ok()?;
        Some(s)
    } else {
        None
    }
}

pub fn set_cluster_id(cluster_id: &str) {
    let path = dirs::config_dir().map(|p| p.join("arroyo").join("cluster-info"));

    if let Some(id) = existing_cluster_id(path.as_ref()) {
        CLUSTER_ID.set(id).unwrap();
    } else {
        CLUSTER_ID.set(cluster_id.to_string()).unwrap();
        if let Some(path) = path {
            let _ = fs::write(&path, cluster_id);
        }
    }
}

pub fn get_cluster_id() -> String {
    CLUSTER_ID.get().map(|s| s.to_string()).unwrap()
}

pub fn log_event(name: &str, mut props: Value) {
    static CLIENT: OnceCell<Client> = OnceCell::new();
    let cluster_id = get_cluster_id();
    if !config().disable_telemetry {
        let name = name.to_string();
        tokio::task::spawn(async move {
            let client = CLIENT.get_or_init(Client::new);

            if let Some(props) = props.as_object_mut() {
                props.insert("distinct_id".to_string(), Value::String(cluster_id));
                props.insert("git_sha".to_string(), Value::String(GIT_SHA.to_string()));
                props.insert("version".to_string(), Value::String(VERSION.to_string()));
                props.insert(
                    "build_timestamp".to_string(),
                    Value::String(BUILD_TIMESTAMP.to_string()),
                );
            }

            let obj = json!({
                "api_key": POSTHOG_KEY,
                "event": name,
                "properties": props,
            });

            if let Err(e) = client
                .post("https://events.arroyo.dev/capture")
                .json(&obj)
                .send()
                .await
            {
                debug!("Failed to record event: {}", e);
            }
        });
    }
}

struct AdminState {
    name: String,
}

async fn root<'a>(State(state): State<Arc<AdminState>>) -> String {
    format!("{}\n", state.name)
}

async fn status<'a>() -> String {
    "ok".to_string()
}

async fn metrics() -> Result<Bytes, StatusCode> {
    let encoder = TextEncoder::new();
    let registry = prometheus::default_registry();
    match encoder.encode_to_string(&registry.gather()) {
        Ok(s) => Ok(Bytes::from(s)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn metrics_proto() -> Result<Bytes, StatusCode> {
    let encoder = ProtobufEncoder::new();
    let registry = prometheus::default_registry();
    let mut buf = vec![];
    encoder
        .encode(&registry.gather(), &mut buf)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(buf.into())
}

async fn config_route() -> Result<String, StatusCode> {
    Ok(toml::to_string(&*config()).unwrap())
}

async fn details<'a>(State(state): State<Arc<AdminState>>) -> String {
    serde_json::to_string_pretty(&json!({
        "service": state.name,
        "git_sha": GIT_SHA,
        "version": VERSION,
        "build_timestamp": BUILD_TIMESTAMP,
    }))
    .unwrap()
}

pub async fn handle_get_heap() -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
    require_profiling_activated(&prof_ctl)?;
    let pprof = prof_ctl
        .dump_pprof()
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(pprof)
}

/// Checks whether jemalloc profiling is activated an returns an error response if not.
fn require_profiling_activated(
    prof_ctl: &jemalloc_pprof::JemallocProfCtl,
) -> Result<(), (StatusCode, String)> {
    if prof_ctl.activated() {
        Ok(())
    } else {
        Err((
            axum::http::StatusCode::FORBIDDEN,
            "heap profiling not activated".into(),
        ))
    }
}

pub async fn start_admin_server(service: &str) -> anyhow::Result<()> {
    let addr = config().admin.bind_address;
    let port = config().admin.http_port;

    info!("Starting {} admin server on {}:{}", service, addr, port);

    let state = Arc::new(AdminState {
        name: format!("arroyo-{}", service),
    });
    let app = Router::new()
        .route("/status", get(status))
        .route("/name", get(root))
        .route("/metrics", get(metrics))
        .route("/metrics.pb", get(metrics_proto))
        .route("/details", get(details))
        .route("/config", get(config_route))
        .route("/debug/pprof/heap", get(handle_get_heap))
        .with_state(state);

    let addr = SocketAddr::new(addr, port);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .map_err(|e| anyhow!("Failed to start admin HTTP server: {}", e))
}

lazy_static! {
    static ref REQUEST_COUNTER: IntCounter =
        register_int_counter!("grpc_request_counter", "grpc requests").unwrap();
}

#[derive(Debug, Clone, Default)]
pub struct GrpcErrorLogMiddlewareLayer;

impl<S> Layer<S> for GrpcErrorLogMiddlewareLayer {
    type Service = GrpcErrorLogMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        GrpcErrorLogMiddleware { inner: service }
    }
}

#[derive(Debug, Clone)]
pub struct GrpcErrorLogMiddleware<S> {
    inner: S,
}

impl<S> Service<hyper::Request<Body>> for GrpcErrorLogMiddleware<S>
where
    S: Service<hyper::Request<Body>, Response = hyper::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
        let clone = self.inner.clone();
        let path = req.uri().clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            // Do extra async work here...
            let response = inner.call(req).await?;

            let code = response
                .headers()
                .get("grpc-status")
                .iter()
                .flat_map(|status| status.to_str().ok())
                .flat_map(|status| status.parse::<i32>().ok())
                .find_map(|code| match code {
                    0 => Some(GrpcCode::Ok),
                    1 => Some(GrpcCode::Cancelled),
                    2 => Some(GrpcCode::Unknown),
                    3 => Some(GrpcCode::InvalidArgument),
                    4 => Some(GrpcCode::DeadlineExceeded),
                    5 => Some(GrpcCode::NotFound),
                    6 => Some(GrpcCode::AlreadyExists),
                    7 => Some(GrpcCode::PermissionDenied),
                    8 => Some(GrpcCode::ResourceExhausted),
                    9 => Some(GrpcCode::FailedPrecondition),
                    10 => Some(GrpcCode::Aborted),
                    11 => Some(GrpcCode::OutOfRange),
                    12 => Some(GrpcCode::Unimplemented),
                    13 => Some(GrpcCode::Internal),
                    14 => Some(GrpcCode::Unavailable),
                    15 => Some(GrpcCode::DataLoss),
                    16 => Some(GrpcCode::Unauthenticated),
                    _ => None,
                });

            if let Some(code) = code {
                span!(
                    Level::ERROR,
                    "response failed",
                    code = format!("{:?}", code),
                    path = format!("{:?}", path)
                );
            }

            Ok(response)
        })
    }
}

pub fn grpc_server() -> Server<
    Stack<
        Stack<
            GrpcErrorLogMiddlewareLayer,
            Stack<TraceLayer<SharedClassifier<GrpcErrorsAsFailures>>, tower::layer::util::Identity>,
        >,
        tower::layer::util::Identity,
    >,
> {
    let layer = tower::ServiceBuilder::new()
        .layer(TraceLayer::new_for_grpc().on_failure(DefaultOnFailure::new().level(Level::TRACE)))
        .layer(GrpcErrorLogMiddlewareLayer)
        .into_inner();

    Server::builder().layer(layer)
}

pub async fn wrap_start(
    name: &str,
    addr: SocketAddr,
    result: impl Future<Output = Result<(), impl Error>>,
) -> anyhow::Result<()> {
    result.await.map_err(|e| {
        anyhow!(
            "Failed to start {} server on {}: {}",
            name,
            addr,
            e.source()
                .map(|e| e.to_string())
                .unwrap_or_else(|| e.to_string())
        )
    })
}
