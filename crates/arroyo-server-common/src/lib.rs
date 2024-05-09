#![allow(clippy::type_complexity)]

pub mod shutdown;

use anyhow::anyhow;
use arroyo_types::{admin_port, telemetry_enabled, POSTHOG_KEY};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use hyper::Body;
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use prometheus::{register_int_counter, IntCounter, TextEncoder};
#[cfg(not(target_os = "freebsd"))]
use pyroscope::{pyroscope::PyroscopeAgentRunning, PyroscopeAgent};
#[cfg(not(target_os = "freebsd"))]
use pyroscope_pprofrs::{pprof_backend, PprofConfig};
use reqwest::Client;
use serde_json::{json, Value};
use std::error::Error;
use std::fs;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::BoxBody;
use tonic::transport::Server;
use tower::layer::util::Stack;
use tower::{Layer, Service};
use tower_http::classify::{GrpcCode, GrpcErrorsAsFailures, SharedClassifier};
use tower_http::trace::{DefaultOnFailure, TraceLayer};

use tracing::metadata::LevelFilter;
use tracing::{debug, info, span, warn, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_log::LogTracer;

pub const BUILD_TIMESTAMP: &str = env!("VERGEN_BUILD_TIMESTAMP");
pub const GIT_SHA: &str = env!("VERGEN_GIT_SHA");
pub const VERSION: &str = "0.11.0-dev";

#[cfg(not(target_os = "freebsd"))]
const PYROSCOPE_SERVER_ADDRESS_ENV: &str = "PYROSCOPE_SERVER_ADDRESS";

static CLUSTER_ID: OnceCell<String> = OnceCell::new();

pub fn init_logging(name: &str) -> Option<WorkerGuard> {
    if let Err(e) = LogTracer::init() {
        eprintln!("Failed to initialize log tracer {:?}", e);
    }

    let stdout_log = tracing_subscriber::fmt::layer()
        .with_line_number(false)
        .with_file(false)
        .with_span_events(FmtSpan::NONE)
        .with_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        );

    let subscriber = Registry::default().with(stdout_log);

    let mut guard = None;

    let json_log = if std::env::var("PROD").is_ok() {
        let log_dir = std::env::var("LOG_DIR").unwrap_or_else(|_| "/var/log/arroyo".to_string());

        fs::create_dir_all(&log_dir).unwrap();

        let file_appender = tracing_appender::rolling::daily(&log_dir, name);
        let (non_blocking, g) = tracing_appender::non_blocking(file_appender);
        guard = Some(g);

        let json_log = tracing_subscriber::fmt::layer()
            .event_format(tracing_logfmt::EventsFormatter)
            .fmt_fields(tracing_logfmt::FieldsFormatter)
            .with_writer(non_blocking)
            .with_filter(EnvFilter::from_default_env());
        Some(json_log)
    } else {
        None
    };

    let subscriber = subscriber.with(json_log);

    tracing::subscriber::set_global_default(subscriber).expect("Unable to set global subscriber");

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

pub fn set_cluster_id(cluster_id: &str) {
    CLUSTER_ID.set(cluster_id.to_string()).unwrap();
}

pub fn get_cluster_id() -> String {
    CLUSTER_ID.get().map(|s| s.to_string()).unwrap()
}

pub fn log_event(name: &str, mut props: Value) {
    static CLIENT: OnceCell<Client> = OnceCell::new();
    let cluster_id = get_cluster_id();
    if telemetry_enabled() {
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

async fn details<'a>(State(state): State<Arc<AdminState>>) -> String {
    serde_json::to_string_pretty(&json!({
        "service": state.name,
        "git_sha": GIT_SHA,
        "version": VERSION,
        "build_timestamp": BUILD_TIMESTAMP,
    }))
    .unwrap()
}

pub async fn start_admin_server(service: &str, default_port: u16) -> anyhow::Result<()> {
    let port = admin_port(service, default_port);

    info!("Starting {} admin server on 0.0.0.0:{}", service, port);

    let state = Arc::new(AdminState {
        name: format!("arroyo-{}", service),
    });
    let app = Router::new()
        .route("/status", get(status))
        .route("/name", get(root))
        .route("/metrics", get(metrics))
        .route("/details", get(details))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port).parse().unwrap();

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .map_err(|e| anyhow!("Failed to start admin HTTP server: {}", e))
}

#[cfg(not(target_os = "freebsd"))]
pub fn try_profile_start(
    application_name: impl ToString,
    tags: Vec<(&str, &str)>,
) -> Option<PyroscopeAgent<PyroscopeAgentRunning>> {
    let agent = PyroscopeAgent::builder(
        std::env::var(PYROSCOPE_SERVER_ADDRESS_ENV).ok()?,
        application_name.to_string(),
    )
    .backend(pprof_backend(PprofConfig::new().sample_rate(100)))
    .tags(tags)
    .build();
    let agent = match agent {
        Ok(agent) => agent,
        Err(err) => {
            warn!("failed to build pyroscope agent with {}", err);
            return None;
        }
    };
    match agent.start() {
        Ok(agent) => Some(agent),
        Err(err) => {
            warn!("failed to start pyroscope agent with {}", err);
            None
        }
    }
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
    result: impl Future<Output = Result<(), tonic::transport::Error>>,
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
