#![allow(clippy::type_complexity)]
use arroyo_types::ADMIN_PORT_ENV;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, get_service};
use axum::Router;
use hyper::Body;
use lazy_static::lazy_static;
use prometheus::{register_int_counter, IntCounter, TextEncoder};
use pyroscope::pyroscope::PyroscopeAgentRunning;
use pyroscope::PyroscopeAgent;
use pyroscope_pprofrs::{pprof_backend, PprofConfig};
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fs, io};
use tokio::select;
use tokio::sync::broadcast::Receiver;
use tonic::body::BoxBody;
use tonic::transport::Server;
use tower::layer::util::Stack;
use tower::{Layer, Service};
use tower_http::classify::{GrpcCode, GrpcErrorsAsFailures, SharedClassifier};
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::{DefaultOnFailure, TraceLayer};

use tracing::metadata::LevelFilter;
use tracing::{info, span, warn, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

use tracing_appender::non_blocking::WorkerGuard;

const PYROSCOPE_SERVER_ADDRESS_ENV: &str = "PYROSCOPE_SERVER_ADDRESS";

pub fn init_logging(name: &str) -> Option<WorkerGuard> {
    let stdout_log = tracing_subscriber::fmt::layer()
        .with_line_number(false)
        .with_file(false)
        .with_span_events(FmtSpan::NEW)
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

async fn handle_error(_err: io::Error) -> impl IntoResponse {
    (StatusCode::INTERNAL_SERVER_ERROR, "Something went wrong...")
}

pub fn start_admin_server(name: String, default_port: u16, mut shutdown: Receiver<i32>) {
    let port = std::env::var(ADMIN_PORT_ENV)
        .map(|t| u16::from_str(&t).unwrap_or_else(|_| panic!("ADMIN_PORT={} is not valid", t)))
        .unwrap_or(default_port);

    info!("Starting {} admin server on 0.0.0.0:{}", name, port);

    let serve_dir = ServeDir::new("arroyo-console/dist")
        .not_found_service(ServeFile::new("arroyo-console/dist/index.html"));
    let serve_dir = get_service(serve_dir).handle_error(handle_error);

    let state = Arc::new(AdminState { name });
    let app = Router::new()
        .route("/status", get(status))
        .route("/name", get(root))
        .route("/metrics", get(metrics))
        .nest_service("/", serve_dir.clone())
        .fallback_service(serve_dir)
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port).parse().unwrap();

    tokio::spawn(async move {
        select! {
            result = axum::Server::bind(&addr)
            .serve(app.into_make_service()) => {
                result.unwrap();
            }
            _ = shutdown.recv() => {

            }
        }
    });
}

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

struct TowerMetrics<S> {
    inner: S,
}

lazy_static! {
    static ref REQUEST_COUNTER: IntCounter =
        register_int_counter!("grpc_request_counter", "grpc requests").unwrap();
}

impl<S, Request> Service<Request> for TowerMetrics<S>
where
    S: Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        self.inner.call(req)
    }
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
