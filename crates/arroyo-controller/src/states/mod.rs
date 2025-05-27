use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use std::{fmt::Debug, sync::Arc};

use arroyo_rpc::grpc::api::ArrowProgram;

use arroyo_server_common::log_event;
use serde_json::json;
use thiserror::Error;
use time::OffsetDateTime;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use tracing::{debug, error, info, warn};

use anyhow::{anyhow, Result};
use cornucopia_async::DatabaseSource;

use crate::job_controller::JobController;
use crate::queries::controller_queries;
use crate::types::public::StopMode;
use crate::{schedulers::Scheduler, JobConfig, JobMessage, JobStatus, RunningMessage};
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::config::config;
use arroyo_server_common::shutdown::ShutdownGuard;
use prost::Message;

use self::checkpoint_stopping::CheckpointStopping;
use self::compiling::Compiling;
use self::finishing::Finishing;
use self::recovering::Recovering;
use self::rescaling::Rescaling;
use self::running::Running;
use self::scheduling::Scheduling;
use self::stopping::Stopping;

mod checkpoint_stopping;
mod compiling;
mod finishing;
mod recovering;
mod rescaling;
mod restarting;
mod running;
mod scheduling;
mod stopping;

pub enum Transition {
    Stop,
    Advance(StateHolder),
}

#[derive(Error, Debug)]
pub enum StateError {
    #[error("fatal error: {message:?}")]
    FatalError {
        message: String,
        source: anyhow::Error,
    },
    #[error("retryable error: {message:?} ")]
    RetryableError {
        state: Box<dyn State>,
        message: String,
        source: anyhow::Error,
        retries: usize,
    },
}

pub fn fatal(message: impl Into<String>, source: anyhow::Error) -> StateError {
    StateError::FatalError {
        message: message.into(),
        source,
    }
}

#[derive(Debug)]
pub struct Created;

#[async_trait::async_trait]
impl State for Created {
    fn name(&self) -> &'static str {
        "Created"
    }

    async fn next(self: Box<Self>, _: &mut JobContext) -> Result<Transition, StateError> {
        Ok(Transition::next(*self, Compiling))
    }
}

async fn handle_terminal<'a>(ctx: &mut JobContext<'a>) {
    if let Err(e) = ctx
        .scheduler
        .stop_workers(&ctx.config.id, Some(ctx.status.run_id), true)
        .await
    {
        warn!(
            message = "Failed to clean up cluster",
            error = format!("{:?}", e),
            job_id = *ctx.config.id
        );
    }
}

#[derive(Debug)]
pub struct Failed;
#[async_trait::async_trait]
impl State for Failed {
    fn name(&self) -> &'static str {
        "Failed"
    }

    async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        handle_terminal(ctx).await;
        if ctx.config.restart_nonce != ctx.status.restart_nonce {
            // the user has requested a restart
            Ok(Transition::next(*self, Compiling {}))
        } else {
            Ok(Transition::Stop)
        }
    }

    fn is_terminal(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct Finished;

#[async_trait::async_trait]
impl State for Finished {
    fn name(&self) -> &'static str {
        "Finished"
    }

    async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        handle_terminal(ctx).await;
        Ok(Transition::Stop)
    }

    fn is_terminal(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct Stopped {}

#[async_trait::async_trait]
impl State for Stopped {
    fn name(&self) -> &'static str {
        "Stopped"
    }

    async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        handle_terminal(ctx).await;

        if ctx.config.stop_mode == StopMode::none && ctx.config.ttl.is_none() {
            Ok(Transition::next(*self, Compiling {}))
        } else {
            Ok(Transition::Stop)
        }
    }

    fn is_terminal(&self) -> bool {
        true
    }
}

// State transitions
impl TransitionTo<Compiling> for Created {}

impl TransitionTo<Compiling> for Stopped {}

impl TransitionTo<Compiling> for Scheduling {}

impl TransitionTo<Scheduling> for Compiling {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.run_id += 1;
        })
    }
}

impl TransitionTo<Running> for Scheduling {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            // set the start time and clear the finish time, but only if this is an initial start
            // and not a recovery
            if ctx.status.start_time.is_none() || ctx.status.finish_time.is_some() {
                ctx.status.start_time = Some(OffsetDateTime::now_utc());
                ctx.status.finish_time = None;
            }
        })
    }
}

impl TransitionTo<CheckpointStopping> for Running {}
impl TransitionTo<Stopping> for Running {}
impl TransitionTo<Stopping> for Scheduling {}
impl TransitionTo<Stopping> for Compiling {}
impl TransitionTo<Stopping> for Rescaling {}
impl TransitionTo<Finishing> for Running {}
impl TransitionTo<Recovering> for Running {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restarts += 1;
        })
    }
}
impl TransitionTo<Rescaling> for Running {}

impl TransitionTo<Scheduling> for Rescaling {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.run_id += 1;
        })
    }
}

impl TransitionTo<Compiling> for Recovering {}
impl TransitionTo<Compiling> for Failed {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restart_nonce = ctx.config.restart_nonce;
            ctx.status.restarts = 0;
            ctx.status.failure_message = None;
        })
    }
}

fn done_transition(ctx: &mut JobContext) {
    ctx.status.finish_time = Some(OffsetDateTime::now_utc());
    ctx.job_controller = None;
}

impl TransitionTo<Stopped> for Stopping {
    fn update_status(&self) -> TransitionFn {
        Box::new(done_transition)
    }
}

impl TransitionTo<Stopping> for CheckpointStopping {}
impl TransitionTo<Stopped> for CheckpointStopping {
    fn update_status(&self) -> TransitionFn {
        Box::new(done_transition)
    }
}

impl TransitionTo<Finished> for Finishing {
    fn update_status(&self) -> TransitionFn {
        Box::new(done_transition)
    }
}

impl TransitionTo<Restarting> for Running {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restart_nonce = ctx.config.restart_nonce;
        })
    }
}
impl TransitionTo<Restarting> for Restarting {}
impl TransitionTo<Scheduling> for Restarting {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.run_id += 1;
        })
    }
}
impl TransitionTo<Stopping> for Restarting {}
impl TransitionTo<CheckpointStopping> for Restarting {}

// Macro to handle stopping behavior from a running state, where we want to
// support checkpoint stopping
macro_rules! stop_if_desired_running {
    ($self: ident, $config: expr) => {
        use crate::states::checkpoint_stopping::CheckpointStopping;
        use crate::states::stopping::StopBehavior;
        use crate::states::stopping::Stopping;
        use crate::types::public::StopMode;
        use arroyo_rpc::grpc::rpc;
        match $config.stop_mode {
            StopMode::checkpoint => {
                return Ok(Transition::next(*$self, CheckpointStopping {}));
            }
            StopMode::graceful => {
                return Ok(Transition::next(
                    *$self,
                    Stopping {
                        stop_mode: StopBehavior::StopJob(rpc::StopMode::Graceful),
                    },
                ));
            }
            StopMode::immediate => {
                return Ok(Transition::next(
                    *$self,
                    Stopping {
                        stop_mode: StopBehavior::StopJob(rpc::StopMode::Immediate),
                    },
                ));
            }
            StopMode::force => {
                return Ok(Transition::next(
                    *$self,
                    Stopping {
                        stop_mode: StopBehavior::StopWorkers,
                    },
                ));
            }
            StopMode::none => {
                // do nothing
            }
        }
    };
}

// macro to handle stopping behavior from a state where the job is not current running
// (like compiling / scheduling / etc.). in this case, there's nothing active to checkpoint
// so we just move to stopping all cases
macro_rules! stop_if_desired_non_running {
    ($self: ident, $config: expr) => {
        use crate::states::stopping::StopBehavior;
        use crate::states::stopping::Stopping;
        use crate::types::public::StopMode;
        use arroyo_rpc::grpc;
        match $config.stop_mode {
            StopMode::checkpoint | StopMode::graceful | StopMode::immediate => {
                return Ok(Transition::next(
                    *$self,
                    Stopping {
                        stop_mode: StopBehavior::StopJob(grpc::rpc::StopMode::Immediate),
                    },
                ));
            }
            StopMode::force => {
                return Ok(Transition::next(
                    *$self,
                    Stopping {
                        stop_mode: StopBehavior::StopWorkers,
                    },
                ));
            }
            StopMode::none => {
                // do nothing
            }
        }
    };
}

use crate::job_controller::job_metrics::JobMetrics;
use crate::states::restarting::Restarting;
pub(crate) use stop_if_desired_non_running;
pub(crate) use stop_if_desired_running;

pub struct JobContext<'a> {
    config: JobConfig,
    status: &'a mut JobStatus,
    program: &'a mut LogicalProgram,
    db: DatabaseSource,
    scheduler: Arc<dyn Scheduler>,
    rx: &'a mut Receiver<JobMessage>,
    retries_attempted: usize,
    job_controller: Option<JobController>,
    last_transitioned_at: Instant,
    metrics: Arc<tokio::sync::RwLock<HashMap<Arc<String>, JobMetrics>>>,
}

impl JobContext<'_> {
    pub fn handle(&mut self, msg: JobMessage) -> Result<(), StateError> {
        if !matches!(
            msg,
            JobMessage::RunningMessage(RunningMessage::WorkerHeartbeat { .. })
        ) {
            warn!("unhandled job message {:?}", msg);
        }
        Ok(())
    }

    pub fn retryable(
        &self,
        state: Box<dyn State>,
        message: impl Into<String>,
        source: anyhow::Error,
        retries: usize,
    ) -> StateError {
        StateError::RetryableError {
            state,
            message: message.into(),
            source,
            retries: retries.saturating_sub(self.retries_attempted),
        }
    }
}

#[async_trait::async_trait]
pub trait State: Sync + Send + 'static + Debug {
    fn name(&self) -> &'static str;

    #[allow(unused)]
    fn is_terminal(&self) -> bool {
        false
    }

    async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError>;
}

type TransitionFn = Box<dyn Fn(&mut JobContext) + Send>;

pub trait TransitionTo<S: State> {
    fn update_status(&self) -> TransitionFn {
        Box::new(|_| {})
    }
}

pub struct StateHolder {
    state: Box<dyn State>,
    update_fn: TransitionFn,
}

impl Transition {
    #[allow(clippy::multiple_bound_locations)]
    pub fn next<ThisState: State, NextState: State>(t: ThisState, n: NextState) -> Transition
    where
        ThisState: TransitionTo<NextState>,
    {
        Transition::Advance(StateHolder {
            state: Box::new(n),
            update_fn: t.update_status(),
        })
    }
}

#[allow(clippy::needless_lifetimes)]
async fn execute_state<'a>(
    state: Box<dyn State>,
    mut ctx: JobContext<'a>,
) -> (Option<Box<dyn State>>, JobContext<'a>) {
    let state_name = state.name();

    debug!(
        message = "executing state",
        job_id = *ctx.config.id,
        state = state_name,
        config = format!("{:?}", ctx.config)
    );

    let next: Option<Box<dyn State>> = match state.next(&mut ctx).await {
        Ok(Transition::Advance(s)) => {
            info!(
                message = "state transition",
                job_id = *ctx.config.id,
                from = state_name,
                to = s.state.name(),
                duration_ms = ctx.last_transitioned_at.elapsed().as_millis()
            );

            log_event(
                "state_transition",
                json!({
                    "service": "controller",
                    "job_id": ctx.config.id,
                    "from": state_name,
                    "to": s.state.name(),
                    "scheduler": &config().controller.scheduler,
                    "duration_ms": ctx.last_transitioned_at.elapsed().as_millis() as u64,
                }),
            );

            (s.update_fn)(&mut ctx);
            ctx.retries_attempted = 0;
            ctx.last_transitioned_at = Instant::now();

            Some(s.state)
        }
        Ok(Transition::Stop) => None,
        Err(StateError::FatalError { message, source })
        | Err(StateError::RetryableError {
            message,
            source,
            retries: 1,
            ..
        })
        | Err(StateError::RetryableError {
            message,
            source,
            retries: 0,
            ..
        }) => {
            error!(
                message = "fatal state error",
                job_id = *ctx.config.id,
                state = state_name,
                error_message = message,
                error = format!("{:?}", source)
            );
            log_event(
                "fatal_state_error",
                json!({
                    "service": "controller",
                    "job_id": ctx.config.id,
                    "state": state_name,
                    "error_message": message,
                    "error": format!("{:?}", source),
                    "retries": 0,
                }),
            );
            ctx.status.failure_message = Some(message);
            ctx.status.finish_time = Some(OffsetDateTime::now_utc());
            let s: Box<dyn State> = Box::new(Failed {});
            Some(s)
        }
        Err(StateError::RetryableError {
            state,
            message,
            source,
            retries,
        }) => {
            error!(
                message = "retryable state error",
                job_id = *ctx.config.id,
                state = state_name,
                error_message = message,
                error = format!("{:?}", source),
                retries,
            );
            log_event(
                "state_error",
                json!({
                    "service": "controller",
                    "job_id": ctx.config.id,
                    "state": state_name,
                    "error_message": message,
                    "error": format!("{:?}", source),
                    "retries": retries,
                }),
            );

            tokio::time::sleep(Duration::from_millis(500)).await;
            ctx.retries_attempted += 1;
            Some(state)
        }
    };

    if let Some(s) = &next {
        ctx.status.state = s.name().to_string();

        ctx.status
            .update_db(&ctx.db)
            .await
            .expect("Failed to update status");
    }

    (next, ctx)
}

#[allow(clippy::too_many_arguments)]
async fn run_to_completion(
    config: Arc<RwLock<(JobConfig, AppliedStatus)>>,
    mut program: LogicalProgram,
    mut status: JobStatus,
    mut state: Box<dyn State>,
    db: DatabaseSource,
    mut rx: Receiver<JobMessage>,
    scheduler: Arc<dyn Scheduler>,
    metrics: Arc<tokio::sync::RwLock<HashMap<Arc<String>, JobMetrics>>>,
) {
    let mut ctx = JobContext {
        config: config.read().unwrap().0.clone(),
        status: &mut status,
        program: &mut program,
        db: db.clone(),
        scheduler,
        rx: &mut rx,
        retries_attempted: 0,
        job_controller: None,
        last_transitioned_at: Instant::now(),
        metrics,
    };

    loop {
        config.write().unwrap().1 = AppliedStatus::Applied;
        match execute_state(state, ctx).await {
            (Some(new_state), new_ctx) => {
                state = new_state;
                ctx = new_ctx;
            }
            (None, _) => break,
        }

        ctx.config = config.read().unwrap().0.clone();
    }
}

#[derive(Copy, Clone)]
enum AppliedStatus {
    Applied,
    NotApplied,
}

pub struct StateMachine {
    tx: Option<Sender<JobMessage>>,
    config: Arc<RwLock<(JobConfig, AppliedStatus)>>,
    pub(crate) state: Arc<RwLock<String>>,
    metrics: Arc<tokio::sync::RwLock<HashMap<Arc<String>, JobMetrics>>>,
    db: DatabaseSource,
    scheduler: Arc<dyn Scheduler>,
}

impl StateMachine {
    pub async fn new(
        config: JobConfig,
        status: JobStatus,
        db: DatabaseSource,
        scheduler: Arc<dyn Scheduler>,
        shutdown_guard: ShutdownGuard,
        metrics: Arc<tokio::sync::RwLock<HashMap<Arc<String>, JobMetrics>>>,
    ) -> Self {
        let mut this = Self {
            tx: None,
            config: Arc::new(RwLock::new((config, AppliedStatus::NotApplied))),
            state: Arc::new(RwLock::new(status.state.clone())),
            metrics,
            db,
            scheduler,
        };

        this.start(status, shutdown_guard).await;

        this
    }

    fn decode_program(bs: &[u8]) -> anyhow::Result<LogicalProgram> {
        ArrowProgram::decode(bs)
            .map_err(|e| anyhow!("Failed to decode program: {:?}", e))?
            .try_into()
            .map_err(|e| anyhow!("Failed to construct graph from program: {:?}", e))
    }

    async fn get_program(
        db: &DatabaseSource,
        job_id: &str,
        id: i64,
    ) -> anyhow::Result<Option<LogicalProgram>> {
        let res = controller_queries::fetch_get_program(&db.client().await?, &id)
            .await
            .map_err(|e| anyhow!("Failed to fetch program from database: {:?}", e))?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Could not find program for job_id {job_id}"))?;

        Ok(if res.proto_version == 2 {
            match Self::decode_program(&res.program) {
                Ok(p) => Some(p),
                Err(e) => {
                    warn!("Failed to start {}: {}", job_id, e);
                    None
                }
            }
        } else {
            None
        })
    }

    async fn start(&mut self, mut status: JobStatus, shutdown_guard: ShutdownGuard) {
        if !self.done() {
            // we're already running, don't do anything
            return;
        }

        // TODO: This seems pretty error-prone and easy to miss adding when we add states
        let initial_state: Option<Box<dyn State>> = match status.state.as_str() {
            "Created" => Some(Box::new(Created {})),
            "Stopped" => Some(Box::new(Stopped {})),
            "Finished" => Some(Box::new(Finished {})),
            "Failed" => Some(Box::new(Failed {})),
            "Compiling" | "Scheduling" | "Running" | "Recovering" | "Rescaling" => {
                Some(Box::new(Compiling {}))
            }
            "Stopping" | "CheckpointStopping" => {
                // TODO: do we need to handle a failure in CheckpointStopping specially?
                if status.finish_time.is_none() {
                    status.finish_time = Some(OffsetDateTime::now_utc());
                }
                Some(Box::new(Stopped {}))
            }
            "Finishing" => {
                if status.finish_time.is_none() {
                    status.finish_time = Some(OffsetDateTime::now_utc());
                }
                Some(Box::new(Finished {}))
            }
            s => {
                panic!("Unhandled state {} in recovery", s);
            }
        };

        if let Some(initial_state) = initial_state {
            status.state = initial_state.name().to_string();
            status.update_db(&self.db).await.unwrap();
            let (tx, rx) = channel(1024);
            {
                let config = self.config.clone();
                let db = self.db.clone();
                let scheduler = self.scheduler.clone();
                let metrics = self.metrics.clone();
                let pipeline_id = config.read().unwrap().0.pipeline_id;
                match Self::get_program(&db, &status.id, pipeline_id).await {
                    Ok(Some(program)) => {
                        shutdown_guard.into_spawn_task(async move {
                            let id = { config.read().unwrap().0.id.clone() };
                            info!(message = "starting state machine", job_id = *id);
                            run_to_completion(
                                config,
                                program,
                                status,
                                initial_state,
                                db,
                                rx,
                                scheduler,
                                metrics,
                            )
                            .await;
                            info!(message = "finished state machine", job_id = *id);
                            Ok(())
                        });
                    }
                    Ok(None) => {
                        // this is a bad/old pipeline, skip it
                    }
                    Err(e) => {
                        // something went wrong, we'll retry on the next go around
                        warn!("Failed to start {}: {:?}", status.id, e);
                    }
                }
            }

            self.tx = Some(tx);
        }
    }

    pub async fn update(
        &mut self,
        config: JobConfig,
        status: JobStatus,
        shutdown_guard: &ShutdownGuard,
    ) {
        *self.state.write().unwrap() = status.state.clone();
        if self.config.read().unwrap().0 != config {
            let update = JobMessage::ConfigUpdate(config.clone());
            {
                let mut c = self.config.write().unwrap();
                *c = (config, AppliedStatus::NotApplied);
            }
            if self.send(update).await.is_err() {
                self.start(status, shutdown_guard.clone_temporary()).await;
            }
        } else {
            let applied = self.config.read().unwrap().1;
            self.restart_if_needed(applied, status, shutdown_guard)
                .await;
        }
    }

    pub async fn send(&mut self, msg: JobMessage) -> Result<(), &'static str> {
        if let Some(tx) = &self.tx {
            tx.send(msg).await.map_err(|_| "State machine is inactive")
        } else {
            Err("State machine is inactive")
        }
    }

    pub fn done(&self) -> bool {
        if let Some(tx) = &self.tx {
            tx.is_closed()
        } else {
            true
        }
    }

    // for states that should be running, check them and restart if needed
    async fn restart_if_needed(
        &mut self,
        applied: AppliedStatus,
        status: JobStatus,
        shutdown_guard: &ShutdownGuard,
    ) {
        match (applied, status.state.as_str()) {
            (_, "Running" | "Recovering" | "Rescaling") | (AppliedStatus::NotApplied, _) => {
                // done() means there isn't a task running, but these states
                // need to be advanced.
                if self.done() {
                    self.start(status, shutdown_guard.clone_temporary()).await;
                }
            }
            _ => {}
        }
    }
}
