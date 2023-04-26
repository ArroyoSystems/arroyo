use std::sync::RwLock;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};

use arroyo_datastream::Program;
use arroyo_rpc::grpc::api::PipelineProgram;

use deadpool_postgres::Pool;
use thiserror::Error;
use time::OffsetDateTime;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use tracing::{error, info, warn};

use anyhow::Result;

use crate::job_controller::JobController;
use crate::queries::controller_queries;
use crate::types::public::StopMode;
use crate::{schedulers::Scheduler, JobConfig, JobMessage, JobStatus};
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

    async fn next(self: Box<Self>, _: &mut Context) -> Result<Transition, StateError> {
        Ok(Transition::next(*self, Compiling))
    }
}

#[derive(Debug)]
pub struct Failed;
#[async_trait::async_trait]
impl State for Failed {
    fn name(&self) -> &'static str {
        "Failed"
    }

    async fn next(self: Box<Self>, ctx: &mut Context) -> Result<Transition, StateError> {
        if let Err(e) = ctx
            .scheduler
            .stop_workers(&ctx.config.id, Some(ctx.status.run_id), true)
            .await
        {
            warn!(
                message = "Failed to clean up cluster",
                error = format!("{:?}", e),
                job_id = ctx.config.id
            );
        }

        Ok(Transition::Stop)
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

    async fn next(self: Box<Self>, _: &mut Context) -> Result<Transition, StateError> {
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

    async fn next(self: Box<Self>, ctx: &mut Context) -> Result<Transition, StateError> {
        if let Err(e) = ctx
            .scheduler
            .stop_workers(&ctx.config.id, Some(ctx.status.run_id), true)
            .await
        {
            return Err(ctx.retryable(self, "failed to clean cluster", e, 20));
        }

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
impl TransitionTo<Stopping> for Rescaling {}

impl TransitionTo<Compiling> for Recovering {}

fn done_transition(ctx: &mut Context) {
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

pub struct Context<'a> {
    config: JobConfig,
    status: &'a mut JobStatus,
    program: &'a mut Program,
    pool: Pool,
    scheduler: Arc<dyn Scheduler>,
    rx: &'a mut Receiver<JobMessage>,
    retries_attempted: usize,
    job_controller: Option<JobController>,
}

impl<'a> Context<'a> {
    pub fn handle(&mut self, msg: JobMessage) -> Result<(), StateError> {
        warn!("unhandled job message {:?}", msg);
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

    fn is_terminal(&self) -> bool {
        false
    }

    async fn next(self: Box<Self>, ctx: &mut Context) -> Result<Transition, StateError>;
}

type TransitionFn = Box<dyn Fn(&mut Context) + Send>;

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

async fn execute_state<'a>(
    state: Box<dyn State>,
    mut ctx: Context<'a>,
) -> (Option<Box<dyn State>>, Context<'a>) {
    let state_name = state.name();

    let next: Option<Box<dyn State>> = match state.next(&mut ctx).await {
        Ok(Transition::Advance(s)) => {
            info!(
                message = "state transition",
                job_id = ctx.config.id,
                from = state_name,
                to = s.state.name()
            );

            (s.update_fn)(&mut ctx);
            ctx.retries_attempted = 0;

            Some(s.state)
        }
        Ok(Transition::Stop) => None,
        Err(StateError::FatalError { message, source })
        | Err(StateError::RetryableError {
            message,
            source,
            retries: 0,
            ..
        }) => {
            error!(
                message = "fatal state error",
                job_id = ctx.config.id,
                state = state_name,
                error_message = message,
                error = format!("{:?}", source)
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
                job_id = ctx.config.id,
                state = state_name,
                error_message = message,
                error = format!("{:?}", source),
                retries,
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
            ctx.retries_attempted += 1;
            Some(state)
        }
    };

    if let Some(s) = &next {
        ctx.status.state = s.name().to_string();

        ctx.status
            .update_db(&ctx.pool)
            .await
            .expect("Failed to update status");
    }

    (next, ctx)
}

pub async fn run_to_completion(
    config: Arc<RwLock<JobConfig>>,
    mut status: JobStatus,
    mut state: Box<dyn State>,
    pool: Pool,
    mut rx: Receiver<JobMessage>,
    scheduler: Arc<dyn Scheduler>,
) {
    let c = pool.get().await.unwrap();
    let id = config.read().unwrap().definition_id;
    let mut program: Program = {
        let res = controller_queries::get_program()
            .bind(&c, &id)
            .one()
            .await
            .unwrap();

        PipelineProgram::decode(&res[..])
            .unwrap()
            .try_into()
            .unwrap()
    };

    let mut ctx = Context {
        config: config.read().unwrap().clone(),
        status: &mut status,
        program: &mut program,
        pool: pool.clone(),
        scheduler,
        rx: &mut rx,
        retries_attempted: 0,
        job_controller: None,
    };

    loop {
        match execute_state(state, ctx).await {
            (Some(new_state), new_ctx) => {
                state = new_state;
                ctx = new_ctx;
            }
            (None, _) => break,
        }

        ctx.config = config.read().unwrap().clone();
    }
}

pub struct StateMachine {
    tx: Option<Sender<JobMessage>>,
    config: Arc<RwLock<JobConfig>>,
    pool: Pool,
    scheduler: Arc<dyn Scheduler>,
}

impl StateMachine {
    pub async fn new(
        config: JobConfig,
        status: JobStatus,
        pool: Pool,
        scheduler: Arc<dyn Scheduler>,
    ) -> Self {
        let mut this = Self {
            tx: None,
            config: Arc::new(RwLock::new(config)),
            pool,
            scheduler,
        };

        this.start(status).await;

        this
    }

    async fn start(&mut self, mut status: JobStatus) {
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
            "Compiling" | "Scheduling" | "Running" | "Recovering" => Some(Box::new(Compiling {})),
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
            status.update_db(&self.pool).await.unwrap();
            let (tx, rx) = channel(1024);
            {
                let config = self.config.clone();
                let pool = self.pool.clone();
                let scheduler = self.scheduler.clone();
                tokio::spawn(async move {
                    let id = { config.read().unwrap().id.clone() };
                    info!(message = "starting state machine", job_id = id);
                    run_to_completion(config, status, initial_state, pool, rx, scheduler).await;
                    info!(message = "finished state machine", job_id = id);
                });
            }

            self.tx = Some(tx);
        }
    }

    pub async fn update(&mut self, config: JobConfig, status: JobStatus) {
        if *self.config.read().unwrap() != config {
            let update = JobMessage::ConfigUpdate(config.clone());
            {
                let mut c = self.config.write().unwrap();
                *c = config;
            }
            if self.send(update).await.is_err() {
                self.start(status).await;
            }
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
}
