use crate::JobMessage;
use crate::states::leader_stopping::{LeaderStopBehavior, LeaderStopping};
use crate::states::recovering::Recovering;
use crate::states::{JobContext, State, StateError, Transition, TransitionTo, fatal};
use crate::types::public::StopMode as SqlStopMode;
use anyhow::{anyhow, bail};
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::job_status_grpc_client::JobStatusGrpcClient;
use arroyo_rpc::grpc::rpc::{JobFailure, JobState, JobStatusReq, JobStopMode, StopJobReq};
use arroyo_rpc::{job_status_client, retry};
use arroyo_types::JobId;
use std::time::{Duration, Instant};
use tonic::transport::Channel;
use tracing::{info, warn};

pub struct LeaderManager {
    leader_client: JobStatusGrpcClient<Channel>,
    pub job_id: JobId,
    pub generation: u64,
    pub last_heartbeat: Instant,
}

impl LeaderManager {
    pub async fn connect(job_id: JobId, run_id: u64, addr: String) -> anyhow::Result<Self> {
        let leader_client = retry!(
            job_status_client("controller", &config().worker.tls, addr.clone()).await,
            5,
            Duration::from_millis(100),
            Duration::from_secs(2),
            |e| warn!(job_id = *job_id.0, message = "failed to connect to worker leader", error = ?e)
        )?;

        Ok(Self {
            job_id,
            generation: run_id,
            leader_client,
            last_heartbeat: Instant::now(),
        })
    }

    pub async fn poll_leader_status(&mut self) -> anyhow::Result<rpc::JobStatus> {
        let response = retry!(
            self.leader_client.get_job_status(JobStatusReq {
                  job_id: self.job_id.to_string(),
                }).await,
                5,
                Duration::from_millis(100),
                Duration::from_secs(2),
                |e| warn!(job_id = *self.job_id.0, message = "failed to poll for job status", error = ?e)
        )?.into_inner();

        if response.job_id != *self.job_id.0 {
            bail!(
                "leader returned job status for wrong job: expected {}, got {}",
                self.job_id,
                response.job_id
            );
        }

        if response.generation != self.generation {
            bail!(
                "leader returned job status for wrong run: expected {}, got {}",
                self.generation,
                response.generation
            );
        }

        let status = response
            .job_status
            .ok_or_else(|| anyhow!("leader returned empty job status"))?;

        self.last_heartbeat = Instant::now();

        Ok(status)
    }

    pub async fn stop_leader(&mut self, stop_mode: JobStopMode) -> anyhow::Result<()> {
        info!(
            message = "sending stop request to leader",
            job_id = *self.job_id.0,
            stop_mode = ?stop_mode,
        );

        self.leader_client
            .stop_job(StopJobReq {
                stop_mode: stop_mode as i32,
            })
            .await?;

        Ok(())
    }

    pub async fn wait_for_state(&mut self, expected: rpc::JobState) -> anyhow::Result<()> {
        let mut timer = tokio::time::interval(Duration::from_millis(200));
        loop {
            let status = self.poll_leader_status().await?;

            let state = JobState::try_from(status.job_state)
                .map_err(|e| anyhow!("received invalid job state from leader: {e}"))?;

            if state == expected {
                return Ok(());
            }

            match state {
                JobState::JobUnknown => bail!("received unknown job status"),
                JobState::JobInitializing
                | JobState::JobRunning
                | JobState::JobStopping
                | JobState::JobFinishing
                | JobState::JobFailing => {
                    // non-terminal states, continue waiting
                }
                JobState::JobStopped | JobState::JobFinished | JobState::JobFailed => {
                    bail!(
                        "reached unexpected terminal state {:?} while waiting for {:?}",
                        state,
                        expected
                    );
                }
            }

            timer.tick().await;
        }
    }
}

pub async fn handle_leader_stopping<'a, S, T>(
    state: S,
    ctx: &mut JobContext<'a>,
    expected_state: rpc::JobState,
    next: T,
    timeout: Option<Duration>,
) -> Result<Transition, StateError>
where
    S: State,
    T: State,
    S: TransitionTo<T>,
    S: TransitionTo<LeaderStopping>,
    S: TransitionTo<Recovering>,
{
    let started = Instant::now();

    loop {
        let timeout = timeout
            .map(|t| (started + t).saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::MAX);

        tokio::select! {
            msg = ctx.rx.recv() => {
                match msg {
                    Some(JobMessage::ConfigUpdate(c)) => {
                        let next = match c.stop_mode {
                            SqlStopMode::force => {
                                Some(LeaderStopBehavior::StopWorkers)
                            }
                            SqlStopMode::immediate => {
                                Some(LeaderStopBehavior::StopJob(JobStopMode::JobStopImmediate))
                            }
                            SqlStopMode::graceful => {
                                Some(LeaderStopBehavior::StopJob(JobStopMode::JobStopGraceful))
                            }
                            SqlStopMode::none | SqlStopMode::checkpoint => {
                                // do nothing
                                None
                            }
                        };

                        if let Some(stop_behavior) = next {
                            return Ok(Transition::next(state, LeaderStopping {
                                stop_behavior
                            }));
                        }
                    }
                    Some(msg) => {
                        warn!(job_id = *ctx.config.id, ?msg, "unexpected job message in leader leader mode");
                    }
                    None => {
                        panic!("job queue shut down");
                    }
                }
            }
            resp = ctx.leader_manager.as_mut().expect("leader manager not initialized").wait_for_state(expected_state) => {
                if let Err(e) = resp {
                    return Err(fatal("failed while waiting for final checkpoint", e));
                }
                return Ok(Transition::next(
                    state,
                    next,
                ));
            }
            _ = tokio::time::sleep(timeout) => {
                return ctx.handle_job_failure(state, JobFailure {
                    operator_id: None,
                    task_id: None,
                    subtask_index: None,
                    message: "timed out while taking final checkpoint".to_string(),
                    error_domain: rpc::ErrorDomain::Internal as i32,
                    retry_hint: rpc::RetryHint::WithBackoff as i32,
                }).await;
            }
        }
    }
}
