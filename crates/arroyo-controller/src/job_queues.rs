//! A handle to the controller's per-job message queues that schedulers can use.
//!
//! Its first use is letting a scheduler register workers it discovered itself
//! (for example, by asking its platform for each worker's address and polling
//! it until it's ready) instead of waiting for each worker to call
//! `RegisterWorker`. Such a scheduler keeps the [`JobQueues`] it receives
//! through [`Scheduler::set_job_queues`], starts its workers with
//! `worker.registration = scheduler`, returns
//! [`WorkerRegistration::Scheduler`] from [`Scheduler::worker_registration`],
//! and calls [`JobQueues::connect_worker`] once a worker is reachable.
//!
//! [`Scheduler::set_job_queues`]: crate::schedulers::Scheduler::set_job_queues
//! [`Scheduler::worker_registration`]: crate::schedulers::Scheduler::worker_registration

use crate::JobMessage;
use crate::states::StateMachine;
use arroyo_rpc::config::WorkerRegistration;
use arroyo_types::{MachineId, WorkerId};
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;
use tonic::Status;
use tracing::warn;

pub(crate) type JobStates = Arc<Mutex<HashMap<String, StateMachine>>>;

/// A worker a scheduler found and confirmed is reachable at `rpc_address`.
///
/// The controller doesn't check these against the worker, so each field must
/// match what the worker was started with.
#[derive(Debug, Clone)]
pub struct DiscoveredWorker {
    /// The worker's `worker.id`.
    pub worker_id: WorkerId,
    /// The worker's `worker.machine-id`.
    pub machine_id: MachineId,
    pub generation: u64,
    pub rpc_address: String,
    pub data_address: String,
    /// The worker's `worker.task-slots`. The controller assigns this many
    /// subtasks to the worker; a larger value overloads it.
    pub slots: usize,
}

/// Cheap to clone.
///
/// Holds the job map weakly: the scheduler keeps this handle, and the job
/// state machines hold the scheduler, so a strong reference would be a cycle
/// that keeps the controller alive after it's dropped.
#[derive(Clone)]
pub struct JobQueues {
    jobs: Weak<Mutex<HashMap<String, StateMachine>>>,
}

impl JobQueues {
    pub(crate) fn new(jobs: &JobStates) -> Self {
        Self {
            jobs: Arc::downgrade(jobs),
        }
    }

    /// Queues `worker` for its job, as if it had called `RegisterWorker`.
    ///
    /// `Ok` means only that the message was queued, not that the job accepted
    /// the worker. The job picks workers up while it is scheduling, and ignores
    /// one whose generation isn't the job's current one. A worker that isn't
    /// picked up surfaces as the job's worker startup timeout.
    ///
    /// The fields of `worker` must match what the worker was started with: it
    /// rejects RPCs addressed to a different worker id.
    ///
    /// Fails if the job isn't known to this controller, its state machine
    /// isn't active, or the controller has been dropped.
    ///
    /// This waits for room in the job's queue, which the job doesn't drain
    /// while it's inside [`Scheduler::start_workers`]. Don't await this from
    /// `start_workers`; connect workers from a separate task instead.
    ///
    /// [`Scheduler::start_workers`]: crate::schedulers::Scheduler::start_workers
    pub async fn connect_worker(
        &self,
        job_id: &str,
        worker: DiscoveredWorker,
    ) -> Result<(), Status> {
        self.send(
            job_id,
            JobMessage::WorkerConnect {
                worker_id: worker.worker_id,
                machine_id: worker.machine_id,
                generation: worker.generation,
                rpc_address: worker.rpc_address,
                data_address: worker.data_address,
                slots: worker.slots,
                registered_by: WorkerRegistration::Scheduler,
            },
        )
        .await
    }

    pub(crate) async fn send(&self, job_id: &str, msg: JobMessage) -> Result<(), Status> {
        // Keep per-job backpressure from holding the global job map lock.
        let tx = {
            let jobs = self.jobs.upgrade().ok_or_else(|| {
                Status::unavailable("Cannot handle message: controller has shut down")
            })?;
            let jobs = jobs.lock().await;
            let Some(sm) = jobs.get(job_id) else {
                warn!(message = "Received message for unknown job id", %job_id);
                return Err(Status::failed_precondition(format!(
                    "No job with id {job_id}"
                )));
            };

            sm.sender().ok_or_else(|| {
                Status::failed_precondition(format!(
                    "Cannot handle message for {job_id}: State machine is inactive"
                ))
            })?
        };

        tx.send(msg).await.map_err(|_| {
            Status::failed_precondition(format!(
                "Cannot handle message for {job_id}: State machine is inactive"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[tokio::test]
    async fn connect_worker_for_unknown_job_fails() {
        let jobs: JobStates = Arc::new(Mutex::new(HashMap::new()));
        let queues = JobQueues::new(&jobs);
        let err = queues
            .connect_worker(
                "missing",
                DiscoveredWorker {
                    worker_id: WorkerId(1),
                    machine_id: MachineId(Arc::new("m".to_string())),
                    generation: 1,
                    rpc_address: "http://[::1]:6900".to_string(),
                    data_address: "[::1]:6903".to_string(),
                    slots: 1,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }
}
