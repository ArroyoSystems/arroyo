use std::fmt::Debug;
use std::future::Future;
use std::ops::Sub;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{collections::HashSet, time::SystemTime};

use crate::inq_reader::InQReader;
use arrow::array::types::{TimestampNanosecondType, UInt64Type};
use arrow::array::{Array, PrimitiveArray, RecordBatch, UInt64Array};
use arrow::compute::kernels::numeric::{div, rem};
use arroyo_types::{ArrowMessage, CheckpointBarrier, Data, SignalMessage, TaskInfo};
use bincode::{Decode, Encode};

use crate::context::OperatorContext;
use crate::operator::{ConstructedOperator, Registry};
use operator::{OperatorConstructor, OperatorNode};
use tokio_stream::Stream;

pub mod connector;
pub mod context;
pub mod inq_reader;
pub mod operator;
pub mod udfs;

pub trait TimerT: Data + PartialEq + Eq + 'static {}

impl<T: Data + PartialEq + Eq + 'static> TimerT for T {}

pub fn server_for_hash_array(
    hash: &PrimitiveArray<UInt64Type>,
    n: usize,
) -> anyhow::Result<PrimitiveArray<UInt64Type>> {
    let range_size = u64::MAX / (n as u64);
    let range_scalar = UInt64Array::new_scalar(range_size);
    let server_scalar = UInt64Array::new_scalar(n as u64);
    let division = div(hash, &range_scalar)?;
    let mod_array = rem(&division, &server_scalar)?;
    let result: &PrimitiveArray<UInt64Type> = mod_array.as_any().downcast_ref().unwrap();
    Ok(result.clone())
}

pub enum SourceFinishType {
    // stop messages should be propagated through the dataflow
    Graceful,
    // shuts down the operator immediately, triggering immediate shut-downs across the dataflow
    Immediate,
    // EndOfData messages are propagated, causing MAX_WATERMARK and flushing all timers
    Final,
}

impl From<SourceFinishType> for Option<SignalMessage> {
    fn from(value: SourceFinishType) -> Self {
        match value {
            SourceFinishType::Graceful => Some(SignalMessage::Stop),
            SourceFinishType::Immediate => None,
            SourceFinishType::Final => Some(SignalMessage::EndOfData),
        }
    }
}

pub enum ControlOutcome {
    Continue,
    Stop,
    StopAndSendStop,
    Finish,
    StopAfterCommit,
}

#[derive(Debug)]
pub struct CheckpointCounter {
    inputs: Vec<Option<u32>>,
    counter: Option<usize>,
}

impl CheckpointCounter {
    pub fn new(size: usize) -> CheckpointCounter {
        CheckpointCounter {
            inputs: vec![None; size],
            counter: None,
        }
    }

    pub fn is_blocked(&self, idx: usize) -> bool {
        self.inputs[idx].is_some()
    }

    pub fn all_clear(&self) -> bool {
        self.inputs.iter().all(|x| x.is_none())
    }

    pub fn mark(&mut self, idx: usize, checkpoint: &CheckpointBarrier) -> bool {
        assert!(self.inputs[idx].is_none());

        if self.inputs.len() == 1 {
            return true;
        }

        self.inputs[idx] = Some(checkpoint.epoch);
        self.counter = match self.counter {
            None => Some(self.inputs.len() - 1),
            Some(1) => {
                for v in self.inputs.iter_mut() {
                    *v = None;
                }
                None
            }
            Some(n) => Some(n - 1),
        };

        self.counter.is_none()
    }
}

#[allow(unused)]
pub struct RunContext<St: Stream<Item = (usize, ArrowMessage)> + Send + Sync> {
    pub task_info: Arc<TaskInfo>,
    pub name: String,
    pub counter: CheckpointCounter,
    pub closed: HashSet<usize>,
    pub sel: InQReader<St>,
    pub in_partitions: usize,
    pub blocked: Vec<St>,
    pub final_message: Option<ArrowMessage>,
    // TODO: ticks
}

#[derive(Encode, Decode, Clone, Debug, PartialEq, Eq)]
pub struct ArrowTimerValue {
    pub time: SystemTime,
    pub key: Vec<u8>,
    pub data: Vec<u8>,
}

pub trait ErasedConstructor: Send {
    fn with_config(
        &self,
        config: &[u8],
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator>;
}

impl<T: OperatorConstructor> ErasedConstructor for T {
    fn with_config(
        &self,
        config: &[u8],
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        self.with_config(prost::Message::decode(config).unwrap(), registry)
    }
}

pub fn get_timestamp_col<'a>(
    batch: &'a RecordBatch,
    ctx: &mut OperatorContext,
) -> &'a PrimitiveArray<TimestampNanosecondType> {
    batch
        .column(ctx.out_schema.as_ref().unwrap().timestamp_index)
        .as_any()
        .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
        .unwrap()
}

pub struct RateLimiter {
    last: Instant,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimiter {
    pub fn new() -> Self {
        RateLimiter {
            last: Instant::now().sub(Duration::from_secs(60)),
        }
    }

    pub async fn rate_limit<F, Fut>(&mut self, f: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()> + Send,
    {
        if self.last.elapsed() > Duration::from_secs(5) {
            f().await;
            self.last = Instant::now();
        }
    }
}
