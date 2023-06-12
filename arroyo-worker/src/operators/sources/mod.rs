use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use std::fmt::Debug;
use std::io;
use std::io::BufRead;

use crate::engine::{Context, StreamNode};
use crate::SourceFinishType;
use arroyo_macro::source_fn;
use arroyo_rpc::grpc::{StopMode, TableDescriptor};
use arroyo_rpc::ControlMessage;
use arroyo_state::tables::GlobalKeyedState;
use arroyo_types::*;
use bincode::{Decode, Encode};
use serde::de::DeserializeOwned;
use std::time::{Duration, Instant, SystemTime};
use tokio::select;
use tokio::time::sleep;
use tracing::{debug, info};

pub mod eventsource;
pub mod nexmark;

#[derive(Encode, Decode, Debug, Copy, Clone, Eq, PartialEq)]
pub struct ImpulseSourceState {
    counter: usize,
    start_time: SystemTime,
}

#[derive(Debug, Clone, Copy)]
pub enum ImpulseSpec {
    Delay(Duration),
    EventsPerSecond(f32),
}

#[derive(StreamNode, Debug)]
pub struct ImpulseSourceFunc {
    interval: Option<Duration>,
    spec: ImpulseSpec,
    limit: usize,
    state: ImpulseSourceState,
}

#[source_fn(out_t = ImpulseEvent)]
impl ImpulseSourceFunc {
    /* This method is mainly useful for testing,
      so you can always ensure it starts at the beginning of an interval.
    */
    pub fn new_aligned(
        interval: Duration,
        spec: ImpulseSpec,
        alignment_duration: Duration,
        limit: usize,
        start_time: SystemTime,
    ) -> ImpulseSourceFunc {
        let start_millis = to_millis(start_time);
        let truncated_start_millis =
            start_millis - (start_millis % alignment_duration.as_millis() as u64);
        let start_time = from_millis(truncated_start_millis);
        Self::new(Some(interval), spec, limit, start_time)
    }

    pub fn new(
        interval: Option<Duration>,
        spec: ImpulseSpec,
        limit: usize,
        start_time: SystemTime,
    ) -> ImpulseSourceFunc {
        ImpulseSourceFunc {
            interval,
            spec,
            limit,
            state: ImpulseSourceState {
                counter: 0,
                start_time,
            },
        }
    }

    fn name(&self) -> String {
        "impulse-source".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("i", "impulse source state")]
    }

    async fn on_start(&mut self, ctx: &mut Context<(), ImpulseEvent>) {
        let s = ctx
            .state
            .get_global_keyed_state::<usize, ImpulseSourceState>('i')
            .await;

        if let Some(state) = s.get(&ctx.task_info.task_index) {
            self.state = *state;
        }
    }

    async fn run(&mut self, ctx: &mut Context<(), ImpulseEvent>) -> SourceFinishType {
        let delay = match self.spec {
            ImpulseSpec::Delay(d) => d,
            ImpulseSpec::EventsPerSecond(eps) => {
                Duration::from_secs_f32(1.0 / (eps / ctx.task_info.parallelism as f32))
            }
        };

        while self.state.counter < self.limit {
            let timestamp = self
                .interval
                .map(|d| self.state.start_time + d * self.state.counter as u32)
                .unwrap_or_else(SystemTime::now);
            ctx.collect(Record {
                timestamp,
                key: None,
                value: ImpulseEvent {
                    counter: self.state.counter as u64,
                    subtask_index: ctx.task_info.task_index as u64,
                },
            })
            .await;

            self.state.counter += 1;
            ctx.state
                .get_global_keyed_state('i')
                .await
                .insert(ctx.task_info.task_index, self.state)
                .await;

            match ctx.control_rx.try_recv() {
                Ok(ControlMessage::Checkpoint(c)) => {
                    // checkpoint our state
                    debug!("starting checkpointing {}", ctx.task_info.task_index);
                    if self.checkpoint(c, ctx).await {
                        return SourceFinishType::Immediate;
                    }
                }
                Ok(ControlMessage::Stop { mode }) => {
                    info!("Stopping impulse source {:?}", mode);

                    match mode {
                        StopMode::Graceful => {
                            return SourceFinishType::Graceful;
                        }
                        StopMode::Immediate => {
                            return SourceFinishType::Immediate;
                        }
                    }
                }
                Err(_) => {
                    // no messages
                }
            }

            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }

        SourceFinishType::Final
    }
}

#[derive(StreamNode)]
pub struct FileSourceFunc<T>
where
    T: DeserializeOwned + Data,
{
    input_files: Vec<PathBuf>,
    interval: Duration,
    state: HashMap<PathBuf, usize>,
    _t: PhantomData<T>,
}

#[source_fn(out_t = T)]
impl<T> FileSourceFunc<T>
where
    T: DeserializeOwned + Data,
{
    pub fn from_dir(dir: &Path, interval: Duration) -> Self {
        info!("Creating FileSourceFunc from dir {:?}", dir);

        if !dir.is_dir() {
            panic!("{:?} is not dir", dir);
        }

        let input_files = dir
            .read_dir()
            .unwrap()
            .filter_map(|f| f.ok())
            .filter(|f| f.metadata().map(|m| m.is_file()).unwrap_or(false))
            .map(|f| f.path())
            .collect();

        Self {
            input_files,
            interval,
            state: HashMap::new(),
            _t: PhantomData,
        }
    }

    pub fn from_files(input_files: Vec<PathBuf>, interval: Duration) -> Self {
        info!("Creating FileSourceFunc from files {:?}", input_files);
        Self {
            input_files,
            interval,
            state: HashMap::new(),
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        "FileSource".to_string()
    }

    async fn on_start(&mut self, ctx: &mut Context<(), T>) {
        self.state = {
            let mut store: GlobalKeyedState<String, _, _> =
                ctx.state.get_global_keyed_state('f').await;
            let state = store.get_all();
            let state: HashMap<PathBuf, usize> = state
                .into_iter()
                .map(|v: &(PathBuf, usize)| (*v).clone())
                .collect();
            state
        };
    }

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
        let input_files: Vec<PathBuf> = self
            .input_files
            .iter()
            .enumerate()
            .filter(|(i, _)| i % ctx.task_info.parallelism == ctx.task_info.task_index)
            .map(|(_, x)| x.clone())
            .collect();

        let interval = self.interval;
        // TODO: split input files amongst tasks
        for path in input_files {
            if self.state.get(&path) == Some(&usize::MAX) {
                info!("skipping already read file: {:?}", path);
                continue;
            }

            let to_skip = *self.state.get(&path).unwrap_or(&0);

            let file = File::open(&path).unwrap();

            let mut last_check = Instant::now();
            let mut i = 0;
            for line in io::BufReader::new(file).lines() {
                while i < to_skip {
                    i += 1;
                    continue;
                }

                let s = line.unwrap();
                let value = serde_json::from_str(&s).unwrap();
                ctx.collector
                    .collect(Record::<(), T>::from_value(SystemTime::now(), value).unwrap())
                    .await;

                i += 1;

                if !interval.is_zero() || last_check.elapsed() > Duration::from_millis(500) {
                    {
                        let mut store = ctx.state.get_global_keyed_state('f').await;
                        store
                            .insert(path.to_string_lossy().to_string(), (path.clone(), i))
                            .await;
                    }
                    select! {
                        val = ctx.control_rx.recv() => {
                            match val {
                                Some(ControlMessage::Checkpoint(c)) => {
                                    // checkpoint our state
                                    debug!("starting checkpointing {}", ctx.task_info.task_index);
                                    if self.checkpoint(c, ctx).await {
                                        return SourceFinishType::Immediate;
                                    }
                                },
                                Some(ControlMessage::Stop { mode }) => {
                                    debug!("Stopping file source {:?}", mode);

                                    match mode {
                                        StopMode::Graceful => {
                                            return SourceFinishType::Graceful;
                                        }
                                        StopMode::Immediate => {
                                            return SourceFinishType::Immediate;
                                        }
                                    }
                                }
                                _ => {},
                            }
                        },
                        _val = sleep(interval) => {
                            // continue
                        }
                    }
                    last_check = Instant::now();
                }
            }
        }

        info!("file source finished");
        SourceFinishType::Final
    }
}

#[derive(Encode, Decode)]
struct FileSourceState {
    completed: HashSet<PathBuf>,
    current_file: Option<(PathBuf, usize)>,
}
