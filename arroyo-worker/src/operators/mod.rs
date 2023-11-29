use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::{fmt::Debug, path::PathBuf};

use std::marker::PhantomData;
use std::ops::Add;

use crate::engine::{CheckpointCounter, Collector, Context, StreamNode};
use crate::stream_node::ProcessFuncTrait;
use crate::ControlOutcome;
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::{CheckpointMetadata, TableDescriptor};
use arroyo_types::{
    from_millis, to_millis, CheckpointBarrier, Data, GlobalKey, Key, Message, Record, TaskInfo,
    UpdatingData, Watermark, Window,
};
use bincode::{config, Decode, Encode};
use std::time::{Duration, SystemTime};
use tracing::{debug, info};
use wasmtime::{
    Caller, Engine, InstanceAllocationStrategy, Linker, Module, PoolingAllocationConfig, Store,
    TypedFunc,
};
pub mod aggregating_window;
pub mod functions;
pub mod join_with_expiration;
pub mod joins;
pub mod sinks;
pub mod sliding_top_n_aggregating_window;
pub mod tumbling_aggregating_window;
pub mod tumbling_top_n_window;
pub mod updating_aggregate;
pub mod windows;

#[cfg(test)]
mod test {
    use crate::operators::WasmOperator;
    use crate::{engine::Context, operators::TimeWindowAssigner};
    use arroyo_types::{from_millis, to_millis, Message, Record};
    use std::time::{Duration, SystemTime};

    use super::SlidingWindowAssigner;

    #[tokio::test]
    #[ignore]
    async fn test_wasm() {
        let mut operator = WasmOperator::<String, Vec<u64>, String, u64>::in_path(
            "../wasm/test_wasm_fns.wasm",
            "sum_process",
        )
        .unwrap();

        let ts = SystemTime::now();
        let record = Record {
            timestamp: ts,
            key: Some("hello".to_string()),
            value: vec![1u64; 1093345],
        };

        let v = record.value.len();

        let (mut ctx, mut data_rx) = Context::new_for_test();

        operator.on_start(&mut ctx).await;

        operator.process_element(&record, &mut ctx).await;

        let item = data_rx.try_recv().unwrap();
        let result: Message<String, u64> = item.into();

        match result {
            Message::Record(record) => {
                assert_eq!("hello", record.key.unwrap());
                assert_eq!(v as u64, record.value);
            }
            _ => {
                unreachable!("received non-record variant");
            }
        }
    }

    #[test]
    fn test_sliding_window_assignment() {
        let assigner = SlidingWindowAssigner {
            size: Duration::from_secs(5),
            slide: Duration::from_secs(5),
        };
        let start_millis = to_millis(SystemTime::now());
        let truncated_start_millis =
            start_millis - (start_millis % Duration::from_secs(10).as_millis() as u64);
        let start = from_millis(truncated_start_millis);
        assert_eq!(
            1,
            <SlidingWindowAssigner as TimeWindowAssigner<(), ()>>::windows(&assigner, start).len()
        );
    }
}

#[derive(Encode, Decode, Copy, Clone, Debug, PartialEq)]
pub struct PeriodicWatermarkGeneratorState {
    last_watermark_emitted_at: SystemTime,
    max_watermark: SystemTime,
}

#[derive(StreamNode)]
pub struct PeriodicWatermarkGenerator<K: Key, D: Data> {
    interval: Duration,
    watermark_function: Box<dyn Fn(&Record<K, D>) -> SystemTime + Send>,
    state_cache: PeriodicWatermarkGeneratorState,
    idle_time: Option<Duration>,
    last_event: SystemTime,
    idle: bool,
    _t: PhantomData<(K, D)>,
}

#[process_fn(in_k=K, in_t=D, out_k=K, out_t=D, tick_ms=1_000)]
impl<K: Key, D: Data> PeriodicWatermarkGenerator<K, D> {
    pub fn fixed_lateness(
        interval: Duration,
        idle_time: Option<Duration>,
        max_lateness: Duration,
    ) -> PeriodicWatermarkGenerator<K, D> {
        PeriodicWatermarkGenerator {
            interval,
            watermark_function: Box::new(move |record| record.timestamp - max_lateness),
            state_cache: PeriodicWatermarkGeneratorState {
                last_watermark_emitted_at: SystemTime::UNIX_EPOCH,
                max_watermark: SystemTime::UNIX_EPOCH,
            },
            idle_time,
            last_event: SystemTime::now(),
            idle: false,
            _t: PhantomData,
        }
    }

    pub fn watermark_function(
        interval: Duration,
        idle_time: Option<Duration>,
        watermark_function: Box<dyn Fn(&Record<K, D>) -> SystemTime + Send>,
    ) -> Self {
        PeriodicWatermarkGenerator {
            interval,
            watermark_function,
            state_cache: PeriodicWatermarkGeneratorState {
                last_watermark_emitted_at: SystemTime::UNIX_EPOCH,
                max_watermark: SystemTime::UNIX_EPOCH,
            },
            idle_time,
            last_event: SystemTime::now(),
            idle: false,
            _t: PhantomData,
        }
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table(
            "s",
            "periodic watermark generator state",
        )]
    }

    fn name(&self) -> String {
        "periodic_watermark_generator".to_string()
    }

    async fn on_start(&mut self, ctx: &mut Context<K, D>) {
        let gs = ctx.state.get_global_keyed_state('s').await;
        self.last_event = SystemTime::now();

        let state =
            *(gs.get(&ctx.task_info.task_index)
                .unwrap_or(&PeriodicWatermarkGeneratorState {
                    last_watermark_emitted_at: SystemTime::UNIX_EPOCH,
                    max_watermark: SystemTime::UNIX_EPOCH,
                }));

        self.state_cache = state;
    }

    async fn on_close(&mut self, ctx: &mut Context<K, D>) {
        // send final watermark on close
        ctx.collector
            .broadcast(Message::Watermark(Watermark::EventTime(from_millis(
                u64::MAX,
            ))))
            .await;
    }

    async fn process_element(&mut self, record: &Record<K, D>, ctx: &mut Context<K, D>) {
        ctx.collector.collect(record.clone()).await;
        self.last_event = SystemTime::now();

        let watermark = (self.watermark_function)(record);

        self.state_cache.max_watermark = self.state_cache.max_watermark.max(watermark);
        if self.idle
            || record
                .timestamp
                .duration_since(self.state_cache.last_watermark_emitted_at)
                .unwrap_or(Duration::ZERO)
                > self.interval
        {
            debug!(
                "[{}] Emitting watermark {}",
                ctx.task_info.task_index,
                to_millis(watermark)
            );
            ctx.collector
                .broadcast(Message::Watermark(Watermark::EventTime(watermark)))
                .await;
            self.state_cache.last_watermark_emitted_at = record.timestamp;
            self.idle = false;
        }
    }

    async fn handle_checkpoint(&mut self, _: &CheckpointBarrier, ctx: &mut Context<K, D>) {
        let mut gs = ctx.state.get_global_keyed_state('s').await;

        gs.insert(ctx.task_info.task_index, self.state_cache).await;
    }

    async fn handle_tick(&mut self, _: u64, ctx: &mut Context<K, D>) {
        if let Some(idle_time) = self.idle_time {
            if self.last_event.elapsed().unwrap_or(Duration::ZERO) > idle_time && !self.idle {
                info!(
                    "Setting partition {} to idle after {:?}",
                    ctx.task_info.task_index, idle_time
                );
                ctx.broadcast(Message::Watermark(Watermark::Idle)).await;
                self.idle = true;
            }
        }
    }
}

pub trait TimeWindowAssigner<K: Key, T: Data>: Copy + Clone + Send + 'static {
    fn windows(&self, ts: SystemTime) -> Vec<Window>;

    fn next(&self, window: Window) -> Window;

    fn safe_retention_duration(&self) -> Option<Duration>;
}

pub trait WindowAssigner<K: Key, T: Data>: Clone + Send {}

#[derive(Clone, Copy)]
pub struct TumblingWindowAssigner {
    size: Duration,
}

impl<K: Key, T: Data> TimeWindowAssigner<K, T> for TumblingWindowAssigner {
    fn windows(&self, ts: SystemTime) -> Vec<Window> {
        let key = to_millis(ts) / (self.size.as_millis() as u64);
        vec![Window {
            start: from_millis(key * self.size.as_millis() as u64),
            end: from_millis((key + 1) * (self.size.as_millis() as u64)),
        }]
    }

    fn next(&self, window: Window) -> Window {
        Window {
            start: window.end,
            end: window.end + self.size,
        }
    }

    fn safe_retention_duration(&self) -> Option<Duration> {
        Some(self.size)
    }
}
#[derive(Clone, Copy)]
pub struct InstantWindowAssigner {}

impl<K: Key, T: Data> TimeWindowAssigner<K, T> for InstantWindowAssigner {
    fn windows(&self, ts: SystemTime) -> Vec<Window> {
        vec![Window {
            start: ts,
            end: ts + Duration::from_nanos(1),
        }]
    }

    fn next(&self, window: Window) -> Window {
        Window {
            start: window.start + Duration::from_micros(1),
            end: window.end + Duration::from_micros(1),
        }
    }

    fn safe_retention_duration(&self) -> Option<Duration> {
        Some(Duration::ZERO)
    }
}

#[derive(Copy, Clone)]
pub struct SlidingWindowAssigner {
    size: Duration,
    slide: Duration,
}
//  012345678
//  --x------
// [--x]
//  [-x-]
//   [x--]
//    [---]

impl SlidingWindowAssigner {
    fn start(&self, ts: SystemTime) -> SystemTime {
        let ts_millis = to_millis(ts);
        let earliest_window_start = ts_millis - self.size.as_millis() as u64;

        let remainder = earliest_window_start % (self.slide.as_millis() as u64);

        from_millis(earliest_window_start - remainder + self.slide.as_millis() as u64)
    }
}

impl<K: Key, T: Data> TimeWindowAssigner<K, T> for SlidingWindowAssigner {
    fn windows(&self, ts: SystemTime) -> Vec<Window> {
        let mut windows =
            Vec::with_capacity(self.size.as_millis() as usize / self.slide.as_millis() as usize);

        let mut start = self.start(ts);

        while start <= ts {
            windows.push(Window {
                start,
                end: start + self.size,
            });
            start += self.slide;
        }

        windows
    }

    fn next(&self, window: Window) -> Window {
        let start_time = window.start + self.slide;
        Window {
            start: start_time,
            end: start_time + self.size,
        }
    }

    fn safe_retention_duration(&self) -> Option<Duration> {
        Some(self.size)
    }
}

struct WasmOperatorEnv<K: Key, T: Data> {
    //ctx: Arc<Mutex<Option<Context<K, T>>>>,
    ctx: Option<Collector<K, T>>,
    record: Option<Vec<u8>>,
}

#[derive(StreamNode)]
pub struct WasmOperator<InKey: Key, InT: Data, OutK: Key + 'static, OutT: Data + 'static> {
    name: String,
    wasm_fn: Option<TypedFunc<u32, ()>>,
    module: Module,
    store: Store<WasmOperatorEnv<OutK, OutT>>,
    _key_t: PhantomData<InKey>,
    _data_t: PhantomData<InT>,
}

#[process_fn(in_k=InKey, in_t=InT, out_k=OutKey, out_t=OutT)]
impl<InKey: Key, InT: Data, OutKey: Key, OutT: Data> WasmOperator<InKey, InT, OutKey, OutT> {
    pub fn new(
        name: &str,
    ) -> Result<WasmOperator<InKey, InT, OutKey, OutT>, Box<dyn std::error::Error>> {
        Self::in_path("wasm_fns_bg.wasm", name)
    }

    pub fn in_path(
        path: &str,
        name: &str,
    ) -> Result<WasmOperator<InKey, InT, OutKey, OutT>, Box<dyn std::error::Error>> {
        let env = WasmOperatorEnv {
            ctx: None,
            record: None,
        };

        let mut config = wasmtime::Config::default();
        config.wasm_memory64(true);
        config.async_support(true);
        config.memory_init_cow(true);
        let mut pooling_config = PoolingAllocationConfig::default();
        pooling_config
            .instance_count(100)
            .instance_size(1 << 20)
            .instance_tables(1)
            .instance_table_elements(10_000)
            .instance_memories(1)
            .instance_memory_pages(300);
        config.allocation_strategy(InstanceAllocationStrategy::Pooling(pooling_config));

        let engine = Engine::new(&config)?;
        let store = Store::new(&engine, env);

        debug!("Loading WASM from {:?}", PathBuf::from_str(path).unwrap());
        let wasm_bytes = fs::read(path).expect("could not find module");

        let module = Module::from_binary(store.engine(), &wasm_bytes)?;

        Ok(WasmOperator {
            name: name.to_string(),
            module,
            wasm_fn: None,
            store,
            _key_t: PhantomData,
            _data_t: PhantomData,
        })
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    async fn on_start(&mut self, _ctx: &mut Context<OutKey, OutT>) {
        let mut linker = Linker::new(self.store.engine());
        linker
            .func_wrap2_async(
                "env",
                "send",
                |mut caller: Caller<'_, WasmOperatorEnv<OutKey, OutT>>, ptr: u32, len: u32| {
                    Box::new(async move {
                        let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
                        let slice = &mem.data(&caller)[ptr as usize..(ptr + len) as usize];
                        let (record, _): (Record<OutKey, OutT>, usize) =
                            bincode::decode_from_slice(slice, config::standard()).unwrap();

                        caller
                            .data_mut()
                            .ctx
                            .as_mut()
                            .unwrap()
                            .collect(record)
                            .await;

                        0i32
                    })
                },
            )
            .unwrap();

        linker
            .func_wrap(
                "env",
                "fetch_data",
                |mut caller: Caller<'_, WasmOperatorEnv<OutKey, OutT>>, ptr: u32| {
                    let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
                    let bytes = caller.data_mut().record.take().unwrap();

                    mem.data_mut(&mut caller)[ptr as usize..ptr as usize + bytes.len()]
                        .copy_from_slice(&bytes);

                    0i32
                },
            )
            .unwrap();

        let instance = linker
            .instantiate_async(&mut self.store, &self.module)
            .await
            .unwrap();

        let wasm_fn = instance
            .get_typed_func::<u32, ()>(&mut self.store, &self.name)
            .unwrap();

        self.wasm_fn = Some(wasm_fn);
    }

    async fn process_element(
        &mut self,
        record: &Record<InKey, InT>,
        ctx: &mut Context<OutKey, OutT>,
    ) {
        let bytes = bincode::encode_to_vec(record, config::standard()).unwrap();
        let len = bytes.len();
        self.store.data_mut().record = Some(bytes);
        // TODO: is this clone correct?
        self.store.data_mut().ctx = Some(ctx.collector.clone());

        if let Err(e) = self
            .wasm_fn
            .unwrap()
            .call_async(&mut self.store, len as u32)
            .await
        {
            eprintln!("Wasm runtime error in {}:\n{:?}", self.name, e);
            eprintln!("bytes_len = {}", len);
        }

        self.store.data_mut().ctx = None;
    }
}

#[derive(StreamNode)]
pub struct ToGlobalOperator<K: Key, V: Data> {
    _t: PhantomData<(K, V)>,
}

#[process_fn(in_k = K, in_t = V, out_k = GlobalKey, out_t = V)]
impl<K: Key, V: Data> ToGlobalOperator<K, V> {
    pub fn new() -> Self {
        ToGlobalOperator { _t: PhantomData }
    }

    fn name(&self) -> String {
        "ToGlobal".to_string()
    }

    async fn process_element(&mut self, record: &Record<K, V>, ctx: &mut Context<GlobalKey, V>) {
        let value = record.value.clone();
        ctx.collector
            .collect(Record {
                timestamp: record.timestamp,
                key: Some(GlobalKey {}),
                value,
            })
            .await;
    }
}

#[derive(StreamNode)]
pub struct FlattenOperator<K: Key, V: Data> {
    pub name: String,
    _t: PhantomData<(K, V)>,
}
#[process_fn(in_k = K, in_t = Vec<V>, out_k = K, out_t = V)]
impl<K: Key, V: Data> FlattenOperator<K, V> {
    pub fn new(name: String) -> Self {
        FlattenOperator {
            name,
            _t: PhantomData,
        }
    }
    fn name(&self) -> String {
        self.name.clone()
    }
    async fn process_element(&mut self, record: &Record<K, Vec<V>>, ctx: &mut Context<K, V>) {
        let record = record.clone();
        for val in record.value {
            let unrolled_record = Record {
                timestamp: record.timestamp,
                key: record.key.clone(),
                value: val,
            };
            ctx.collector.collect(unrolled_record).await;
        }
    }
}
pub struct MapOperator<InKey: Key, InT: Data, OutKey: Key, OutT: Data> {
    pub name: String,
    pub map_fn: Box<dyn Fn(&Record<InKey, InT>, &TaskInfo) -> Record<OutKey, OutT> + Send>,
}

#[async_trait::async_trait]
impl<InKey: Key, InT: Data, OutKey: Key, OutT: Data> ProcessFuncTrait
    for MapOperator<InKey, InT, OutKey, OutT>
{
    type InKey = InKey;
    type InT = InT;
    type OutKey = OutKey;
    type OutT = OutT;
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_element(
        &mut self,
        record: &Record<InKey, InT>,
        ctx: &mut Context<OutKey, OutT>,
    ) {
        let record = (self.map_fn)(record, &ctx.task_info);
        ctx.collector.collect(record).await;
    }
}

#[derive(StreamNode)]
pub struct OptionMapOperator<InKey: Key, InT: Data, OutKey: Key, OutT: Data> {
    pub name: String,
    pub map_fn: Box<dyn Fn(&Record<InKey, InT>, &TaskInfo) -> Option<Record<OutKey, OutT>> + Send>,
}

#[process_fn(in_k = InKey, in_t = InT, out_k = OutKey, out_t = OutT)]
impl<InKey: Key, InT: Data, OutKey: Key, OutT: Data> OptionMapOperator<InKey, InT, OutKey, OutT> {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_element(
        &mut self,
        record: &Record<InKey, InT>,
        ctx: &mut Context<OutKey, OutT>,
    ) {
        let option = (self.map_fn)(record, &ctx.task_info);
        if let Some(record) = option {
            ctx.collector.collect(record).await;
        }
    }
}

impl<K: Key, InT: Data, OutT: Data> OptionMapOperator<K, UpdatingData<InT>, K, UpdatingData<OutT>> {
    pub fn updating_operator(name: String, map_fn: fn(&InT) -> Option<OutT>) -> Self {
        Self {
            name,
            map_fn: Box::new(move |record, _task_info| {
                let output_value = updating_data_map_function(&record.value, map_fn);
                output_value.map(|value| Record {
                    timestamp: record.timestamp,
                    key: record.key.clone(),
                    value,
                })
            }),
        }
    }
}

#[derive(StreamNode)]
pub struct KeyMapUpdatingOperator<T: Data, OutKey: Key> {
    pub name: String,
    pub map_fn: Box<dyn Fn(&T) -> OutKey + Send>,
}

#[process_fn(in_k = (), in_t = UpdatingData<T>, out_k = OutKey, out_t = UpdatingData<T>)]
impl<T: Data, OutKey: Key> KeyMapUpdatingOperator<T, OutKey> {
    fn name(&self) -> String {
        self.name.clone()
    }

    pub fn new(name: String, map_fn: fn(&T) -> OutKey) -> Self {
        Self {
            name,
            map_fn: Box::new(map_fn),
        }
    }

    async fn process_element(
        &mut self,
        record: &Record<(), UpdatingData<T>>,
        ctx: &mut Context<OutKey, UpdatingData<T>>,
    ) {
        let records = match &record.value {
            UpdatingData::Retract(retract) => {
                vec![Record {
                    timestamp: record.timestamp,
                    key: Some((self.map_fn)(retract)),
                    value: UpdatingData::Retract(retract.clone()),
                }]
            }
            UpdatingData::Update { old, new } => {
                let old_key = (self.map_fn)(old);
                let new_key = (self.map_fn)(new);
                if old_key == new_key {
                    vec![Record {
                        timestamp: record.timestamp,
                        key: Some(old_key),
                        value: UpdatingData::Update {
                            old: old.clone(),
                            new: new.clone(),
                        },
                    }]
                } else {
                    let old_record = Record {
                        timestamp: record.timestamp,
                        key: Some(old_key),
                        value: UpdatingData::Retract(old.clone()),
                    };
                    let new_record = Record {
                        timestamp: record.timestamp,
                        key: Some(new_key),
                        value: UpdatingData::Append(new.clone()),
                    };
                    vec![new_record, old_record]
                }
            }
            UpdatingData::Append(append) => {
                vec![Record {
                    timestamp: record.timestamp,
                    key: Some((self.map_fn)(append)),
                    value: UpdatingData::Append(append.clone()),
                }]
            }
        };
        for record in records {
            ctx.collector.collect(record).await;
        }
    }
}

// general function for handling UpdatingData in an OptionMapOperator
pub fn updating_data_map_function<InT: Data, OutT: Data>(
    update: &UpdatingData<InT>,
    map_fn: fn(&InT) -> Option<OutT>,
) -> Option<UpdatingData<OutT>> {
    match update {
        UpdatingData::Retract(data) => {
            let data = map_fn(data);
            data.map(UpdatingData::Retract)
        }
        UpdatingData::Update { old, new } => {
            let old = map_fn(old);
            let new = map_fn(new);
            match (old, new) {
                (None, None) => None,
                (None, Some(new)) => Some(UpdatingData::Append(new)),
                (Some(old), None) => Some(UpdatingData::Retract(old)),
                (Some(old), Some(new)) => {
                    if old == new {
                        None
                    } else {
                        Some(UpdatingData::Update { old, new })
                    }
                }
            }
        }
        UpdatingData::Append(data) => {
            let data = map_fn(data);
            data.map(UpdatingData::Append)
        }
    }
}

#[derive(StreamNode)]
pub struct ArrayMapOperator<InKey: Key, InT: Data, OutKey: Key, OutT: Data> {
    pub name: String,
    pub map_fn: Box<dyn Fn(&Record<InKey, InT>, &TaskInfo) -> Option<Record<OutKey, OutT>> + Send>,
}

#[process_fn(in_k = InKey, in_t = Vec<InT>, out_k = OutKey, out_t = OutT)]
impl<InKey: Key, InT: Data, OutKey: Key, OutT: Data> ArrayMapOperator<InKey, InT, OutKey, OutT> {
    fn name(&self) -> String {
        self.name.clone()
    }
    async fn process_element(
        &mut self,
        record: &Record<InKey, Vec<InT>>,
        ctx: &mut Context<OutKey, OutT>,
    ) {
        let record = record.clone();
        for val in record.value {
            let unrolled_record = Record {
                timestamp: record.timestamp,
                key: record.key.clone(),
                value: val,
            };
            let result = (self.map_fn)(&unrolled_record, &ctx.task_info);
            if let Some(result) = result {
                ctx.collector.collect(result).await
            }
        }
    }
}

#[derive(StreamNode)]
pub struct FlatMapOperator<InKey: Key, InT: Data, OutKey: Key, OutT: Data> {
    pub name: String,
    pub flat_map: Box<dyn Fn(&Record<InKey, InT>, &TaskInfo) -> Vec<Record<OutKey, OutT>> + Send>,
}

#[process_fn(in_k = InKey, in_t = InT, out_k = OutKey, out_t = OutT)]
impl<InKey: Key, InT: Data, OutKey: Key, OutT: Data> FlatMapOperator<InKey, InT, OutKey, OutT> {
    fn name(&self) -> String {
        self.name.clone()
    }
    async fn process_element(
        &mut self,
        record: &Record<InKey, InT>,
        ctx: &mut Context<OutKey, OutT>,
    ) {
        let outputs = (self.flat_map)(record, &ctx.task_info);
        for rec in outputs {
            ctx.collector.collect(rec).await
        }
    }
}

#[derive(StreamNode)]
pub struct FilterOperator<K: Key, T: Data> {
    pub name: String,
    pub predicate_fn: Box<dyn Fn(&Record<K, T>, &TaskInfo) -> bool + Send>,
}

#[process_fn(in_k = K, in_t = T, out_k = K , out_t = T)]
impl<K: Key, T: Data> FilterOperator<K, T> {
    fn name(&self) -> String {
        self.name.clone()
    }
    async fn process_element(&mut self, record: &Record<K, T>, ctx: &mut Context<K, T>) {
        if (self.predicate_fn)(record, &ctx.task_info) {
            ctx.collector.collect(record.clone()).await;
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum AggregateBehavior {
    Sum,
    Min,
    Max,
}

#[derive(StreamNode)]
pub struct AggregateOperator<InKey: Key, N: Add<Output = N> + Ord + Copy + Data> {
    behavior: AggregateBehavior,
    _t: PhantomData<(InKey, N)>,
}

#[process_fn(in_k = InKey, in_t = Vec<N>, out_k = InKey, out_t = Option<N>)]
impl<InKey: Key, N: Add<Output = N> + Ord + Copy + Data> AggregateOperator<InKey, N> {
    pub fn new(behavior: AggregateBehavior) -> AggregateOperator<InKey, N> {
        Self {
            behavior,
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        format!("{:?}", self.behavior)
    }

    async fn process_element(
        &mut self,
        record: &Record<InKey, Vec<N>>,
        ctx: &mut Context<InKey, Option<N>>,
    ) {
        let v = match self.behavior {
            AggregateBehavior::Sum => {
                if record.value.is_empty() {
                    None
                } else {
                    Some(
                        record.value[1..]
                            .iter()
                            .fold(record.value[0], |s, t| s + *t),
                    )
                }
            }
            AggregateBehavior::Min => record.value.iter().min().copied(),
            AggregateBehavior::Max => record.value.iter().max().copied(),
        };

        let record = Record {
            timestamp: record.timestamp,
            key: record.key.clone(),
            value: v,
        };

        ctx.collector.collect(record).await
    }
}

#[derive(StreamNode)]
pub struct CountOperator<K: Key, V: Data> {
    _t: PhantomData<(K, V)>,
}

#[process_fn(in_k = K, in_t = Vec<V>, out_k = K, out_t = usize)]
impl<K: Key, V: Data> CountOperator<K, V> {
    pub fn new() -> Self {
        Self { _t: PhantomData }
    }

    fn name(&self) -> String {
        "Count".to_string()
    }

    async fn process_element(&mut self, record: &Record<K, Vec<V>>, ctx: &mut Context<K, usize>) {
        let key = record.key.clone();
        let value = record.value.len();
        ctx.collector
            .collect(Record {
                timestamp: record.timestamp,
                key,
                value,
            })
            .await;
    }
}

#[derive(StreamNode)]
pub struct AggregateFunctionOperator<InKey: Key, N: Data + Copy + Ord> {
    pub name: String,
    pub map_fn: Box<dyn Fn(&Record<InKey, Vec<N>>, &TaskInfo) -> Option<N> + Send>,
}

#[process_fn(in_k = InKey, in_t = Vec<N>, out_k = InKey, out_t = Option<N>)]
impl<InKey: Key, N: Ord + Copy + Data> AggregateFunctionOperator<InKey, N> {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn process_element(
        &mut self,
        record: &Record<InKey, Vec<N>>,
        ctx: &mut Context<InKey, Option<N>>,
    ) {
        let aggregate_value = (self.map_fn)(record, &ctx.task_info);
        let output = Record {
            timestamp: record.timestamp,
            key: record.key.clone(),
            value: aggregate_value,
        };
        ctx.collector.collect(output).await;
    }
}
