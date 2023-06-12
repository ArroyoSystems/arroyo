use std::fs;
use std::str::FromStr;
use std::{fmt::Debug, path::PathBuf};

use std::marker::PhantomData;
use std::ops::Add;

use crate::engine::{Collector, Context, StreamNode};
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::TableDescriptor;
use arroyo_types::{
    from_millis, to_millis, CheckpointBarrier, Data, GlobalKey, Key, Message, Record, TaskInfo,
    Window,
};
use bincode::{config, Decode, Encode};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::time::{Duration, SystemTime};
use tracing::debug;
use wasmtime::{
    Caller, Engine, InstanceAllocationStrategy, InstanceLimits, Linker, Module,
    PoolingAllocationStrategy, Store, TypedFunc,
};
pub mod aggregating_window;
pub mod functions;
pub mod join_with_expiration;
pub mod joins;
pub mod sinks;
pub mod sliding_top_n_aggregating_window;
pub mod sources;
pub mod tumbling_aggregating_window;
pub mod tumbling_top_n_window;
pub mod windows;

pub struct UserError {
    pub name: String,
    pub details: String,
}

impl UserError {
    pub fn new(name: impl Into<String>, details: impl Into<String>) -> UserError {
        UserError {
            name: name.into(),
            details: details.into(),
        }
    }
}

#[derive(Clone, Copy)]
pub enum SerializationMode {
    Json,
    // https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
    JsonSchemaRegistry,
    RawJson,
}

impl SerializationMode {
    pub fn deserialize_slice<T: DeserializeOwned>(&self, msg: &[u8]) -> Result<T, UserError> {
        match self {
            SerializationMode::Json => serde_json::from_slice(msg)
                .map_err(|err|
                    UserError::new("Deserialization error", format!("Failed to deserialize message '{}' from json, with error {}",
                        String::from_utf8_lossy(msg), err))),
            SerializationMode::JsonSchemaRegistry => serde_json::from_slice(&msg[5..])
                .map_err(|err|
                    UserError::new("Deserialization error", format!("Failed to deserialize message '{}' from confluent schema registry json, with error {}",
                        String::from_utf8_lossy(msg), err))),
            SerializationMode::RawJson => {
                let s = String::from_utf8_lossy(&msg);
                let j = json! {
                    { "value": s }
                };

                // TODO: this is inefficient, because we know that T is RawJson in this case and can much more directly
                //  produce that value. However, without specialization I don't know how to get the compiler to emit
                //  the optimized code that case.
                serde_json::from_value(j)
                    .map_err(|e| UserError::new("Deserialization error", format!("Could not represent data as RawJson: {:?}", e)))
            },
        }
    }

    pub fn deserialize_str<T: DeserializeOwned>(&self, msg: &str) -> Result<T, UserError> {
        match self {
            SerializationMode::Json => serde_json::from_str(msg).map_err(|err| {
                UserError::new(
                    "Deserialization error",
                    format!(
                        "Failed to deserialize message '{}' from json, with error {}",
                        msg, err
                    ),
                )
            }),
            SerializationMode::JsonSchemaRegistry => {
                panic!("cannot read json schema registry data from str")
            }
            SerializationMode::RawJson => {
                let j = json! {
                    { "value": msg }
                };

                // TODO: this is inefficient, because we know that T is RawJson in this case and can much more directly
                //  produce that value. However, without specialization I don't know how to get the compiler to emit
                //  the optimized code that case.
                serde_json::from_value(j).map_err(|e| {
                    UserError::new(
                        "Deserialization error",
                        format!("Could not represent data as RawJson: {:?}", e),
                    )
                })
            }
        }
    }
}

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
    _t: PhantomData<(K, D)>,
}

#[process_fn(in_k=K, in_t=D, out_k=K, out_t=D)]
impl<K: Key, D: Data> PeriodicWatermarkGenerator<K, D> {
    pub fn fixed_lateness(
        interval: Duration,
        max_lateness: Duration,
    ) -> PeriodicWatermarkGenerator<K, D> {
        PeriodicWatermarkGenerator {
            interval,
            watermark_function: Box::new(move |record| record.timestamp - max_lateness),
            state_cache: PeriodicWatermarkGeneratorState {
                last_watermark_emitted_at: SystemTime::UNIX_EPOCH,
                max_watermark: SystemTime::UNIX_EPOCH,
            },
            _t: PhantomData,
        }
    }

    pub fn watermark_function(
        interval: Duration,
        watermark_function: Box<dyn Fn(&Record<K, D>) -> SystemTime + Send>,
    ) -> Self {
        PeriodicWatermarkGenerator {
            interval,
            watermark_function,
            state_cache: PeriodicWatermarkGeneratorState {
                last_watermark_emitted_at: SystemTime::UNIX_EPOCH,
                max_watermark: SystemTime::UNIX_EPOCH,
            },
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
            .broadcast(Message::Watermark(from_millis(u64::MAX)))
            .await;
    }

    async fn process_element(&mut self, record: &Record<K, D>, ctx: &mut Context<K, D>) {
        ctx.collector.collect(record.clone()).await;

        let watermark = (self.watermark_function)(record);

        self.state_cache.max_watermark = self.state_cache.max_watermark.max(watermark);
        if record
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
            ctx.collector.broadcast(Message::Watermark(watermark)).await;
            self.state_cache.last_watermark_emitted_at = record.timestamp;
        }
    }

    async fn handle_checkpoint(&mut self, _: &CheckpointBarrier, ctx: &mut Context<K, D>) {
        let mut gs = ctx.state.get_global_keyed_state('s').await;

        gs.insert(ctx.task_info.task_index, self.state_cache).await;
    }
}

pub trait TimeWindowAssigner<K: Key, T: Data>: Copy + Clone + Send + 'static {
    fn windows(&self, ts: SystemTime) -> Vec<Window>;

    fn next(&self, window: Window) -> Window;

    fn safe_retention_duration(&self) -> Option<Duration>;
}

#[derive(Clone, Copy)]
pub struct TumblingWindowAssigner {
    size: Duration,
}

impl<K: Key, T: Data> TimeWindowAssigner<K, T> for TumblingWindowAssigner {
    fn windows(&self, ts: SystemTime) -> Vec<Window> {
        let key = to_millis(ts) / (self.size.as_millis() as u64);
        vec![Window {
            start_time: from_millis(key * self.size.as_millis() as u64),
            end_time: from_millis((key + 1) * (self.size.as_millis() as u64)),
        }]
    }

    fn next(&self, window: Window) -> Window {
        Window {
            start_time: window.end_time,
            end_time: window.end_time + self.size,
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
            start_time: ts,
            end_time: ts + Duration::from_nanos(1),
        }]
    }

    fn next(&self, window: Window) -> Window {
        Window {
            start_time: window.start_time + Duration::from_micros(1),
            end_time: window.end_time + Duration::from_micros(1),
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
                start_time: start,
                end_time: start + self.size,
            });
            start += self.slide;
        }

        windows
    }

    fn next(&self, window: Window) -> Window {
        let start_time = window.start_time + self.slide;
        Window {
            start_time,
            end_time: start_time + self.size,
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
        //config.allocation_strategy(InstanceAllocationStrategy::pooling());
        config.allocation_strategy(InstanceAllocationStrategy::Pooling {
            strategy: PoolingAllocationStrategy::ReuseAffinity,
            instance_limits: InstanceLimits {
                count: 100,
                size: 1 << 20,
                tables: 1,
                table_elements: 10_000,
                memories: 1,
                memory_pages: 300,
            },
        });
        //config.static_memory_maximum_size(0);

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
            .get_typed_func::<u32, (), _>(&mut self.store, &self.name)
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

#[derive(StreamNode)]
pub struct MapOperator<InKey: Key, InT: Data, OutKey: Key, OutT: Data> {
    pub name: String,
    pub map_fn: Box<dyn Fn(&Record<InKey, InT>, &TaskInfo) -> Record<OutKey, OutT> + Send>,
}

#[process_fn(in_k = InKey, in_t = InT, out_k = OutKey, out_t = OutT)]
impl<InKey: Key, InT: Data, OutKey: Key, OutT: Data> MapOperator<InKey, InT, OutKey, OutT> {
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

#[derive(StreamNode)]
pub struct FlatMapOperator<InKey: Key, InT: Data, OutKey: Key, OutT: Data> {
    pub name: String,
    pub map_fn: Box<dyn Fn(&Record<InKey, InT>, &TaskInfo) -> Option<Record<OutKey, OutT>> + Send>,
}

#[process_fn(in_k = InKey, in_t = Vec<InT>, out_k = OutKey, out_t = OutT)]
impl<InKey: Key, InT: Data, OutKey: Key, OutT: Data> FlatMapOperator<InKey, InT, OutKey, OutT> {
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
