use crate::context::{ArrowContext, BatchReceiver};
use crate::inq_reader::InQReader;
use crate::udfs::{ArroyoUdaf, UdafArg};
use crate::{CheckpointCounter, ControlOutcome, SourceFinishType};
use anyhow::anyhow;
use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use arroyo_datastream::logical::DylibUdfConfig;
use arroyo_metrics::TaskCounters;
use arroyo_rpc::grpc::{TableConfig, TaskCheckpointEventType};
use arroyo_rpc::{ControlMessage, ControlResp};
use arroyo_storage::StorageProvider;
use arroyo_types::{ArrowMessage, CheckpointBarrier, SignalMessage, Watermark};
use arroyo_udf_host::parse::inner_type;
use arroyo_udf_host::{ContainerOrLocal, LocalUdf, SyncUdfDylib, UdfDylib, UdfInterface};
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::{
    create_udaf, AggregateUDF, ScalarUDF, Signature, TypeSignature, Volatility, WindowUDF,
};
use dlopen2::wrapper::Container;
use futures::future::OptionFuture;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Barrier;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn, Instrument};

pub trait OperatorConstructor: Send {
    type ConfigT: prost::Message + Default;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<OperatorNode>;
}

pub enum OperatorNode {
    Source(Box<dyn SourceOperator + Send>),
    Operator(Box<dyn ArrowOperator + Send>),
}

// TODO: this is required currently because the FileSystemSink code isn't sync
unsafe impl Sync for OperatorNode {}

impl OperatorNode {
    pub fn from_source(source: Box<dyn SourceOperator + Send>) -> Self {
        OperatorNode::Source(source)
    }

    pub fn from_operator(operator: Box<dyn ArrowOperator + Send>) -> Self {
        OperatorNode::Operator(operator)
    }

    pub fn name(&self) -> String {
        match self {
            OperatorNode::Source(s) => s.name(),
            OperatorNode::Operator(s) => s.name(),
        }
    }

    pub fn tables(&self) -> HashMap<String, TableConfig> {
        match self {
            OperatorNode::Source(s) => s.tables(),
            OperatorNode::Operator(s) => s.tables(),
        }
    }

    async fn run_behavior(
        &mut self,
        ctx: &mut ArrowContext,
        in_qs: &mut [BatchReceiver],
        ready: Arc<Barrier>,
    ) -> Option<SignalMessage> {
        match self {
            OperatorNode::Source(s) => {
                s.on_start(ctx).await;

                ready.wait().await;
                info!(
                    "Running source {}-{}",
                    ctx.task_info.operator_name, ctx.task_info.task_index
                );
                let result = s.run(ctx).await;

                s.on_close(ctx).await;

                result.into()
            }
            OperatorNode::Operator(o) => operator_run_behavior(o, ctx, in_qs, ready).await,
        }
    }

    pub async fn start(
        mut self: Box<Self>,
        mut ctx: ArrowContext,
        mut in_qs: Vec<BatchReceiver>,
        ready: Arc<Barrier>,
    ) {
        info!(
            "Starting task {}-{}",
            ctx.task_info.operator_name, ctx.task_info.task_index
        );

        let final_message = self.run_behavior(&mut ctx, &mut in_qs, ready).await;

        if let Some(final_message) = final_message {
            ctx.broadcast(ArrowMessage::Signal(final_message)).await;
        }

        info!(
            "Task finished {}-{}",
            ctx.task_info.operator_name, ctx.task_info.task_index
        );

        ctx.control_tx
            .send(ControlResp::TaskFinished {
                operator_id: ctx.task_info.operator_id.clone(),
                task_index: ctx.task_info.task_index,
            })
            .await
            .expect("control response unwrap");
    }
}

async fn run_checkpoint(checkpoint_barrier: CheckpointBarrier, ctx: &mut ArrowContext) -> bool {
    let watermark = ctx.watermarks.last_present_watermark();

    ctx.table_manager
        .checkpoint(checkpoint_barrier, watermark)
        .await;

    ctx.send_checkpoint_event(checkpoint_barrier, TaskCheckpointEventType::FinishedSync)
        .await;

    ctx.broadcast(ArrowMessage::Signal(SignalMessage::Barrier(
        checkpoint_barrier,
    )))
    .await;

    checkpoint_barrier.then_stop
}

#[async_trait]
pub trait SourceOperator: Send + 'static {
    fn name(&self) -> String;

    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }

    #[allow(unused_variables)]
    async fn on_start(&mut self, ctx: &mut ArrowContext) {}

    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType;

    #[allow(unused_variables)]
    async fn on_close(&mut self, ctx: &mut ArrowContext) {}

    async fn start_checkpoint(
        &mut self,
        checkpoint_barrier: CheckpointBarrier,
        ctx: &mut ArrowContext,
    ) -> bool {
        ctx.send_checkpoint_event(
            checkpoint_barrier,
            TaskCheckpointEventType::StartedCheckpointing,
        )
        .await;

        run_checkpoint(checkpoint_barrier, ctx).await
    }
}

async fn operator_run_behavior(
    this: &mut Box<dyn ArrowOperator + Send>,
    ctx: &mut ArrowContext,
    in_qs: &mut [BatchReceiver],
    ready: Arc<Barrier>,
) -> Option<SignalMessage> {
    this.on_start(ctx).await;

    ready.wait().await;
    info!(
        "Running operator {}-{}",
        ctx.task_info.operator_name, ctx.task_info.task_index
    );

    let task_info = ctx.task_info.clone();
    let name = this.name();
    let mut counter = CheckpointCounter::new(in_qs.len());
    let mut closed: HashSet<usize> = HashSet::new();
    let mut sel = InQReader::new();
    let in_partitions = in_qs.len();

    for (i, q) in in_qs.iter_mut().enumerate() {
        let stream = async_stream::stream! {
          while let Some(item) = q.recv().await {
            yield(i,item);
          }
        };
        sel.push(Box::pin(stream));
    }
    let mut blocked = vec![];
    let mut final_message = None;

    let mut ticks = 0u64;
    let mut interval =
        tokio::time::interval(this.tick_interval().unwrap_or(Duration::from_secs(60)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let operator_future: OptionFuture<_> = this.future_to_poll().into();
        tokio::select! {
            Some(control_message) = ctx.control_rx.recv() => {
                this.handle_controller_message(control_message, ctx).await;
            }

            p = sel.next() => {
                match p {
                    Some(((idx, message), s)) => {
                        let local_idx = idx;

                        debug!("[{}] Handling message {}-{}, {:?}",
                            ctx.task_info.operator_name, 0, local_idx, message);

                        match message {
                            ArrowMessage::Data(record) => {
                                TaskCounters::BatchesReceived.for_task(&ctx.task_info, |c| c.inc());
                                TaskCounters::MessagesReceived.for_task(&ctx.task_info, |c| c.inc_by(record.num_rows() as u64));
                                TaskCounters::BytesReceived.for_task(&ctx.task_info, |c| c.inc_by(record.get_array_memory_size() as u64));
                                this.process_batch_index(idx, in_partitions, record, ctx)
                                    .instrument(tracing::trace_span!("handle_fn",
                                        name,
                                        operator_id = task_info.operator_id,
                                        subtask_idx = task_info.task_index)
                                ).await;
                            }
                            ArrowMessage::Signal(signal) => {
                                match this.handle_control_message(idx, &signal, &mut counter, &mut closed, in_partitions, ctx).await {
                                    ControlOutcome::Continue => {}
                                    ControlOutcome::Stop => {
                                        // just stop; the stop will have already been broadcast for example by
                                        // a final checkpoint
                                        break;
                                    }
                                    ControlOutcome::Finish => {
                                        final_message = Some(SignalMessage::EndOfData);
                                        break;
                                    }
                                    ControlOutcome::StopAndSendStop => {
                                        final_message = Some(SignalMessage::Stop);
                                        break;
                                    }
                                }
                            }
                        }

                        if counter.is_blocked(idx){
                            blocked.push(s);
                        } else {
                            if counter.all_clear() && !blocked.is_empty(){
                                for q in blocked.drain(..){
                                    sel.push(q);
                                }
                            }
                            sel.push(s);
                        }
                    }
                    None => {
                        info!("[{}] Stream completed",ctx.task_info.operator_name);
                        break;
                    }
                }
            }
            Some(val) = operator_future => {
                this.handle_future_result(val, ctx).await;
            }
            _ = interval.tick() => {
                this.handle_tick(ticks, ctx).await;
                ticks += 1;
            }
        }
    }
    this.on_close(&final_message, ctx).await;
    final_message
}

#[async_trait::async_trait]
pub trait ArrowOperator: Send + 'static {
    async fn handle_watermark_int(&mut self, watermark: Watermark, ctx: &mut ArrowContext) {
        // process timers
        tracing::trace!(
            "handling watermark {:?} for {}-{}",
            watermark,
            ctx.task_info.operator_name,
            ctx.task_info.task_index
        );

        if let Watermark::EventTime(_t) = watermark {
            // let finished = ProcessFnUtils::finished_timers(t, ctx).await;
            //
            // for (k, tv) in finished {
            //     self.handle_timer(k, tv.data, ctx).await;
            // }
        }

        if let Some(watermark) = self.handle_watermark(watermark, ctx).await {
            ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(watermark)))
                .await;
        }
    }

    async fn handle_controller_message(
        &mut self,
        control_message: ControlMessage,
        ctx: &mut ArrowContext,
    ) {
        match control_message {
            ControlMessage::Checkpoint(_) => {
                error!("shouldn't receive checkpoint")
            }
            ControlMessage::Stop { .. } => {
                error!("shouldn't receive stop")
            }
            ControlMessage::Commit { epoch, commit_data } => {
                self.handle_commit(epoch, &commit_data, ctx).await;
            }
            ControlMessage::LoadCompacted { compacted } => {
                ctx.load_compacted(compacted).await;
            }
            ControlMessage::NoOp => {}
        }
    }

    async fn handle_control_message(
        &mut self,
        idx: usize,
        message: &SignalMessage,
        counter: &mut CheckpointCounter,
        closed: &mut HashSet<usize>,
        in_partitions: usize,
        ctx: &mut ArrowContext,
    ) -> ControlOutcome {
        match message {
            SignalMessage::Barrier(t) => {
                debug!(
                    "received barrier in {}-{}-{}-{}",
                    self.name(),
                    ctx.task_info.operator_id,
                    ctx.task_info.task_index,
                    idx
                );

                if counter.all_clear() {
                    ctx.control_tx
                        .send(ControlResp::CheckpointEvent(arroyo_rpc::CheckpointEvent {
                            checkpoint_epoch: t.epoch,
                            operator_id: ctx.task_info.operator_id.clone(),
                            subtask_index: ctx.task_info.task_index as u32,
                            time: SystemTime::now(),
                            event_type: TaskCheckpointEventType::StartedAlignment,
                        }))
                        .await
                        .unwrap();
                }

                if counter.mark(idx, t) {
                    debug!(
                        "Checkpointing {}-{}-{}",
                        self.name(),
                        ctx.task_info.operator_id,
                        ctx.task_info.task_index
                    );

                    ctx.send_checkpoint_event(*t, TaskCheckpointEventType::StartedCheckpointing)
                        .await;

                    self.handle_checkpoint(*t, ctx).await;

                    ctx.send_checkpoint_event(*t, TaskCheckpointEventType::FinishedOperatorSetup)
                        .await;

                    if run_checkpoint(*t, ctx).await {
                        return ControlOutcome::Stop;
                    }
                }
            }
            SignalMessage::Watermark(watermark) => {
                debug!(
                    "received watermark {:?} in {}-{}",
                    watermark,
                    self.name(),
                    ctx.task_info.task_index
                );

                let watermark = ctx
                    .watermarks
                    .set(idx, *watermark)
                    .expect("watermark index is too big");

                if let Some(watermark) = watermark {
                    if let Watermark::EventTime(_t) = watermark {
                        // TOOD: pass to table_manager
                    }

                    self.handle_watermark_int(watermark, ctx).await;
                }
            }
            SignalMessage::Stop => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return ControlOutcome::StopAndSendStop;
                }
            }
            SignalMessage::EndOfData => {
                closed.insert(idx);
                if closed.len() == in_partitions {
                    return ControlOutcome::Finish;
                }
            }
        }
        ControlOutcome::Continue
    }

    fn name(&self) -> String;

    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }

    fn tick_interval(&self) -> Option<Duration> {
        None
    }

    #[allow(unused_variables)]
    async fn on_start(&mut self, ctx: &mut ArrowContext) {}

    async fn process_batch_index(
        &mut self,
        _index: usize,
        _in_partitions: usize,
        batch: RecordBatch,
        ctx: &mut ArrowContext,
    ) {
        self.process_batch(batch, ctx).await
    }

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext);

    #[allow(clippy::type_complexity)]
    fn future_to_poll(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>> {
        None
    }

    #[allow(unused_variables)]
    async fn handle_future_result(&mut self, result: Box<dyn Any + Send>, ctx: &mut ArrowContext) {}

    #[allow(unused_variables)]
    async fn handle_timer(&mut self, key: Vec<u8>, value: Vec<u8>, ctx: &mut ArrowContext) {}

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut ArrowContext,
    ) -> Option<Watermark> {
        Some(watermark)
    }

    #[allow(unused_variables)]
    async fn handle_checkpoint(&mut self, b: CheckpointBarrier, ctx: &mut ArrowContext) {}

    #[allow(unused_variables)]
    async fn handle_commit(
        &mut self,
        epoch: u32,
        commit_data: &HashMap<String, HashMap<u32, Vec<u8>>>,
        ctx: &mut ArrowContext,
    ) {
        warn!("default handling of commit with epoch {:?}", epoch);
    }

    #[allow(unused_variables)]
    async fn handle_tick(&mut self, tick: u64, ctx: &mut ArrowContext) {}

    #[allow(unused_variables)]
    async fn on_close(&mut self, final_message: &Option<SignalMessage>, ctx: &mut ArrowContext) {}
}

#[derive(Default)]
pub struct Registry {
    dylibs: Arc<std::sync::Mutex<HashMap<String, Arc<UdfDylib>>>>,
    udfs: HashMap<String, Arc<ScalarUDF>>,
    udafs: HashMap<String, Arc<AggregateUDF>>,
    udwfs: HashMap<String, Arc<WindowUDF>>,
}

impl Registry {
    pub async fn load_dylib(
        &mut self,
        name: &str,
        config: &DylibUdfConfig,
    ) -> anyhow::Result<Arc<UdfDylib>> {
        {
            let dylibs = self.dylibs.lock().unwrap();
            if let Some(dylib) = dylibs.get(&config.dylib_path) {
                return Ok(Arc::clone(dylib));
            }
        }

        // Download a UDF dylib from the object store
        let signature = Signature::exact(config.arg_types.clone(), Volatility::Volatile);

        let udf = StorageProvider::get_url(&config.dylib_path)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Unable to fetch UDF dylib from '{}': {:?}",
                    config.dylib_path, e
                )
            });

        // write the dylib to a local file
        let local_udfs_dir = "/tmp/arroyo/local_udfs";
        tokio::fs::create_dir_all(local_udfs_dir)
            .await
            .map_err(|e| anyhow!("unable to create local udfs dir: {:?}", e))?;

        let dylib_file_name = Path::new(&config.dylib_path)
            .file_name()
            .ok_or_else(|| anyhow!("Invalid dylib path: {}", config.dylib_path))?;
        let local_dylib_path = Path::new(local_udfs_dir).join(dylib_file_name);

        tokio::fs::write(&local_dylib_path, udf)
            .await
            .map_err(|e| anyhow!("unable to write dylib to file: {:?}", e))?;

        let interface = if config.is_async {
            UdfInterface::Async(Arc::new(ContainerOrLocal::Container(unsafe {
                Container::load(local_dylib_path).unwrap()
            })))
        } else {
            UdfInterface::Sync(Arc::new(ContainerOrLocal::Container(unsafe {
                Container::load(local_dylib_path).unwrap()
            })))
        };

        let udf = Arc::new(UdfDylib::new(
            name.to_string(),
            signature,
            config.return_type.clone(),
            interface,
        ));

        self.dylibs
            .lock()
            .unwrap()
            .insert(config.dylib_path.clone(), udf.clone());

        if !config.is_async {
            self.add_udfs(&udf, config);
        }

        Ok(udf)
    }

    pub fn add_local_udf(&mut self, local_udf: &LocalUdf) {
        let udf = Arc::new(UdfDylib::new(
            (*local_udf.config.name).to_string(),
            (*local_udf.config.signature).clone(),
            (*local_udf.config.return_type).clone(),
            local_udf.config.udf.clone(),
        ));

        self.dylibs
            .lock()
            .unwrap()
            .insert(local_udf.config.name.to_string(), udf.clone());

        if !local_udf.is_async {
            self.add_udfs(
                &udf,
                &DylibUdfConfig {
                    dylib_path: local_udf.config.name.to_string(),
                    arg_types: match &local_udf.config.signature.type_signature {
                        TypeSignature::Exact(exact) => exact.clone(),
                        _ => {
                            panic!("only exact type signatures are supported for udfs")
                        }
                    },
                    return_type: (*local_udf.config.return_type).clone(),
                    aggregate: local_udf.is_aggregate,
                    is_async: local_udf.is_async,
                },
            );
        }
    }

    fn add_udfs(&mut self, dylib: &UdfDylib, config: &DylibUdfConfig) {
        let dylib: SyncUdfDylib = dylib.try_into().unwrap();
        if config.aggregate {
            let output_type = Arc::new(config.return_type.clone());

            let args: Vec<_> = config
                .arg_types
                .iter()
                .map(|arg| {
                    UdafArg::new(match arg {
                        DataType::List(f) => Arc::clone(f),
                        _ => {
                            panic!("arg type {:?} for UDAF {} is not a list", arg, dylib.name())
                        }
                    })
                })
                .collect();

            let name = dylib.name().to_string();
            let udaf = Arc::new(create_udaf(
                &name,
                config
                    .arg_types
                    .iter()
                    .map(|t| inner_type(t).expect("UDAF arg is not a vec"))
                    .collect(),
                Arc::new(config.return_type.clone()),
                Volatility::Volatile,
                Arc::new(move |_| {
                    Ok(Box::new(ArroyoUdaf::new(
                        args.clone(),
                        output_type.clone(),
                        Arc::new(dylib.clone()),
                    )))
                }),
                Arc::new(config.arg_types.clone()),
            ));
            self.udafs.insert(name, udaf);
        } else {
            self.udfs
                .insert(dylib.name().to_string(), Arc::new(ScalarUDF::from(dylib)));
        }
    }

    pub fn get_dylib(&self, path: &str) -> Option<Arc<UdfDylib>> {
        self.dylibs.lock().unwrap().get(path).cloned()
    }

    pub fn add_udf(&mut self, udf: Arc<ScalarUDF>) {
        self.udfs.insert(udf.name().to_string(), udf);
    }

    pub fn add_udaf(&mut self, udaf: Arc<AggregateUDF>) {
        self.udafs.insert(udaf.name().to_string(), udaf);
    }
}

impl FunctionRegistry for Registry {
    fn udfs(&self) -> HashSet<String> {
        self.udfs.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> DFResult<Arc<ScalarUDF>> {
        self.udfs
            .get(name)
            .cloned()
            .ok_or_else(|| DataFusionError::Execution(format!("UDF {} not found", name)))
    }

    fn udaf(&self, name: &str) -> DFResult<Arc<AggregateUDF>> {
        self.udafs
            .get(name)
            .cloned()
            .ok_or_else(|| DataFusionError::Execution(format!("UDAF {name} not found")))
    }

    fn udwf(&self, name: &str) -> DFResult<Arc<WindowUDF>> {
        self.udwfs
            .get(name)
            .cloned()
            .ok_or_else(|| DataFusionError::Execution(format!("UDWF {name} not found")))
    }

    fn register_function_rewrite(
        &mut self,
        _: Arc<dyn FunctionRewrite + Send + Sync>,
    ) -> DFResult<()> {
        // no-op
        Ok(())
    }

    fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> DFResult<Option<Arc<ScalarUDF>>> {
        Ok(self.udfs.insert(udf.name().to_string(), udf))
    }

    fn register_udaf(&mut self, udaf: Arc<AggregateUDF>) -> DFResult<Option<Arc<AggregateUDF>>> {
        Ok(self.udafs.insert(udaf.name().to_string(), udaf))
    }

    fn register_udwf(&mut self, udwf: Arc<WindowUDF>) -> DFResult<Option<Arc<WindowUDF>>> {
        Ok(self.udwfs.insert(udwf.name().to_string(), udwf))
    }
}
