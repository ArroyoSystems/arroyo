use arroyo_types::{ArrowMessage, SignalMessage};
use async_trait::async_trait;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use anyhow::anyhow;
use arrow_array::RecordBatch;
use async_ffi::FfiFuture;
use datafusion_expr::ScalarUDFImpl;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::executor::block_on;
use tokio::time::error::Elapsed;
use tracing::{debug, info, warn};
use arroyo_datastream::logical::DylibUdfConfig;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::{ArrowOperator, OperatorConstructor, OperatorNode, Registry};
use arroyo_operator::udfs::{FfiArraySchemaPair, UdfDylib};
use arroyo_rpc::grpc::api::{AsyncUdfOperator, AsyncUdfOrdering};
use arroyo_rpc::grpc::TableConfig;
use arroyo_state::global_table_config;



pub struct AsyncMapOperator{
    name: String,
    udf: Arc<AsyncUdfDylib>,
    max_concurrency: u64,
    
    input_projection: Box<dyn ExecutionPlan>,
    final_projection: Box<dyn ExecutionPlan>,
    next_id: usize, // i.e. inputs received so far, should start at 0
    inputs: VecDeque<Option<RecordBatch>>,
    watermarks: VecDeque<(usize, arroyo_types::Watermark)>,
}

impl OperatorConstructor for AsyncMapOperator {
    type ConfigT = AsyncUdfOperator;

    fn with_config(&self, config: Self::ConfigT, registry: Arc<Registry>) -> anyhow::Result<OperatorNode> {
        let udf_config = config.udf.ok_or_else(|| anyhow!("no UDF config"))?.into();
        let ordered = match AsyncUdfOrdering::try_from(config.ordering) {
            Err(_) | Ok(AsyncUdfOrdering::Ordered) => true,
            Ok(AsyncUdfOrdering::Unordered) => false
        };
        
        // TODO: figure out a better strategy here
        let udf = block_on(registry.load_dylib(&config.name, &udf_config))?;
        
        let input_projection = PhysicalPlanNode::decode(&mut config.)

        let futures = if ordered {
            info!("Using ordered futures");
            FuturesEnum::Ordered(FuturesOrdered::new())
        } else {
            info!("Using unordered futures");
            FuturesEnum::Unordered(FuturesUnordered::new())
        };

        Ok(OperatorNode::from_operator(
            Box::new(Self {
            name,
            udf,
            futures,
            max_concurrency,
            next_id: 0,
            inputs: VecDeque::new(),
            watermarks: VecDeque::new(),
        })))
    }
}

impl AsyncMapOperator {
    async fn on_collect(&mut self, id: usize, ctx: &mut ArrowContext) {
        // mark the input as collected by setting it to None,
        // then pop all the Nones from the front of the queue
        let index = self.inputs.len() - (self.next_id - id);
        self.inputs[index] = None;
        while let Some(None) = self.inputs.front() {
            self.inputs.pop_front();
            if let Some((index, _watermark)) = self.watermarks.front() {
                // if index is 3, then that means that the watermark came in after 3 elements.
                // if we've read in 9 more, then next_id would be 12, and 12 - 3 = 9
                if *index + self.inputs.len() == self.next_id {
                    let (_index, watermark) = self.watermarks.pop_front().unwrap();
                    ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(watermark)))
                        .await;
                }
            }
        }
    }
    
}

#[async_trait]
impl ArrowOperator for AsyncMapOperator {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("a", "AsyncMapOperator state")
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        // TODO: state
        
        // let gs = ctx
        //     .table_manager
        //     .get_global_keyed_state::<(usize, usize), RecordBatch>('a')
        //     .await;
        // 
        // gs.get_key_values()
        //     .into_iter()
        //     .filter(|(k, _)| {
        //         let (task_index, _) = k;
        //         task_index % ctx.task_info.parallelism == ctx.task_info.task_index
        //     })
        //     .for_each(|(_, v)| {
        //         self.inputs.insert(self.next_id, Some(v.clone()));
        //         self.futures.push_back((self.udf)(
        //             self.next_id,
        //             v.value.clone(),
        //             self.udf_context.clone(),
        //         ));
        //         self.next_id += 1;
        //     });
    }

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext) {
        self.inputs.push_back(Some(batch.clone()));

        for row in batch.num_rows() {
            
        }
        self.udf.invoke()
        
        self.futures.push_back(self.ud(
            self.next_id,
            record.value.clone(),
            self.udf_context.clone(),
        ));
        self.next_id += 1;
    }

    async fn handle_tick(&mut self, tick: u64, ctx: &mut ArrowContext) {
        
    }

    async fn handle_watermark(
        &mut self,
        watermark: arroyo_types::Watermark,
        _ctx: &mut ArrowContext,
    ) {
        self.watermarks.push_back((self.next_id, watermark));
    }

    async fn handle_checkpoint(&mut self, _: &CheckpointBarrier, ctx: &mut ArrowContext) {
        let mut gs = ctx
            .state
            .get_global_keyed_state::<(usize, usize), Record<InKey, InT>>('a')
            .await;

        for (i, record) in self.inputs.iter().filter_map(|x| x.clone()).enumerate() {
            let key = (ctx.task_info.task_index, i);
            gs.insert(key, record).await;
        }
    }
    
    async fn on_close(
        &mut self,
        ctx: &mut Context<InKey, OutT>,
        final_message: &Option<Message<InKey, OutT>>,
    ) {
        if let Some(Message::EndOfData) = final_message {
            debug!(
                "AsyncMapOperator end of data with {} futures",
                self.futures.len()
            );
            while let Some((id, result)) = self.futures.next().await {
                self.handle_future(id, result, ctx).await;
            }
        }
        self.udf_context.close().await;
    }
    
}