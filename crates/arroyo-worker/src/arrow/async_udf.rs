use crate::arrow::StatelessPhysicalExecutor;
use anyhow::anyhow;
use arrow::row::{OwnedRow, Row, RowConverter, SortField};
use arrow_array::{make_array, Array, RecordBatch, UInt64Array};
use arrow_schema::{Field, Schema};
use arroyo_datastream::logical::DylibUdfConfig;
use arroyo_df::ASYNC_RESULT_FIELD;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::{ArrowOperator, OperatorConstructor, OperatorNode, Registry};
use arroyo_rpc::grpc::api;
use arroyo_rpc::grpc::TableConfig;
use arroyo_state::global_table_config;
use arroyo_types::{ArrowMessage, CheckpointBarrier, SignalMessage, Watermark};
use arroyo_udf_host::AsyncUdfDylib;
use async_trait::async_trait;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

enum InputOrOutput {
    Input(OwnedRow),
    Output(OwnedRow),
}

pub struct AsyncUdfOperator {
    name: String,
    udf: AsyncUdfDylib,
    ordered: bool,
    max_concurrency: u32,
    config: api::AsyncUdfOperator,
    registry: Arc<Registry>,
    input_exprs: Vec<Arc<dyn PhysicalExpr>>,
    final_exprs: Vec<Arc<dyn PhysicalExpr>>,
    next_id: u64, // i.e. inputs received so far, should start at 0
    inputs: HashMap<u64, InputOrOutput>,
    watermarks: VecDeque<(u64, Watermark)>,
    row_converter: RowConverter,
    input_schema: Option<Arc<Schema>>,
}

pub struct AsyncUdfConstructor;

impl OperatorConstructor for AsyncUdfConstructor {
    type ConfigT = api::AsyncUdfOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<OperatorNode> {
        let udf_config: DylibUdfConfig = config
            .udf
            .clone()
            .ok_or_else(|| anyhow!("no UDF config"))?
            .into();
        let ordered = match api::AsyncUdfOrdering::try_from(config.ordering) {
            Err(_) | Ok(api::AsyncUdfOrdering::Ordered) => true,
            Ok(api::AsyncUdfOrdering::Unordered) => false,
        };

        let udf = registry.get_dylib(&udf_config.dylib_path).ok_or_else(|| {
            anyhow!(
                "Async map operator configured to use UDF {} at {} which was not loaded at startup",
                config.name, udf_config.dylib_path,
            )
        })?;

        Ok(OperatorNode::from_operator(Box::new(AsyncUdfOperator {
            name: config.name.clone(),
            udf: (&*udf).try_into()?,
            ordered,
            max_concurrency: config.max_concurrency,
            config,
            registry,
            input_exprs: vec![],
            final_exprs: vec![],
            next_id: 0,
            inputs: HashMap::new(),
            watermarks: VecDeque::new(),
            row_converter: RowConverter::new(vec![]).unwrap(),
            input_schema: None,
        })))
    }
}

impl AsyncUdfOperator {
    async fn on_collect(&mut self, id: usize, ctx: &mut ArrowContext) {
        // mark the input as collected by setting it to None,
        // then pop all the Nones from the front of the queue
        // let index = self.inputs.len() - (self.next_id - id);
        // self.inputs[index] = None;
        // while let Some(None) = self.inputs.front() {
        //     self.inputs.pop_front();
        //     if let Some((index, _watermark)) = self.watermarks.front() {
        //         // if index is 3, then that means that the watermark came in after 3 elements.
        //         // if we've read in 9 more, then next_id would be 12, and 12 - 3 = 9
        //         if *index + self.inputs.len() == self.next_id {
        //             let (_index, watermark) = self.watermarks.pop_front().unwrap();
        //             ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(watermark)))
        //                 .await;
        //         }
        //     }
        // }
    }
}

#[async_trait]
impl ArrowOperator for AsyncUdfOperator {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("a", "AsyncMapOperator state")
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        self.row_converter = RowConverter::new(ctx.in_schemas[0].schema.fields.iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect()
        ).unwrap();
        
        let mut input_fields = ctx.in_schemas[0].schema.fields.to_vec();
        input_fields.push(Arc::new(Field::new(
            ASYNC_RESULT_FIELD,
            self.udf.return_type().clone(),
            true,
        )));

        let mut post_udf_fields = input_fields.clone();
        post_udf_fields.push(Arc::new(Field::new(
            ASYNC_RESULT_FIELD,
            self.udf.return_type().clone(),
            true,
        )));
        let input_schema = Arc::new(Schema::new(input_fields));
        let post_udf_schema = Arc::new(Schema::new(post_udf_fields));

        self.input_exprs = self
            .config
            .arg_exprs
            .iter()
            .map(|expr| {
                parse_physical_expr(
                    &PhysicalExprNode::decode(&mut expr.as_slice()).unwrap(),
                    &*self.registry,
                    &input_schema,
                )
                .unwrap()
            })
            .collect();

        self.final_exprs = self
            .config
            .final_exprs
            .iter()
            .map(|expr| {
                parse_physical_expr(
                    &PhysicalExprNode::decode(&mut expr.as_slice()).unwrap(),
                    &*self.registry,
                    &post_udf_schema,
                )
                .unwrap()
            })
            .collect();

        self.input_schema = Some(input_schema);

        // TODO: state
        self.udf.start(self.ordered);

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

    fn tick_interval(&self) -> Option<Duration> {
        Some(Duration::from_millis(50))
    }

    async fn process_batch(&mut self, batch: RecordBatch, _: &mut ArrowContext) {
        let arg_batch: Vec<_> = self
            .input_exprs
            .iter()
            .map(|expr| {
                expr.evaluate(&batch)
                    .unwrap()
                    .into_array(batch.num_rows())
                    .unwrap()
            })
            .collect();

        let rows = self.row_converter.convert_columns(batch.columns()).unwrap();
        for (i, row) in rows.iter().enumerate() {
            let args: Vec<_> = arg_batch
                .iter()
                .map(|v| {
                    arrow::compute::take(&*v, &UInt64Array::from(vec![i as u64]), None)
                        .unwrap()
                        .to_data()
                })
                .collect();
            self.inputs
                .insert(self.next_id, InputOrOutput::Input(row.owned()));
            self.udf
                .send(self.next_id, args)
                .await
                .expect("failed to send data to async UDF runtime");
            self.next_id += 1;
        }
    }

    async fn handle_tick(&mut self, _: u64, ctx: &mut ArrowContext) {
        println!("HANDLIN TICK");
        let Some((ids, results)) = self
            .udf
            .drain_results()
            .expect("failed to get results from async UDF executor")
        else {
            return;
        };

        println!("GOT RESULTS: {:?}", ids);
        
        let mut rows = vec![];
        for id in ids.values() {
            let InputOrOutput::Input(row) = self.inputs.get(id).expect("missing input record")
            else {
                warn!("Got result for already computed row");
                continue;
            };

            rows.push(row.row());
        }

        let mut cols = self.row_converter.convert_rows(rows.into_iter()).unwrap();
        cols.push(make_array(results));

        let batch = RecordBatch::try_new(self.input_schema.as_ref().unwrap().clone(), cols)
            .expect("could not construct record batch from async UDF result");

        let result: Vec<_> = self
            .final_exprs
            .iter()
            .map(|expr| {
                expr.evaluate(&batch)
                    .unwrap()
                    .into_array(batch.num_rows())
                    .unwrap()
            })
            .collect();

        let result =
            RecordBatch::try_new(ctx.out_schema.as_ref().unwrap().schema.clone(), result).unwrap();

        ctx.collector.collect(result).await;
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut ArrowContext,
    ) -> Option<Watermark> {
        self.watermarks.push_back((self.next_id, watermark));
        Some(watermark)
    }

    async fn handle_checkpoint(&mut self, _: CheckpointBarrier, ctx: &mut ArrowContext) {
        // let mut gs = ctx
        //     .state
        //     .get_global_keyed_state::<(usize, usize), Record<InKey, InT>>('a')
        //     .await;
        //
        // for (i, record) in self.inputs.iter().filter_map(|x| x.clone()).enumerate() {
        //     let key = (ctx.task_info.task_index, i);
        //     gs.insert(key, record).await;
        // }
    }

    async fn on_close(&mut self, final_message: &Option<SignalMessage>, ctx: &mut ArrowContext) {
        // if let Some(SignalMessage::EndOfData) = final_message {
        //     debug!(
        //         "AsyncMapOperator end of data with {} futures",
        //         self.futures.len()
        //     );
        //     while let Some((id, result)) = self.futures.next().await {
        //         self.handle_future(id, result, ctx).await;
        //     }
        // }
    }
}
