use anyhow::anyhow;
use arrow::row::{OwnedRow, RowConverter, SortField};
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
use bincode::{Decode, Encode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

pub struct AsyncUdfOperator {
    name: String,
    udf: AsyncUdfDylib,
    ordered: bool,
    allowed_in_flight: u32,
    timeout: Duration,
    config: api::AsyncUdfOperator,
    registry: Arc<Registry>,
    input_exprs: Vec<Arc<dyn PhysicalExpr>>,
    final_exprs: Vec<Arc<dyn PhysicalExpr>>,
    next_id: u64, // i.e. inputs received so far, should start at 0

    inputs: BTreeMap<u64, OwnedRow>,
    outputs: BTreeMap<u64, OwnedRow>,

    watermarks: VecDeque<(u64, Watermark)>,
    input_row_converter: RowConverter,
    output_row_converter: RowConverter,
    input_schema: Option<Arc<Schema>>,
}

pub struct AsyncUdfConstructor;

#[derive(Debug, Clone, Encode, Decode)]
pub struct AsyncUdfState {
    inputs: Vec<(u64, Vec<u8>)>,
    outputs: Vec<(u64, Vec<u8>)>,
    watermarks: VecDeque<(u64, Watermark)>,
}

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
                "Async UDF operator configured to use UDF {} at {} which was not loaded at startup",
                config.name,
                udf_config.dylib_path,
            )
        })?;

        Ok(OperatorNode::from_operator(Box::new(AsyncUdfOperator {
            name: config.name.clone(),
            udf: (&*udf).try_into()?,
            ordered,
            allowed_in_flight: config.max_concurrency,
            timeout: Duration::from_micros(config.timeout_micros),
            config,
            registry,
            input_exprs: vec![],
            final_exprs: vec![],
            next_id: 0,
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            watermarks: VecDeque::new(),
            input_row_converter: RowConverter::new(vec![]).unwrap(),
            output_row_converter: RowConverter::new(vec![]).unwrap(),
            input_schema: None,
        })))
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
        info!("Starting async UDF with timeout {:?}", self.timeout);
        self.input_row_converter = RowConverter::new(
            ctx.in_schemas[0]
                .schema
                .fields
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )
        .unwrap();

        self.output_row_converter = RowConverter::new(
            ctx.out_schema
                .as_ref()
                .unwrap()
                .schema
                .fields
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )
        .unwrap();

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
                    &DefaultPhysicalExtensionCodec {},
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
                    &DefaultPhysicalExtensionCodec {},
                )
                .unwrap()
            })
            .collect();

        self.input_schema = Some(input_schema);

        self.udf
            .start(self.ordered, self.timeout, self.allowed_in_flight);

        let gs = ctx
            .table_manager
            .get_global_keyed_state::<usize, AsyncUdfState>("a")
            .await
            .unwrap();

        // This strategy is not correct when the pipeline is rescaled, as the new partitions
        // and the old partitions are not aligned in the actual dataflow. To fix this we need
        // to retain keys throughout the dataflow.
        gs.get_all()
            .iter()
            .filter(|(task_index, _)| {
                **task_index % ctx.task_info.parallelism == ctx.task_info.task_index
            })
            .for_each(|(_, state)| {
                for (k, v) in &state.inputs {
                    self.inputs
                        .insert(*k, self.input_row_converter.parser().parse(v).owned());
                }

                for (k, v) in &state.outputs {
                    self.outputs
                        .insert(*k, self.output_row_converter.parser().parse(v).owned());
                }

                self.watermarks = state.watermarks.clone();
            });

        // start futures for the recovered input data
        for (id, row) in &self.inputs {
            let args = self
                .input_row_converter
                .convert_rows([row.row()].into_iter())
                .unwrap()
                .into_iter()
                .map(|t| t.to_data())
                .collect();
            self.udf
                .send(*id, args)
                .await
                .expect("failed to send data to async UDF runtime");
        }

        self.next_id = self
            .inputs
            .last_key_value()
            .map(|(id, _)| *id)
            .unwrap_or(0)
            .max(
                self.outputs
                    .last_key_value()
                    .map(|(id, _)| *id)
                    .unwrap_or(0),
            )
            .max(
                self.watermarks
                    .iter()
                    .last()
                    .map(|(id, _)| *id)
                    .unwrap_or(0),
            )
            + 1;
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

        let rows = self
            .input_row_converter
            .convert_columns(batch.columns())
            .unwrap();

        for (i, row) in rows.iter().enumerate() {
            let args: Vec<_> = arg_batch
                .iter()
                .map(|v| {
                    arrow::compute::take(v, &UInt64Array::from(vec![i as u64]), None)
                        .unwrap()
                        .to_data()
                })
                .collect();
            self.inputs.insert(self.next_id, row.owned());
            self.udf
                .send(self.next_id, args)
                .await
                .expect("failed to send data to async UDF runtime");
            self.next_id += 1;
        }
    }

    async fn handle_tick(&mut self, _: u64, ctx: &mut ArrowContext) {
        let Some((ids, results)) = self
            .udf
            .drain_results()
            .expect("failed to get results from async UDF executor")
        else {
            return;
        };

        let mut rows = vec![];
        for id in ids.values() {
            let row = self.inputs.get(id).expect("missing input record");
            rows.push(row.row());
        }

        let mut cols = self.input_row_converter.convert_rows(rows).unwrap();
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

        // iterate through the batch convert to rows and push into our map
        let out_rows = self
            .output_row_converter
            .convert_columns(&result)
            .expect("could not convert output columns to rows");

        for (row, id) in out_rows.into_iter().zip(ids.values()) {
            self.outputs.insert(*id, row.owned());
            self.inputs.remove(id);
        }

        self.flush_output(ctx).await;
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut ArrowContext,
    ) -> Option<Watermark> {
        self.watermarks.push_back((self.next_id, watermark));
        None
    }

    async fn handle_checkpoint(&mut self, _: CheckpointBarrier, ctx: &mut ArrowContext) {
        let gs = ctx.table_manager.get_global_keyed_state("a").await.unwrap();

        let state = AsyncUdfState {
            inputs: self
                .inputs
                .iter()
                .map(|(id, row)| (*id, row.row().as_ref().to_vec()))
                .collect(),
            outputs: self
                .outputs
                .iter()
                .map(|(id, row)| (*id, row.row().as_ref().to_vec()))
                .collect(),
            watermarks: self.watermarks.clone(),
        };

        gs.insert(ctx.task_info.task_index, state).await;
    }

    async fn on_close(&mut self, final_message: &Option<SignalMessage>, ctx: &mut ArrowContext) {
        if let Some(SignalMessage::EndOfData) = final_message {
            while !self.inputs.is_empty() && !self.outputs.is_empty() {
                self.handle_tick(0, ctx).await;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

impl AsyncUdfOperator {
    async fn flush_output(&mut self, ctx: &mut ArrowContext) {
        // check if we can emit any records -- these are ones received before our most recent
        // watermark -- once all records from before a watermark have been processed, we can
        // remove and emit the watermark
        //
        //  0   1   2   3     4   5   6   7     8   9
        // [o] [o] [i] [o] | [o] [o] [i] [o] | [i] [i]
        //                 ^
        //    this is our first watermark, which has
        //    id 4.we can emit all of the ready values
        //    before it (0, 1, and 3). once 2 completes,
        //    we can clear and emit the watermark
        loop {
            let (watermark_id, watermark) = self
                .watermarks
                .pop_front()
                .map(|(id, watermark)| (id, Some(watermark)))
                .unwrap_or((u64::MAX, None));

            let mut rows = vec![];
            loop {
                let Some((id, row)) = self.outputs.pop_first() else {
                    break;
                };

                if id >= watermark_id {
                    self.outputs.insert(id, row);
                    break;
                }

                rows.push(row);
            }

            let cols = self
                .output_row_converter
                .convert_rows(rows.iter().map(|t| t.row()))
                .expect("failed to convert output rows");
            let batch = RecordBatch::try_new(ctx.out_schema.as_ref().unwrap().schema.clone(), cols)
                .expect("failed to construct record batch");

            ctx.collect(batch).await;

            let Some(watermark) = watermark else {
                break;
            };

            let oldest_unprocessed = *self.inputs.keys().next().unwrap_or(&u64::MAX);

            if watermark_id <= oldest_unprocessed {
                // we've processed everything before this watermark, we can emit and drop it
                ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(watermark)))
                    .await;
            } else {
                // we still have messages preceding this watermark to work on
                self.watermarks.push_front((watermark_id, watermark));
                break;
            }
        }
    }
}
