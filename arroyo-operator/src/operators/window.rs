use std::sync::Arc;
use std::time::{Duration, SystemTime};
use arrow::row::{RowConverter, SortField};
use arrow_array::{Array, ArrayAccessor, StructArray, TimestampNanosecondArray};
use arrow_array::cast::AsArray;
use arrow_array::iterator::ArrayIter;
use async_trait::async_trait;
use datafusion_common::ScalarValue;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_proto::protobuf::PhysicalExprNode;
use arroyo_rpc::grpc::{api, TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_rpc::grpc::api::{window, WindowExpressionOperator};
use arroyo_types::{ArroyoRecordBatch, from_nanos, Window};
use crate::operator::{ArrowContext, ArrowOperator, ArrowOperatorConstructor, InstantWindowAssigner, SlidingWindowAssigner, TimeWindowAssigner, TumblingWindowAssigner};
use crate::operators::{exprs_from_proto};

pub enum WindowOperation {
    Aggregate,
    Flatten,
}

// impl WindowOperation {
//     async fn operate(&self, key: &mut K, window: Window, table: char, ctx: &mut Context<K, OutT>) {
//         let mut state = ctx.state.get_key_time_multi_map(table).await;
//
//         match self {
//             WindowOperation::Aggregate(aggregator) => {
//                 let value = {
//                     let vs: Vec<&T> = state.get_time_range(key, window.start, window.end).await;
//                     (aggregator)(key, window, vs)
//                 };
//
//                 let record = Record {
//                     timestamp: window.end - Duration::from_nanos(1),
//                     key: Some(key.clone()),
//                     value,
//                 };
//
//                 ctx.collect(record).await;
//             }
//             WindowOperation::Flatten(flatten) => {
//                 let values = {
//                     let vs: Vec<&T> = state.get_time_range(key, window.start, window.end).await;
//                     (flatten)(&key, window, vs)
//                 };
//
//                 for v in values {
//                     let record = Record {
//                         timestamp: window.end - Duration::from_nanos(1),
//                         key: Some(key.clone()),
//                         value: v,
//                     };
//                     ctx.collect(record).await;
//                 }
//             }
//         }
//     }
// }

pub struct KeyedWindowFunc {
    assigner: Box<dyn TimeWindowAssigner>,
    operation: WindowOperation,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl ArrowOperatorConstructor<WindowExpressionOperator> for KeyedWindowFunc {
    fn from_config(config: WindowExpressionOperator) -> Box<dyn ArrowOperator> {
        let exprs = exprs_from_proto(config.expressions);

        let f = KeyedWindowFunc {
            assigner: match config.window.unwrap().window.unwrap() {
                window::Window::SlidingWindow(s) => {
                    Box::new(SlidingWindowAssigner {
                        size: Duration::from_micros(s.size_micros),
                        slide:Duration::from_micros(s.slide_micros),
                    })
                }
                window::Window::TumblingWindow(t) => {
                    Box::new(TumblingWindowAssigner {
                        size: Duration::from_micros(t.size_micros)
                    })
                }
                window::Window::InstantWindow(_) => {
                    Box::new(InstantWindowAssigner {})
                }
                window::Window::SessionWindow(_) => {
                    todo!()
                }
            },
            operation: if config.flatten {
                WindowOperation::Flatten
            } else {
                WindowOperation::Aggregate
            },
            exprs,
        };


        Box::new(f)
    }
}

#[async_trait]
impl ArrowOperator for KeyedWindowFunc {

    fn name(&self) -> String {
        "KeyWindow".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![TableDescriptor {
            name: "w".to_string(),
            description: "window state".to_string(),
            table_type: TableType::KeyTimeMultiMap as i32,
            delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
            write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
            retention_micros: self.assigner.safe_retention_duration().unwrap().as_micros() as u64,
        }]
    }

    async fn handle_timer(&mut self, mut key: Vec<u8>, data: Vec<u8>, ctx: &mut ArrowContext) {
        // self.operation.operate(&mut key, window, 'w', ctx).await;

        // // clear everything before our start time (we're guaranteed that timers execute in order,
        // // so with fixed-width windows there won't be any earlier data)
        // let next = self.assigner.next(window);
        // let mut state: KeyTimeMultiMap<K, T, _> = ctx.state.get_key_time_multi_map('w').await;
        //
        // state
        //     .clear_time_range(&mut key, SystemTime::UNIX_EPOCH, next.start)
        //     .await;
    }

    async fn process_batch(&mut self, batch: ArroyoRecordBatch, ctx: &mut ArrowContext) {
        let timestamp_col = ctx.in_schemas[0].timestamp_col;
        let key_col = ctx.in_schemas[0].key_col.unwrap();

        let timestamps: &TimestampNanosecondArray = batch.columns[timestamp_col].as_any().downcast_ref().unwrap();

        for (i, timestamp) in timestamps.iter().enumerate() {
            let windows = self.assigner.windows(from_nanos(timestamp.unwrap() as u128));
            let watermark = ctx
                .last_present_watermark()
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let mut has_window = false;
            let mut key = ScalarValue::try_from_array(&batch.columns[key_col], i).unwrap();
            for w in windows {
                if w.end > watermark {
                    has_window = true;

                    ctx.schedule_timer(&mut key, w.end, w).await;
                }
            }

            if has_window {
                let key = record.key.as_ref().unwrap().clone();
                let value = record.value.clone();
                ctx.state
                    .get_key_time_multi_map('w')
                    .await
                    .insert(record.timestamp, key, value)
                    .await;
            }
        }


        todo!()
    }
}