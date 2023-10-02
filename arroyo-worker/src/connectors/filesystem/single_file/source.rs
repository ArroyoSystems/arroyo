use std::{marker::PhantomData, time::SystemTime};

use arroyo_macro::{source_fn, StreamNode};
use arroyo_rpc::{grpc::StopMode, ControlMessage, OperatorConfig};
use arroyo_types::{Data, Record};
use serde::de::DeserializeOwned;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
};
use tracing::info;

use crate::{engine::Context, SourceFinishType};

use super::SingleFileTable;

#[derive(StreamNode)]
pub struct FileSourceFunc<K: Data, T: DeserializeOwned + Data> {
    input_file: String,
    lines_read: usize,
    _t: PhantomData<(K, T)>,
}

#[source_fn(out_t = T)]
impl<K: Data, T: DeserializeOwned + Data> FileSourceFunc<K, T> {
    pub fn from_config(config_str: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config_str).expect("Invalid config for FileSourceFunc");
        let table: SingleFileTable =
            serde_json::from_value(config.table).expect("Invalid table config for FileSourceFunc");
        Self {
            input_file: table.path,
            lines_read: 0,
            _t: PhantomData,
        }
    }
    pub fn tables(&self) -> Vec<arroyo_rpc::grpc::TableDescriptor> {
        vec![arroyo_state::global_table('f', "file_source")]
    }

    fn name(&self) -> String {
        "SingleFileSource".to_string()
    }

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
        if ctx.task_info.task_index != 0 {
            return SourceFinishType::Final;
        }
        self.lines_read = ctx
            .state
            .get_global_keyed_state('f')
            .await
            .get(&self.input_file)
            .map(|v| *v)
            .unwrap_or_default();

        let file = File::open(&self.input_file).await.unwrap();
        let mut lines = BufReader::new(file).lines();

        let mut i = 0;

        while let Some(s) = lines.next_line().await.unwrap() {
            if i < self.lines_read {
                i += 1;
                continue;
            }
            let value = serde_json::from_str(&s).unwrap();
            ctx.collector
                .collect(Record::<(), T>::from_value(SystemTime::now(), value).unwrap())
                .await;

            self.lines_read += 1;
            i += 1;

            // wait for a control message after each line
            match ctx.control_rx.recv().await {
                Some(ControlMessage::Checkpoint(c)) => {
                    ctx.state
                        .get_global_keyed_state('f')
                        .await
                        .insert(self.input_file.clone(), self.lines_read)
                        .await;
                    // checkpoint our state
                    if self.checkpoint(c, ctx).await {
                        return SourceFinishType::Immediate;
                    }
                }
                Some(ControlMessage::Stop { mode }) => {
                    info!("Stopping file source {:?}", mode);

                    match mode {
                        StopMode::Graceful => {
                            return SourceFinishType::Graceful;
                        }
                        StopMode::Immediate => {
                            return SourceFinishType::Immediate;
                        }
                    }
                }
                Some(ControlMessage::NoOp) => {
                    // No-op messages allow the source to advance and process a record
                }
                _ => {}
            }
        }
        info!("file source finished");
        SourceFinishType::Final
    }
}
