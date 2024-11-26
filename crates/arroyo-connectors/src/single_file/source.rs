use std::{collections::HashMap, time::SystemTime};

use arroyo_operator::context::OperatorContext;
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::{
    formats::{BadData, Format, Framing},
    grpc::rpc::{StopMode, TableConfig},
    ControlMessage,
};
use async_trait::async_trait;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
};
use tracing::info;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SingleFileSourceFunc {
    pub input_file: String,
    pub lines_read: usize,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub wait_for_control: bool,
}

impl SingleFileSourceFunc {
    async fn run(&mut self, ctx: &mut OperatorContext) -> SourceFinishType {
        if ctx.task_info.task_index != 0 {
            return SourceFinishType::Final;
        }
        ctx.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
        );

        let state: &mut arroyo_state::tables::global_keyed_map::GlobalKeyedView<String, usize> =
            ctx.table_manager.get_global_keyed_state("f").await.unwrap();

        self.lines_read = state.get(&self.input_file).copied().unwrap_or_default();

        let file = File::open(&self.input_file).await.expect(&self.input_file);
        let mut lines = BufReader::new(file).lines();

        let mut i = 0;

        while let Some(s) = lines.next_line().await.unwrap() {
            if i < self.lines_read {
                i += 1;
                continue;
            }
            ctx.deserialize_slice(s.as_bytes(), SystemTime::now(), None)
                .await
                .unwrap();
            if ctx.should_flush() {
                ctx.flush_buffer().await.unwrap();
            }

            self.lines_read += 1;
            i += 1;

            // wait for a control message after each line
            let return_type = if self.wait_for_control {
                self.handle_control(ctx.control_rx.recv().await, ctx).await
            } else {
                self.handle_control(ctx.control_rx.try_recv().ok(), ctx)
                    .await
            };

            if let Some(value) = return_type {
                return value;
            }
        }
        ctx.flush_buffer().await.unwrap();
        info!("file source finished");
        SourceFinishType::Final
    }

    async fn handle_control(
        &mut self,
        msg: Option<ControlMessage>,
        ctx: &mut OperatorContext,
    ) -> Option<SourceFinishType> {
        match msg {
            Some(ControlMessage::Checkpoint(c)) => {
                ctx.flush_buffer().await.unwrap();
                let state: &mut arroyo_state::tables::global_keyed_map::GlobalKeyedView<
                    String,
                    usize,
                > = ctx.table_manager.get_global_keyed_state("f").await.unwrap();
                state.insert(self.input_file.clone(), self.lines_read).await;
                // checkpoint our state
                if self.start_checkpoint(c, ctx).await {
                    return Some(SourceFinishType::Immediate);
                }
            }
            Some(ControlMessage::Stop { mode }) => {
                info!("Stopping file source {:?}", mode);

                match mode {
                    StopMode::Graceful => {
                        return Some(SourceFinishType::Graceful);
                    }
                    StopMode::Immediate => {
                        return Some(SourceFinishType::Immediate);
                    }
                }
            }
            Some(ControlMessage::NoOp) => {
                // No-op messages allow the source to advance and process a record
            }
            _ => {}
        }
        None
    }
}

#[async_trait]
impl SourceOperator for SingleFileSourceFunc {
    fn name(&self) -> String {
        "SingleFileSource".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("f", "file_source")
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) {
        let s: &mut arroyo_state::tables::global_keyed_map::GlobalKeyedView<String, usize> = ctx
            .table_manager
            .get_global_keyed_state("f")
            .await
            .expect("should have table f in file source");

        if let Some(state) = s.get(&self.input_file) {
            self.lines_read = *state;
        }
    }
    async fn run(&mut self, ctx: &mut OperatorContext) -> SourceFinishType {
        self.run(ctx).await
    }
}
