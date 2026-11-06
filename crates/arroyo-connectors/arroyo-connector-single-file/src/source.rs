use std::{collections::HashMap, time::SystemTime};

use arroyo_operator::SourceFinishType;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_rpc::{
    ControlMessage, connector_err,
    errors::DataflowResult,
    formats::{BadData, Format, Framing},
    grpc::rpc::{StopMode, TableConfig},
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
    async fn handle_control(
        &mut self,
        msg: Option<ControlMessage>,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Option<SourceFinishType> {
        match msg {
            Some(ControlMessage::Checkpoint(c)) => {
                let _ = collector.flush_buffer().await;
                let state: &mut arroyo_state::tables::global_keyed_map::GlobalKeyedView<
                    String,
                    usize,
                > = ctx.table_manager.get_global_keyed_state("f").await.ok()?;
                state.insert(self.input_file.clone(), self.lines_read).await;
                // checkpoint our state
                if self.start_checkpoint(c, ctx, collector).await {
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

    async fn on_start(&mut self, ctx: &mut SourceContext) -> DataflowResult<()> {
        let s: &mut arroyo_state::tables::global_keyed_map::GlobalKeyedView<String, usize> =
            ctx.table_manager.get_global_keyed_state("f").await?;

        if let Some(state) = s.get(&self.input_file) {
            self.lines_read = *state;
        }
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        if ctx.task_info.task_index != 0 {
            return Ok(SourceFinishType::Final);
        }
        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &[],
        );

        let state: &mut arroyo_state::tables::global_keyed_map::GlobalKeyedView<String, usize> =
            ctx.table_manager.get_global_keyed_state("f").await?;

        self.lines_read = state.get(&self.input_file).copied().unwrap_or_default();

        let file = File::open(&self.input_file).await.map_err(|e| {
            connector_err!(
                User,
                NoRetry,
                "failed to open file '{}': {}",
                self.input_file,
                e
            )
        })?;
        let mut lines = BufReader::new(file).lines();

        let mut i = 0;

        while let Some(s) = lines.next_line().await.map_err(|e| {
            connector_err!(
                External,
                WithBackoff,
                "failed to read line from file '{}': {}",
                self.input_file,
                e
            )
        })? {
            if i < self.lines_read {
                i += 1;
                continue;
            }
            collector
                .deserialize_slice(s.as_bytes(), SystemTime::now(), None)
                .await?;
            if collector.should_flush() {
                collector.flush_buffer().await?;
            }

            self.lines_read += 1;
            i += 1;

            // wait for a control message after each line
            let return_type = if self.wait_for_control {
                self.handle_control(ctx.control_rx.recv().await, ctx, collector)
                    .await
            } else {
                self.handle_control(ctx.control_rx.try_recv().ok(), ctx, collector)
                    .await
            };

            if let Some(value) = return_type {
                return Ok(value);
            }
        }

        collector.flush_buffer().await?;
        info!("file source finished");
        Ok(SourceFinishType::Final)
    }
}
