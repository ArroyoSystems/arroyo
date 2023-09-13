use std::time::SystemTime;

use crate::{
    engine::{Context, TimerValue},
    TIMER_TABLE,
};

use arroyo_rpc::grpc::TaskCheckpointEventType;

use arroyo_types::{Data, Key};

pub struct ProcessFnUtils {}

impl ProcessFnUtils {
    pub async fn finished_timers<OutK: Key, OutT: Data, Timer: Data + Eq + PartialEq>(
        watermark: SystemTime,
        ctx: &mut Context<OutK, OutT>,
    ) -> Vec<(OutK, TimerValue<OutK, Timer>)> {
        let mut state = ctx
            .state
            .get_time_key_map(TIMER_TABLE, ctx.last_present_watermark())
            .await;
        state.evict_all_before_watermark(watermark)
    }

    pub async fn send_checkpoint_event<OutK: Key, OutT: Data>(
        barrier: arroyo_types::CheckpointBarrier,
        ctx: &mut Context<OutK, OutT>,
        event_type: TaskCheckpointEventType,
    ) {
        // These messages are received by the engine control thread,
        // which then sends a TaskCheckpointEventReq to the controller.
        ctx.control_tx
            .send(arroyo_rpc::ControlResp::CheckpointEvent(
                arroyo_rpc::CheckpointEvent {
                    checkpoint_epoch: barrier.epoch,
                    operator_id: ctx.task_info.operator_id.clone(),
                    subtask_index: ctx.task_info.task_index as u32,
                    time: std::time::SystemTime::now(),
                    event_type,
                },
            ))
            .await
            .unwrap();
    }
}
