use arroyo_rpc::ControlResp;
use arroyo_sql_macro::full_pipeline_codegen;
use arroyo_worker::engine::{Engine, StreamConfig};

full_pipeline_codegen! {"bench", "
CREATE TABLE blackhole_sink (
    extra TEXT
) WITH (
    'connector' = 'blackhole'
);
INSERT INTO blackhole_sink SELECT coalesce(bid.extra, auction.extra, person.extra) FROM nexmark
"}

#[tokio::main]
pub async fn main() {
    let graph = bench::make_graph();
    let program = arroyo_worker::engine::Program::local_from_logical("bench".into(), &graph);
    let (running_engine, mut control_rx) = Engine::for_local(program, "bench".into())
        .start(StreamConfig {
            restore_epoch: None,
        })
        .await;
    // wait for the engine to finish
    while let Some(control_resp) = control_rx.recv().await {
        // make sure it isn't a failure
        if let ControlResp::Error { .. } = &control_resp {
            panic!("Engine failed with {:?}", control_resp);
        }
    }
}
