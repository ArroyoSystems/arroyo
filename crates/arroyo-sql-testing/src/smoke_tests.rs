use anyhow::{bail, Result};
use arroyo_datastream::logical::{
    LogicalEdge, LogicalEdgeType, LogicalGraph, LogicalNode, LogicalProgram, OperatorName,
};
use arroyo_df::{parse_and_get_arrow_program, ArroyoSchemaProvider, SqlConfig};
use arroyo_state::parquet::ParquetBackend;
use petgraph::algo::has_path_connecting;
use petgraph::visit::EdgeRef;
use rstest::rstest;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::{env, time::SystemTime};
use tokio::sync::mpsc::Receiver;

use crate::udfs::get_udfs;
use arroyo_rpc::grpc::{StopMode, TaskCheckpointCompletedReq, TaskCheckpointEventReq};
use arroyo_rpc::{CompactionResult, ControlMessage, ControlResp};
use arroyo_state::checkpoint_state::CheckpointState;
use arroyo_types::{to_micros, CheckpointBarrier};
use arroyo_udf_host::LocalUdf;
use arroyo_worker::engine::{Engine, StreamConfig};
use arroyo_worker::engine::{Program, RunningEngine};
use petgraph::{Direction, Graph};
use serde_json::Value;
use test_log::test as test_log;
use tokio::fs::read_to_string;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::info;

#[test_log(rstest)]
fn for_each_file(#[files("src/test/queries/*.sql")] path: PathBuf) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            run_smoketest(&path).await;
        });
}

async fn run_smoketest(path: &Path) {
    // read text at path
    let test_name = path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .split('.')
        .next()
        .unwrap();
    let query = read_to_string(path).await.unwrap();
    let fail = query.starts_with("--fail");
    let error_message = query.starts_with("--fail=").then(|| {
        query
            .lines()
            .next()
            .unwrap()
            .split_once('=')
            .unwrap()
            .1
            .trim()
    });
    match correctness_run_codegen(test_name, query.clone(), 20).await {
        Ok(_) => {}
        Err(err) => {
            if fail {
                if let Some(error_message) = error_message {
                    assert!(
                        err.to_string().contains(error_message),
                        "expected error message '{}' not found; instead got '{}'",
                        error_message,
                        err
                    );
                }
            } else {
                panic!("smoke test failed: {}", err);
            }
        }
    }
}

struct SmokeTestContext<'a> {
    job_id: Arc<String>,
    engine: &'a RunningEngine,
    control_rx: &'a mut Receiver<ControlResp>,
    tasks_per_operator: HashMap<String, usize>,
}

async fn checkpoint(ctx: &mut SmokeTestContext<'_>, epoch: u32) {
    let checkpoint_id = epoch as i64;
    let mut checkpoint_state = CheckpointState::new(
        ctx.job_id.clone(),
        checkpoint_id.to_string(),
        epoch,
        0,
        ctx.tasks_per_operator.clone(),
    );

    // trigger a checkpoint, pass the messages to the CheckpointState

    let barrier = CheckpointBarrier {
        epoch,
        min_epoch: 0,
        timestamp: SystemTime::now(),
        then_stop: false,
    };

    for source in ctx.engine.source_controls() {
        source
            .send(ControlMessage::Checkpoint(barrier))
            .await
            .unwrap();
    }

    while !checkpoint_state.done() {
        let c: ControlResp = ctx.control_rx.recv().await.unwrap();

        match c {
            ControlResp::CheckpointEvent(c) => {
                let req = TaskCheckpointEventReq {
                    worker_id: 1,
                    time: to_micros(c.time),
                    job_id: (*ctx.job_id).clone(),
                    operator_id: c.operator_id,
                    subtask_index: c.subtask_index,
                    epoch: c.checkpoint_epoch,
                    event_type: c.event_type as i32,
                };
                checkpoint_state.checkpoint_event(req).unwrap();
            }
            ControlResp::CheckpointCompleted(c) => {
                let req = TaskCheckpointCompletedReq {
                    worker_id: 1,
                    time: c.subtask_metadata.finish_time,
                    job_id: (*ctx.job_id).clone(),
                    operator_id: c.operator_id,
                    epoch: c.checkpoint_epoch,
                    needs_commit: false,
                    metadata: Some(c.subtask_metadata),
                };
                checkpoint_state.checkpoint_finished(req).await.unwrap();
            }
            _ => {}
        }
    }

    checkpoint_state.save_state().await.unwrap();

    info!("Smoke test checkpoint completed");
}

async fn compact(
    job_id: Arc<String>,
    running_engine: &RunningEngine,
    tasks_per_operator: HashMap<String, usize>,
    epoch: u32,
) {
    let operator_controls = running_engine.operator_controls();
    for (operator, _) in tasks_per_operator {
        if let Ok(compacted) =
            ParquetBackend::compact_operator(job_id.clone(), operator.clone(), epoch).await
        {
            let operator_controls = operator_controls.get(&operator).unwrap();
            for s in operator_controls {
                s.send(ControlMessage::LoadCompacted {
                    compacted: CompactionResult {
                        operator_id: operator.to_string(),
                        compacted_tables: compacted.clone(),
                    },
                })
                .await
                .unwrap();
            }
        }
    }
}

async fn advance(engine: &RunningEngine, count: i32) {
    // let the engine run for a bit, process some records
    for source in engine.source_controls() {
        for _ in 0..count {
            let _ = source.send(ControlMessage::NoOp).await;
        }
    }
}

async fn run_until_finished(engine: &RunningEngine, control_rx: &mut Receiver<ControlResp>) {
    while control_rx.try_recv().is_ok()
        || control_rx
            .try_recv()
            .is_err_and(|e| e == TryRecvError::Empty)
    {
        advance(engine, 10).await;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn set_internal_parallelism(graph: &mut Graph<LogicalNode, LogicalEdge>, parallelism: usize) {
    let watermark_nodes: HashSet<_> = graph
        .node_indices()
        .filter(|index| {
            let operator_name = graph.node_weight(*index).unwrap().operator_name;
            matches!(operator_name, OperatorName::ExpressionWatermark)
        })
        .collect();
    let indices: Vec<_> = graph
        .node_indices()
        .filter(
            |index| match graph.node_weight(*index).unwrap().operator_name {
                OperatorName::ExpressionWatermark
                | OperatorName::ConnectorSource
                | OperatorName::ConnectorSink => false,
                _ => {
                    for watermark_node in watermark_nodes.iter() {
                        if has_path_connecting(&graph.clone(), *watermark_node, *index, None) {
                            return true;
                        }
                    }
                    false
                }
            },
        )
        .collect();
    for node in indices {
        graph.node_weight_mut(node).unwrap().parallelism = parallelism;
    }
    if parallelism > 1 {
        let mut edges_to_make_shuffle = vec![];
        for node in graph.externals(Direction::Outgoing) {
            for edge in graph.edges_directed(node, Direction::Incoming) {
                edges_to_make_shuffle.push(edge.id());
            }
        }
        for node in graph.node_indices() {
            if graph.node_weight(node).unwrap().operator_name == OperatorName::ExpressionWatermark {
                for edge in graph.edges_directed(node, Direction::Outgoing) {
                    edges_to_make_shuffle.push(edge.id());
                }
            }
        }
        for edge in edges_to_make_shuffle {
            graph.edge_weight_mut(edge).unwrap().edge_type = LogicalEdgeType::Shuffle;
        }
    }
}

async fn run_and_checkpoint(job_id: Arc<String>, program: Program, checkpoint_interval: i32) {
    let tasks_per_operator = program.tasks_per_operator();
    let engine = Engine::for_local(program, job_id.to_string());
    let (running_engine, mut control_rx) = engine
        .start(StreamConfig {
            restore_epoch: None,
        })
        .await;
    info!("Smoke test checkpointing enabled");
    env::set_var("MIN_FILES_TO_COMPACT", "2");

    let ctx = &mut SmokeTestContext {
        job_id: job_id.clone(),
        engine: &running_engine,
        control_rx: &mut control_rx,
        tasks_per_operator: tasks_per_operator.clone(),
    };

    // trigger a couple checkpoints
    advance(&running_engine, checkpoint_interval).await;
    checkpoint(ctx, 1).await;
    advance(&running_engine, checkpoint_interval).await;
    checkpoint(ctx, 2).await;
    advance(&running_engine, checkpoint_interval).await;

    compact(job_id, &running_engine, tasks_per_operator.clone(), 2).await;

    // trigger checkpoint 3, which will include the compacted files
    advance(&running_engine, checkpoint_interval).await;
    checkpoint(ctx, 3).await;
    // shut down the engine
    for source in running_engine.source_controls() {
        source
            .send(ControlMessage::Stop {
                mode: StopMode::Graceful,
            })
            .await
            .unwrap();
    }
    run_until_finished(&running_engine, &mut control_rx).await;
}

async fn finish_from_checkpoint(job_id: &str, program: Program) {
    let engine = Engine::for_local(program, job_id.to_string());
    let (running_engine, mut control_rx) = engine
        .start(StreamConfig {
            restore_epoch: Some(3),
        })
        .await;

    info!("Restored engine, running until finished");
    run_until_finished(&running_engine, &mut control_rx).await;
}

async fn run_pipeline_and_assert_outputs(
    job_id: &str,
    mut graph: LogicalGraph,
    checkpoint_interval: i32,
    output_location: String,
    golden_output_location: String,
    udfs: &[LocalUdf],
) {
    // remove output_location before running the pipeline
    if std::path::Path::new(&output_location).exists() {
        std::fs::remove_file(&output_location).unwrap();
    }

    let get_program =
        |graph: &LogicalGraph| Program::local_from_logical(job_id.to_string(), graph, udfs);

    run_completely(
        job_id,
        get_program(&graph),
        output_location.clone(),
        golden_output_location.clone(),
    )
    .await;

    set_internal_parallelism(&mut graph, 2);

    run_and_checkpoint(
        Arc::new(job_id.to_string()),
        get_program(&graph),
        checkpoint_interval,
    )
    .await;

    set_internal_parallelism(&mut graph, 3);

    finish_from_checkpoint(job_id, get_program(&graph)).await;

    check_output_files(
        "resuming from checkpointing",
        output_location,
        golden_output_location,
    )
    .await;
}

async fn run_completely(
    job_id: &str,
    program: Program,
    output_location: String,
    golden_output_location: String,
) {
    let engine = Engine::for_local(program, job_id.to_string());
    let (running_engine, mut control_rx) = engine
        .start(StreamConfig {
            restore_epoch: None,
        })
        .await;

    run_until_finished(&running_engine, &mut control_rx).await;

    check_output_files(
        "initial run",
        output_location.clone(),
        golden_output_location,
    )
    .await;
    if std::path::Path::new(&output_location).exists() {
        std::fs::remove_file(&output_location).unwrap();
    }
}

// return the inner value and whether it is a retract
fn decode_debezium(value: &Value) -> Result<(Value, bool)> {
    if !is_debezium(value) {
        bail!("not a debezium record");
    }
    let op = value.get("op").unwrap().as_str().unwrap();
    match op {
        "c" => Ok((value.get("after").unwrap().clone(), false)),
        "d" => Ok((value.get("before").unwrap().clone(), true)),
        _ => bail!("unknown op {}", op),
    }
}

fn is_debezium(value: &Value) -> bool {
    let Some(op) = value.get("op") else {
        return false;
    };
    op.as_str().is_some()
}

fn check_debezium(
    output_location: String,
    golden_output_location: String,
    output_lines: Vec<Value>,
    golden_output_lines: Vec<Value>,
) {
    let output_deduped = dedup_debezium(output_lines);
    let golden_output_deduped = dedup_debezium(golden_output_lines);
    assert_eq!(
        output_deduped, golden_output_deduped,
        "failed to check debezium equality for\noutput: {}\ngolden: {}",
        output_location, golden_output_location
    );
}

fn dedup_debezium(values: Vec<Value>) -> HashMap<String, i64> {
    let mut deduped = HashMap::new();
    for value in &values {
        let (row_data, value) = decode_debezium(value).unwrap();
        let row_data_str = roundtrip(&row_data);
        let count = deduped.entry(row_data_str.clone()).or_insert(0);
        if value {
            *count -= 1;
        } else {
            *count += 1;
        }
        if *count == 0 {
            deduped.remove(&row_data_str);
        }
    }
    deduped
}

fn roundtrip(v: &Value) -> String {
    // round trip string through a btreemap to get consistent key ordering
    serde_json::to_string(&serde_json::from_value::<BTreeMap<String, Value>>(v.clone()).unwrap())
        .unwrap()
}

async fn check_output_files(
    check_name: &str,
    output_location: String,
    golden_output_location: String,
) {
    let mut output_lines: Vec<Value> = read_to_string(output_location.clone())
        .await
        .unwrap_or_else(|_| panic!("output file not found at {}", output_location))
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();

    let mut golden_output_lines: Vec<Value> = read_to_string(golden_output_location.clone())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "golden output file not found at {}, want to compare to {}",
                golden_output_location, output_location
            )
        })
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    if output_lines.len() != golden_output_lines.len() {
        // might be updating, in which case lets see if we can cancel out rows
        let Some(first_output) = output_lines.first() else {
            panic!(
                "failed at check {}, output has 0 lines, expect {} lines.\noutput: {}\ngolden: {}",
                check_name,
                golden_output_lines.len(),
                output_location,
                golden_output_location
            );
        };
        if is_debezium(first_output) {
            check_debezium(
                output_location,
                golden_output_location,
                output_lines,
                golden_output_lines,
            );
            return;
        }

        panic!(
            "failed at check {}, output has {} lines, expect {} lines.\noutput: {}\ngolden: {}",
            check_name,
            output_lines.len(),
            golden_output_lines.len(),
            output_location,
            golden_output_location
        );
    }

    output_lines.sort_by_cached_key(roundtrip);
    golden_output_lines.sort_by_cached_key(roundtrip);
    output_lines
        .into_iter()
        .zip(golden_output_lines.into_iter())
        .enumerate()
        .for_each(|(i, (output_line, golden_output_line))| {
            assert_eq!(
                output_line, golden_output_line,
                "check {}: line {} of output and golden output differ\nactual:{}\nexpected:{})",
                check_name, i, output_location, golden_output_location
            )
        });
}

pub async fn correctness_run_codegen(
    test_name: impl Into<String>,
    query: impl Into<String>,
    checkpoint_interval: i32,
) -> Result<()> {
    let test_name = test_name.into();
    let parent_directory = std::env::current_dir()
        .unwrap()
        .to_string_lossy()
        .to_string();

    // Depending on run location the directory might end with arroyo-sql-testing.
    // If so, remove it.
    let parent_directory = if parent_directory.ends_with("arroyo-sql-testing") {
        parent_directory
            .strip_suffix("arroyo-sql-testing")
            .unwrap()
            .to_string()
    } else {
        parent_directory
    };

    // replace $input_file with the current directory and then inputs/query_name.json
    let physical_input_dir = format!("{}/arroyo-sql-testing/inputs/", parent_directory,);

    let query_string = query.into().replace("$input_dir", &physical_input_dir);

    // replace $output_file with the current directory and then outputs/query_name.json
    let physical_output = format!(
        "{}/arroyo-sql-testing/outputs/{}.json",
        parent_directory, test_name
    );

    let query_string = query_string.replace("$output_path", &physical_output);
    let golden_output_location = format!(
        "{}/arroyo-sql-testing/golden_outputs/{}.json",
        parent_directory, test_name
    );

    let udfs = get_udfs();

    let logical_program = get_graph(query_string.clone(), &udfs).await?;
    run_pipeline_and_assert_outputs(
        &test_name,
        logical_program.graph,
        checkpoint_interval,
        physical_output,
        golden_output_location,
        &udfs,
    )
    .await;
    Ok(())
}

async fn get_graph(query_string: String, udfs: &[LocalUdf]) -> Result<LogicalProgram> {
    let mut schema_provider = ArroyoSchemaProvider::new();
    for udf in udfs {
        schema_provider
            .add_rust_udf(udf.def, udf.config.name.as_str())
            .unwrap();
    }

    // TODO: test with higher parallelism
    let program = parse_and_get_arrow_program(
        query_string,
        schema_provider,
        SqlConfig {
            default_parallelism: 1,
        },
    )
    .await?
    .program;
    Ok(program)
}
