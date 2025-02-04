use anyhow::Result;
use arroyo_datastream::logical::{
    LogicalEdge, LogicalEdgeType, LogicalGraph, LogicalNode, LogicalProgram, OperatorName,
};
use arroyo_planner::{parse_and_get_arrow_program, ArroyoSchemaProvider, SqlConfig};
use arroyo_state::parquet::ParquetBackend;
use petgraph::algo::has_path_connecting;
use petgraph::visit::EdgeRef;
use rstest::rstest;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::{env, time::SystemTime};
use tokio::sync::mpsc::{channel, Receiver};

use crate::udfs::get_udfs;
use arroyo_rpc::config;
use arroyo_rpc::grpc::rpc::{StopMode, TaskCheckpointCompletedReq, TaskCheckpointEventReq};
use arroyo_rpc::{CompactionResult, ControlMessage, ControlResp};
use arroyo_state::checkpoint_state::CheckpointState;
use arroyo_types::{to_micros, CheckpointBarrier};
use arroyo_udf_host::LocalUdf;
use arroyo_worker::engine::Engine;
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
    config::config();
    config::update(|c| {
        // reduce the batch size to increase consistency
        c.pipeline.source_batch_size = 32;
    });

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

    let pk = query.starts_with("--pk=").then(|| {
        query
            .lines()
            .next()
            .unwrap()
            .split_once('=')
            .unwrap()
            .1
            .trim()
            .split(',')
            .collect::<Vec<_>>()
    });

    match (
        correctness_run_codegen(test_name, query.clone(), pk.as_deref(), 20).await,
        fail,
    ) {
        (Ok(_), false) => {
            // ok
        }
        (Ok(_), true) => {
            panic!("Expected pipeline to fail, but it passed");
        }
        (Err(err), true) => {
            if let Some(error_message) = error_message {
                assert!(
                    err.to_string().contains(error_message),
                    "expected error message '{}' not found; instead got '{}'",
                    error_message,
                    err
                );
            }
        }
        (Err(err), false) => {
            panic!("Expected pipeline to pass, but it failed: {:?}", err);
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
                    node_id: c.node_id,
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
                    node_id: c.node_id,
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
    let operator_to_node = running_engine.operator_to_node();
    let operator_controls = running_engine.operator_controls();
    for (operator, _) in tasks_per_operator {
        if let Ok(compacted) =
            ParquetBackend::compact_operator(job_id.clone(), &operator, epoch).await
        {
            let node_id = operator_to_node.get(&operator).unwrap();
            let operator_controls = operator_controls.get(node_id).unwrap();
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
            graph
                .node_weight(*index)
                .unwrap()
                .operator_chain
                .iter()
                .any(|(c, _)| c.operator_name == OperatorName::ExpressionWatermark)
        })
        .collect();

    let indices: Vec<_> = graph
        .node_indices()
        .filter(|index| {
            !watermark_nodes.contains(index)
                && graph
                    .node_weight(*index)
                    .unwrap()
                    .operator_chain
                    .iter()
                    .any(|(c, _)| match c.operator_name {
                        OperatorName::ExpressionWatermark
                        | OperatorName::ConnectorSource
                        | OperatorName::ConnectorSink => false,
                        _ => {
                            for watermark_node in watermark_nodes.iter() {
                                if has_path_connecting(&*graph, *watermark_node, *index, None) {
                                    return true;
                                }
                            }
                            false
                        }
                    })
        })
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
            if graph
                .node_weight(node)
                .unwrap()
                .operator_chain
                .iter()
                .any(|(c, _)| c.operator_name == OperatorName::ExpressionWatermark)
            {
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

async fn run_and_checkpoint(
    job_id: Arc<String>,
    program: Program,
    tasks_per_operator: HashMap<String, usize>,
    control_rx: &mut Receiver<ControlResp>,
    checkpoint_interval: i32,
) {
    let engine = Engine::for_local(program, job_id.to_string());
    let running_engine = engine.start().await;
    info!("Smoke test checkpointing enabled");
    env::set_var(
        "ARROYO__CONTROLLER__COMPACTION__CHECKPOINTS_TO_COMPACT",
        "2",
    );

    let ctx = &mut SmokeTestContext {
        job_id: job_id.clone(),
        engine: &running_engine,
        control_rx,
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
    run_until_finished(&running_engine, control_rx).await;
}

async fn finish_from_checkpoint(
    job_id: &str,
    program: Program,
    control_rx: &mut Receiver<ControlResp>,
) {
    let engine = Engine::for_local(program, job_id.to_string());
    let running_engine = engine.start().await;

    info!("Restored engine, running until finished");
    run_until_finished(&running_engine, control_rx).await;
}

fn tasks_per_operator(graph: &LogicalGraph) -> HashMap<String, usize> {
    graph
        .node_weights()
        .flat_map(|node| {
            node.operator_chain
                .iter()
                .map(|(op, _)| (op.operator_id.clone(), node.parallelism))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn run_pipeline_and_assert_outputs(
    job_id: &str,
    mut graph: LogicalGraph,
    checkpoint_interval: i32,
    output_location: String,
    golden_output_location: String,
    udfs: &[LocalUdf],
    primary_keys: Option<&[&str]>,
) {
    // remove output_location before running the pipeline
    if std::path::Path::new(&output_location).exists() {
        std::fs::remove_file(&output_location).unwrap();
    }

    println!("Running completely");

    let (control_tx, mut control_rx) = channel(128);
    run_completely(
        job_id,
        Program::local_from_logical(job_id.to_string(), &graph, udfs, None, control_tx).await,
        output_location.clone(),
        golden_output_location.clone(),
        primary_keys,
        &mut control_rx,
    )
    .await;

    // debezium sources can't be arbitrarily split because they are effectively stateful
    // and ordering matters
    if primary_keys.is_none() {
        set_internal_parallelism(&mut graph, 2);
    }

    let (control_tx, mut control_rx) = channel(128);

    println!("Run and checkpoint");
    run_and_checkpoint(
        Arc::new(job_id.to_string()),
        Program::local_from_logical(job_id.to_string(), &graph, udfs, None, control_tx).await,
        tasks_per_operator(&graph),
        &mut control_rx,
        checkpoint_interval,
    )
    .await;

    if primary_keys.is_none() {
        set_internal_parallelism(&mut graph, 3);
    }

    let (control_tx, mut control_rx) = channel(128);

    println!("Finish from checkpoint");
    finish_from_checkpoint(
        job_id,
        Program::local_from_logical(job_id.to_string(), &graph, udfs, Some(3), control_tx).await,
        &mut control_rx,
    )
    .await;

    check_output_files(
        "resuming from checkpointing",
        output_location,
        golden_output_location,
        primary_keys,
    )
    .await;
}

async fn run_completely(
    job_id: &str,
    program: Program,
    output_location: String,
    golden_output_location: String,
    primary_keys: Option<&[&str]>,
    control_rx: &mut Receiver<ControlResp>,
) {
    let engine = Engine::for_local(program, job_id.to_string());
    let running_engine = engine.start().await;

    run_until_finished(&running_engine, control_rx).await;

    check_output_files(
        "initial run",
        output_location.clone(),
        golden_output_location,
        primary_keys,
    )
    .await;
    if std::path::Path::new(&output_location).exists() {
        std::fs::remove_file(&output_location).unwrap();
    }
}

fn get_key(v: &str, primary_keys: Option<&[&str]>) -> Vec<String> {
    let v: Value = serde_json::from_str(v).unwrap();

    println!("getting key from '{v}'");

    match primary_keys {
        Some(pks) => pks
            .iter()
            .map(|pk| v.get(pk).expect("primary key not found in row").to_string())
            .collect::<Vec<_>>(),
        None => {
            vec![v.to_string()]
        }
    }
}

fn merge_debezium(rows: Vec<Value>, primary_keys: Option<&[&str]>) -> HashSet<String> {
    let mut state = HashMap::new();
    for r in rows {
        let before = r.get("before").map(roundtrip);
        let after = r.get("after").map(roundtrip);
        let op = r.get("op").expect("no 'op' for debezium");

        match op.as_str().expect("op isn't string") {
            "c" => {
                let key = get_key(after.as_ref().expect("no after for c"), primary_keys);
                assert!(
                    state.insert(key, after.expect("no after for c")).is_none(),
                    "'c' for existing row"
                );
            }
            "u" => {
                let key = get_key(&before.expect("no before for 'u'"), primary_keys);
                assert!(
                    state.remove(&key).is_some(),
                    "'u' for non-existent row ({})",
                    r
                );
                let key = get_key(after.as_ref().expect("no after for 'u'"), primary_keys);
                assert!(
                    state
                        .insert(key, after.expect("no after for 'u'"))
                        .is_none(),
                    "'u' overwrote existing row"
                );
            }
            "d" => {
                let key = get_key(&before.expect("no before for 'd'"), primary_keys);
                assert!(
                    state.remove(&key).is_some(),
                    "'d' for non-existent row: {} ({:?})",
                    r,
                    primary_keys
                );
            }
            c => {
                panic!("unknown debezium op '{}'", c);
            }
        }
    }

    state.into_values().collect()
}

fn is_debezium(value: &Value) -> bool {
    let Some(op) = value.get("op") else {
        return false;
    };
    op.as_str().is_some()
}

fn check_debezium(
    name: &str,
    output_location: String,
    golden_output_location: String,
    output_lines: Vec<Value>,
    golden_output_lines: Vec<Value>,
    primary_keys: Option<&[&str]>,
) {
    let output_merged = merge_debezium(output_lines, primary_keys);
    let golden_output_medged = merge_debezium(golden_output_lines, primary_keys);
    assert_eq!(
        output_merged, golden_output_medged,
        "failed to check debezium equality ({}) for\noutput: {}\ngolden: {}",
        name, output_location, golden_output_location
    );
}
fn roundtrip(v: &Value) -> String {
    if v.is_null() {
        return "null".to_string();
    }
    // round trip string through a btreemap to get consistent key ordering
    serde_json::to_string(&serde_json::from_value::<BTreeMap<String, Value>>(v.clone()).unwrap())
        .unwrap()
}

async fn check_output_files(
    check_name: &str,
    output_location: String,
    golden_output_location: String,
    primary_keys: Option<&[&str]>,
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
                check_name,
                output_location,
                golden_output_location,
                output_lines,
                golden_output_lines,
                primary_keys,
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
    primary_keys: Option<&[&str]>,
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
        primary_keys,
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
