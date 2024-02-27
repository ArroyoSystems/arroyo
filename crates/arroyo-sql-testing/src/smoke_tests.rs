#![allow(warnings)]
use anyhow::Result;
use arroyo_datastream::logical::{
    LogicalEdge, LogicalEdgeType, LogicalNode, LogicalProgram, OperatorName, ProgramConfig,
};
use arroyo_df::{parse_and_get_arrow_program, ArroyoSchemaProvider, SqlConfig};
use arroyo_state::parquet::ParquetBackend;
use petgraph::algo::has_path_connecting;
use petgraph::visit::EdgeRef;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::time::Duration;
use std::{env, fmt::Debug, time::SystemTime};
use tokio::sync::mpsc::Receiver;

use arroyo_rpc::grpc::{StopMode, TaskCheckpointCompletedReq, TaskCheckpointEventReq};
use arroyo_rpc::{CompactionResult, ControlMessage, ControlResp};
use arroyo_state::checkpoint_state::CheckpointState;
use arroyo_types::{to_micros, CheckpointBarrier};
use arroyo_worker::engine::{Engine, StreamConfig};
use arroyo_worker::engine::{Program, RunningEngine};
use petgraph::{Direction, Graph};
use serde_json::{ser, Value};
use test_log::test;
use tokio::fs::read_to_string;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::info;

struct SmokeTestContext<'a> {
    job_id: String,
    engine: &'a RunningEngine,
    control_rx: &'a mut Receiver<ControlResp>,
    tasks_per_operator: HashMap<String, usize>,
}

async fn checkpoint(ctx: &mut SmokeTestContext<'_>, epoch: u32) {
    let checkpoint_id = epoch as i64;
    let mut checkpoint_state = CheckpointState::new(
        ctx.job_id.clone(),
        checkpoint_id,
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
                    job_id: ctx.job_id.clone(),
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
                    job_id: ctx.job_id.clone(),
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
    job_id: String,
    running_engine: &RunningEngine,
    tasks_per_operator: HashMap<String, usize>,
    epoch: u32,
) {
    let operator_controls = running_engine.operator_controls();
    for (operator, parallelism) in tasks_per_operator {
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
    loop {
        match control_rx.try_recv() {
            Ok(_) | Err(TryRecvError::Empty) => advance(engine, 10).await,
            Err(TryRecvError::Disconnected) => return,
        }
    }
}

fn set_internal_parallelism(graph: &mut Graph<LogicalNode, LogicalEdge>, parallelism: usize) {
    let watermark_nodes: HashSet<_> = graph
        .node_indices()
        .filter(
            |index| match graph.node_weight(*index).unwrap().operator_name {
                OperatorName::ExpressionWatermark => true,
                _ => false,
            },
        )
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

async fn test_checkpoints_and_compaction(
    job_id: String,
    _running_engine: &RunningEngine,
    checkpoint_interval: i32,
    mut control_rx: &mut Receiver<ControlResp>,
    tasks_per_operator: HashMap<String, usize>,
    mut graph: Graph<LogicalNode, LogicalEdge>,
) {
    set_internal_parallelism(&mut graph, 2);

    run_and_checkpoint(job_id.clone(), &graph, checkpoint_interval).await;
    set_internal_parallelism(&mut graph, 3);
    finish_from_checkpoint(job_id, &graph, Some(3)).await;
}

async fn run_and_checkpoint(
    job_id: String,
    graph: &Graph<LogicalNode, LogicalEdge>,
    checkpoint_interval: i32,
) {
    let program = Program::local_from_logical(job_id.clone(), graph);
    let tasks_per_operator = program.tasks_per_operator();
    let engine = Engine::for_local(program, job_id.clone());
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

    // compact checkpoint 2
    // TODO: compaction
    // compact(
    //     job_id.clone(),
    //     running_engine,
    //     tasks_per_operator.clone(),
    //     2,
    // )
    // .await;

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

async fn finish_from_checkpoint(
    job_id: String,
    graph: &Graph<LogicalNode, LogicalEdge>,
    restore_epoch: Option<u32>,
) {
    let program = Program::local_from_logical(job_id.clone(), &graph);
    let engine = Engine::for_local(program, job_id.clone());
    let (running_engine, mut control_rx) = engine
        .start(StreamConfig {
            restore_epoch: Some(3),
        })
        .await;

    info!("Restored engine, running until finished");
    run_until_finished(&running_engine, &mut control_rx).await;
}

async fn run_pipeline_and_assert_outputs(
    graph: Graph<LogicalNode, LogicalEdge>,
    job_id: String,
    checkpoint_interval: i32,
    output_location: String,
    golden_output_location: String,
) {
    // remove output_location before running the pipeline
    if std::path::Path::new(&output_location).exists() {
        std::fs::remove_file(&output_location).unwrap();
    }

    let program = Program::local_from_logical(job_id.clone(), &graph);
    run_completely(
        Program::local_from_logical(job_id.clone(), &graph),
        job_id.clone(),
        output_location.clone(),
        golden_output_location.clone(),
    )
    .await;
    let program = Program::local_from_logical(job_id.clone(), &graph);
    let tasks_per_operator = program.tasks_per_operator();
    let engine = Engine::for_local(program, job_id.clone());
    let (running_engine, mut control_rx) = engine
        .start(StreamConfig {
            restore_epoch: None,
        })
        .await;

    test_checkpoints_and_compaction(
        job_id,
        &running_engine,
        checkpoint_interval,
        &mut control_rx,
        tasks_per_operator,
        graph,
    )
    .await;

    run_until_finished(&running_engine, &mut control_rx).await;

    check_output_files(output_location, golden_output_location).await;
}

async fn run_completely(
    program: Program,
    job_id: String,
    output_location: String,
    golden_output_location: String,
) {
    let tasks_per_operator = program.tasks_per_operator();
    let engine = Engine::for_local(program, job_id.clone());
    let (running_engine, mut control_rx) = engine
        .start(StreamConfig {
            restore_epoch: None,
        })
        .await;

    run_until_finished(&running_engine, &mut control_rx).await;

    check_output_files(output_location.clone(), golden_output_location).await;
    if std::path::Path::new(&output_location).exists() {
        std::fs::remove_file(&output_location).unwrap();
    }
}

async fn check_output_files(output_location: String, golden_output_location: String) {
    fn roundtrip(v: &Value) -> String {
        // round trip string through a btreemap to get consistent key ordering
        serde_json::to_string(
            &serde_json::from_value::<BTreeMap<String, Value>>(v.clone()).unwrap(),
        )
        .unwrap()
    }

    let mut output_lines: Vec<Value> = read_to_string(output_location.clone())
        .await
        .expect(&format!("output file not found at {}", output_location))
        .lines()
        .map(|s| serde_json::from_str(s).expect(s))
        .collect();

    let mut golden_output_lines: Vec<Value> = read_to_string(golden_output_location.clone())
        .await
        .expect(
            format!(
                "golden output file not found at {}, want to compare to {}",
                golden_output_location, output_location
            )
            .as_str(),
        )
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    if output_lines.len() != golden_output_lines.len() {
        panic!(
            "output {} and golden output {} have different number of lines",
            output_location, golden_output_location
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
                "line {} of output and golden output differ ({}, {})",
                i, output_location, golden_output_location
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
    let logical_program = get_graph(query_string.clone()).await?;
    run_pipeline_and_assert_outputs(
        logical_program.graph,
        test_name.into(),
        checkpoint_interval,
        physical_output,
        golden_output_location,
    )
    .await;
    Ok(())
}

async fn get_graph(query_string: String) -> Result<LogicalProgram> {
    let mut schema_provider = ArroyoSchemaProvider::new();

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

#[tokio::test]
async fn select_star() -> Result<()> {
    correctness_run_codegen(
        "select_star".to_string(),
        "CREATE TABLE cars (
      timestamp TIMESTAMP,
      driver_id BIGINT,
      event_type TEXT,
      location TEXT
    ) WITH (
      connector = 'single_file',
      path = '$input_dir/cars.json',
      format = 'json',
      type = 'source'
    );

    CREATE TABLE cars_output (
      timestamp TIMESTAMP,
      driver_id BIGINT,
      event_type TEXT,
      location TEXT
    ) WITH (
      connector = 'single_file',
      path = '$output_path',
      format = 'json',
      type = 'sink'
    );
    INSERT INTO cars_output SELECT * FROM cars"
            .to_string(),
        200,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn windowed_outer_join() -> Result<()> {
    correctness_run_codegen(
        "windowed_outer_join",
        "CREATE TABLE cars (
          timestamp TIMESTAMP not null,
          driver_id BIGINT,
          event_type TEXT,
          location TEXT
        ) WITH (
          connector = 'single_file',
          path = '$input_dir/cars.json',
          format = 'json',
          type = 'source',
          event_time_field = 'timestamp'
        );
        CREATE TABLE hourly_aggregates (
          hour TIMESTAMP,
          drivers BIGINT,
          pickups BIGINT
        ) WITH (
          connector = 'single_file',
          path = '$output_path',
          format = 'json',
          type = 'sink'
        );
        INSERT INTO hourly_aggregates
        SELECT window.start as hour, dropoff_drivers, pickup_drivers FROM (
        SELECT dropoffs.window as window, dropoff_drivers, pickup_drivers
        FROM (
          SELECT  TUMBLE(INTERVAL '1' hour) as window,
          COUNT(distinct driver_id) as dropoff_drivers FROM cars where event_type = 'dropoff'
          GROUP BY 1
        ) dropoffs
        FULL OUTER JOIN (
          SELECT  TUMBLE(INTERVAL '1' hour) as window,
          COUNT(distinct driver_id) as pickup_drivers FROM cars where event_type = 'pickup'
          GROUP BY 1
        ) pickups
        ON dropoffs.window.start = pickups.window.start)
      ",
        100,
    )
    .await?;
    Ok(())
}

/*

correctness_run_codegen! {"aggregates", 10,
"CREATE TABLE impulse_source (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'impulse',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source'
);
CREATE TABLE aggregates (
  min BIGINT,
  max BIGINT,
  sum BIGINT,
  count BIGINT,
  avg DOUBLE
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'debezium_json',
  type = 'sink'
);
INSERT INTO aggregates SELECT min(counter), max(counter), sum(counter), count(*), avg(counter)  FROM impulse_source"}
*/

#[tokio::test]
async fn hourly_by_event_type() -> Result<()> {
    correctness_run_codegen(
        "hourly_by_event_type",
        "CREATE TABLE cars(
  timestamp TIMESTAMP,
  driver_id BIGINT,
  event_type TEXT,
  location TEXT
) WITH (
  connector = 'single_file',
  path = '$input_dir/cars.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);
CREATE TABLE group_by_aggregate (
  event_type TEXT,
  hour TIMESTAMP,
  count BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO group_by_aggregate
SELECT event_type, window.start as hour, count
FROM (
SELECT event_type, TUMBLE(INTERVAL '1' HOUR) as window, COUNT(*) as count
FROM cars
GROUP BY 1,2);
",
        200,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn session_windows() -> Result<()> {
    correctness_run_codegen("session_window", "CREATE TABLE impulse_source (
    timestamp TIMESTAMP,
    counter bigint unsigned not null,
    subtask_index bigint unsigned not null
  ) WITH (
    connector = 'single_file',
    path = '$input_dir/impulse.json',
    format = 'json',
    type = 'source',
    event_time_field = 'timestamp'
  );
  
  CREATE TABLE session_window_output (
    start timestamp,
    end timestamp,
    user_id bigint,
    rows bigint,
  ) WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'json',
    type = 'sink'
  );

  INSERT INTO session_window_output
  SELECT window.start, window.end, user_id, rows FROM (
      SELECT SESSION(interval '20 seconds') as window, Case when counter%10=0 then 0 else counter END as user_id, count(*) as rows
      FROM impulse_source GROUP BY window, user_id);
  ", 20).await?;
    Ok(())
}

#[tokio::test]
async fn overall_tumbling_aggregate() -> Result<()> {
    correctness_run_codegen(
        "overall_tumbling",
        "CREATE TABLE impulse_source (
    timestamp TIMESTAMP,
    counter bigint unsigned not null,
    subtask_index bigint unsigned not null
  ) WITH (
    connector = 'single_file',
    path = '$input_dir/impulse.json',
    format = 'json',
    type = 'source',
    event_time_field = 'timestamp'
  );

  CREATE TABLE overall_tumbling_table (
    start timestamp,
    end timestamp,
    rows bigint,
  ) WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'json',
    type = 'sink'
  );

  INSERT INTO overall_tumbling_table
  SELECT window.start as start, window.end as end, rows FROM (
  SELECT  TUMBLE(INTERVAL '1' hour) as window,
  COUNT(*) as rows
  FROM impulse_source
  GROUP BY 1)
  ",
        10,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn delayed_join() -> Result<()> {
    correctness_run_codegen(
        "delayed_join",
        "CREATE TABLE impulse_source (
    timestamp TIMESTAMP,
    counter bigint unsigned not null,
    subtask_index bigint unsigned not null
  ) WITH (
    connector = 'single_file',
    path = '$input_dir/impulse.json',
    format = 'json',
    type = 'source',
    event_time_field = 'timestamp'
  );

  CREATE TABLE delayed_impulse_source (
    timestamp TIMESTAMP,
    counter bigint unsigned not null,
    subtask_index bigint unsigned not null,
    watermark TIMESTAMP GENERATED ALWAYS AS (timestamp - INTERVAL '1 day') STORED
  ) WITH (
    connector = 'single_file',
    path = '$input_dir/impulse.json',
    format = 'json',
    type = 'source',
    event_time_field = 'timestamp'
  );

  CREATE TABLE join_result (
    left_counter bigint unsigned not null,
  ) WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'json',
    type = 'sink'
  );

  INSERT INTO join_result
  SELECT l.counter as left_counter 
  FROM impulse_source l
  JOIN (SELECT 2 * counter as counter FROM delayed_impulse_source) r
  ON l.counter = r.counter
  ",
        10,
    )
    .await?;
    Ok(())
}

/*
#[tokio::test]
async fn month_loose_watermark() -> Result<()> {
    correctness_run_codegen(
        "month_loose_watermark",
        "CREATE TABLE cars(
          timestamp TIMESTAMP,
          driver_id BIGINT,
          event_type TEXT,
          location TEXT,
          watermark TIMESTAMP GENERATED ALWAYS AS (timestamp - INTERVAL '1 minute') STORED
        ) WITH (
          connector = 'single_file',
          path = '$input_dir/cars.json',
          format = 'json',
          type = 'source',
          event_time_field = 'timestamp',
          watermark_field = 'watermark'
        );
        CREATE TABLE group_by_aggregate (
          month TIMESTAMP,
          count BIGINT
        ) WITH (
          connector = 'single_file',
          path = '$output_path',
          format = 'json',
          type = 'sink'
        );
        INSERT INTO group_by_aggregate
        SELECT window.start as month, count
        FROM (
        SELECT TUMBLE(INTERVAL '1' month) as window, COUNT(*) as count
        FROM cars
        GROUP BY 1);
        ",
        200,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn tight_watermark() -> Result<()> {
    correctness_run_codegen(
        "tight_watermark",
        "CREATE TABLE cars(
  timestamp TIMESTAMP,
  driver_id BIGINT,
  event_type TEXT,
  location TEXT
) WITH (
  connector = 'single_file',
  path = '$input_dir/cars.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp',
  watermark_field = 'timestamp'
);
CREATE TABLE group_by_aggregate (
  timestamp TIMESTAMP,
  count BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO group_by_aggregate
SELECT window.end as timestamp, count
FROM (
SELECT TUMBLE(INTERVAL '1' second) as window, COUNT(*) as count
FROM cars
GROUP BY 1);
",
        20,
    )
    .await?;
    Ok(())
}

correctness_run_codegen! {"suspicious_drivers", 200,
"CREATE TABLE cars(
  timestamp TIMESTAMP,
  driver_id BIGINT,
  event_type TEXT,
  location TEXT
) WITH (
  connector = 'single_file',
  path = '$input_dir/sorted_cars.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp',
  watermark_field = 'timestamp'
);
CREATE TABLE suspicious_drivers (
  drivers BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'debezium_json',
  type = 'sink'
);
INSERT INTO suspicious_drivers
SELECT count(*) drivers FROM
(
SELECT driver_id, sum(case when event_type = 'pickup' then 1 else 0 END ) as pickups,
sum(case when event_type = 'dropoff' THEN 1 else 0 END) as dropoffs
FROM cars
GROUP BY 1
) WHERE pickups < dropoffs
"}

correctness_run_codegen! {"most_active_driver_last_hour_unaligned", 200,
"CREATE TABLE cars (
  timestamp TIMESTAMP,
  driver_id BIGINT,
  event_type TEXT,
  location TEXT
) WITH (
  connector = 'single_file',
  path = '$input_dir/cars.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);
CREATE TABLE most_active_driver (
  driver_id BIGINT,
  count BIGINT,
  start TIMESTAMP,
  end TIMESTAMP,
  row_number BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO most_active_driver
SELECT * FROM (
  SELECT *, ROW_NUMBER()  OVER (
    PARTITION BY hop(INTERVAL '40' minute, INTERVAL '1' hour )
    ORDER BY count DESC, driver_id desc) as row_number
  FROM (
      SELECT driver_id, count, window.start as start, window.end as end FROM (
  SELECT driver_id,
  hop(INTERVAL '40' minute, INTERVAL '1' hour ) as window,
         count(*) as count
         FROM cars
         GROUP BY 1,2)) ) where row_number = 1
"}

correctness_run_codegen! {"most_active_driver_last_hour", 200,
"CREATE TABLE cars (
  timestamp TIMESTAMP,
  driver_id BIGINT,
  event_type TEXT,
  location TEXT
) WITH (
  connector = 'single_file',
  path = '$input_dir/cars.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);
CREATE TABLE most_active_driver (
  driver_id BIGINT,
  count BIGINT,
  start TIMESTAMP,
  end TIMESTAMP,
  row_number BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO most_active_driver
SELECT * FROM (
  SELECT *, ROW_NUMBER()  OVER (
    PARTITION BY hop(INTERVAL '1' minute, INTERVAL '1' hour )
    ORDER BY count DESC, driver_id desc) as row_number
  FROM (
      SELECT driver_id, count, window.start as start, window.end as end FROM (
  SELECT driver_id,
  hop(INTERVAL '1' minute, INTERVAL '1' hour ) as window,
         count(*) as count
         FROM cars
         GROUP BY 1,2)) ) where row_number = 1
"}

correctness_run_codegen! {"outer_join", 200,
"CREATE TABLE cars (
  timestamp TIMESTAMP,
  driver_id BIGINT,
  event_type TEXT,
  location TEXT
) WITH (
  connector = 'single_file',
  path = '$input_dir/cars.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);
CREATE TABLE driver_status (
  driver_id BIGINT,
  has_pickup BOOLEAN,
  has_dropoff BOOLEAN
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'debezium_json',
  type = 'sink'
);
INSERT INTO driver_status
SELECT distinct COALESCE(pickups.driver_id, dropoffs.driver_id) as
driver_id, pickups.driver_id is not null as has_pickup,
dropoffs.driver_id is not null as has_dropoff
FROM
(SELECT driver_id FROM cars WHERE event_type = 'pickup' and driver_id % 100 = 0) pickups
FULL OUTER JOIN
(SELECT driver_id FROM cars WHERE event_type = 'dropoff' and driver_id % 100 = 0) dropoffs
ON pickups.driver_id = dropoffs.driver_id
"}

correctness_run_codegen! {"windowed_inner_join", 200,
"CREATE TABLE cars (
  timestamp TIMESTAMP,
  driver_id BIGINT,
  event_type TEXT,
  location TEXT
) WITH (
  connector = 'single_file',
  path = '$input_dir/cars.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);
CREATE TABLE hourly_aggregates (
  hour TIMESTAMP,
  drivers BIGINT,
  pickups BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO hourly_aggregates
SELECT window.start as hour, dropoff_drivers, pickup_drivers FROM (
SELECT dropoffs.window as window, dropoff_drivers, pickup_drivers
FROM (
  SELECT  TUMBLE(INTERVAL '1' hour) as window,
   COUNT(distinct driver_id) as dropoff_drivers FROM cars where event_type = 'dropoff'
  GROUP BY 1
) dropoffs
INNER JOIN (
  SELECT  TUMBLE(INTERVAL '1' hour) as window,
   COUNT(distinct driver_id) as pickup_drivers FROM cars where event_type = 'pickup'
  GROUP BY 1
) pickups
ON dropoffs.window = pickups.window)
"}

correctness_run_codegen! {"aggregates", 10,
"CREATE TABLE impulse_source (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source'
);
CREATE TABLE aggregates (
  min BIGINT,
  max BIGINT,
  sum BIGINT,
  count BIGINT,
  avg DOUBLE
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'debezium_json',
  type = 'sink'
);
INSERT INTO aggregates SELECT min(counter), max(counter), sum(counter), count(*), avg(counter)  FROM impulse_source"}

// test double negative UDF
correctness_run_codegen! {"double_negative_udf", 10,
"CREATE TABLE impulse_source (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source'
);
CREATE TABLE double_negative_udf (
  counter bigint
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO double_negative_udf
SELECT double_negative(counter) FROM impulse_source",
"pub fn double_negative(x: u64) -> i64 {
  -2 * (x as i64)
}"}

correctness_run_codegen! {"async_udf", 10,
"CREATE TABLE impulse_source (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source'
);
CREATE TABLE double_negative_udf (
  counter bigint
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO double_negative_udf
SELECT double_negative(counter) FROM impulse_source",
"pub async fn double_negative(x: u64) -> i64 {
  -2 * (x as i64)
}"}

// test UDAF
correctness_run_codegen! {"udaf", 10,
"CREATE TABLE impulse_source (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source'
);
CREATE TABLE udaf (
  median bigint,
  none_value double,
  max_product bigint
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO udaf SELECT median, none_value, max_product FROM (
  SELECT  tumble(interval '1' month) as window, my_median(counter) as median, none_udf(counter) as none_value,
  max_product(counter, subtask_index) as max_product
FROM impulse_source
GROUP BY 1)",
"pub fn my_median(args: Vec<u64>) -> f64 {
  let mut args = args;
  args.sort();
  let mid = args.len() / 2;
  if args.len() % 2 == 0 {
      (args[mid] + args[mid - 1]) as f64 / 2.0
  } else {
      args[mid] as f64
  }
}
pub fn none_udf(args: Vec<u64>) -> Option<f64> {
  None
}
pub fn max_product(first_arg: Vec<u64>, second_arg: Vec<u64>) -> u64 {
  let pairs = first_arg.iter().zip(second_arg.iter());
  pairs.map(|(x, y)| x * y).max().unwrap()
}"}

// filter updating aggregates
correctness_run_codegen! {"filter_updating_aggregates", 10,
"CREATE TABLE impulse_source (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source'
);
CREATE TABLE filter_updating_aggregates (
  subtasks BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'debezium_json',
  type = 'sink'
);

INSERT INTO filter_updating_aggregates
SELECT * FROM (
  SELECT count(distinct subtask_index) as subtasks FROM impulse_source
)
WHERE subtasks >= 1"}

correctness_run_codegen! {"union", 10,
"CREATE TABLE impulse_source (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source'
);
CREATE TABLE second_impulse_source (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source'
);
CREATE TABLE union_output (
  counter bigint
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO union_output
SELECT counter  FROM impulse_source
UNION ALL SELECT counter FROM second_impulse_source"
}

correctness_run_codegen! {"session_window", 10,
"CREATE TABLE impulse_source (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);

CREATE TABLE session_window_output (
  start timestamp,
  end timestamp,
  user_id bigint,
  rows bigint,
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);

INSERT INTO session_window_output
SELECT window.start, window.end, user_id, rows FROM (
    SELECT SESSION(interval '20 seconds') as window, Case when counter%10=0 then 0 else counter END as user_id, count(*) as rows
    FROM impulse_source GROUP BY window, user_id);
"}

correctness_run_codegen! {"offset_impulse_join", 10,
"CREATE TABLE impulse_source (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);
CREATE TABLE delayed_impulse_source (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null,
  watermark timestamp GENERATED ALWAYS AS (timestamp - INTERVAL '10 minute')
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp',
  watermark_field = 'watermark'
);
CREATE TABLe offset_output (
  start timestamp,
  counter bigint
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO offset_output
SELECT window.start, a.counter as counter
FROM (SELECT TUMBLE(interval '1 second'),  counter, count(*) FROM impulse_source GROUP BY 1,2) a

JOIN (SELECT TUMBLE(interval '1 second') as window, counter , count(*) FROM delayed_impulse_source GROUP BY 1,2) b
ON a.counter = b.counter;"}

correctness_run_codegen! {"updating_left_join", 10,
"CREATE TABLE impulse (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);


CREATE TABLE output (
  left_counter bigint,
  counter_mod_2 bigint,
  right_count bigint
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'debezium_json',
  type = 'sink'
);

INSERT INTO output
select counter as left_counter, counter_mod_2, right_count from impulse left join
     (select counter % 2 as counter_mod_2, cast(count(*) as bigint UNSIGNED) as right_count from impulse where counter < 3 group by 1)
    on counter = right_count where counter < 3;"}

correctness_run_codegen! {"updating_right_join", 10,
"CREATE TABLE impulse (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);


CREATE TABLE output (
  left_counter bigint,
  counter_mod_2 bigint,
  right_count bigint
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'debezium_json',
  type = 'sink'
);

INSERT INTO output
select counter as left_counter, counter_mod_2, right_count from impulse right join
     (select counter % 2 as counter_mod_2, cast(count(*) as bigint UNSIGNED) as right_count from impulse where counter < 3 group by 1)
    on counter = right_count where counter < 3;"}

correctness_run_codegen! {"updating_inner_join", 10,
"CREATE TABLE impulse (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);


CREATE TABLE output (
  left_counter bigint,
  counter_mod_2 bigint,
  right_count bigint
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'debezium_json',
  type = 'sink'
);

INSERT INTO output
select counter as left_counter, counter_mod_2, right_count from impulse inner join
     (select counter % 2 as counter_mod_2, cast(count(*) as bigint UNSIGNED) as right_count from impulse where counter < 3 group by 1)
    on counter = right_count where counter < 3;"}

correctness_run_codegen! {"updating_full_join", 10,
"CREATE TABLE impulse (
  timestamp TIMESTAMP,
  counter bigint unsigned not null,
  subtask_index bigint unsigned not null
) WITH (
  connector = 'single_file',
  path = '$input_dir/impulse.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);


CREATE TABLE output (
  left_counter bigint,
  counter_mod_2 bigint,
  right_count bigint
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'debezium_json',
  type = 'sink'
);

INSERT INTO output
select counter as left_counter, counter_mod_2, right_count from impulse full outer join
     (select counter % 2 as counter_mod_2, cast(count(*) as bigint UNSIGNED) as right_count from impulse where counter < 3 group by 1)
    on counter = right_count where counter < 3;"}
*/
