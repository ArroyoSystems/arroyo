#![allow(warnings)]
use arroyo_state::parquet::ParquetBackend;
use std::collections::HashMap;
use std::{env, fmt::Debug, time::SystemTime};
use tokio::sync::mpsc::Receiver;

use arroyo_rpc::grpc::{StopMode, TaskCheckpointCompletedReq, TaskCheckpointEventReq};
use arroyo_rpc::{ControlMessage, ControlResp};
use arroyo_sql_macro::correctness_run_codegen;
use arroyo_state::checkpoint_state::CheckpointState;
use arroyo_types::{to_micros, CheckpointBarrier};
use arroyo_worker::engine::{Program, RunningEngine};
use arroyo_worker::{
    engine::{Engine, StreamConfig},
    LogicalEdge, LogicalNode,
};
use petgraph::Graph;
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
        if let Ok(Some(compacted)) =
            ParquetBackend::compact_operator(parallelism, job_id.clone(), operator.clone(), epoch)
                .await
        {
            let operator_controls = operator_controls.get(&operator).unwrap();
            for s in operator_controls {
                s.send(ControlMessage::LoadCompacted {
                    compacted: compacted.clone(),
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
        advance(engine, 100).await;
    }
}

async fn test_checkpoints_and_compaction(
    job_id: String,
    running_engine: &RunningEngine,
    checkpoint_interval: i32,
    mut control_rx: &mut Receiver<ControlResp>,
    tasks_per_operator: HashMap<String, usize>,
    graph: Graph<LogicalNode, LogicalEdge>,
) {
    info!("Smoke test checkpointing enabled");
    env::set_var("MIN_FILES_TO_COMPACT", "2");

    let ctx = &mut SmokeTestContext {
        job_id: job_id.clone(),
        engine: running_engine,
        control_rx: &mut control_rx,
        tasks_per_operator: tasks_per_operator.clone(),
    };

    // trigger a couple checkpoints
    advance(running_engine, checkpoint_interval).await;
    checkpoint(ctx, 1).await;
    advance(running_engine, checkpoint_interval).await;
    checkpoint(ctx, 2).await;
    advance(running_engine, checkpoint_interval).await;

    // compact checkpoint 2
    compact(
        job_id.clone(),
        running_engine,
        tasks_per_operator.clone(),
        2,
    )
    .await;

    // trigger checkpoint 3, which will include the compacted files
    advance(running_engine, checkpoint_interval).await;
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
    run_until_finished(running_engine, &mut control_rx).await;

    // create a new engine, restore from the last checkpoint
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
    let mut output_lines: Vec<_> = read_to_string(output_location.clone())
        .await
        .expect(&format!("output file not found at {}", output_location))
        .lines()
        .map(|x| x.to_string())
        .collect();

    let mut golden_output_lines: Vec<_> = read_to_string(golden_output_location.clone())
        .await
        .expect(
            format!(
                "golden output file not found at {}, want to compare to {}",
                golden_output_location, output_location
            )
            .as_str(),
        )
        .lines()
        .map(|x| x.to_string())
        .collect();
    if output_lines.len() != golden_output_lines.len() {
        panic!("output and golden output have different number of lines");
    }

    output_lines.sort();
    golden_output_lines.sort();
    output_lines
        .into_iter()
        .zip(golden_output_lines.into_iter())
        .enumerate()
        .for_each(|(i, (output_line, golden_output_line))| {
            assert_eq!(
                output_line, golden_output_line,
                "line {} of output and golden output differ",
                i
            )
        });
}

correctness_run_codegen! {"select_star", 200,
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
INSERT INTO cars_output SELECT * FROM cars"}

correctness_run_codegen! {"hourly_by_event_type", 200,
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
"}

correctness_run_codegen! {"month_loose_watermark", 200,
"CREATE TABLE cars(
  timestamp TIMESTAMP,
  driver_id BIGINT,
  event_type TEXT,
  location TEXT,
  watermark TIMESTAMP GENERATED ALWAYS AS (timestamp - INTERVAL '1 minute')
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
"}

correctness_run_codegen! {"tight_watermark", 200,
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
"}

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

correctness_run_codegen! {"windowed_outer_join", 200,
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
FULL OUTER JOIN (
  SELECT  TUMBLE(INTERVAL '1' hour) as window,
   COUNT(distinct driver_id) as pickup_drivers FROM cars where event_type = 'pickup'
  GROUP BY 1
) pickups
ON dropoffs.window = pickups.window)
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
