#![allow(warnings)]
use std::{alloc::System, fmt::Debug, sync::Arc, time::SystemTime};

use arroyo_rpc::ControlResp;
use arroyo_sql_macro::{correctness_run_codegen, full_pipeline_codegen};
use arroyo_worker::{
    engine::{Engine, StreamConfig},
    LocalRunner, LogicalEdge, LogicalNode,
};
use petgraph::Graph;
use tokio::fs::read_to_string;

async fn run_pipeline_and_assert_outputs(
    graph: Graph<LogicalNode, LogicalEdge>,
    name: String,
    output_location: String,
    golden_output_location: String,
) {
    // remove output_location before running the pipeline
    if std::path::Path::new(&output_location).exists() {
        std::fs::remove_file(&output_location).unwrap();
    }

    let program = arroyo_worker::engine::Program::local_from_logical(name.clone(), &graph);
    let (running_engine, mut control_rx) = Engine::for_local(program, name.clone())
        .start(StreamConfig {
            restore_epoch: None,
        })
        .await;
    // wait for the engine to finish
    while let Some(control_resp) = control_rx.recv().await {}

    check_output_files(output_location, golden_output_location).await;
}

async fn check_output_files(output_location: String, golden_output_location: String) {
    let mut output_lines: Vec<_> = read_to_string(output_location.clone())
        .await
        .unwrap()
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

correctness_run_codegen! {"select_star",
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

correctness_run_codegen! {"hourly_by_event_type",
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

correctness_run_codegen! {"month_loose_watermark",
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

correctness_run_codegen! {"tight_watermark",
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

correctness_run_codegen! {"suspicious_drivers",
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

correctness_run_codegen! {"most_active_driver_last_hour_unaligned",
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

correctness_run_codegen! {"most_active_driver_last_hour",
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

correctness_run_codegen! {"outer_join",
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

correctness_run_codegen! {"windowed_outer_join",
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

correctness_run_codegen! {"windowed_inner_join",
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
