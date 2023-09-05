#![allow(warnings)]
use std::{fmt::Debug, sync::Arc};

use arroyo_rpc::ControlResp;
use arroyo_sql_macro::{correctness_run_codegen, full_pipeline_codegen};
use arroyo_worker::{
    engine::{Engine, StreamConfig},
    LocalRunner, LogicalEdge, LogicalNode,
};
use petgraph::Graph;
use tokio::fs::read_to_string;

pub(crate) struct CompiledIntegrationTestProgram {
    graph: Graph<LogicalNode, LogicalEdge>,
    name: String,
    output_location: String,
    golden_output_location: String,
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

async fn run_pipeline_and_assert_outputs(
    graph: Graph<LogicalNode, LogicalEdge>,
    name: String,
    output_location: String,
    golden_output_location: String,
) {
    let program = arroyo_worker::engine::Program::local_from_logical(name, &graph);
    let (running_engine, mut control_rx) = Engine::for_local(program, "test".to_string())
        .start(StreamConfig {
            restore_epoch: None,
        })
        .await;
    while let Some(control_resp) = control_rx.recv().await {}

    // get the lines of output and golden output
    let mut output_lines: Vec<_> = read_to_string(output_location)
        .await
        .unwrap()
        .lines()
        .map(|x| x.to_string())
        .collect();
    let mut golden_output_lines: Vec<_> = read_to_string(golden_output_location.clone())
        .await
        .expect(&golden_output_location)
        .lines()
        .map(|x| x.to_string())
        .collect();
    if output_lines.len() != golden_output_lines.len() {
        panic!("output and golden output have different number of lines");
    }
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

full_pipeline_codegen! {"query_5_join",
"WITH bids as (SELECT bid.auction as auction, bid.datetime as datetime
    FROM (select bid from  nexmark) where bid is not null)
    SELECT AuctionBids.auction as auction, AuctionBids.num as count
    FROM (
      SELECT
        B1.auction,
        HOP(INTERVAL '2' SECOND, INTERVAL '10' SECOND) as window,
        count(*) AS num

      FROM bids B1
      GROUP BY
        1,2
    ) AS AuctionBids
    JOIN (
      SELECT
        max(num) AS maxn,
        window
      FROM (
        SELECT
          count(*) AS num,
          HOP(INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS window
        FROM bids B2
        GROUP BY
          B2.auction,2
        ) AS CountBids
      GROUP BY 2
    ) AS MaxBids
    ON
       AuctionBids.num = MaxBids.maxn
       and AuctionBids.window = MaxBids.window;"}

full_pipeline_codegen! {"watermark_test",
"CREATE TABLE person (
  id bigint,
  name TEXT,
  email TEXT,
  date_string text,
  datetime datetime GENERATED ALWAYS AS (CAST(date_string as timestamp)),
  watermark datetime GENERATED ALWAYS AS (CAST(date_string as timestamp) - interval '1 second')
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  type = 'source',
  topic = 'person',
  format = 'json',
  event_time_field = 'datetime',
  watermark_field = 'watermark'
);

SELECT id, name, email FROM person;"}

full_pipeline_codegen! {"sliding_count_distinct",
"WITH bids as (
  SELECT bid.auction as auction, bid.price as price, bid.bidder as bidder, bid.extra as extra, bid.datetime as datetime
  FROM nexmark where bid is not null)

SELECT * FROM (
SELECT bidder, COUNT( distinct auction) as distinct_auctions
FROM bids B1
GROUP BY bidder, HOP(INTERVAL '3 second', INTERVAL '10' minute)) WHERE distinct_auctions > 2"}

full_pipeline_codegen! {"right_join",
"SELECT *
FROM (SELECT bid.auction as auction, bid.price as price
FROM nexmark WHERE bid is not null) bids

RIGHT JOIN (SELECT auction.id as id, auction.initial_bid as initial_bid
FROM nexmark where auction is not null) auctions on bids.auction = auctions.id;"}

full_pipeline_codegen! {"inner_join",
"SELECT *
FROM (SELECT bid.auction as auction, bid.price as price
FROM nexmark WHERE bid is not null) bids

JOIN (SELECT auction.id as id, auction.initial_bid as initial_bid
FROM nexmark where auction is not null) auctions on bids.auction = auctions.id;"}

full_pipeline_codegen! {"left_join",
"SELECT *
FROM (SELECT bid.auction as auction, bid.price as price
FROM nexmark WHERE bid is not null) bids

LEFT JOIN (SELECT auction.id as id, auction.initial_bid as initial_bid
FROM nexmark where auction is not null) auctions on bids.auction = auctions.id;"}

full_pipeline_codegen! {"non_null_outer_join",
"CREATE TABLE join_input (
  key BIGINT NOT NULL,
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  type = 'source',
  topic = 'join_input',
  format = 'json'
);
SELECT * FROM join_input a
full outer join join_input b on a.key =b.key;"}

full_pipeline_codegen! {"debezium_source", "CREATE table debezium_source (
  bids_auction int,
  price int,
  auctions_id int,
  initial_bid int
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  type = 'source',
  topic = 'updating',
  format = 'debezium_json'
);

SELECT * FROM debezium_source"}

full_pipeline_codegen! {"forced_debezium_sink", "
CREATE TABLE kafka_raw_sink (
  sum bigint,
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  type = 'sink',
  topic = 'raw_sink',
  format = 'debezium_json'
);
INSERT INTO kafka_raw_sink
SELECT bid.price FROM nexmark;
"}

full_pipeline_codegen! {"filter_on_updating_aggregates", "
SELECT auction  / 2 as half_auction
FROM (
SELECT auction FROM (
SELECT count(*) as bids, bid.auction as auction from nexmark where bid is not null
GROUP BY 2
) WHERE bids > 1 and bids < 10
)
WHERE auction % 2 = 0"}

full_pipeline_codegen! {"create_parquet_s3_source",
"CREATE TABLE bids (
  auction bigint,
  bidder bigint,
  price bigint,
  datetime timestamp
) WITH (
  connector ='filesystem',
  path = 'https://s3.us-west-2.amazonaws.com/demo/s3-uri',
  format = 'json',
  rollover_seconds = '5'
);

INSERT INTO Bids select bid.auction, bid.bidder, bid.price , bid.datetime FROM nexmark where bid is not null;"}

full_pipeline_codegen! {"cast_bug",
"SELECT CAST(1 as FLOAT)
from nexmark; "}

full_pipeline_codegen! {"session_window",
"SELECT count(*), session(INTERVAL '10' SECOND) AS window
from nexmark
group by window, auction.id; "}

full_pipeline_codegen! {"virtual_field_implicit_cast",
"create table demo_stream (
  timestamp BIGINT NOT NULL,
  event_time TIMESTAMP GENERATED ALWAYS AS (CAST(from_unixtime(timestamp * 1000000000) as TIMESTAMP))
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  topic = 'demo-stream',
  format = 'json',
  type = 'source',
  event_time_field = 'event_time'
);

select * from demo_stream;
"}

full_pipeline_codegen! {"count_over_case",
"SELECT count(case when person.name = 'click' then 1 else null end) as clicks
from nexmark
group by tumble(interval '1 second');
"}

full_pipeline_codegen! {"aggregates_non_null",
"create table demo_stream (
  v BIGINT NOT NULL,
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  topic = 'test',
  format = 'json',
  type = 'source'
);

select
  session('30 seconds') as window,
  sum(v) as clicks
from demo_stream
group by window;
"}

full_pipeline_codegen! {"aggregates_null",
"create table demo_stream (
  v BIGINT,
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  topic = 'test',
  format = 'json',
  type = 'source'
);

select
  session('30 seconds') as window,
  sum(v) as clicks
from demo_stream
group by window;
"}

full_pipeline_codegen! {"two_phase_aggregates",
"create table demo_stream (
  nullable_int BIGINT,
  non_nullable_int BIGINT NOT NULL,
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  topic = 'test',
  format = 'json',
  type = 'source'
);

select
  hop(interval '10 seconds', interval '30 seconds') as window,
  sum(nullable_int) as nullable_sum,
  sum(non_nullable_int) as non_nullable_sum,
  avg(nullable_int) as nullable_avg,
  avg(non_nullable_int) as non_nullable_avg,
  max(nullable_int) as nullable_max,
  max(non_nullable_int) as non_nullable_max,
  min(nullable_int) as nullable_min,
  min(non_nullable_int) as non_nullable_min

from demo_stream
group by window;
"}

full_pipeline_codegen! {"two_phase_tumble",
"create table demo_stream (
  nullable_int BIGINT,
  non_nullable_int BIGINT NOT NULL,
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  topic = 'test',
  format = 'json',
  type = 'source'
);

select
  tumble(interval '10 seconds') as window,
  sum(nullable_int) as nullable_sum,
  sum(non_nullable_int) as non_nullable_sum,
  avg(nullable_int) as nullable_avg,
  avg(non_nullable_int) as non_nullable_avg,
  max(nullable_int) as nullable_max,
  max(non_nullable_int) as non_nullable_max,
  min(nullable_int) as nullable_min,
  min(non_nullable_int) as non_nullable_min

from demo_stream
group by window;
"}

full_pipeline_codegen! {"simple_aggregates",
"create table demo_stream (
  nullable_int BIGINT,
  non_nullable_int BIGINT NOT NULL,
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  topic = 'test',
  format = 'json',
  type = 'source'
);

select
  hop(interval '10 seconds', interval '30 seconds') as window,
  sum(nullable_int) as nullable_sum,
  sum(non_nullable_int) as non_nullable_sum,
  avg(nullable_int) as nullable_avg,
  avg(non_nullable_int) as non_nullable_avg,
  max(nullable_int) as nullable_max,
  max(non_nullable_int) as non_nullable_max,
  min(nullable_int) as nullable_min,
  min(non_nullable_int) as non_nullable_min,
  count(distinct nullable_int) as nullable_distinct_count,
  count(distinct non_nullable_int) as non_nullable_distinct_count

from demo_stream
group by window;
"}

full_pipeline_codegen! {"top_n_tumbling",
"SELECT * FROM (
  SELECT *, ROW_NUMBER()  OVER (
      PARTITION BY window
      ORDER BY count DESC) as row_number
  FROM (
    SELECT bid.auction as auction,
           hop(INTERVAL '1' minute, INTERVAL '1' minute ) as window,
           count(*) as count
      FROM nexmark
      GROUP BY 1, 2)) where row_number = 1
"}

full_pipeline_codegen! {"top_n",
"SELECT * FROM ( SELECT *, ROW_NUMBER()  OVER (
  PARTITION BY window
  ORDER BY price DESC) as row_number
FROM (
SELECT bid.auction as auction,
       hop(INTERVAL '2' second, INTERVAL '10' second ) as window,
       sum(bid.price) as price
  FROM nexmark
  GROUP BY 1, 2)) WHERE row_number < 4
"}

full_pipeline_codegen! {"top_n_offset",
"SELECT * FROM (
  SELECT *, ROW_NUMBER()  OVER (
      PARTITION BY window
      ORDER BY price DESC) as row_number
  FROM (
    SELECT bid.auction as auction,
           hop(INTERVAL '2' second, INTERVAL '9' second ) as window,
           sum(bid.price) as price
      FROM nexmark
      GROUP BY 1, 2)) where row_number = 1
"}

full_pipeline_codegen! {"row_number",
"
  SELECT ROW_NUMBER()  OVER (
      PARTITION BY window
      ORDER BY price DESC) as row_number, auction, price
  FROM (
    SELECT bid.auction as auction,
           hop(INTERVAL '2' second, INTERVAL '9' second ) as window,
           sum(bid.price) as price
      FROM nexmark
      GROUP BY 1, 2)
"}

full_pipeline_codegen! {"updating_aggregate_with_changing_key",
"
SELECT sum(auction), total_price % 2 as price_mod_two FROM (
SELECT sum(bid.price) as total_price, bid.auction as auction FROM nexmark
GROUP BY 2)
GROUP BY 2;
"}

full_pipeline_codegen! {"join_matching_columns",
"create table table_one (
  a_field BIGINT
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  topic = 'test',
  format = 'json',
  type = 'source'
);
create table table_two (
  a_field BIGINT
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  topic = 'test',
  format = 'json',
  type = 'source'
);

SELECT * FROM table_one LEFT OUTER JOIN table_two ON table_one.a_field = table_two.a_field;

"}

full_pipeline_codegen! {"raw_string_test",
"CREATE TABLE raw_sink (
  output TEXT
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  type = 'sink',
  topic = 'outputs',
  format = 'raw_string'
);

INSERT INTO raw_sink
SELECT bid.channel
FROM nexmark;
"}

full_pipeline_codegen! {"raw_string_test_not_null",
"CREATE TABLE raw_sink (
  output TEXT NOT NULL
) WITH (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  type = 'sink',
  topic = 'outputs',
  format = 'raw_string'
);

INSERT INTO raw_sink
SELECT 'test'
FROM nexmark;
"}
