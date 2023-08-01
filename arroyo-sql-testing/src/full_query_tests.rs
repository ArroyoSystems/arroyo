use arroyo_sql_macro::full_pipeline_codegen;

full_pipeline_codegen! {"select_star", "SELECT * FROM nexmark"}

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

full_pipeline_codegen! {"sum_of_sums_updating",
"SELECT bids, count(*) as occurrences FROM (
  SELECT count(*) as bids, bid.auction as auction FROM nexmark where bid is not null group by 2)
GROUP BY 1"}

full_pipeline_codegen! {"create_parquet_s3_source",
"CREATE TABLE bids (
  auction bigint,
  bidder bigint,
  price bigint,
  datetime timestamp
) WITH (
  connector ='filesystem',
  path = 'https://s3.us-west-2.amazonaws.com/demo/s3-uri',
  format = 'parquet',
  rollover_seconds = '5'
);

INSERT INTO Bids select bid.auction, bid.bidder, bid.price , bid.datetime FROM nexmark where bid is not null;"}

full_pipeline_codegen! {"cast_bug",
"SELECT CAST(1 as FLOAT)
from nexmark; "}
