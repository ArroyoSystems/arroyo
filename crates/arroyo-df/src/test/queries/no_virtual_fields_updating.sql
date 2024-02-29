--fail=can't read from a source with virtual fields and update mode
CREATE table debezium_source (
    bids_auction int,
    price int,
    auctions_id int,
    initial_bid int,
    date_string text,
    datetime datetime GENERATED ALWAYS AS (CAST(date_string as timestamp)) STORED,
    watermark datetime GENERATED ALWAYS AS (CAST(date_string as timestamp) - interval '1 second') STORED
  ) WITH (
    connector = 'kafka',
    bootstrap_servers = 'localhost:9092',
    type = 'source',
    topic = 'updating',
    format = 'debezium_json'
  );
SELECT * FROM debezium_source