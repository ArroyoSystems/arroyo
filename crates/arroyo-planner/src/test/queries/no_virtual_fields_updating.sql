--fail=Failed to create table debezium_source caused by Error during planning: can't use virtual fields with debezium format
CREATE table debezium_source (
    bids_auction int,
    price int,
    auctions_id int,
    initial_bid int,
    date_string text,
    datetime datetime GENERATED ALWAYS AS (CAST(date_string as timestamp)) STORED,
    "watermark" datetime GENERATED ALWAYS AS (CAST(date_string as timestamp) - interval '1 second') STORED
  ) WITH (
    connector = 'single_file',
    type = 'source',
    path = 'updating.json',
    format = 'debezium_json'
  );
SELECT * FROM debezium_source