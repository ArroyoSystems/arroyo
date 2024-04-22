--fail=Error during planning: can't currently nest updating aggregates
CREATE TABLE debezium_input (
    count int
  ) WITH (
    connector = 'kafka',
    bootstrap_servers = 'localhost:9092',
    type = 'source',
    topic = 'updating',
    format = 'debezium_json'
);

SELECT count(*) FROM debezium_input