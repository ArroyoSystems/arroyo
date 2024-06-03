CREATE table debezium_source (
    count int
  ) WITH (
    connector = 'kafka',
    bootstrap_servers = 'localhost:9092',
    type = 'source',
    topic = 'updating',
    format = 'debezium_json'
);

CREATE table sink (
    count int
) WITH (
    connector = 'kafka',
    bootstrap_servers = 'localhost:9092',
    type = 'sink',
    topic = 'sink',
    format = 'debezium_json'
);

INSERT into sink
SELECT * FROM debezium_source