CREATE table debezium_source (
    id INT PRIMARY KEY,
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
SELECT count FROM debezium_source