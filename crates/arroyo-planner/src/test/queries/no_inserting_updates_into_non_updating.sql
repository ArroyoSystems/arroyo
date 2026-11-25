CREATE table debezium_source (
    id INT PRIMARY KEY,
    count int
) WITH (
    connector = 'single_file',
    type = 'source',
    path = 'updating.json',
    format = 'debezium_json'
);

CREATE table sink (
    count int
) WITH (
    connector = 'single_file',
    type = 'sink',
    path = 'sink.json',
    format = 'debezium_json'
);

INSERT into sink
SELECT count FROM debezium_source