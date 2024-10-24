CREATE TABLE impulse_source (
    timestamp TIMESTAMP,
    counter BIGINT UNSIGNED NOT NULL,
    subtask_index BIGINT UNSIGNED NOT NULL
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

INSERT INTO aggregates
SELECT
    MIN(counter),
    MAX(counter),
    SUM(counter),
    COUNT(*),
    AVG(counter)
FROM impulse_source;
