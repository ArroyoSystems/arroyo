--pk=bucket
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
    bucket BIGINT PRIMARY KEY,
    row_count BIGINT,
    value_count BIGINT,
    min BIGINT,
    max BIGINT,
    sum BIGINT,
    avg DOUBLE
) WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'debezium_json',
    type = 'sink'
);

INSERT INTO aggregates
WITH nullable_values AS (
    SELECT
        counter % 3 AS bucket,
        -- Bucket 0 is all NULL, bucket 1 mixes NULL and non-NULL, and bucket 2 has no NULLs.
        CASE
            WHEN counter % 3 = 0 THEN NULL
            WHEN counter % 3 = 1 AND counter % 2 = 0 THEN NULL
            ELSE CAST(counter AS BIGINT)
        END AS value
    FROM impulse_source
)
SELECT bucket, COUNT(*), COUNT(value), MIN(value), MAX(value), SUM(value), AVG(value)
FROM nullable_values
GROUP BY bucket;
