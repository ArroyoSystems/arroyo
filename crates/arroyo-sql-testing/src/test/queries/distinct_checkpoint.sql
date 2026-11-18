--pk=group_id
-- The input has 100 inserts before any deletes, keeping duplicate values in the
-- checkpoint restored by the default harness before applying the retractions.
CREATE TABLE distinct_input (
    id BIGINT PRIMARY KEY,
    group_id TEXT,
    v BIGINT,
    label TEXT
) WITH (
    connector = 'single_file',
    path = '$input_dir/distinct_checkpoint.json',
    format = 'debezium_json',
    type = 'source'
);

CREATE TABLE distinct_output WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'debezium_json',
    type = 'sink'
);

INSERT INTO distinct_output
SELECT group_id,
       count(*) AS total_count,
       count(DISTINCT v) AS distinct_count,
       count(DISTINCT label) AS distinct_label_count,
       sum(DISTINCT v) AS distinct_sum
FROM distinct_input
GROUP BY group_id;
