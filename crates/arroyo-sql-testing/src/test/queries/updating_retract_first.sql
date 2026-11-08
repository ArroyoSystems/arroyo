--pk=g
-- A retraction that arrives before its insert is a change still to be matched, not one to drop.
CREATE TABLE source (
    id INT PRIMARY KEY,
    g TEXT,
    v INT
) WITH (
    connector = 'single_file',
    path = '$input_dir/updating_retract_first.json',
    format = 'debezium_json',
    type = 'source'
);

CREATE TABLE output (
    g TEXT PRIMARY KEY,
    mn INT,
    mx INT,
    s BIGINT,
    n BIGINT,
    dv BIGINT
) WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'debezium_json',
    type = 'sink'
);

INSERT INTO output
SELECT g, min(v), max(v), sum(v), count(*), count(distinct v)
FROM source
GROUP BY g;
