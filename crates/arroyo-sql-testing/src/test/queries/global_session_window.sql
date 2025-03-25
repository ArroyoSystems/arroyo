CREATE TABLE impulse_source (
    timestamp TIMESTAMP,
    counter BIGINT UNSIGNED NOT NULL,
    subtask_index BIGINT UNSIGNED NOT NULL,
    WATERMARK FOR timestamp
) WITH (
    connector = 'single_file',
    path = '$input_dir/impulse.json',
    format = 'json',
    type = 'source'
);

CREATE TABLE session_window_output (
    start TIMESTAMP,
    end TIMESTAMP,
    rows BIGINT
) WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'json',
    type = 'sink'
);

INSERT INTO session_window_output
SELECT
    window.start,
    window.end,
    rows
FROM (
    SELECT
        SESSION(INTERVAL '20 seconds') AS window,
        COUNT(*) AS rows
    FROM impulse_source
    GROUP BY window
);
