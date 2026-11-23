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
    user_id BIGINT,
    rows BIGINT
) WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'json',
    type = 'sink'
);

-- Each user has a row every 400 ms and never a gap of 500 ms: one session per user, although a
-- batch of the source spans much more than the gap.
INSERT INTO session_window_output
SELECT
    window.start,
    window.end,
    user_id,
    rows
FROM (
    SELECT
        SESSION(INTERVAL '500 milliseconds') AS window,
        counter % 2 AS user_id,
        COUNT(*) AS rows
    FROM impulse_source
    GROUP BY window, user_id
);
