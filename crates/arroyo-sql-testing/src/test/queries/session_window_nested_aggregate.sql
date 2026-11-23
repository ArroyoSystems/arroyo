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
    sessions BIGINT,
    rows BIGINT
) WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'json',
    type = 'sink'
);

-- One session per counter. The rows are 200 ms apart, so a session's end falls on the event
-- time of a later row, and for some sessions on a watermark.
INSERT INTO session_window_output
SELECT
    window.start,
    window.end,
    sessions,
    rows
FROM (
    SELECT
        window,
        COUNT(*) AS sessions,
        SUM(rows) AS rows
    FROM (
        SELECT
            SESSION(INTERVAL '2 seconds') AS window,
            counter,
            COUNT(*) AS rows
        FROM impulse_source
        GROUP BY window, counter
    )
    GROUP BY window
);
