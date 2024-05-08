CREATE TABLE analytic_events (
    browser TEXT,
    browserLanguage TEXT,
    host TEXT,
    path TEXT,
    referer TEXT,
    screen_height INT,
    screen_width INT,
    source_type TEXT,
    time BIGINT,
    timestamp TEXT,
    title TEXT,
    url TEXT
) WITH (
    connector = 'kafka',
    format = 'json',
    type = 'source',
    bootstrap_servers = 'kafka:9092',
    topic = 'javascript-events'
);

CREATE TABLE metrics_sink (
    time TIMESTAMP,
    metric TEXT,
    value FLOAT,
    tag TEXT
) WITH (
    connector = 'kafka',
    format = 'debezium_json',
    'json.include_schema' = 'true',
    type = 'sink',
    bootstrap_servers = 'kafka:9092',
    topic = 'metrics'
);

INSERT INTO metrics_sink
SELECT window.end, 'views_1_minute', count, path FROM (
    SELECT count (*) as count, path, hop(interval '5 seconds', '1 minute') as window
    FROM analytic_events
    GROUP BY path, window);

INSERT INTO metrics_sink
SELECT window.end, 'views_15_minute', count, path FROM (
    SELECT count (*) as count, path, hop(interval '5 seconds', '15 minute') as window
    FROM analytic_events
    GROUP BY path, window);

INSERT INTO metrics_sink
SELECT window.end, 'views_1_hour', count, path FROM (
    SELECT count (*) as count, path, hop(interval '5 seconds', '1 hour') as window
    FROM analytic_events
    GROUP BY path, window);


CREATE TABLE top_pages_sink (
    time TIMESTAMP,
    page TEXT,
    count FLOAT,
    rank FLOAT
) WITH (
    connector = 'kafka',
    format = 'debezium_json',
    'json.include_schema' = 'true',
    type = 'sink',
    bootstrap_servers = 'kafka:9092',
    topic = 'metrics'
);

INSERT INTO top_pages_sink
SELECT window.end, path, count, row_num FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY window
        ORDER BY count DESC) as row_num
    FROM (
        SELECT count(*) as count, path, hop(interval '5 seconds', interval '1 hour') as window
        FROM analytic_events
        GROUP BY path, window)) WHERE row_num <= 10;