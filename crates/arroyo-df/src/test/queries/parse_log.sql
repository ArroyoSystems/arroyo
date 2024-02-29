create table logs (
    value TEXT NOT NULL,
    parsed TEXT GENERATED ALWAYS AS (parse_log(value)) stored,
    event_time TIMESTAMP GENERATED ALWAYS AS
        (CAST(extract_json_string(parse_log(value), '$.timestamp') as TIMESTAMP)) stored
) with (
    connector = 'kafka',
    type = 'source',
    format = 'raw_string',
    bootstrap_servers = 'localhost:9092',
    topic = 'apache_logs',
    'source.offset' = 'earliest'
);

SELECT count(*)
FROM logs
WHERE extract_json(parsed, '$.status_code')[1] = '500'
GROUP BY hop(interval '5 seconds', interval '5 minutes');
