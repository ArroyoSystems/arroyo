create table logs (
    value TEXT NOT NULL,
    parsed TEXT GENERATED ALWAYS AS (parse_log(value)),
    event_time TIMESTAMP GENERATED ALWAYS AS
        (CAST(extract_json_string(parse_log(value), '$.timestamp') as TIMESTAMP))
) with (
    connector = 'single_file',
    type = 'source',
    format = 'raw_string',
    path = 'apache_logs.log'
);

SELECT count(*)
FROM logs
WHERE extract_json(parsed, '$.status_code')[1] = '500'
GROUP BY hop(interval '5 seconds', interval '5 minutes');
