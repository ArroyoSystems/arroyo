create table metrics (
    value TEXT,
    parsed TEXT generated always as (parse_prom(value)) stored
) WITH (
    connector = 'polling_http',
    endpoint = 'http://localhost:9100/metrics',
    format = 'raw_string',
    framing = 'newline',
    emit_behavior = 'changed',
    poll_interval_ms = '1000'
);


select avg(idle) as idle from (
  select irate(value) as idle,
         cpu,
         hop(interval '5 seconds', interval '60 seconds') as window
  from (
           SELECT
               extract_json_string(parsed, '$.labels.cpu') AS cpu,
               extract_json_string(parsed, '$.labels.mode') AS mode,
               CAST(extract_json_string(parsed, '$.value') AS FLOAT) AS value
           FROM metrics
           WHERE extract_json_string(parsed, '$.name') = 'node_cpu_seconds_total'
)
  where mode = 'idle'
  group by cpu, window) GROUP BY window;



-- CREATE VIEW cpu_metrics AS
-- SELECT
--     extract_json_string(parsed, '$.labels.cpu') AS cpu,
--     extract_json_string(parsed, '$.labels.mode') AS mode,
--     CAST(extract_json_string(parsed, '$.value') AS FLOAT) AS value
-- FROM metrics
-- WHERE extract_json_string(parsed, '$.name') = 'node_cpu_seconds_total';
--
-- CREATE VIEW idle_cpu AS
-- select avg(idle) as idle from (
--     select irate(array_agg(value)) as idle,
--     cpu,
--     hop(interval '5 seconds', interval '60 seconds') as window
--     from cpu_metrics
--     where mode = 'idle'
--     group by cpu, window);
--
-- CREATE TABLE slack (
--     text TEXT
-- ) WITH (
--     connector = 'webhook',
--     endpoint = 'https://hooks.slack.com/services/XXXXXX/XXXXXX/XXXXXX',
--     headers = 'Content-Type:application/json',
--     format = 'json'
-- );
--
-- INSERT into slack
-- SELECT concat('CPU usage is ', round((1.0 - idle) * 100), '%')
-- FROM idle_cpu
-- WHERE idle < 0.1;
