create table logs (
    ip TEXT,
    user_id TEXT,
    timestamp TIMESTAMP,
    request TEXT,
    size INT
) with (
    connector = 'sse',
    endpoint = 'http://localhost:9563/sse',
    format = 'json'
);

SELECT count(*), sum(size)  FROM
    HOP(logs, timestamp, INTERVAL '5 MINUTES', INTERVAL '10 MINUTES');