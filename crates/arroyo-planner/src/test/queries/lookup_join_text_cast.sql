CREATE TABLE events (
    event_id TEXT,
    timestamp TIMESTAMP,
    customer_id TEXT
) WITH (
    connector = 'kafka',
    topic = 'events',
    type = 'source',
    format = 'json',
    bootstrap_servers = 'broker:9092'
);

CREATE TEMPORARY TABLE customers (
    customer_id TEXT METADATA FROM 'key' PRIMARY KEY,
    customer_name TEXT
) WITH (
    connector = 'redis',
    format = 'json',
    address = 'redis://localhost:6379'
);

SELECT e.event_id, c.customer_name
FROM events e
LEFT JOIN customers c
ON CAST(e.customer_id AS TEXT) = c.customer_id;
