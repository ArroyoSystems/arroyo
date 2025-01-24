CREATE TABLE events (
    event_id TEXT,
    timestamp TIMESTAMP,
    customer_id TEXT,
    event_type TEXT
) WITH (
    connector = 'kafka',
    topic = 'events',
    type = 'source',
    format = 'json',
    bootstrap_servers = 'broker:9092'
);

create temporary table customers (
    customer_id TEXT PRIMARY KEY GENERATED ALWAYS AS (metadata('key')) STORED,
    customer_name TEXT,
    plan TEXT
) with (
    connector = 'redis',
    format = 'raw_string',
    address = 'redis://localhost:6379',
    format = 'json',
    'lookup.cache.max_bytes' = 1000000,
    'lookup.cache.ttl' = interval '5' second
);

SELECT  e.event_id,  e.timestamp,  e.customer_id,  e.event_type, c.customer_name, c.plan
FROM  events e
LEFT JOIN customers c
ON e.customer_id = c.customer_id
WHERE c.plan = 'Premium';
