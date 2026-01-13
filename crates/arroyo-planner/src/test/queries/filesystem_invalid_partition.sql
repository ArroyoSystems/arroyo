--fail=Failed to create table events_sink caused by Error during planning: Arrow error: Schema error: Unable to get field named "not_a_real_field". Valid fields: ["id", "timestamp", "ip", "user_id", "platform", "app_version", "type", "properties", "_timestamp"]
create table events (
    id TEXT PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    ip TEXT,
    user_id TEXT,
    platform TEXT,
    app_version TEXT,
    type TEXT NOT NULL,
    properties JSON
) with (
    connector = 'kafka',
    bootstrap_servers = 'localhost:9092',
    topic = 'analytics',
    format = 'json',
    type = 'source'
);

create table events_sink (
     id TEXT,
     timestamp TIMESTAMP NOT NULL,
     ip TEXT,
     user_id TEXT,
     platform TEXT,
     app_version TEXT,
     type TEXT NOT NULL,
     properties JSON    
) with (
    connector = 'delta',
    path = 's3://my-s3-bucket/data/events',
    format = 'parquet',
    'filename.strategy' = 'uuid',
    'parquet.compression' = 'zstd',
    'partitioning.time_pattern' = '%Y/%m/%d/%H',
    'partitioning.fields' = [type, not_a_real_field],
    'rolling_policy.interval' = interval '6000 seconds',
    type = 'sink'
);

INSERT INTO events
SELECT * from events;