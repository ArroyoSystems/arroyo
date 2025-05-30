--fail=Failed to create table events_sink caused by Error during planning: partition field 'not_a_real_field' does not exist in the schema
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
    time_partition_pattern = '%Y/%m/%d/%H',
    partition_fields = [type, not_a_real_field],
    rollover_seconds = 6000
);

INSERT INTO events
SELECT * from events;