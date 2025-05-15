-- from https://www.arroyo.dev/blog/building-a-real-time-data-lake
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

create table account_created_sink with (
    connector = 'delta',
    path = 's3://my-s3-bucket/data/account_created',
    format = 'parquet',
    'filename.strategy' = 'uuid',
    'parquet.compression' = 'zstd',
    time_partition_pattern = '%Y/%m/%d/%H',
    rollover_seconds = 6000
);

INSERT INTO account_created_sink
SELECT
	id,
	timestamp,
	ip,
	user_id,
	platform,
	app_version,
	properties->>'signup_method' as signup_method,
	properties->>'campaign_id' as campaign_id,
	properties->>'product' as product
FROM events
WHERE type = 'account_created';
