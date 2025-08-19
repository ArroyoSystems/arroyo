CREATE TABLE nexmark (
    auction bigint,
    bidder bigint,
    price bigint,
    channel text,
    url  text,
    datetime timestamp,
    extra text
) WITH (
    connector = 'filesystem',
    format = 'parquet', 
    type = 'source',
    path = '/home/data',
    'source.regex_pattern' = '00001-000.parquet',
    event_time_field = datetime
);

CREATE TABLE counts as (SELECT count(*) FROM nexmark);

SELECT * FROM counts