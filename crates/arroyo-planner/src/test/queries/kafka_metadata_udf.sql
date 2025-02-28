create table users (
    id TEXT,
    name TEXT,
    offset BIGINT METADATA FROM 'offset_id',
    topic TEXT METADATA FROM 'topic',
    partition INT METADATA FROM 'partition'
) with (
    connector = 'kafka',
    topic = 'order_topic',
    format='json',
    bootstrap_servers = '0.0.0.0:9092',
    type='source'
);

SELECT * FROM users;