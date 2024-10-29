create table users (
    id TEXT,
    name TEXT,
    offset BIGINT GENERATED ALWAYS AS (metadata('offset_id')) STORED,
    topic TEXT GENERATED ALWAYS AS (metadata('topic')) STORED,
    partition INT GENERATED ALWAYS AS (metadata('partition')) STORED
) with (
    connector = 'kafka',
    topic = 'order_topic',
    format='json',
    bootstrap_servers = '0.0.0.0:9092',
    type='source'
);

SELECT * FROM users;