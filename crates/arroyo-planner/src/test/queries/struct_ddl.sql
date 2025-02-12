create table users (
    id TEXT,
    t struct<a int, x struct<b text>>
) with (
    connector = 'kafka',
    format = 'json',
    bootstrap_servers = 'localhost:9092',
    type = 'source',
    topic = 'structs'
);

select id, t.a, t.x.b from users;
