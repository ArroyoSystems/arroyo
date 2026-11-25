create table users (
    id TEXT,
    t struct<a int, x struct<b text>>
) with (
    connector = 'single_file',
    format = 'json',
    type = 'source',
    path = 'structs.json'
);

select id, t.a, t.x.b from users;
