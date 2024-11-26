create table input (
    length JSON,
    diff INT GENERATED ALWAYS AS (extract_json(length, '$.old')[1]) STORED
) with (
    connector = 'sse',
    endpoint = 'https://localhost:9091',
    format = 'json'
);

select * from input;