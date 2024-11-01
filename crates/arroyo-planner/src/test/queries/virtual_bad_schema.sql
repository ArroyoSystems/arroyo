--fail=Schema error: No field named notfield. Valid fields are input.length.
create table input (
    length JSON,
    diff INT GENERATED ALWAYS AS (notfield) STORED
) with (
    connector = 'sse',
    endpoint = 'https://localhost:9091',
    format = 'json'
);

select * from input;