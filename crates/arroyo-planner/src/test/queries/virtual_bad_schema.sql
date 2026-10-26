--fail='notfield' not found
create table input (
    length JSON,
    diff INT GENERATED ALWAYS AS (notfield) STORED
) with (
    connector = 'sse',
    endpoint = 'https://localhost:9091',
    format = 'json'
);

select * from input;