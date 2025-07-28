create table events (
    a TEXT[]
) with (
    connector = 'polling_http',
    endpoint = 'https://example.com',
    format = 'json'
);

create view names as (
    select unnest(a)->'name' as name
    from events);

select name from names
group by name, tumble(interval '1 second');