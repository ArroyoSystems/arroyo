create table my_table (
    a INT,
    b DECIMAL(10, 5)
) with (
    connector = 'polling_http',
    endpoint = 'https://example.com',
    format = 'json'
);

select * from my_table;