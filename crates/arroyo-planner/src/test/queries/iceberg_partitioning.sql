create table impulse with (
    connector = 'impulse',
    event_rate = 100
);

create table sink (
    id INT,
    ts TIMESTAMP(6) NOT NULL,
    count INT
) with (
    connector = 'iceberg',
    'catalog.type' = 'rest',
    'catalog.rest.url' = 'http://localhost:8118/catalog',
    type = 'sink',
    table_name = 'my-ice-table',
    format = 'parquet',
    'rolling_policy.interval' = interval '30 seconds'
) PARTITIONED BY (
    bucket(count, 4),
    hour(ts)
);

insert into sink
select subtask_index, row_time(), counter
from impulse;