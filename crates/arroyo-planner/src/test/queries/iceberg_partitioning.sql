create table impulse with (
    connector = 'impulse',
    event_rate = 100
);

create table sink (
    id INT,
    ts TIMESTAMP(6) NOT NULL,
    ts2 TIMESTAMP(6) NOT NULL,
    ts3 TIMESTAMP(6) NOT NULL,
    ts4 TIMESTAMP(6) NOT NULL,
    b TEXT,
    count INT
) with (
    connector = 'iceberg',
    'catalog.type' = 'rest',
    'catalog.rest.url' = 'http://localhost:8118/catalog',
    type = 'sink',
    table_name = 'my-ice-table',
    format = 'parquet',
    'rolling_policy.interval' = interval '30 seconds',
    'shuffle_by_partition.enabled' = true
) PARTITIONED BY (
    bucket(count, 4),
    truncate(count, 1),
    truncate(b, 10),
    identity(ts),
    identity(b),
    hour(ts),
    day(ts2),
    month(ts3),
    year(ts4),
    void(count)
);

insert into sink
select subtask_index, row_time(), row_time() as ts2, row_time() as t3, row_time() as ts4,
       cast(counter as TEXT) as counter_text, counter
from impulse;