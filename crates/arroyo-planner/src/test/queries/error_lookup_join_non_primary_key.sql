--fail=the right-side of a look-up join condition must be a PRIMARY KEY column, but 'value' is not
create table impulse with (
    connector = 'impulse',
    event_rate = '2'
);

create temporary table lookup (
    key TEXT METADATA FROM 'key' PRIMARY KEY,
    value TEXT,
    len INT
) with (
    connector = 'redis',
    format = 'raw_string',
    address = 'redis://localhost:6379',
    format = 'json',
    'lookup.cache.max_bytes' = 100000
);

select A.counter, B.key, B.value, len
from impulse A inner join lookup B
on cast((A.counter % 10) as TEXT) = B.value;