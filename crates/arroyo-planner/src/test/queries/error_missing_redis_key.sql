--fail=Redis lookup tables must have a PRIMARY KEY field defined as `field_name TEXT GENERATED ALWAYS AS (metadata('key')) STORED`
create table impulse with (
    connector = 'impulse',
    event_rate = '2'
);

create table lookup (
    key TEXT PRIMARY KEY, 
    value TEXT
) with (
    connector = 'redis',
    format = 'json',
    address = 'redis://localhost:6379',
    type = 'lookup'
);

select A.counter, B.key, B.value
from impulse A left join lookup B
on cast((A.counter % 10) as TEXT) = B.key;
