create table impulse with (
    connector = 'impulse',
    event_rate = '10'
);

select wallclock() as ingest_time, counter
from impulse;
