create table impulse with (
    connector = 'impulse',
    event_rate = '10'
);

select sha256(cast(counter as TEXT))
from impulse;