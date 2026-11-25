create table impulse with (
    connector = 'impulse',
    event_rate = '10'
);

create table sink with (
    connector = 'single_file',
    format = 'debezium_json',
    type = 'sink',
    path = 'outputs.json'
);

insert into sink
select count(*)
from impulse
group by hop(interval '5 seconds', interval '30 seconds');