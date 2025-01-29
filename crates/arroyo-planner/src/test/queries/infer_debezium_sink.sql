create table impulse with (
    connector = 'impulse',
    event_rate = '10'
);

create table sink with (
    connector = 'kafka',
    bootstrap_servers = 'localhost:9092',
    format = 'debezium_json',
    type = 'sink',
    topic = 'outputs'
);

insert into sink
select count(*)
from impulse
group by hop(interval '5 seconds', interval '30 seconds');