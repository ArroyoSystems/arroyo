create table sink (
    timestamp TIMESTAMP NOT NULL,
    user TEXT,
    event TEXT
) with (
    connector = 'kafka',
    bootstrap_servers = 'localhost:9092',
    format = 'json',
    'sink.timestamp_field' = 'timestamp',
    'sink.key_field' = 'user',
    type = 'sink',
    topic = 'events'
);


insert into sink
select timestamp, user, event
from source;
