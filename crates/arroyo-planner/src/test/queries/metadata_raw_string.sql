create table mqtt (
    value TEXT,
    my_topic TEXT METADATA FROM 'topic'
) with (
    connector = 'mqtt',
    url = 'tcp://localhost:1883',
    topic = 'plant/#',
    type = 'source',
    format = 'raw_string'
);

select my_topic from mqtt;