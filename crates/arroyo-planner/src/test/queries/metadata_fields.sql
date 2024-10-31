create table mqtt (
    name TEXT,
    value INT,
    my_topic TEXT GENERATED ALWAYS AS (metadata('topic')) STORED
) with (
    connector = 'mqtt',
    url = 'tcp://localhost:1883',
    topic = 'plant/#',
    type = 'source',
    format = 'json'
);

select my_topic from mqtt;