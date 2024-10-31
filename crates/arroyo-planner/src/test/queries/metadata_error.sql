--fail=Failed to create table mqtt caused by Error during planning: incorrect data type for metadata field 'topic'; expected TEXT, but found INT
create table mqtt (
    name TEXT,
    value INT,
    topic INT GENERATED ALWAYS AS (metadata('topic')) STORED
) with (
    connector = 'mqtt',
    url = 'tcp://localhost:1883',
    topic = 'plant/#',
    type = 'source',
    format = 'json'
);

select topic from mqtt;