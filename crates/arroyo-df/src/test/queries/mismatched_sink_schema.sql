CREATE TABLE source (a int, b int) WITH (
    connector = 'kafka',
    bootstrap_servers = 'localhost:9092',
    topic = 'source',
    format = 'json',
    type = 'source'
);

CREATE TABLE sink (a text, b text) WITH (
    connector = 'kafka',
    bootstrap_servers = 'localhost:9092',
    topic = 'sink',
    format = 'json',
    type = 'sink'
);


INSERT INTO sink SELECT a, b FROM source;
