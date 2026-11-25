CREATE TABLE source (a int, b int) WITH (
    connector = 'single_file',
    path = 'source.json',
    format = 'json',
    type = 'source'
);

CREATE TABLE sink (a text, b text) WITH (
    connector = 'single_file',
    path = 'sink.json',
    format = 'json',
    type = 'sink'
);


INSERT INTO sink SELECT a, b FROM source;
