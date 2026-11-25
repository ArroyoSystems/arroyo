CREATE TABLE debezium_input (
    id int PRIMARY KEY,
    count int
) WITH (
    connector = 'single_file',
    type = 'source',
    path = 'updating.json',
    format = 'debezium_json'
);

SELECT count(*) FROM debezium_input