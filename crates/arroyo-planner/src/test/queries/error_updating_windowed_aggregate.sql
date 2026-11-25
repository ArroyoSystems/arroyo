--fail=Error during planning: can't handle updating input to a windowed aggregate
CREATE TABLE debezium_input (
    id int PRIMARY KEY,
    count int
) WITH (
    connector = 'kafka',
    bootstrap_servers = 'localhost:9092',
    type = 'source',
    topic = 'updating',
    format = 'debezium_json'
);

SELECT count(*), sum(count) FROM debezium_input GROUP BY tumble(interval '1 hour');
