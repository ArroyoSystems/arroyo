CREATE TABLE nexmark with (
    connector = 'nexmark',
    event_rate = '100'
);

CREATE TABLE output WITH (
    connector = 'kafka',
    type = 'sink',
    bootstrap_servers = 'localhost:9092',
    format = 'json',
    'topic' = 'outputs'
);

INSERT INTO output
SELECT count(*), auction.id from nexmark
GROUP BY auction.id, hop(interval '5 seconds', interval '1 hour');
