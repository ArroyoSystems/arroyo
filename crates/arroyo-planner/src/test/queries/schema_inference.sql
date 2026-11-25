CREATE TABLE nexmark with (
    connector = 'nexmark',
    event_rate = '100'
);

CREATE TABLE output WITH (
    connector = 'single_file',
    type = 'sink',
    format = 'json',
    path = 'outputs.json'
);

INSERT INTO output
SELECT count(*), auction.id from nexmark
GROUP BY auction.id, hop(interval '5 seconds', interval '1 hour');
