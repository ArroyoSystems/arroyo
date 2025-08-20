--fail=Error during planning: can't handle updating left side of join
CREATE TABLE nexmark (
    auction bigint,
    bidder bigint,
    price bigint,
    channel text,
    url  text,
    datetime timestamp,
    extra text
) WITH (
    connector = 'filesystem',
    format = 'parquet', 
    type = 'source',
    path = '/home/data',
    'source.regex_pattern' = '00001-000.parquet',
    event_time_field = datetime
);

CREATE TABLE counts as (SELECT count(*) as counts, bidder FROM nexmark GROUP BY 2);

SELECT a.counts, b.counts FROM counts A join counts B on A.bidder = b.bidder