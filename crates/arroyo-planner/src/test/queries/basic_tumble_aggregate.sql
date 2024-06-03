CREATE TABLE Nexmark WITH (
    connector = 'nexmark',
    event_rate = '10'
);

SELECT
    bid.auction as auction,
    tumble(INTERVAL '1' second) as window,
    count(*) as count
FROM
    nexmark
where
    bid is not null
GROUP BY
    1,
    2