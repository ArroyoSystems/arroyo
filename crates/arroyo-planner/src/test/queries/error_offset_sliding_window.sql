--fail=Error during planning: hop() width 600s currently must be a multiple of slide 180s
CREATE TABLE Nexmark WITH (
    connector = 'nexmark',
    event_rate = '10'
);

SELECT
    bid.auction as auction,
    hop(interval '3 minute', interval '10 minute') as window,
    count(*) as count
FROM
    nexmark
where
    bid is not null
GROUP BY
1,2