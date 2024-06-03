--fail=must have window in aggregate. Make sure you are calling one of the windowing functions (hop, tumble, session) or using the window field of the input
CREATE TABLE Nexmark WITH (
    connector = 'nexmark',
    event_rate = '10'
);


SELECT count(*) as auctions FROM (
SELECT
    bid.auction as auction,
    tumble(interval '1 minute') as window,
    count(*) as count
FROM
    nexmark
where
    bid is not null
GROUP BY
    1,2)