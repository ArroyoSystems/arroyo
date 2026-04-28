CREATE TABLE nexmark WITH (
    connector = 'nexmark',
    event_rate = 10
);

SELECT
    median(bid.auction) AS median_auction,
    variance(bid.auction) AS variance_auction
FROM
    nexmark
WHERE
    bid IS NOT NULL;
