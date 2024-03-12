--fail=This feature is not implemented: window in group by does not match input window
CREATE TABLE Nexmark WITH (
    connector = 'nexmark',
    event_rate = '10'
);

SELECT
    count(*) as auctions,
    tumble(interval '2 minute') as second_window
FROM
    (
        SELECT
            bid.auction as auction,
            tumble(interval '1 minute') as window,
            count(*) as count
        FROM  
            nexmark
        where
            bid is not null
        GROUP BY
            1,
            2
    )
GROUP BY
    2