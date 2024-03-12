CREATE TABLE Nexmark WITH (
    connector = 'nexmark',
    event_rate = '10'
);

SELECT
    count(*) as auction,
    tumble(interval '1 minute') as second_window
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