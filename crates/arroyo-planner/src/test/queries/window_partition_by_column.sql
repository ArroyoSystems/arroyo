CREATE TABLE nexmark with (
    connector = 'nexmark',
    event_rate = '500000',
    runtime = '20'
);


SELECT * FROM (
    SELECT *,  row_number() OVER (
        PARTITION BY window, auction
        ORDER BY count DESC) AS row_num
    FROM (SELECT count(*) AS count, bid.auction AS auction,
        hop(interval '2 seconds', interval '60 seconds') AS window 
            FROM nexmark WHERE bid is not null
            GROUP BY 2, window)) WHERE row_num <= 5