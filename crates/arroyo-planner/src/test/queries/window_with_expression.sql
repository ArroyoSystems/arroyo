CREATE TABLE impulse with (
    connector = 'impulse',
    event_rate = '500000'
);

SELECT
    *,
    row_number() OVER (PARTITION BY 2 * counter, window) as row_number
FROM
    (
        SELECT
            count(*) AS count,
            counter,
            hop(interval '2 seconds', interval '60 seconds') AS window
        FROM
            impulse
        GROUP BY
            2,
            window
    )