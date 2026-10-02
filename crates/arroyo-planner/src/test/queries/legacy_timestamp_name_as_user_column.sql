CREATE TABLE impulse WITH (
    connector = 'impulse',
    event_rate = '10'
);

SELECT CAST(counter AS BIGINT) AS _timestamp FROM impulse;
