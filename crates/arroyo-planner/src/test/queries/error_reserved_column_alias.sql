--fail=column name '__arroyo_internal_ts' is invalid; names starting with '__arroyo' are reserved
CREATE TABLE impulse WITH (
    connector = 'impulse',
    event_rate = '10'
);

SELECT counter AS __arroyo_internal_ts FROM impulse;
