--fail=column name '__ARROYO_X' is invalid; names starting with '__arroyo' are reserved
CREATE TABLE impulse WITH (
    connector = 'impulse',
    event_rate = '10'
);

SELECT counter AS "__ARROYO_X" FROM impulse;
