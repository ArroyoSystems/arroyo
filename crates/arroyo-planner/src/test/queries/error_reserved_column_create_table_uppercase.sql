--fail=column name '__ARROYO_CUSTOM' in table 'cars' is invalid; names starting with '__arroyo' are reserved
CREATE TABLE cars (
    timestamp TIMESTAMP,
    __ARROYO_CUSTOM BIGINT
) WITH (
    connector = 'single_file',
    path = '$input_dir/cars.json',
    format = 'json',
    type = 'source'
);

SELECT timestamp FROM cars;
