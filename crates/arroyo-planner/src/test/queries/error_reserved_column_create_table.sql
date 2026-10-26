--fail=column name '__arroyo_custom' in table 'cars' is invalid; names starting with '__arroyo' are reserved
CREATE TABLE cars (
    timestamp TIMESTAMP,
    __arroyo_custom BIGINT
) WITH (
    connector = 'single_file',
    path = '$input_dir/cars.json',
    format = 'json',
    type = 'source'
);

SELECT timestamp FROM cars;
