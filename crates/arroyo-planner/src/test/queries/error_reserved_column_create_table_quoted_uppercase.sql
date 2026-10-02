--fail=column name '__Arroyo_Custom' in table 'cars' is invalid; names starting with '__arroyo' are reserved
CREATE TABLE cars (
    timestamp TIMESTAMP,
    "__Arroyo_Custom" BIGINT
) WITH (
    connector = 'single_file',
    path = '$input_dir/cars.json',
    format = 'json',
    type = 'source'
);

SELECT timestamp FROM cars;
