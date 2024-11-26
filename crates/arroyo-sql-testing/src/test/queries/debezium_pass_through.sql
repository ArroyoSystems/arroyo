--pk=id
CREATE TABLE debezium_source (
    id INT PRIMARY KEY,
    customer_name TEXT,
    product_name TEXT,
    quantity INTEGER,
    price FLOAT,
    order_date TIMESTAMP,
    status TEXT
) WITH (
    connector = 'single_file',
    path = '$input_dir/aggregate_updates.json',
    format = 'debezium_json',
    type = 'source'
);

CREATE TABLE output (
    id INT PRIMARY KEY,
    customer_name TEXT,
    product_name TEXT,
    quantity INTEGER,
    price FLOAT,
    order_date TIMESTAMP,
    status TEXT
) WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'debezium_json',
    type = 'sink'
);

INSERT INTO output
SELECT *
FROM debezium_source;
