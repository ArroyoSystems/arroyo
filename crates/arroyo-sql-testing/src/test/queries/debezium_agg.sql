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
    id TEXT PRIMARY KEY,
    c INT,
    d INT,
    m INT,
    q INT
) WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'debezium_json',
    type = 'sink'
);

INSERT INTO output
SELECT concat('p_', product_name), count(*), count(distinct customer_name), median(quantity), sum(quantity + 5) + 10
FROM debezium_source
group by concat('p_', product_name);