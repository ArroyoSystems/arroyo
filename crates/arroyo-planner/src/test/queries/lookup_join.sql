CREATE TABLE orders (
    order_id INT,
    user_id INT,
    product_id INT,
    quantity INT,
    order_timestamp TIMESTAMP
) with (
    connector = 'kafka',
    bootstrap_servers = 'localhost:9092',
    type = 'source',
    topic = 'orders',
    format = 'json'
);

CREATE TEMPORARY TABLE products (
    key TEXT PRIMARY KEY,
    product_name TEXT,
    unit_price FLOAT,
    category TEXT,
    last_updated TIMESTAMP
) with (
    connector = 'redis',
    format = 'json',
    type = 'lookup',
    address = 'redis://localhost:6379'
);

SELECT 
    o.order_id,
    o.user_id,
    o.quantity,
    o.order_timestamp,
    p.product_name,
    p.unit_price,
    p.category,
    (o.quantity * p.unit_price) as total_amount
FROM orders o
    JOIN products p
    ON concat('blah', o.product_id) = p.key;