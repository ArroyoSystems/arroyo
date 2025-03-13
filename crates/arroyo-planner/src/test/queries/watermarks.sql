CREATE TABLE orders (
  customer_id INT,
  order_id INT,
  date_string TEXT NOT NULL,
  timestamp TIMESTAMP GENERATED ALWAYS AS (CAST(date_string as TIMESTAMP)),
  watermark FOR timestamp as CAST(date_string as TIMESTAMP) - INTERVAL '5 seconds'
) WITH (
  connector = 'kafka',
  format = 'json',
  type = 'source',
  bootstrap_servers = 'localhost:9092',
  topic = 'order_topic'
);

CREATE TABLE users (
  customer_id INT,
  timestamp TIMESTAMP,
  watermark FOR timestamp
) WITH (
  connector = 'kafka',
  format = 'json',
  type = 'source',
  bootstrap_servers = 'localhost:9092',
  topic = 'order_topic'
);


select * from orders;