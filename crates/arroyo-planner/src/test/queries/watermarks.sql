CREATE TABLE orders (
  customer_id INT,
  order_id INT,
  date_string TEXT NOT NULL,
  timestamp TIMESTAMP GENERATED ALWAYS AS (CAST(date_string as TIMESTAMP)),
  watermark FOR timestamp as CAST(date_string as TIMESTAMP) - INTERVAL '5 seconds'
) WITH (
  connector = 'single_file',
  format = 'json',
  type = 'source',
  path = 'order_topic.json'
);

CREATE TABLE users (
  customer_id INT,
  timestamp TIMESTAMP,
  watermark FOR timestamp
) WITH (
  connector = 'single_file',
  format = 'json',
  type = 'source',
  path = 'order_topic.json'
);


select * from orders;