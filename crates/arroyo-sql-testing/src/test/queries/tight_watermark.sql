CREATE TABLE cars(
  timestamp TIMESTAMP,
  driver_id BIGINT,
  event_type TEXT,
  location TEXT
) WITH (
  connector = 'single_file',
  path = '$input_dir/cars.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp',
  watermark_field = 'timestamp'
);
CREATE TABLE group_by_aggregate (
  timestamp TIMESTAMP,
  count BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
CREATE VIEW group_by_view AS (SELECT window.end as timestamp, count
FROM (
SELECT TUMBLE(INTERVAL '1' hour) as window, COUNT(*) as count
FROM cars
GROUP BY 1));
INSERT INTO group_by_aggregate
SELECT timestamp, count FROM group_by_view 