CREATE TABLE cars (
  timestamp TIMESTAMP,
  driver_id BIGINT,
  event_type TEXT,
  location TEXT
) WITH (
  connector = 'single_file',
  path = '$input_dir/cars.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);
CREATE TABLE hourly_aggregates (
  hour TIMESTAMP,
  drivers BIGINT,
  pickups BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO hourly_aggregates
SELECT window.start as hour, dropoff_drivers, pickup_drivers FROM (
SELECT dropoffs.window as window, dropoff_drivers, pickup_drivers
FROM (
  SELECT TUMBLE(INTERVAL '1' hour) as window,
   COUNT(distinct driver_id) as dropoff_drivers FROM cars where event_type = 'dropoff'
  GROUP BY 1
) dropoffs
INNER JOIN (
  SELECT  TUMBLE(INTERVAL '1' hour) as window,
   COUNT(distinct driver_id) as pickup_drivers FROM cars where event_type = 'pickup'
  GROUP BY 1
) pickups
ON dropoffs.window.start = pickups.window.start)