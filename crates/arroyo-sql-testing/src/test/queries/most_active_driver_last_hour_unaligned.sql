--fail=Error during planning: hop() width 3600s currently must be a multiple of slide 2400s
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
CREATE TABLE most_active_driver (
  driver_id BIGINT,
  count BIGINT,
  start TIMESTAMP,
  end TIMESTAMP,
  row_number BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO most_active_driver
SELECT driver_id, count, window.start, window.end, row_number FROM (
  SELECT *, ROW_NUMBER()  OVER (
    PARTITION BY window
    ORDER BY count DESC, driver_id desc) as row_number
  FROM (
      SELECT driver_id, count, window FROM (
  SELECT driver_id,
  hop(INTERVAL '40' minute, INTERVAL '1' hour ) as window,
         count(*) as count
         FROM cars
         GROUP BY 1,2)) ) where row_number = 1