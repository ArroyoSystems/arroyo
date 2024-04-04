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
  event_time_field = 'timestamp'
);

CREATE TABLE output WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'json',
    type = 'sink'
);
INSERT INTO output 
SELECT window.start as start, window.end as end, drivers
FROM (
SELECT tumble(interval '1 hour') as window, count(distinct driver_id) as drivers

FROM (
    SELECT driver_id, count(*) as pickups
    FROM cars where event_type = 'pickup'
    GROUP BY 1,tumble(interval '1 hour') 
) WHERE pickups > 2
GROUP BY 1);

