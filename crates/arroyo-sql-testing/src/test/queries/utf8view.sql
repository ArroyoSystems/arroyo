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

CREATE TABLE group_by_aggregate WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);

INSERT INTO group_by_aggregate
SELECT substr(event_type, 1, strpos(event_type, ':') - 1) AS event_type,
       window.start AS hour, count
FROM (
  SELECT CAST(concat(event_type, ':buffered view key') AS TEXT) AS event_type,
         TUMBLE(INTERVAL '1' HOUR) AS window,
         COUNT(*) AS count
  FROM cars
  GROUP BY 1, 2
);
