--fail=Updating joins must include an equijoin condition

CREATE TABLE cars (
  timestamp TIMESTAMP,
  car_id TEXT,
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

CREATE TABLE passengers (
  timestamp TIMESTAMP,
  passenger_id BIGINT
) WITH (
  connector = 'single_file',
  path = '$input_dir/cars.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);

select passenger_id, car_id
from passengers
join cars ON passenger_id < car_id;