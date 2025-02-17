--pk=drivers
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
      event_time_field = timestamp,
      watermark_field = timestamp
    );

CREATE TABLE active_drivers (
  drivers BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'debezium_json',
  type = 'sink'
);

insert into active_drivers
select count(*) from (
  select driver_id, count(*) from cars group by driver_id  having count(*) > 85
);