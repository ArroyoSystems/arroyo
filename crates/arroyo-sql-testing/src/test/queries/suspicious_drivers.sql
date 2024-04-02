--fail=This feature is not implemented: must have window in aggregate. Make sure you are calling one of the windowing functions (hop, tumble, session) or using the window field of the input
CREATE TABLE cars(
      timestamp TIMESTAMP,
      driver_id BIGINT,
      event_type TEXT,
      location TEXT
    ) WITH (
      connector = 'single_file',
      path = '$input_dir/sorted_cars.json',
      format = 'json',
      type = 'source',
      event_time_field = 'timestamp',
      watermark_field = 'timestamp'
    );
    CREATE TABLE suspicious_drivers (
      drivers BIGINT
    ) WITH (
      connector = 'single_file',
      path = '$output_path',
      format = 'debezium_json',
      type = 'sink'
    );
    INSERT INTO suspicious_drivers
    SELECT count(*) drivers FROM
    (
    SELECT driver_id, sum(case when event_type = 'pickup' then 1 else 0 END ) as pickups,
    sum(case when event_type = 'dropoff' THEN 1 else 0 END) as dropoffs
    FROM cars
    GROUP BY 1
    ) WHERE pickups < dropoffs