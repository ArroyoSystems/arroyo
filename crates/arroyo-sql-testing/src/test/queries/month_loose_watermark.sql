CREATE TABLE cars(
          timestamp TIMESTAMP NOT NULL,
          driver_id BIGINT,
          event_type TEXT,
          location TEXT,
          watermark for timestamp AS (timestamp - INTERVAL '1 minute')
        ) WITH (
          connector = 'single_file',
          path = '$input_dir/cars.json',
          format = 'json',
          type = 'source'
        );
        CREATE TABLE group_by_aggregate (
          month TIMESTAMP,
          count BIGINT
        ) WITH (
          connector = 'single_file',
          path = '$output_path',
          format = 'json',
          type = 'sink'
        );
        INSERT INTO group_by_aggregate
        SELECT window.start as month, count
        FROM (
        SELECT TUMBLE(INTERVAL '30 days') as window, COUNT(*) as count
        FROM cars
        GROUP BY 1);