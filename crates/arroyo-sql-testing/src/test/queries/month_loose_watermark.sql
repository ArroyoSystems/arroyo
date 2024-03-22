CREATE TABLE cars(
          timestamp TIMESTAMP,
          driver_id BIGINT,
          event_type TEXT,
          location TEXT,
          watermark TIMESTAMP GENERATED ALWAYS AS (timestamp - INTERVAL '1 minute') STORED
        ) WITH (
          connector = 'single_file',
          path = '$input_dir/cars.json',
          format = 'json',
          type = 'source',
          event_time_field = 'timestamp',
          watermark_field = 'watermark'
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
        SELECT TUMBLE(INTERVAL '1' month) as window, COUNT(*) as count
        FROM cars
        GROUP BY 1);