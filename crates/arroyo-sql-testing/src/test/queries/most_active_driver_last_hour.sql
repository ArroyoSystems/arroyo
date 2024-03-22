CREATE TABLE cars (
          timestamp TIMESTAMP,
          driver_id BIGINT,
          event_type TEXT,
          location TEXT,
          watermark timestamp GENERATED ALWAYS AS (timestamp - interval '1 hour') STORED
        ) WITH (
          connector = 'single_file',
          path = '$input_dir/cars.json',
          format = 'json',
          type = 'source',
          event_time_field = 'timestamp',
          watermark_field = 'watermark'
        );
        CREATE TABLE most_active_driver (
          start TIMESTAMP,
          end TIMESTAMP,
          driver_id BIGINT,
          count BIGINT,
          row_number BIGINT
        ) WITH (
          connector = 'single_file',
          path = '$output_path',
          format = 'json',
          type = 'sink'
        );
        INSERT INTO most_active_driver
        SELECT  window.start, window.end, driver_id, count, row_number FROM (
          SELECT *, ROW_NUMBER()  OVER (
            PARTITION BY window
            ORDER BY count DESC, driver_id desc) as row_number
          FROM (
          SELECT driver_id,
          hop(INTERVAL '1' minute, INTERVAL '1' hour ) as window,
                 count(*) as count
                 FROM cars
                 GROUP BY 1,2) ) where row_number = 1
