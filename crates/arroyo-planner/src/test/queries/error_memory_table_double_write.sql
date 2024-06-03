--fail=Can only insert into a memory table once
CREATE TABLE cars (
      timestamp TIMESTAMP,
      driver_id BIGINT,
      event_type TEXT,
      location TEXT
    ) WITH (
      connector = 'single_file',
      path = '$input_dir/cars.json',
      format = 'json',
      type = 'source'
    );

    CREATE TABLE memory (
      event_type TEXT,
      location TEXT,
      driver_id BIGINT,
    );

    CREATE TABLE cars_output (
      driver_id BIGINT,
      event_type TEXT
    ) WITH (
      connector = 'single_file',
      path = '$output_path',
      format = 'json',
      type = 'sink'
    );
    INSERT INTO memory SELECT event_type, location, driver_id as other_driver_id FROM cars;
    INSERT INTO memory SELECT location, event_type, driver_id FROM cars; 
    INSERT INTO cars_output SELECT driver_id as other_driver_id, event_type FROM memory;