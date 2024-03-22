CREATE TABLE impulse_source (
      timestamp TIMESTAMP,
      counter bigint unsigned not null,
      subtask_index bigint unsigned not null
    ) WITH (
      connector = 'single_file',
      path = '$input_dir/impulse.json',
      format = 'json',
      type = 'source',
      event_time_field = 'timestamp'
    );
    CREATE TABLE delayed_impulse_source (
      timestamp TIMESTAMP,
      counter bigint unsigned not null,
      subtask_index bigint unsigned not null,
      watermark timestamp GENERATED ALWAYS AS (timestamp - INTERVAL '10 minute') STORED
    ) WITH (
      connector = 'single_file',
      path = '$input_dir/impulse.json',
      format = 'json',
      type = 'source',
      event_time_field = 'timestamp',
      watermark_field = 'watermark'
    );
    CREATE TABLE offset_output (
      start timestamp,
      counter bigint
    ) WITH (
      connector = 'single_file',
      path = '$output_path',
      format = 'json',
      type = 'sink'
    );
    INSERT INTO offset_output
    SELECT window.start, a.counter as counter
    FROM (SELECT TUMBLE(interval '1 second'),  counter, count(*) FROM impulse_source GROUP BY 1,2) a
    JOIN (SELECT TUMBLE(interval '1 second') as window, counter , count(*) FROM delayed_impulse_source GROUP BY 1,2) b
    ON a.counter = b.counter