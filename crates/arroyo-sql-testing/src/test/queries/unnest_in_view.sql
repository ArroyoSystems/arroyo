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

    CREATE TABLE unnest_output (
     counter bigint unsigned not null
    ) WITH (
      connector = 'single_file',
      path = '$output_path',
      format = 'json',
      type = 'sink'
    );

    CREATE VIEW unnest_view AS
    SELECT unnest(counters) as counter FROM (
    SELECT array_agg(counter) as counters, tumble(interval '1 day') FROM impulse_source GROUP BY tumble(interval '1 day'));

    INSERT INTO unnest_output SELECT counter FROM unnest_view;