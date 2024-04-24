CREATE TABLE impulse_source (
      timestamp TIMESTAMP,
      counter bigint unsigned not null,
      subtask_index bigint unsigned not null
    ) WITH (
      connector = 'single_file',
      path = '$input_dir/impulse.json',
      format = 'json',
      type = 'source'
    );

    CREATE TABLE second_impulse_source (
      timestamp TIMESTAMP,
      counter bigint unsigned not null,
      subtask_index bigint unsigned not null
    ) WITH (
      connector = 'single_file',
      path = '$input_dir/impulse.json',
      format = 'json',
      type = 'source'
    );

    CREATE TABLE union_output (
      counter bigint,
      source text
    ) WITH (
      connector = 'single_file',
      path = '$output_path',
      format = 'json',
      type = 'sink'
    );

    INSERT INTO union_output
    SELECT counter, 'first' as source FROM impulse_source
    UNION ALL SELECT counter, 'second' as source FROM second_impulse_source