--fail=This feature is not implemented: must have window in aggregate. Make sure you are calling one of the windowing functions (hop, tumble, session) or using the window field of the input
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
    CREATE TABLE aggregates (
      min BIGINT,
      max BIGINT,
      sum BIGINT,
      count BIGINT,
      avg DOUBLE
    ) WITH (
      connector = 'single_file',
      path = '$output_path',
      format = 'debezium_json',
      type = 'sink'
    );

INSERT INTO aggregates SELECT min(counter), max(counter), sum(counter), count(*), avg(counter)  FROM impulse_source