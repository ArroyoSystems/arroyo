CREATE TABLE debezium_source (
      min BIGINT,
      max BIGINT,
      sum BIGINT,
      count BIGINT,
      avg DOUBLE
    ) WITH (
      connector = 'single_file',
      path = '$input_dir/aggregate_updates.json',
      format = 'debezium_json',
      type = 'source'
    );

CREATE TABLE output (
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

    INSERT INTO output 
    SELECT *
    FROM debezium_source