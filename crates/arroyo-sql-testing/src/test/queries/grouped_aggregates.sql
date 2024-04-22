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
      counter_mod BIGINT,
      min BIGINT,
      max BIGINT,
      sum BIGINT,
      count BIGINT,
      avg DOUBLE,
    ) WITH (
      connector = 'single_file',
      path = '$output_path',
      format = 'debezium_json',
      type = 'sink'
    );

INSERT INTO aggregates SELECT counter % 5, min(counter), max(counter), sum(counter), count(*), avg(counter)
   FROM impulse_source
GROUP BY 1