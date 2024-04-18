CREATE TABLE impulse_source
(
    timestamp     TIMESTAMP,
    counter       bigint unsigned not null,
    subtask_index bigint unsigned not null
) WITH (
      connector = 'single_file',
      path = '$input_dir/impulse.json',
      format = 'json',
      type = 'source'
);
CREATE TABLE double_negative_udf
(
    counter bigint
) WITH (
      connector = 'single_file',
      path = '$output_path',
      format = 'json',
      type = 'sink'
);

INSERT INTO double_negative_udf
SELECT double_negative(counter)
FROM impulse_source