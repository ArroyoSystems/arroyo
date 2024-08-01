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

CREATE TABLE udaf (
  median bigint,
  none_value double,
  max_product bigint
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);

INSERT INTO udaf
SELECT median, none_value, max_product FROM (
  SELECT
      tumble(interval '30' day) as window,
      my_median(counter) as median,
      none_udf(counter) as none_value,
      max_product(counter, subtask_index) as max_product
FROM impulse_source
GROUP BY 1)