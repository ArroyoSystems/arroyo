CREATE TABLE impulse (
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


CREATE VIEW impulse_odd AS (
  SELECT * FROM impulse
  WHERE counter % 2 == 1
);

CREATE TABLE output (
  left_count bigint,
  right_count bigint
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'debezium_json',
  type = 'sink'
);

INSERT INTO output
SELECT A.counter, B.counter
FROM impulse A
JOIN impulse_odd B ON A.counter = B.counter;