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

CREATE TABLE session_window_output (
  start timestamp,
  end timestamp,
  user_id bigint,
  rows bigint
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);

INSERT INTO session_window_output
SELECT window.start, window.end, user_id, rows FROM (
    SELECT SESSION(interval '20 seconds') as window, CASE WHEN counter % 10 = 0 THEN 0 ELSE counter END as user_id, count(*) as rows
FROM impulse_source GROUP BY window, user_id)