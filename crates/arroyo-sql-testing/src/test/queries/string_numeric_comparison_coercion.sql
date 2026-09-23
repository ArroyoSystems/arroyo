--checkpoint-interval=1
CREATE TABLE string_ids (
  user_id TEXT,
  event_name TEXT
) WITH (
  connector = 'single_file',
  path = '$input_dir/string_numeric_comparison_coercion.json',
  format = 'json',
  type = 'source'
);

CREATE TABLE string_ids_output (
  user_id TEXT,
  event_name TEXT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);

INSERT INTO string_ids_output
SELECT user_id, event_name
FROM string_ids
WHERE user_id != 123;
