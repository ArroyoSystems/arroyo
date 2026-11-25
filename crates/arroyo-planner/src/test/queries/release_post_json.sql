CREATE TABLE events (
  value JSON
) WITH (
  connector = 'single_file',
  path = 'events.json',
  format = 'json',
  type = 'source',
  'json.unstructured' = 'true'
);

SELECT 
  -- using the json_get function
  json_get('user', 'name')::TEXT as name,
  -- or using the -> operator
  value->'user'->'email' as email,
  -- field presence check can be done with the ? operator
  value ? 'id' as has_id
FROM events;
