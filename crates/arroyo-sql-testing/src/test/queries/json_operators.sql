CREATE TABLE cars (
    value JSON
) WITH (
    connector = 'single_file',
    path = '$input_dir/cars.json',
    format = 'json',
    type = 'source',
    'json.unstructured' = 'true'
);

CREATE TABLE sink WITH (
      connector = 'single_file',
      path = '$output_path',
      format = 'json',
      type = 'sink'
);


insert into sink
select 'test' as a, value->'driver_id' as b, value->'event_type' as c, value->'not_a_field' as d
from cars;