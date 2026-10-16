CREATE TABLE events (
    counter BIGINT UNSIGNED NOT NULL
) WITH (
    connector = 'single_file',
    path = '$input_dir/impulse.json',
    format = 'json',
    type = 'source'
);

-- Infer the sink schema to exercise aliases for the view's internal timestamp.
CREATE TABLE output WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'json',
    type = 'sink'
);

CREATE VIEW event_view AS SELECT counter FROM events WHERE counter < 10;

INSERT INTO output SELECT * FROM event_view;
