--fail=row_group_size must be between 1 and 1073741824 bytes, got 2199023255552
CREATE TABLE impulse WITH (
    connector = 'impulse',
    event_rate = '10'
);

CREATE TABLE sink (
    counter bigint
) WITH (
    connector = 'filesystem',
    type = 'sink',
    path = '/tmp/output',
    format = 'parquet',
    'parquet.row_group_size' = '2TB'
);

INSERT INTO sink SELECT counter FROM impulse;
