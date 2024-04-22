CREATE TABLE impulse_source (
      timestamp TIMESTAMP,
      counter bigint unsigned not null,
      subtask_index bigint unsigned not null
    ) WITH (
      connector = 'single_file',
      path = '$input_dir/impulse.json',
      format = 'json',
      event_time_field = 'timestamp',
      type = 'source'
    );
CREATE TABLE impulse_sink (
    count bigint,
    min bigint,
    max bigint,
    start timestamp,
    end timestamp
) WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'json',
    type = 'sink'
);

INSERT INTO impulse_sink
SELECT count, min, max, window.start, window.end FROM (
    SELECT
     hop(interval '2 second', interval '10 second' ) as window,
count(*) as count,
min(counter) as min,
max(counter) as max
from impulse_source
GROUP BY 1
);