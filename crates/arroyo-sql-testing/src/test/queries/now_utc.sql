CREATE TABLE events (
    timestamp TIMESTAMP(3),
    counter BIGINT
) WITH (
    connector = 'single_file',
    path = '$input_dir/impulse.json',
    format = 'json',
    type = 'source'
);

CREATE TABLE output WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'json',
    type = 'sink'
);

CREATE VIEW nullable_events AS
SELECT counter, CASE WHEN counter = 1 THEN NULL ELSE timestamp END AS event_ts
FROM events;

-- Keep clock-dependent values out of the golden output. The last row also
-- exercises the projection after checkpoint recovery.
INSERT INTO output
SELECT
    counter,
    event_ts,
    arrow_typeof(event_ts) AS source_type,
    arrow_typeof(now()) AS now_type,
    arrow_typeof(COALESCE(event_ts, now())) AS coalesced_type,
    CASE WHEN event_ts IS NOT NULL THEN COALESCE(event_ts, now()) END AS coalesced,
    CASE
        WHEN event_ts IS NULL THEN COALESCE(event_ts, now()) = CAST(now() AS TIMESTAMP(3))
        ELSE COALESCE(event_ts, now()) = event_ts
    END AS value_matches
FROM nullable_events
WHERE counter IN (0, 1, 99);
