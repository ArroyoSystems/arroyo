CREATE TABLE mastodon (
    value TEXT
) WITH (
    connector = 'sse',
    format = 'raw_string',
    endpoint = 'http://mastodon.arroyo.dev/api/v1/streaming/public',
    events = 'update'
);

CREATE VIEW tags AS (
    SELECT tag FROM (
        SELECT extract_json_string(value, '$.tags[*].name') AS tag
     FROM mastodon)
    WHERE tag is not null
);

SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY window
        ORDER BY count DESC) as row_num
    FROM (SELECT count(*) as count,
        tag,
        hop(interval '5 seconds', interval '15 minutes') as window
            FROM tags
            group by tag, window)) WHERE row_num <= 5;
