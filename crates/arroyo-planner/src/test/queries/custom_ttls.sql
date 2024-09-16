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

set updating_ttl = '5 seconds';

select count(distinct tag) from tags;