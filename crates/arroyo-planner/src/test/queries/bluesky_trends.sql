create table firesky (
    value BYTEA
) with (
    connector = 'websocket',
    endpoint = 'wss://firesky.tv/ws/app',
    format = 'raw_bytes'
);
 
create view tags as (
    select unnest(extract_json(cbor_to_json(value),
        '$.info.post.facets[*].features[*].tag')) as tag
    from firesky);
 
 
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY window
        ORDER BY count DESC) as row_num
    FROM (SELECT count(*) as count,
        tag,
        hop(interval '5 seconds', interval '15 minutes') as window
            FROM tags
            group by tag, window)) WHERE row_num <= 5;
