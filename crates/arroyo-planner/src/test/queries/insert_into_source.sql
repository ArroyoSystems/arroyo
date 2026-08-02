--fail=attempted to insert into table 'source', but it is a source
CREATE TABLE source with (
    connector = 'impulse',
    event_rate = 10
);

INSERT INTO source
select * from source;