create table logs (
  ip TEXT
) with (
  connector = 'kafka',
  bootstrap_servers = 'localhost:9092',
  format = 'json',
  type = 'source',
  topic ='logs',
  'source.offset' = 'latest'
);

SELECT count(*) as count, city, hop(interval '5 seconds', interval '15 minutes') as window
FROM (
 select get_city(logs.ip) as city
 FROM logs
)
where city is not null
group by city, window;
