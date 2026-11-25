create table logs (
  ip TEXT
) with (
  connector = 'single_file',
  format = 'json',
  type = 'source',
  path = 'logs.json'
);

SELECT count(*) as count, city, hop(interval '5 seconds', interval '15 minutes') as window
FROM (
 select get_city(logs.ip) as city
 FROM logs
)
where city is not null
group by city, window;
