create table impulse with (
     connector = 'impulse',
     event_rate = '10'
);

select count(*) from impulse
group by hop(interval '10 seconds', interval '10 seconds');