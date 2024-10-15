create table impulse with (
    connector = 'impulse',
    event_rate = '10'
);

select counter % 10, count(*) from impulse
group by counter % 10;