CREATE TABLE impulse WITH(
 connector = 'impulse',
 event_rate = '10000'
);


SELECT evens.even_counter FROM
    (SELECT counter as even_counter FROM impulse where counter % 2 = 0) evens
        JOIN impulse on evens.even_counter = impulse.counter;