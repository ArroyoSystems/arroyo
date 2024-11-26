CREATE TABLE cars (
	timestamp TIMESTAMP,
	driver_id BIGINT,
	event_type TEXT,
	location TEXT
) WITH (
	connector = 'single_file',
	path = 'cars.json',
	format = 'json',
	type = 'source'
);

CREATE TABLE cars_output (
	timestamp TIMESTAMP,
	driver_id BIGINT,
	event_type TEXT,
	location TEXT
) WITH (
	connector = 'single_file',
	path = 'cars_output.json',
	format = 'json',
	type = 'sink'
);
INSERT INTO cars_output SELECT * FROM cars WHERE driver_id = 100 AND event_type = 'pickup';
INSERT INTO cars_output SELECT * FROM cars WHERE driver_id = 101 AND event_type = 'dropoff';
