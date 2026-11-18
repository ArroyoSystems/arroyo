--fail=attempted to read from table 'cars_output', but it is a sink

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
INSERT INTO cars_output SELECT * from cars_output;
