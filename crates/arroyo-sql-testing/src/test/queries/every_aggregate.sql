--pk=event_type
CREATE TABLE cars (
      timestamp TIMESTAMP NOT NULL,
      driver_id BIGINT,
      event_type TEXT,
      location TEXT,
      watermark for timestamp AS (timestamp - interval '1 hour')
) WITH (
      connector = 'single_file',
      path = '$input_dir/cars.json',
      format = 'json',
      type = 'source'
);

CREATE TABLE every_aggregate WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'debezium_json',
  type = 'sink'
);

INSERT INTO every_aggregate
SELECT
    event_type,
    round(avg(driver_id), 4) AS avg_driver,
    bit_and(driver_id) AS bit_and_driver,
    bit_or(driver_id) AS bit_or_driver,
    bit_xor(driver_id) AS bit_xor_driver,
    bool_and(driver_id > 0) AS bool_and_driver,
    bool_or(driver_id > 0) AS bool_or_driver,
    count(*) AS cnt,
    max(driver_id) AS max_driver,
    median(driver_id) AS median_driver,
    min(driver_id) AS min_driver,
    sum(driver_id) AS sum_driver,
    round(corr(driver_id, extract(epoch FROM timestamp)), 4) AS corr_driver_ts,
    round(covar_pop(driver_id, extract(epoch FROM timestamp)), 4) AS covar_pop_driver_ts,
    round(covar_samp(driver_id, extract(epoch FROM timestamp)), 4) AS covar_samp_driver_ts,
    round(stddev(driver_id), 4) AS stddev_driver,
    round(stddev_pop(driver_id), 4) AS stddev_pop_driver,
    round(var(driver_id), 4) AS var_driver,
    round(var_pop(driver_id), 4) AS var_pop_driver,
    round(regr_slope(driver_id, extract(epoch FROM timestamp)), 4) AS regr_slope,
    round(regr_avgx(driver_id, extract(epoch FROM timestamp)), 4) AS regr_avgx,
    round(regr_avgy(driver_id, extract(epoch FROM timestamp)), 4) AS regr_avgy,
    round(regr_count(driver_id, extract(epoch FROM timestamp)), 4) AS regr_count,
    round(regr_intercept(driver_id, extract(epoch FROM timestamp)), 4) AS regr_intercept,
    round(regr_r2(driver_id, extract(epoch FROM timestamp)), 4) AS regr_r2,
    round(regr_sxx(driver_id, extract(epoch FROM timestamp)), 4) AS regr_sxx,
    round(regr_syy(driver_id, extract(epoch FROM timestamp)), 4) AS regr_syy,
    round(regr_sxy(driver_id, extract(epoch FROM timestamp)), 4) AS regr_sxy,
    round(approx_distinct(driver_id), 4) AS approx_distinct_driver,
    round(approx_median(driver_id), 4) AS approx_median_driver,
    round(approx_percentile_cont(0.5) WITHIN GROUP (ORDER BY driver_id), 4) AS approx_percentile_cont_driver,
    round(approx_percentile_cont_with_weight(character_length(location), 0.5) WITHIN GROUP (ORDER BY driver_id), 4) AS approx_percentile_cont_weighted_driver
FROM cars
GROUP BY event_type;