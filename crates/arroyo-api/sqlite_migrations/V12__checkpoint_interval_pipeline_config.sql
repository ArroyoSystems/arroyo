-- Preserve existing per-job checkpoint intervals while moving runtime handling
-- to the generic pipeline config overlay.
UPDATE job_configs
SET pipeline_config = json_set(
    pipeline_config,
    '$.checkpoint.interval',
    CAST(checkpoint_interval_micros AS TEXT) || 'micros'
)
WHERE json_type(pipeline_config, '$.checkpoint.interval') IS NULL;

ALTER TABLE job_configs DROP COLUMN checkpoint_interval_micros;
