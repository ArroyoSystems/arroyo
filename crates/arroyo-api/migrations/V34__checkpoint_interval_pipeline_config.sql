-- Preserve existing per-job checkpoint intervals while moving runtime handling
-- to the generic pipeline config overlay.
UPDATE job_configs
SET pipeline_config = jsonb_set(
    pipeline_config,
    '{checkpoint}',
    COALESCE(pipeline_config -> 'checkpoint', '{}'::jsonb)
        || jsonb_build_object('interval', checkpoint_interval_micros::text || 'micros'),
    true
)
WHERE pipeline_config #> '{checkpoint,interval}' IS NULL;

ALTER TABLE job_configs DROP COLUMN checkpoint_interval_micros;
