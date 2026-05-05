-- Optional R2/S3 URL where this pipeline's checkpoints live. NULL falls
-- back to the controller's global checkpoint_url config. Existing rows
-- on upgrade automatically get NULL — no default needed.
ALTER TABLE pipelines ADD COLUMN state_url TEXT;
-- Optional JSON object of the form: { "key" : "value", ... }
ALTER TABLE pipelines ADD COLUMN tags JSONB;
