-- Optional URL where this pipeline's checkpoints live. NULL falls
-- back to the controller's global checkpoint_url config. Existing rows
-- on upgrade automatically get NULL — no default needed.
-- See [Storage](https://doc.arroyo.dev/deployment/#storage) for a complete reference.
ALTER TABLE pipelines ADD COLUMN state_url TEXT;
-- JSON object of the form: { "key" : "value", ... }. Defaults to an empty map.
ALTER TABLE pipelines ADD COLUMN tags JSONB NOT NULL DEFAULT '{}'::jsonb;
