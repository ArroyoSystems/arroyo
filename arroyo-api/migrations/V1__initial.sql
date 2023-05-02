-----------------
-- API Keys    --
-----------------
CREATE TABLE api_keys (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR NOT NULL,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    name TEXT NOT NULL,
    api_key TEXT NOT NULL
);

-----------------
-- Definitions --
-----------------
create TYPE connection_type as ENUM ('kafka', 'kinesis');

CREATE TABLE connections (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ,
    updated_by VARCHAR,
    deleted_at TIMESTAMPTZ,
    name TEXT NOT NULL,
    type connection_type NOT NULL,
    config JSONB NOT NULL,
    UNIQUE (organization_id, name)
);

create TYPE schema_type as ENUM ('builtin', 'json_schema', 'json_fields', 'raw_json');

CREATE TABLE schemas (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    name TEXT NOT NULL,
    kafka_schema_registry boolean NOT NULL,
    type schema_type NOT NULL,
    config JSONB,

    UNIQUE (organization_id, name)
);

create TYPE source_type as ENUM ('kafka', 'impulse', 'file', 'nexmark');

CREATE TABLE sources (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    name TEXT NOT NULL,

    type source_type NOT NULL,
    connection_id BIGINT REFERENCES connections(id),
    config JSONB,

    schema_id BIGINT REFERENCES schemas(id) NOT NULL,

    UNIQUE (organization_id, name)
);

CREATE TYPE sink_type as ENUM ('kafka', 'state');


CREATE TABLE sinks (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    name TEXT NOT NULL,
    type sink_type NOT NULL,
    connection_id BIGINT REFERENCES connections(id),
    config JSONB,

    UNIQUE (organization_id, name)
);

CREATE TYPE pipeline_type as ENUM ('sql', 'rust');

CREATE TABLE pipelines (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    name TEXT NOT NULL,
    type pipeline_type NOT NULL,
    current_version INT NOT NULL
);

CREATE TABLE pipeline_definitions (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,

    pipeline_id BIGINT REFERENCES pipelines ON DELETE CASCADE,
    version INT NOT NULL,
    textual_repr TEXT,
    program BYTEA NOT NULL,

    UNIQUE (pipeline_id, version)
);


-- NOTE: preview pipelines are not added to these tables
CREATE TABLE pipeline_sources (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGSERIAL REFERENCES pipeline_definitions(id) ON DELETE CASCADE,
    source_id BIGSERIAL REFERENCES sources(id)
);

CREATE TABLE pipeline_sinks (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGSERIAL REFERENCES pipeline_definitions(id) ON DELETE CASCADE,
    sink_id BIGSERIAL REFERENCES sinks(id)
);


------------------------------
-- Controller state machine --
------------------------------
CREATE TYPE stop_mode as ENUM ('none', 'checkpoint', 'graceful', 'immediate', 'force');

CREATE TABLE job_configs (
    id VARCHAR(8) PRIMARY KEY,
    organization_id VARCHAR,

    -- denormalized from pipelines to save a join
    pipeline_name TEXT NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMPTZ,

    ttl_micros BIGINT,

    stop stop_mode DEFAULT 'none',

    pipeline_definition BIGINT REFERENCES pipeline_definitions(id) ON DELETE CASCADE NOT NULL,

    parallelism_overrides JSONB default '{}',

    checkpoint_interval_micros BIGINT NOT NULL DEFAULT 10000000
);

create type checkpoint_state as ENUM ('inprogress', 'ready', 'compacting', 'compacted', 'failed');

CREATE TABLE checkpoints (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR,
    job_id VARCHAR(8) REFERENCES job_configs(id) ON DELETE CASCADE NOT NULL,

    state_backend TEXT NOT NULL,
    epoch INT NOT NULL,
    min_epoch INT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    finish_time TIMESTAMPTZ,
    state checkpoint_state DEFAULT 'inprogress',

    operators JSONB DEFAULT '{}' NOT NULL
);

CREATE INDEX checkpoints_job_id_idx ON checkpoints (job_id);


CREATE TABLE job_statuses (
    id VARCHAR(8) PRIMARY KEY REFERENCES job_configs(id) ON DELETE CASCADE,
    organization_id VARCHAR,
    controller_id BIGINT,

    --! identifies this particular run of the pipeline
    run_id BIGINT NOT NULL DEFAULT 0,

    start_time TIMESTAMPTZ,
    finish_time TIMESTAMPTZ,

    tasks INT,

    failure_message TEXT,
    restarts INT DEFAULT 0,

    pipeline_path VARCHAR,
    wasm_path VARCHAR,

    state TEXT DEFAULT 'Created'
);
