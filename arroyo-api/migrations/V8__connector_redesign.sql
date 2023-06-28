ALTER TABLE connections
ALTER COLUMN type TYPE TEXT using type::text;

ALTER TABLE sources
ALTER COLUMN type TYPE TEXT using type::text;

CREATE TABLE connection_tables (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,

    name TEXT NOT NULL,
    table_type TEXT NOT NULL, -- currently one of source or sink
    connector TEXT NOT NULL,
    connection_id BIGINT REFERENCES connections(id),
    config JSONB,
    -- json-serialized ConnectionSchema
    schema JSONB,

    UNIQUE (organization_id, name)
);

CREATE TABLE connection_table_pipelines (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGSERIAL REFERENCES pipeline_definitions(id) ON DELETE CASCADE,
    connection_table_id BIGSERIAL REFERENCES connection_tables(id)
);

DROP TABLE pipeline_sinks;
DROP TABLE pipeline_sources;
DROP TABLE sinks;
DROP TABLE sources;
