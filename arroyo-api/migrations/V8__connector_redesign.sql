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
    type TEXT NOT NULL, -- currently one of source or sink
    connection_id BIGINT REFERENCES connections(id),
    config JSONB,
    -- json-serialized ConnectionSchema
    schema JSONB,

    UNIQUE (organization_id, name)
);

