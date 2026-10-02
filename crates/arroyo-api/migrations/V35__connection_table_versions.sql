CREATE TABLE connection_table_versions (
    id BIGSERIAL PRIMARY KEY,
    connection_table_id BIGINT NOT NULL
        REFERENCES connection_tables(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    config JSONB,
    schema JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR NOT NULL,
    UNIQUE (connection_table_id, version)
);

INSERT INTO connection_table_versions
    (connection_table_id, version, config, schema, created_at, created_by)
SELECT id, 1, config, schema, created_at, created_by
FROM connection_tables;

ALTER TABLE connection_tables
DROP COLUMN config,
DROP COLUMN schema;

ALTER TABLE connection_table_pipelines
ADD COLUMN connection_version INTEGER NOT NULL DEFAULT 1;

ALTER TABLE connection_table_pipelines
ADD CONSTRAINT connection_table_pipeline_version_fk
FOREIGN KEY (connection_table_id, connection_version)
REFERENCES connection_table_versions(connection_table_id, version);
