ALTER TABLE connection_tables
ADD COLUMN current_version INTEGER NOT NULL DEFAULT 1;

CREATE TABLE connection_table_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_table_id INTEGER NOT NULL,
    version INTEGER NOT NULL,
    config TEXT,
    schema TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT NOT NULL,
    UNIQUE (connection_table_id, version),
    FOREIGN KEY (connection_table_id) REFERENCES connection_tables(id) ON DELETE CASCADE
);

CREATE TRIGGER connection_tables_initial_version
AFTER INSERT ON connection_tables
BEGIN
    INSERT INTO connection_table_versions
        (connection_table_id, version, config, schema, created_at, created_by)
    VALUES
        (NEW.id, NEW.current_version, NEW.config, NEW.schema, NEW.created_at, NEW.created_by);
END;

INSERT INTO connection_table_versions
    (connection_table_id, version, config, schema, created_at, created_by)
SELECT id, 1, config, schema, created_at, created_by
FROM connection_tables;

ALTER TABLE connection_table_pipelines
ADD COLUMN connection_version INTEGER NOT NULL DEFAULT 1;

CREATE TRIGGER connection_table_pipeline_version_insert
BEFORE INSERT ON connection_table_pipelines
WHEN NOT EXISTS (
    SELECT 1
    FROM connection_table_versions
    WHERE connection_table_id = NEW.connection_table_id
      AND version = NEW.connection_version
)
BEGIN
    SELECT RAISE(ABORT, 'connection table version does not exist');
END;

CREATE TRIGGER connection_table_pipeline_version_update
BEFORE UPDATE OF connection_table_id, connection_version ON connection_table_pipelines
WHEN NOT EXISTS (
    SELECT 1
    FROM connection_table_versions
    WHERE connection_table_id = NEW.connection_table_id
      AND version = NEW.connection_version
)
BEGIN
    SELECT RAISE(ABORT, 'connection table version does not exist');
END;
