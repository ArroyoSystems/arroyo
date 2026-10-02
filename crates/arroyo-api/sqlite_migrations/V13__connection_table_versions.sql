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

INSERT INTO connection_table_versions
    (connection_table_id, version, config, schema, created_at, created_by)
SELECT id, 1, config, schema, created_at, created_by
FROM connection_tables;

ALTER TABLE connection_tables
DROP COLUMN config;

ALTER TABLE connection_tables
DROP COLUMN schema;

CREATE TABLE connection_table_pipelines_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_table_id INTEGER NOT NULL,
    connection_version INTEGER NOT NULL DEFAULT 1,
    pub_id TEXT NOT NULL UNIQUE,
    pipeline_id INTEGER,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE,
    FOREIGN KEY (connection_table_id, connection_version)
        REFERENCES connection_table_versions(connection_table_id, version)
);

INSERT INTO connection_table_pipelines_new
    (id, connection_table_id, connection_version, pub_id, pipeline_id)
SELECT id, connection_table_id, 1, pub_id, pipeline_id
FROM connection_table_pipelines;

DROP TABLE connection_table_pipelines;

ALTER TABLE connection_table_pipelines_new
RENAME TO connection_table_pipelines;
