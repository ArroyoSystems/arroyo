CREATE TABLE udfs (
    pub_id VARCHAR PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    prefix TEXT,
    name TEXT NOT NULL,
    definition TEXT NOT NULL,
    language TEXT NOT NULL,
    description TEXT,

    UNIQUE(organization_id, name)
);
