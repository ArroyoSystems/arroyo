CREATE TABLE udfs_new (
  pub_id TEXT PRIMARY KEY,
  organization_id TEXT NOT NULL,
  created_by TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  prefix TEXT,
  name TEXT NOT NULL,
  definition TEXT NOT NULL,
  description TEXT,
  dylib_url TEXT,
  language VARCHAR(15) NOT NULL DEFAULT 'rust',
  UNIQUE (organization_id, name)
);

-- Copy data from the old table to the nfew table
INSERT INTO udfs_new (pub_id, organization_id, created_by, created_at, updated_at,
                      prefix, name, definition, description, dylib_url, language)
SELECT pub_id, organization_id, created_by, created_at, updated_at,
       prefix, name, definition, description, dylib_url, 'rust'
from udfs;

-- Drop the old table
DROP TABLE udfs;

-- Rename the new table to the old table name
ALTER TABLE udfs_new RENAME TO udfs;
