CREATE TABLE cluster_info (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    singleton INT NOT NULL DEFAULT 0 UNIQUE CHECK (singleton = 0)
);

INSERT INTO cluster_info (id, name) VALUES (gen_random_uuid(), 'default');
