CREATE TABLE api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    name TEXT NOT NULL,
    api_key TEXT NOT NULL,
    pub_id TEXT NOT NULL UNIQUE
);

CREATE TABLE checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    organization_id TEXT,
    job_id TEXT NOT NULL,
    state_backend TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    min_epoch INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL,
    finish_time TIMESTAMP,
    state TEXT DEFAULT 'inprogress' NOT NULL,
    operators TEXT DEFAULT '{}' NOT NULL,
    pub_id TEXT NOT NULL UNIQUE
);

CREATE TABLE cluster_info (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    singleton INTEGER DEFAULT 0 NOT NULL,
    CONSTRAINT cluster_info_singleton_check CHECK (singleton = 0)
);

CREATE TABLE connection_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    organization_id TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    updated_by TEXT,
    deleted_at TIMESTAMP,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    config TEXT NOT NULL,
    pub_id TEXT NOT NULL UNIQUE
);

CREATE TABLE connection_table_pipelines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_table_id INTEGER NOT NULL,
    pub_id TEXT NOT NULL UNIQUE,
    pipeline_id INTEGER,
    FOREIGN KEY (pipeline_id) references pipelines(id) ON DELETE CASCADE
);

CREATE TABLE connection_tables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    organization_id TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by TEXT,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP,
    name TEXT NOT NULL,
    table_type TEXT NOT NULL,
    connector TEXT NOT NULL,
    connection_id INTEGER,
    config TEXT,
    schema TEXT,
    pub_id TEXT NOT NULL UNIQUE,
    UNIQUE (organization_id, name),
    FOREIGN KEY (connection_id) references connection_profiles(id) ON DELETE CASCADE
);

CREATE TABLE job_configs (
    id TEXT PRIMARY KEY,
    organization_id TEXT,
    pipeline_name TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by TEXT,
    updated_at TIMESTAMP,
    ttl_micros INTEGER,
    stop TEXT DEFAULT 'none' NOT NULL,
    parallelism_overrides TEXT DEFAULT '{}' NOT NULL,
    checkpoint_interval_micros INTEGER DEFAULT 10000000 NOT NULL,
    pipeline_id INTEGER,
    restart_nonce INTEGER DEFAULT 0 NOT NULL,
    restart_mode TEXT DEFAULT 'safe' NOT NULL,
    FOREIGN KEY (pipeline_id) references pipelines(id) ON DELETE CASCADE
);

CREATE TABLE job_log_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    job_id TEXT,
    operator_id TEXT,
    task_index INTEGER,
    log_level TEXT DEFAULT 'info',
    message TEXT NOT NULL,
    details TEXT,
    pub_id TEXT NOT NULL UNIQUE,
    FOREIGN KEY (job_id) references job_configs(id) ON DELETE CASCADE
);

CREATE TABLE job_statuses (
    id TEXT PRIMARY KEY,
    organization_id TEXT,
    controller_id INTEGER,
    run_id INTEGER DEFAULT 0 NOT NULL,
    start_time TIMESTAMP,
    finish_time TIMESTAMP,
    tasks INTEGER,
    failure_message TEXT,
    restarts INTEGER DEFAULT 0 NOT NULL,
    pipeline_path TEXT,
    wasm_path TEXT,
    state TEXT DEFAULT 'Created' NOT NULL,
    pub_id TEXT NOT NULL UNIQUE,
    restart_nonce INTEGER DEFAULT 0 NOT NULL,
    FOREIGN KEY (id) references job_configs(id) ON DELETE CASCADE
);

CREATE TABLE pipelines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    organization_id TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by TEXT,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    pub_id TEXT NOT NULL UNIQUE,
    textual_repr TEXT NOT NULL,
    program BLOB NOT NULL,
    udfs TEXT DEFAULT '[]' NOT NULL,
    proto_version INTEGER DEFAULT 1 NOT NULL
);

CREATE TABLE udfs (
    pub_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    prefix TEXT,
    name TEXT NOT NULL,
    definition TEXT NOT NULL,
    description TEXT,
    dylib_url TEXT DEFAULT '' NOT NULL,
    UNIQUE (organization_id, name)
);