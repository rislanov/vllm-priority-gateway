CREATE TABLE IF NOT EXISTS config_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    revision INTEGER NOT NULL CHECK (revision >= 0)
);

INSERT OR IGNORE INTO config_meta (singleton, revision) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS clients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    priority_class TEXT NOT NULL CHECK (priority_class IN ('critical', 'high', 'normal', 'background')),
    vllm_priority INTEGER NOT NULL CHECK (vllm_priority BETWEEN -2147483648 AND 2147483647),
    max_concurrency INTEGER NOT NULL CHECK (max_concurrency >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    prefix TEXT NOT NULL,
    secret_hash BLOB NOT NULL CHECK (length(secret_hash) = 32),
    created_at TEXT NOT NULL,
    expires_at TEXT,
    revoked_at TEXT,
    last_used_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_api_keys_prefix ON api_keys(prefix);
CREATE INDEX IF NOT EXISTS idx_api_keys_client_id ON api_keys(client_id);

CREATE TABLE IF NOT EXISTS model_pools (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    public_model_name TEXT NOT NULL UNIQUE,
    upstream_model_name TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS client_model_access (
    client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    model_pool_id INTEGER NOT NULL REFERENCES model_pools(id) ON DELETE CASCADE,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    PRIMARY KEY (client_id, model_pool_id)
);

CREATE TABLE IF NOT EXISTS backends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_pool_id INTEGER NOT NULL REFERENCES model_pools(id) ON DELETE RESTRICT,
    name TEXT NOT NULL UNIQUE,
    base_url TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    draining INTEGER NOT NULL CHECK (draining IN (0, 1)),
    capacity_hint REAL NOT NULL CHECK (capacity_hint > 0),
    running_soft_limit REAL NOT NULL CHECK (running_soft_limit > 0),
    upstream_api_key_env TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backends_model_pool_id ON backends(model_pool_id);
