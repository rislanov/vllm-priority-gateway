CREATE TABLE usage_requests (
    id INTEGER PRIMARY KEY,
    occurred_at_ms INTEGER NOT NULL CHECK (occurred_at_ms >= 0),
    request_id TEXT NOT NULL UNIQUE,
    parent_request_id TEXT,
    client_id INTEGER NOT NULL,
    client_name TEXT NOT NULL,
    model_pool_id INTEGER NOT NULL,
    model_name TEXT NOT NULL,
    backend_name TEXT NOT NULL,
    http_status INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
    ttft_ms INTEGER CHECK (ttft_ms IS NULL OR ttft_ms >= 0),
    retry_count INTEGER NOT NULL CHECK (retry_count >= 0),
    disconnected INTEGER NOT NULL CHECK (disconnected IN (0, 1)),
    usage_available INTEGER NOT NULL CHECK (usage_available IN (0, 1)),
    input_tokens INTEGER CHECK (input_tokens IS NULL OR input_tokens >= 0),
    output_tokens INTEGER CHECK (output_tokens IS NULL OR output_tokens >= 0),
    cache_read_tokens INTEGER CHECK (cache_read_tokens IS NULL OR cache_read_tokens >= 0),
    CHECK (
        (usage_available = 0 AND input_tokens IS NULL AND output_tokens IS NULL AND cache_read_tokens IS NULL)
        OR
        (usage_available = 1 AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL)
    ),
    CHECK (cache_read_tokens IS NULL OR cache_read_tokens <= input_tokens)
);

CREATE INDEX idx_usage_requests_occurred_at ON usage_requests(occurred_at_ms DESC);
CREATE INDEX idx_usage_requests_client_time ON usage_requests(client_id, occurred_at_ms DESC);
CREATE INDEX idx_usage_requests_model_time ON usage_requests(model_pool_id, occurred_at_ms DESC);
