ALTER TABLE clients
    ADD COLUMN max_input_tokens BIGINT NOT NULL DEFAULT 0 CHECK (max_input_tokens BETWEEN 0 AND 1000000000000),
    ADD COLUMN max_output_tokens BIGINT NOT NULL DEFAULT 0 CHECK (max_output_tokens BETWEEN 0 AND 1000000000000),
    ADD COLUMN default_output_tokens BIGINT NOT NULL DEFAULT 0 CHECK (default_output_tokens BETWEEN 0 AND 1000000000000),
    ADD COLUMN max_outstanding_tokens BIGINT NOT NULL DEFAULT 0 CHECK (max_outstanding_tokens BETWEEN 0 AND 1000000000000);

ALTER TABLE model_pools ADD COLUMN request_size_admission JSONB NOT NULL DEFAULT '{}'
    CHECK (jsonb_typeof(request_size_admission) = 'object');

-- NULL identifies leases issued without request size accounting.
ALTER TABLE request_leases ADD COLUMN reserved_tokens BIGINT CHECK (reserved_tokens BETWEEN 1 AND 1000000000000);

ALTER TABLE admission_operations ADD COLUMN token_reservation JSONB
    CHECK (jsonb_typeof(token_reservation) = 'object');
