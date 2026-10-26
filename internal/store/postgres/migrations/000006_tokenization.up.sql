CREATE TABLE tokenization_client_scopes (client_id BIGINT PRIMARY KEY);
CREATE TABLE tokenization_backend_scopes (backend_id BIGINT PRIMARY KEY);
CREATE TABLE tokenization_operations (
 operation_id UUID PRIMARY KEY, input_fingerprint BYTEA NOT NULL CHECK(octet_length(input_fingerprint)=32),
 decision JSONB NOT NULL, completed BOOLEAN NOT NULL DEFAULT FALSE, retain_until TIMESTAMPTZ NOT NULL);
CREATE INDEX tokenization_operations_retention_idx ON tokenization_operations(retain_until);
CREATE TABLE tokenization_leases (
 operation_id UUID PRIMARY KEY REFERENCES tokenization_operations(operation_id),
 client_id BIGINT NOT NULL, backend_id BIGINT NOT NULL, expires_at TIMESTAMPTZ NOT NULL);
CREATE INDEX tokenization_leases_client_idx ON tokenization_leases(client_id,expires_at);
CREATE INDEX tokenization_leases_backend_idx ON tokenization_leases(backend_id,expires_at);
CREATE INDEX tokenization_leases_expiry_idx ON tokenization_leases(expires_at);
CREATE TABLE tokenization_rate_state (client_id BIGINT PRIMARY KEY, balance DOUBLE PRECISION NOT NULL, last_refill_at TIMESTAMPTZ NOT NULL);
