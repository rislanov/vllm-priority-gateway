CREATE TABLE client_admission_scopes (client_id BIGINT PRIMARY KEY, tombstoned BOOLEAN NOT NULL DEFAULT FALSE, updated_at TIMESTAMPTZ NOT NULL);
CREATE TABLE pool_admission_scopes (pool_id BIGINT PRIMARY KEY, tombstoned BOOLEAN NOT NULL DEFAULT FALSE, updated_at TIMESTAMPTZ NOT NULL);
CREATE TABLE admission_operations (
 lease_id UUID PRIMARY KEY, operation_started_at TIMESTAMPTZ NOT NULL, input_fingerprint BYTEA NOT NULL,
 request_id TEXT NOT NULL, replica_id UUID NOT NULL, client_id BIGINT NOT NULL, pool_id BIGINT NOT NULL,
 decision TEXT NOT NULL CHECK(decision IN ('pending','admitted','rejected')), rejection_reason TEXT, retry_at TIMESTAMPTZ,
 decided_at TIMESTAMPTZ, lease_expired_at TIMESTAMPTZ, completed_at TIMESTAMPTZ,
 completion_result TEXT CHECK(completion_result IN ('released','lease_lost')), retain_until TIMESTAMPTZ,
 CHECK((decision='rejected')=(rejection_reason IS NOT NULL)),
 CHECK((completed_at IS NULL)=(completion_result IS NULL)));
CREATE TABLE request_leases (
 lease_id UUID PRIMARY KEY REFERENCES admission_operations(lease_id), request_id TEXT NOT NULL, replica_id UUID NOT NULL,
 client_id BIGINT NOT NULL, pool_id BIGINT NOT NULL, acquired_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL) WITH (fillfactor=70, autovacuum_vacuum_scale_factor=0.02, autovacuum_analyze_scale_factor=0.01);
CREATE INDEX request_leases_client_idx ON request_leases(client_id); CREATE INDEX request_leases_pool_idx ON request_leases(pool_id);
CREATE TABLE coordination_rate_state (client_id BIGINT NOT NULL, rate_kind TEXT NOT NULL CHECK(rate_kind IN ('rpm','tpm')), policy_revision BIGINT NOT NULL, balance DOUBLE PRECISION NOT NULL, last_refill_at TIMESTAMPTZ NOT NULL, PRIMARY KEY(client_id,rate_kind));
CREATE TABLE backend_circuit_state (backend_id BIGINT PRIMARY KEY, backend_revision BIGINT NOT NULL, state TEXT NOT NULL CHECK(state IN ('closed','open','half_open')), generation BIGINT NOT NULL, opened_at TIMESTAMPTZ, half_open_succeeded BOOLEAN NOT NULL, updated_at TIMESTAMPTZ NOT NULL);
CREATE TABLE backend_circuit_failure_receipts (attempt_id UUID PRIMARY KEY, backend_id BIGINT NOT NULL, backend_revision BIGINT NOT NULL, circuit_generation BIGINT NOT NULL, reported_outcome_at TIMESTAMPTZ NOT NULL, event_at TIMESTAMPTZ NOT NULL, first_processed_at TIMESTAMPTZ NOT NULL, applied BOOLEAN NOT NULL, retain_until TIMESTAMPTZ NOT NULL);
CREATE TABLE backend_circuit_failures (attempt_id UUID PRIMARY KEY REFERENCES backend_circuit_failure_receipts(attempt_id) ON DELETE CASCADE, backend_id BIGINT NOT NULL, backend_revision BIGINT NOT NULL, circuit_generation BIGINT NOT NULL, event_at TIMESTAMPTZ NOT NULL);
CREATE INDEX circuit_failures_window_idx ON backend_circuit_failures(backend_id,backend_revision,circuit_generation,event_at);
CREATE TABLE backend_circuit_probes (permit_id UUID PRIMARY KEY, acquisition_started_at TIMESTAMPTZ NOT NULL, input_fingerprint BYTEA NOT NULL, backend_id BIGINT NOT NULL, backend_revision BIGINT NOT NULL, circuit_generation BIGINT NOT NULL, replica_id UUID NOT NULL, expires_at TIMESTAMPTZ NOT NULL, reported_outcome_at TIMESTAMPTZ, event_at TIMESTAMPTZ, processed_at TIMESTAMPTZ, outcome TEXT CHECK(outcome IN ('success','failure','neutral','expired','superseded')), retain_until TIMESTAMPTZ);
CREATE INDEX circuit_probes_identity_idx ON backend_circuit_probes(backend_id,backend_revision,circuit_generation);
CREATE TABLE coordination_replicas (replica_id UUID PRIMARY KEY, started_at TIMESTAMPTZ NOT NULL, heartbeat_at TIMESTAMPTZ NOT NULL, binary_version TEXT NOT NULL, coordination_contract_version INTEGER NOT NULL, policy_fingerprint BYTEA NOT NULL);
