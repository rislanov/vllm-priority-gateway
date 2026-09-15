# PostgreSQL Production Persistence and Distributed Coordination Design

**Date:** 2026-09-14

**Status:** Revised design — review requested

**Source requirements:** `docs/technical-specification.md`, especially sections 38-40, 45-47, 55-57, 60, and Stage 6

## 1. Purpose

Add a PostgreSQL 16+ production profile that makes gateway configuration, analytics, admission limits, rate limits, and circuit-breaker state correct across multiple gateway replicas. Preserve the existing SQLite profile as the default single-replica deployment mode.

PostgreSQL is both the durable configuration/analytics store and the distributed coordination store. Redis is not implemented by this feature. Coordination contracts and backend-neutral contract tests must allow a Redis implementation to be added without changing gateway policy or request handling.

## 2. Goals

- Preserve the current SQLite deployment and behavior by default.
- Add an explicitly selected PostgreSQL profile for two or more gateway replicas.
- Store configuration and usage analytics in PostgreSQL.
- Propagate configuration changes through `LISTEN/NOTIFY` with revision polling as the recovery path.
- Enforce client concurrency and pool gateway-inflight limits across all replicas.
- Enforce client-level requests-per-minute and soft tokens-per-minute limits across all replicas.
- Coordinate circuit open, half-open probe capacity, and recovery across all replicas.
- Preserve the current circuit outcome precedence and generation-safety rules.
- Continue admitted inference streams during a PostgreSQL outage while applying a conservative policy to new requests.
- Expose enough telemetry to distinguish database, configuration, admission, rate-limit, and circuit-coordination failures.
- Provide backend-neutral contract tests for the local, PostgreSQL, and future Redis coordinators.
- Provide opt-in tests against a real PostgreSQL instance without making PostgreSQL or Docker mandatory for the default CI suite.

## 3. Non-goals

- Implementing a Redis or Valkey backend.
- Importing configuration or analytics history from SQLite into PostgreSQL.
- Mixing SQLite configuration with PostgreSQL coordination, or PostgreSQL configuration with local coordination.
- Client-model-specific policy overrides.
- Strict pre-request token reservation or request-body tokenization.
- Persistent on-disk last-known-good configuration outside PostgreSQL.
- Automatic PostgreSQL provisioning, failover, backup, or replica management.
- PostgreSQL table partition automation for analytics.
- Administrative deletion of clients, pools, or backends. Production V1 uses disable and backend drain/resume; tombstone columns and cascading relationships are defensive schema preparation, not a supported delete lifecycle.
- OIDC, RBAC, audit logging, billing, daily/monthly budgets, or Kubernetes discovery.
- Cancelling admitted inference when a distributed lease cannot be renewed.

## 4. Deployment Profiles

The gateway supports exactly two complete profiles:

```text
sqlite:
  SQLite configuration and analytics
  local admission, rate, and circuit coordination
  one gateway replica

postgres:
  PostgreSQL configuration and analytics
  PostgreSQL admission, rate, and circuit coordination
  multiple gateway replicas
```

`LLMGW_DATABASE_DRIVER` accepts `sqlite` or `postgres` and defaults to `sqlite`. The PostgreSQL profile requires `LLMGW_DATABASE_URL`. `LLMGW_DATABASE_PATH` retains its existing SQLite meaning.

The gateway rejects incomplete and mixed configurations during startup. Selecting PostgreSQL without a URL is an error. Supplying PostgreSQL-only settings while the SQLite profile is selected is also an error so that operator mistakes are visible.

## 5. Architecture

### 5.1 Persistence boundaries

`internal/store` becomes the owner of backend-neutral persistence interfaces and the existing command/query parameter types. The current `*store.SQLite` continues implementing these interfaces. The PostgreSQL implementation lives in `internal/store/postgres` and uses native PostgreSQL SQL rather than a shared SQL dialect.

The composed persistence dependencies are:

```go
type ConfigurationStore interface {
	registry.Loader
	AdminStore
	KeyUsageStore
}

type AnalyticsStore interface {
	analytics.RecordStore
	analytics.QueryStore
}

type LifecycleStore interface {
	Close() error
}
```

The exact CRUD methods continue to use the existing `store.CreateClientParams`, `store.UpdateClientParams`, `store.CreatePoolParams`, and related types. Moving these types into HTTP or PostgreSQL packages is prohibited.

The PostgreSQL profile opens three independently bounded runtime pools against the same database:

- configuration/admin;
- analytics;
- latency-sensitive coordination.

This isolation prevents a large analytics query, CSV export, or retention cleanup from consuming connections needed for admission.

### 5.2 Coordination boundaries

`internal/coordination` owns backend-neutral types and interfaces. Implementations live in `internal/coordination/local` and `internal/coordination/postgres`. Reusable behavioral tests live in `internal/coordination/contracttest`.

The admission contract is:

```go
type AdmissionCoordinator interface {
	Acquire(context.Context, AdmissionRequest) (AdmissionDecision, error)
	Renew(context.Context, []LeaseIdentity) ([]RenewResult, error)
	Complete(context.Context, []LeaseCompletion) ([]CompleteResult, error)
	Status() Status
}
```

Before its first coordinator call, the gateway generates one random lease UUID and records one operation-start timestamp. `AdmissionRequest` contains both stable values, the request and replica identifiers, immutable configuration revision, authenticated API-key ID, client and pool IDs, the priority-adjusted effective client concurrency limit, pool gateway-inflight limit, client RPM/TPM policy, and lease TTL. An automatic retry after an ambiguous database result reuses the UUID, timestamp, and identical inputs. Re-evaluating an authoritative rejection is a new logical admission operation and uses a new UUID.

The local coordinator uses the supplied operation timestamp for deterministic tests. The PostgreSQL coordinator uses it only to reject replay outside the documented idempotency window; PostgreSQL server time remains authoritative for expiry, cooldown, rolling-window, and refill decisions. `AdmissionDecision` contains either a lease identity or one bounded rejection reason with an optional retry time.

The circuit contract is:

```go
type CircuitCoordinator interface {
	Reconcile(context.Context, []BackendIdentity) error
	Snapshot(backendID int64, at time.Time) CircuitSnapshot
	Acquire(context.Context, CircuitAcquireRequest) (CircuitDecision, error)
	Complete(context.Context, CircuitCompletion) (CircuitSnapshot, error)
	RenewProbes(context.Context, []ProbeIdentity) ([]RenewResult, error)
	Refresh(context.Context) error
	Status() Status
}
```

`CircuitAcquireRequest` carries a gateway-generated stable attempt UUID, one acquisition-start timestamp, replica identity, requested backend identity, and probe TTL. Automatic acquisition retries preserve these inputs. `CircuitCompletion` carries that UUID, the acquired backend identity and circuit generation, the immutable outcome, and the outcome timestamp captured once when the upstream attempt completes. Retries and outage replay must preserve all of those fields byte-for-byte.

The public contracts use typed reasons rather than backend error strings. The required reasons are `concurrency_exhausted`, `rpm_exhausted`, `tpm_exhausted`, `stale_configuration`, `stale_backend`, `circuit_open`, `probe_capacity_exhausted`, `coordination_unavailable`, `lease_lost`, `stale_operation`, and `idempotency_conflict`.

The SQLite/local profile has an explicit bounded-retention exception to shared idempotency semantics. Each process retains at most 65,536 admission receipts and 65,536 circuit attempts, nominally for 24 hours. At capacity, it evicts the oldest terminal record before admitting new work; it never evicts an active owner. Replaying an evicted UUID within the nominal window may therefore be evaluated as a new operation. Retention is best-effort under memory pressure and does not survive process restart. The local backend shares lease, rate, circuit, and retained-receipt semantics, but does not promise the PostgreSQL backend's durable, strict 24-hour replay guarantee. Local tests must cover this exception explicitly; strict receipt-retention guarantees in sections 11–14 apply to PostgreSQL.

### 5.3 Runtime ownership

`gateway.Service` stops depending on concrete `*admission.Limiter`. It asks the admission coordinator for one atomic request lease after the pool runtime state and effective client limit are known.

`monitor.Manager` continues owning health checks, metrics freshness, pressure, pool hysteresis, and per-replica upstream inflight. It delegates circuit state and probe acquisition to `CircuitCoordinator`. Distributed pool gateway-inflight is supplied by the admission coordinator and overlaid into pool runtime snapshots.

The existing `circuitbreaker.Breaker` remains the reference implementation for the local coordinator. Its semantics are not duplicated in gateway handlers.

## 6. Configuration Model Changes

Client policy adds:

```text
RequestsPerMinute BIGINT >= 0
TokensPerMinute   BIGINT >= 0
```

Zero disables the corresponding limit. Limits are client-wide; this feature does not add per-model overrides.

Clients, model pools, and backends gain monotonically increasing row revisions. A row revision changes only when that row is updated. Backend revision is the circuit identity and changes when any route-affecting backend field changes. The global configuration revision continues changing for every policy mutation.

Admin JSON and HTML forms expose the two client rate limits. Validation rejects negative values and values above the documented safe numeric maxima. The OpenAI-compatible public API remains unchanged.

The shared domain validation applies these explicit implementation limits before any persistence work:

```text
MaxClientConcurrency       = 10_000
MaxPoolGatewayInflight     = 100_000
MaxRequestsPerMinute       = 10_000_000
MaxTokensPerMinute         = 10_000_000_000_000
```

Concurrency values remain Go `int` values and the selected bounds fit a signed 32-bit integer. Rate values use Go `int64` and fit PostgreSQL `BIGINT`. Strict validation applies to every newly created policy and every changed limit before persistence, scope creation, migration backfill, or any loop whose work is proportional to a configured value. New PostgreSQL tables enforce all four upper bounds with hard `CHECK` constraints. The new SQLite rate columns also use hard upper-bound constraints because no legacy rows contain those fields.

Existing SQLite databases receive a compatibility exception for only the two already-shipped concurrency fields. A stored `MaxConcurrency > 10_000` or `MaxGatewayInflight > 100_000` remains loadable and keeps its existing local-runtime behavior. Create rejects such a value. Update may preserve the exact grandfathered value while changing unrelated fields, or lower it to a supported value; it may not increase it or change it to a different above-maximum value. Once lowered into range, it cannot be raised above the maximum again. Shared domain mutation validation therefore accepts an optional previous persisted value; snapshot decoding does not apply new-write maxima to grandfathered rows.

The SQLite migration neither clamps nor rejects grandfathered data and does not rebuild these tables with an incompatible hard upper-bound `CHECK`. Instead it adds database triggers that reject above-maximum inserts and reject an update when the new concurrency value is above the maximum and differs from the old value. Admin JSON applies the same change-aware rule. The Admin form renders an explicit legacy-value warning and permits an unchanged submission; its ordinary HTML `max` constraint applies once the value is within the supported range.

## 7. PostgreSQL Driver and Pools

The PostgreSQL implementation uses `github.com/jackc/pgx/v5` and `pgxpool`. Runtime repositories use native pgx APIs. The migration adapter may use the pgx `database/sql` compatibility layer required by the selected migration driver.

The PostgreSQL settings are:

```text
LLMGW_DATABASE_URL
LLMGW_DATABASE_MIGRATION_URL
LLMGW_POSTGRES_CONFIG_MAX_CONNS=4
LLMGW_POSTGRES_ANALYTICS_MAX_CONNS=4
LLMGW_POSTGRES_COORDINATION_MAX_CONNS=16
LLMGW_CONFIG_POLL_INTERVAL=5s
LLMGW_COORDINATION_TIMEOUT=50ms
```

`LLMGW_DATABASE_MIGRATION_URL` is optional and defaults to `LLMGW_DATABASE_URL`. It exists so runtime traffic may use a transaction-pooled endpoint while migrations use a direct PostgreSQL session. A dedicated `LISTEN` connection prefers the migration URL when supplied; if a session connection cannot be maintained, revision polling remains authoritative.

All three runtime pools explicitly use `pgx.QueryExecModeExec`, including direct connections. The initial release does not use named prepared statements, explicit SQL `PREPARE`, or per-query execution-mode overrides. An incompatible `default_query_exec_mode` in the runtime URL is rejected during startup rather than silently changing this contract. Runtime SQL supplies explicit casts where parameter type inference needs them. This keeps transaction pooling compatible even when the pooler does not track prepared statements; a direct migration connection alone would not provide that compatibility. Pooler validation must exercise server-connection reassignment between transactions, not only a successful ping. See the [pgx PgBouncer compatibility contract](https://pkg.go.dev/github.com/jackc/pgx/v5#hdr-PgBouncer).

URLs are parsed and validated but never emitted in logs, metrics, Admin JSON, or error bodies. Production documentation recommends TLS with certificate verification and a read-write primary endpoint.

### 7.1 PostgreSQL durability and failover contract

Correct distributed coordination requires one writable database history that preserves every acknowledged configuration and coordination commit across an automatic failover. The deployment must fence the former primary before another primary accepts writes, and may promote only a standby that contains every acknowledged commit. A read-write endpoint by itself is not evidence of either property.

Configuration and coordination transactions explicitly use `SET LOCAL synchronous_commit = on`. For PostgreSQL streaming-replication HA, operators must also configure synchronous standbys and a failover policy that promotes a sufficiently durable standby, without automatically falling back to asynchronous commits when synchronous replicas are unavailable. `synchronous_commit = on` without synchronous standbys guarantees local durability only. Equivalent managed-database guarantees are acceptable when documented and validated. Provisioning and controlling failover remain operator responsibilities; the gateway cannot certify fencing or promotion safety from a connection check. The [PostgreSQL synchronous replication documentation](https://www.postgresql.org/docs/16/warm-standby.html#SYNCHRONOUS-REPLICATION) defines the underlying commit guarantees.

Automatic promotion with possible loss of acknowledged commits and restoring an older backup are outside the normal outage-recovery contract. Before making such a database writable to gateways, operators must stop ingress and all gateway replicas, fence the previous database history, reconcile configuration against acknowledged changes including key revocations, and ensure old upstream work has drained or been terminated. Only then may a new deployment register and serve. The ordinary recovery sequence in section 16 must not be used to assert that lost leases, receipts, rate debits, or configuration have been recovered. The runbook must distinguish this disaster-recovery procedure from a lossless failover.

## 8. Schema Migrations

PostgreSQL migrations use `github.com/golang-migrate/migrate/v4` with its pgx v5 database driver and `io/fs` source. Files are embedded into the gateway binary:

```text
internal/store/postgres/migrations/
  000001_configuration.up.sql
  000001_configuration.down.sql
  000002_analytics.up.sql
  000002_analytics.down.sql
  000003_coordination.up.sql
  000003_coordination.down.sql
```

The active circuit-failure history and its reconciliation index are part of `000003_coordination` before the initial PostgreSQL profile release. There is no supported mixed deployment in which an older migration-3 circuit writer runs against this schema; coordination contract version 2 rejects such a replica, and migration files become immutable only after this release boundary.

Startup applies `Up`; `migrate.ErrNoChange` is success. A dirty migration, unsupported database version, migration lock timeout, or schema newer than the binary prevents readiness and startup completion. Production startup never runs `Down` automatically.

The PostgreSQL driver advisory lock prevents concurrent gateway replicas from applying migrations simultaneously. Migrations must use the direct migration URL when the runtime URL passes through a transaction-mode pooler.

Migration files are immutable after merge. The default CI validates unique ordered versions, matched up/down files, and embedded manifest completeness. PostgreSQL itself stores migration version and dirty state; this design does not add a second custom migration-version table.

SQLite retains the current embedded migration runner and `PRAGMA user_version`. A SQLite migration adds rate-policy fields, row revisions, and the compatibility triggers described in section 6 while preserving existing data and defaults. Before commit it verifies that the trigger definitions were installed; it does not treat grandfathered concurrency values as corruption. Existing SQLite databases do not adopt the PostgreSQL migration mechanism.

## 9. PostgreSQL Configuration Store

PostgreSQL tables mirror the SQLite logical model and use native types:

```text
BIGINT GENERATED BY DEFAULT AS IDENTITY
BOOLEAN
BYTEA
TIMESTAMPTZ
DOUBLE PRECISION
```

Every Admin configuration mutation executes in one transaction:

1. validate references and uniqueness;
2. change the target rows and their row revisions;
3. increment `config_meta.revision` exactly once;
4. execute `pg_notify('llmgw_config_changed', revision_text)`;
5. commit;
6. reload the local immutable registry and reconcile monitors.

A mutation that changes or deletes a client or pool first locks the affected admission scope rows in the same global order used by admission: all pool IDs ascending, then all client IDs ascending. The mutation changes policy and the global revision only after those locks are held. This makes admission linearize either entirely before or entirely after a concurrent limit reduction.

PostgreSQL delivers the notification after commit. Every gateway replica also polls `config_meta.revision` at `LLMGW_CONFIG_POLL_INTERVAL` with jitter. Reconnect immediately checks the revision before resuming the wait loop.

`LoadSnapshot` uses one read-only `REPEATABLE READ` transaction. It reads the global revision and every configuration collection, validates the assembled data, and publishes it only when the entire snapshot is valid and newer than the current registry revision.

The first snapshot is mandatory for startup. After successful startup, PostgreSQL failure retains the in-memory last-known-good snapshot. A process starting while PostgreSQL is unavailable fails startup rather than serving from an unverified local disk copy.

An admission request passes its snapshot's global revision and authenticated API-key ID to PostgreSQL. The acquisition function compares the revision with the current database revision. A mismatch returns `stale_configuration`; the gateway synchronously reloads and restarts the entire authentication, authorization, policy-resolution, and admission pipeline once. It must not reuse the previously authenticated client or key. This closes the short stale-snapshot window for API-key revocation and policy reductions.

## 10. PostgreSQL Analytics Store

The PostgreSQL analytics repository preserves all current recorder and query contracts:

- exactly one logical row per accepted request ID;
- nullable usage with the existing validity rules;
- the same UTC half-open time ranges and filters;
- summary, series, breakdown, pagination, and CSV parity;
- bounded batch insert and retention deletion;
- no prompt, response, authorization, session identifier, or upstream secret storage.

The initial PostgreSQL release uses one `usage_requests` table with B-tree indexes matching the current time, client/time, and model/time access patterns. Retention cleanup deletes bounded batches so it does not create long transactions. Automatic partition creation and partition rotation are out of scope.

Analytics uses its own pgx pool. Analytics failure does not rewrite an inference response or consume coordination connections.

## 11. Distributed Admission Leases

PostgreSQL records every active request in one lease ledger, including requests admitted while a pool limit is unlimited. Small persistent scope rows provide the serialization point for admission decisions without turning a limit into a number of pre-created database rows.

```text
client_admission_scopes
  client_id      BIGINT PRIMARY KEY
  updated_at     TIMESTAMPTZ

pool_admission_scopes
  pool_id        BIGINT PRIMARY KEY
  updated_at     TIMESTAMPTZ

admission_operations
  lease_id               UUID PRIMARY KEY
  operation_started_at   TIMESTAMPTZ NOT NULL
  input_fingerprint      BYTEA NOT NULL
  request_id             TEXT NOT NULL
  replica_id             UUID NOT NULL
  client_id              BIGINT NOT NULL
  pool_id                BIGINT NOT NULL
  decision               pending | admitted | rejected
  rejection_reason       TEXT NULL
  retry_at               TIMESTAMPTZ NULL
  decided_at             TIMESTAMPTZ NULL
  lease_expired_at       TIMESTAMPTZ NULL
  completed_at           TIMESTAMPTZ NULL
  completion_result      released | lease_lost | NULL
  retain_until           TIMESTAMPTZ NULL

request_leases
  lease_id       UUID PRIMARY KEY REFERENCES admission_operations(lease_id)
  request_id     TEXT NOT NULL
  replica_id     UUID NOT NULL
  client_id      BIGINT NOT NULL
  pool_id        BIGINT NOT NULL
  acquired_at    TIMESTAMPTZ NOT NULL
  expires_at     TIMESTAMPTZ NOT NULL
```

`request_leases` has non-unique indexes on `client_id` and `pool_id`; `expires_at` is deliberately not indexed so renewal remains eligible for HOT updates. Client-supplied or repeated request IDs are diagnostic values and are not uniqueness keys.

`admission_operations` is the durable idempotency receipt; the active ledger is not used as an operation history. Its fingerprint covers every immutable admission input, including request/replica identity, configuration revision, API-key/client/pool identity, resolved limits and rate policy, operation timestamp, and TTL. It contains no API-key secret or request body. Reuse of a lease UUID with a different fingerprint returns `idempotency_conflict` and performs no rate or lease mutation.

Acquire receipts and completion idempotency share this row. `pending` is an internal, uncommitted claim state; the function must change it to `admitted` or `rejected` before commit. Active receipts are never age-deleted. `terminal_at` means the latest applicable non-null timestamp among rejection `decided_at`, `lease_expired_at`, and `completed_at`; a late completion after expiry therefore extends retention. For a rejected, completed, or expired operation, PostgreSQL sets:

```text
retain_until = max(terminal_at, operation_started_at) + 24h
```

Cleanup may delete a terminal receipt only when `retain_until <= v_now`. An unseen operation is accepted as new only when `operation_started_at > v_now - 24h` and `operation_started_at <= v_now + 5m`; both comparisons use a post-lock PostgreSQL timestamp. At the exact instant a future-skewed receipt becomes eligible for deletion, the strict lower bound therefore makes the same operation stale. Removal of an old receipt cannot turn its UUID into a fresh admission. These windows and boundary operators are part of the coordination contract and replica fingerprint.

Creating a client or pool creates its corresponding scope row. A client or pool limit update locks that scope row before committing the new policy and global configuration revision. Reducing a limit does not cancel existing owners. It immediately prevents new acquisition while the total active count is at or above the new limit. Production V1 does not expose administrative deletion; disable and drain preserve the scope identity and retained receipts. IDs are never reused.

Every request lease always records both client and pool identity. Pool limit zero disables only the rejection check; it does not disable lease creation, renewal, distributed pool-inflight accounting, or later transition to a positive limit.

`try_acquire_admission` executes atomically in one PostgreSQL transaction. Before any capacity check or RPM debit, it resolves idempotency as follows:

1. look up `admission_operations` by the stable lease UUID;
2. if no row exists, insert a `pending` row with `ON CONFLICT DO NOTHING`; a concurrent loser waits for the winner and restarts receipt lookup;
3. verify that an existing row's input fingerprint matches;
4. return a recorded rejection unchanged;
5. return `lease_lost` for a completed or expired admission without re-running it;
6. for an admitted receipt that may still be active, lock its scopes, matching lease row when present, and receipt in the global order; then capture `clock_timestamp()` and return the same lease identity only if that row is still unexpired; otherwise mark the receipt expired and return `lease_lost`;
7. only the transaction that created the `pending` receipt continues to a new admission decision.

The new-operation path then:

1. locks the requested pool scope row and then the requested client scope row;
2. re-reads and checks the global configuration revision, API key, client, pool, enabled policy, and configured limits while those locks are held;
3. locks the applicable client TPM and RPM state rows in fixed rate-kind order, creating missing rows while the client scope is held;
4. after every correctness-relevant row lock has been acquired, assigns `v_now := clock_timestamp()` exactly once;
5. validates the operation-start replay window against `v_now`;
6. counts leases with `expires_at > v_now` for the pool, regardless of its previous or current limit;
7. counts leases with `expires_at > v_now` for the client, including leases admitted under a larger administrative or priority-adjusted limit;
8. rejects when a positive pool limit is not greater than total pool inflight;
9. rejects when the effective client limit is not greater than total client inflight;
10. normalizes and checks the soft TPM balance at `v_now`;
11. normalizes and debits one RPM token at `v_now`;
12. inserts exactly one request lease containing both client and pool identity, changes the receipt to `admitted`, and returns that lease;
13. for an authoritative rejection, records `rejected`, the bounded reason, and optional retry time in the receipt and commits without an RPM debit or request lease.

The supplied effective client limit must be between zero and the current configured client maximum read in step 2. A global revision mismatch returns `stale_configuration`; the gateway reloads and recomputes the effective limit from fresh policy and pool state rather than scaling a stale value inside SQL.

All acquisition, renewal, policy-change, completion, and expiry-cleanup paths lock scopes in the global order `all pool IDs ascending, then all client IDs ascending`. A single-request acquisition therefore locks its pool before its client. After scopes, existing request-lease rows are locked by lease UUID, existing admission-operation rows by lease UUID, and rate rows by fixed rate-kind order. A newly inserted, still-uncommitted `pending` receipt is the only exception: no other transaction can yet resolve it to a lease or scope, so holding it before scope acquisition cannot create the reverse edge of a deadlock. Scope locking serializes every membership-changing operation that could affect the two counts. A database error rolls back the pending receipt and every lease/rate mutation; a policy rejection commits only its durable receipt and any explicitly normalized rate timestamp, never an RPM debit or request lease.

Every PostgreSQL coordination operation that evaluates TTL, cooldown, rolling-window age, or token refill follows one time rule: acquire all rows on which the decision may block, then capture one `clock_timestamp()`, then make every time-dependent comparison and update from that value without acquiring another correctness-relevant lock. `now()`, `CURRENT_TIMESTAMP`, `statement_timestamp()`, and `transaction_timestamp()` are prohibited for these decisions because they can precede lock wait time.

The atomic invariant is:

```text
admit only if active_pool_leases < positive_pool_limit (when enabled)
          and active_client_leases < effective_client_limit
```

Counts include leases created under an older, larger limit. The implementation must not infer inflight from slot ordinals or configured capacity.

Lease identity includes the lease UUID, client ID, pool ID, request ID, replica ID, and expiry. Batch renewal locks all referenced pool scopes ascending, then client scopes ascending, then matching lease rows and operation receipts by UUID. Only after the final lock does it capture `v_now := clock_timestamp()`. It extends only a matching lease with `expires_at > v_now`; otherwise it marks the operation receipt expired and returns `lease_lost`. It never reuses the transaction-start timestamp and never resurrects an expired lease.

Lease TTL and renewal settings are:

```text
LLMGW_LEASE_TTL=90s
LLMGW_LEASE_RENEW_INTERVAL=30s
```

The renewal interval must be positive and no greater than one third of the TTL. PostgreSQL server time is authoritative for expiry. A crashed gateway stops renewing and its lease stops contributing to active counts after TTL.

`request_leases` uses a reduced fillfactor and aggressive per-table autovacuum settings because expiry is updated frequently. A bounded cleanup worker locks the affected scopes, lease rows, and operation receipts in the same order, captures a fresh post-lock `clock_timestamp()`, marks corresponding receipts expired, and deletes expired ledger rows in batches. Correctness never depends on physical deletion because counts and renewals always test expiry. A separate bounded cleanup deletes terminal receipts only after `retain_until`. Distributed pool/client inflight snapshots use a batched `GROUP BY` over unexpired ledger rows and are cached no longer than the metrics interval.

## 12. Lease Manager and Completion

Each gateway process runs one central `LeaseManager`. It owns active distributed lease handles, schedules batch renewal, and records completion without creating one goroutine or one database connection per request.

Completion contains optional actual usage. A PostgreSQL completion function:

1. reads the admission receipt to resolve the trusted pool and client identity, then locks those scope rows in the global order;
2. locks the matching request-lease row when present, then locks and re-checks the receipt, then re-reads the current client policy and locks its TPM state row when TPM is enabled;
3. after the final row lock, assigns `v_now := clock_timestamp()` exactly once;
4. if `completed_at` is already set, returns the stored completion result without another refill, debit, or delete;
5. normalizes the TPM row to the current client policy generation, applies refill capped at current capacity through `v_now`, then debits `input_tokens + output_tokens` and writes `last_refill_at = v_now`;
6. deletes the request lease only when its lease UUID, client ID, and pool ID match;
7. records `completed_at`, `completion_result`, and `retain_until` in the admission receipt;
8. returns `released` when the matching lease was unexpired at `v_now`, `lease_lost` when it was expired or absent, or `already_completed` with the previously stored result for a duplicate completion.

Actual usage is debited once even when the request lease already expired or bounded cleanup removed it: the upstream work still consumed tokens. The durable receipt, rather than caller-supplied scope fields, supplies the trusted client and pool identity. The conditional delete prevents a stale completion from removing any other request. Receipt update, rate normalization/debit, and lease deletion commit or roll back together.

A completion with no retained receipt returns `stale_operation`; a receipt for an admission that was rejected returns `lease_lost`. Neither case mutates a rate row. A late completion for an expired admitted receipt remains valid until its calculated `retain_until` and performs the one actual-usage debit described above. If the current client no longer exists or its TPM limit is zero, completion skips the TPM row while still recording completion and releasing any matching lease.

`cache_read_tokens` is a subset of input tokens and is not added again. Missing or invalid usage releases the lease without a token debit and increments a bounded-cardinality unmetered-completion metric.

Completion delivery is asynchronous and idempotent. The manager retries transient failures with bounded exponential backoff. The pending completion backlog defaults to 4096:

```text
LLMGW_COORDINATION_COMPLETION_BACKLOG=4096
```

When the backlog is full, the response remains successful, lease cleanup falls back to TTL, the soft TPM debit may be lost, and a dedicated failure counter increments. This is permitted only because TPM was explicitly selected as a soft limit. Terminal admission receipts remain until `max(terminal_at, operation_started_at) + 24h`, longer than the manager's retry horizon, and are deleted in bounded batches.

An active stream is never cancelled solely because renewal failed. The manager continues retrying. If the lease expires and another request uses the newly available capacity, a later renewal returns `lease_lost`; the original stream continues and telemetry records temporary overcommit risk.

## 13. Distributed Rate Limits

Client policy supplies two independent limits:

```text
RequestsPerMinute
TokensPerMinute
```

Both use rows in `coordination_rate_state` keyed by client and rate kind. State stores client policy revision, current balance, and `last_refill_at`. A policy-revision change starts a new bucket generation with the new full capacity at the operation's post-lock `v_now`.

RPM is a distributed token bucket. Capacity equals `RequestsPerMinute`, refill is `limit / 60 seconds`, and every successful admission costs one token. The acquisition function serializes changes to one client's RPM row and returns a calculated `retry_at` when exhausted. A rejected concurrency, pool, TPM, or RPM decision may commit its idempotency receipt, but it never debits an RPM token.

TPM is intentionally soft. Admission normalizes the current policy generation, refills to at most capacity through its post-lock `v_now`, and requires at least one available token, but does not reserve an estimated request cost. Successful completion performs the same generation normalization and capped refill before debiting actual `input_tokens + output_tokens`; balance may become negative. Formally, for an unchanged generation:

```text
refilled = min(capacity, balance + max(0, v_now - last_refill_at) * capacity / 60s)
balance = refilled - actual_usage
last_refill_at = v_now
```

For example, a full bucket of 1,000 tokens at `t=0` completed with usage 2,000 at `t=60s` first refills/caps to 1,000 and then becomes `-1,000`; later admission cannot recover an extra full bucket by applying the same elapsed interval again. Further requests are rejected until a later refill makes the balance positive. Requests already in flight may overshoot the configured TPM, and the maximum overshoot depends on their concurrency and actual response sizes.

The gateway does not tokenize request bodies, derive billing usage, or reserve `max_tokens`. Usage missing from a completed upstream response is observable but not charged.

Rate-limit rejection returns HTTP 429 with an OpenAI-shaped `rate_limit_exceeded` error, a bounded reason distinguishing RPM from TPM, and `Retry-After`. Coordination unavailability remains HTTP 503 rather than masquerading as quota exhaustion.

## 14. Distributed Circuit Breaker

PostgreSQL owns the distributed inference-circuit state. Health-check failure, metrics staleness, pressure, and local upstream inflight remain process-local and independent.

```text
backend_circuit_state
  backend_id             BIGINT PRIMARY KEY
  backend_revision       BIGINT
  state                  closed | open | half_open
  generation             BIGINT
  opened_at              TIMESTAMPTZ NULL
  half_open_succeeded    BOOLEAN
  updated_at             TIMESTAMPTZ

backend_circuit_failure_receipts
  attempt_id             UUID PRIMARY KEY
  backend_id             BIGINT
  backend_revision       BIGINT
  circuit_generation     BIGINT
  reported_outcome_at    TIMESTAMPTZ
  event_at               TIMESTAMPTZ
  first_processed_at     TIMESTAMPTZ
  applied                BOOLEAN
  retain_until           TIMESTAMPTZ

backend_circuit_failures
  attempt_id             UUID PRIMARY KEY REFERENCES backend_circuit_failure_receipts(attempt_id) ON DELETE CASCADE
  backend_id             BIGINT
  backend_revision       BIGINT
  circuit_generation     BIGINT
  event_at               TIMESTAMPTZ

backend_circuit_active_failures
  attempt_id             UUID PRIMARY KEY
  backend_id             BIGINT
  backend_revision       BIGINT
  event_at               TIMESTAMPTZ

backend_circuit_probes
  permit_id              UUID PRIMARY KEY -- exactly the acquiring attempt UUID
  acquisition_started_at TIMESTAMPTZ NOT NULL
  input_fingerprint      BYTEA NOT NULL
  backend_id             BIGINT
  backend_revision       BIGINT
  circuit_generation     BIGINT
  replica_id             UUID
  expires_at             TIMESTAMPTZ
  reported_outcome_at    TIMESTAMPTZ NULL
  event_at               TIMESTAMPTZ NULL
  processed_at           TIMESTAMPTZ NULL
  outcome                success | failure | neutral | expired | superseded | NULL
  retain_until           TIMESTAMPTZ NULL
```

### 14.1 Acquire

Before circuit acquisition, the gateway generates a stable attempt UUID and captures `acquisition_started_at` once. A probe's `permit_id` is that attempt UUID; its fingerprint covers the requested backend identity, replica identity, acquisition-start timestamp, and probe TTL. The completion closure retains that UUID and, on its process-local `sync.Once`, captures one immutable `reported_outcome_at`. Retry and PostgreSQL-outage replay send the exact same attempt UUID, outcome, and reported timestamp.

Before either checking probe capacity or taking the closed-state fast path, `acquire_circuit` looks up a retained permit by attempt UUID. Different immutable acquisition inputs return `idempotency_conflict`. For a matching permit it locks the backend state and relevant permits in the circuit lock order, captures a post-lock timestamp, and reconciles expiry. A still-unfinished, unexpired permit for the current eligible backend revision and half-open generation returns the same permit and current expiry without allocating capacity or extending TTL. A completed, expired, or superseded permit, or a permit from an invalidated identity/generation, returns `lease_lost`; the same attempt must never acquire a replacement permit. A retained-permit retry is resolved before the unseen-operation age gate, so a long-running active probe remains recoverable.

For an unseen attempt, the function verifies that the requested backend revision is current, enabled, and not draining; otherwise it returns `stale_backend`. It accepts `acquisition_started_at` only when `acquisition_started_at > v_now - 24h` and `acquisition_started_at <= v_now + 5m`. A closed-state fast path then reads committed state without taking a write lock or creating a permit. A request racing with a newly opened circuit is treated as already admitted, matching ordinary circuit-breaker semantics. This read-only decision has no durable acquisition receipt; an ambiguous retry may re-evaluate it because the gateway must not start upstream work before receiving a successful acquisition result.

Open-to-half-open transition and half-open probe acquisition lock the backend state row, re-check the attempt UUID and backend eligibility, and resolve existing permits before checking capacity. Concurrent same-attempt calls therefore converge on one permit even when capacity is one. After all relevant locks, the function captures `v_now`, reconciles expiry, checks the unseen-operation age gate and capacity, and inserts at most one permit. A lost response after commit may be retried with identical inputs within the bounded coordination retry budget. An authoritative denial may be re-evaluated as a new attempt with a new UUID and timestamp. New attempts cannot revive retained terminal permits, and receipt cleanup cannot make an old timestamp eligible again.

Half-open capacity is global across replicas. Probe permits have a TTL, are scheduled for renewal by the same process-level lease scheduler through `CircuitCoordinator.RenewProbes`, and are counted only for the matching backend revision and circuit generation. Admission-lease completion and circuit-probe completion remain separate idempotent operations because one client request may make more than one backend attempt.

Every time-dependent circuit mutation locks the backend circuit-state row, affected permit or failure rows, and the applicable failure receipt in deterministic ID order before capturing `v_now := clock_timestamp()`. Probe expiry, renewal eligibility, cooldown, failure-window pruning, and database processing timestamps use that post-lock value. The reported outcome timestamp is never regenerated during retry. Circuit SQL never substitutes a transaction-start timestamp.

### 14.2 Completion

Closed-state success and neutral outcomes rely on the process-local once-only completion handle and do not write PostgreSQL circuit state. A qualifying closed-state failure locks the backend circuit-state row first and resolves `backend_circuit_failure_receipts` by attempt UUID before changing failure history. The state lock serializes first receipt creation for that backend. An existing receipt with identical backend identity, acquire generation, and reported timestamp returns its recorded result without inserting, pruning, or refreshing a failure event. Reuse with different immutable fields returns `idempotency_conflict`. A new failure locks current-generation failure rows by attempt ID before capturing `v_now`, then records:

```text
first_processed_at = post-lock v_now
event_at = min(reported_outcome_at, first_processed_at)
retain_until = max(first_processed_at, reported_outcome_at) + CircuitCompletionRetention
CircuitCompletionRetention = 24h
0 < FailureWindow <= CircuitCompletionRetention - 5m
```

`CircuitCompletionRetention` is a fixed coordination-contract constant, not the configured rolling window or a value recomputed from it. The five-minute gap in the maximum supported `FailureWindow` covers the full accepted positive clock skew, so a receipt cannot become deletable while its event could still enter any supported rolling window. Configuration validation rejects a larger failure window before startup, and the limit is part of the replica fingerprint.

The raw reported timestamp is retained for audit and exact replay; `event_at` is the immutable rolling-window timestamp and clamps only future clock skew to first database processing time. An unseen completion is accepted only when `reported_outcome_at > v_now - CircuitCompletionRetention` and `reported_outcome_at <= v_now + 5m`. Cleanup first removes any rolling event and then deletes a receipt only at `retain_until <= v_now`; the foreign key also cascades defensively. The strict-boundary argument is the same as for admission receipts.

A qualifying closed-state failure inserts `backend_circuit_failures` with the same attempt UUID and immutable `event_at`. `backend_circuit_active_failures` is the authoritative rolling history for the current backend revision and circuit lifecycle: it includes qualifying closed-state failures, half-open failures, and expired probes, so reopening a circuit does not discard still-relevant failures merely because the generation changed. Inserting a failure prunes active events relative to the latest retained event, and closed-state reads additionally count only events within `v_now - FailureWindow`. A successful half-open generation, backend revision change, disabling, or draining clears the active lifecycle. Failure receipts remain independent and are never removed by rolling-window pruning. If the threshold is reached, `opened_at` is the latest retained `event_at`. A delayed event already outside the rolling window records its receipt but does not affect circuit state.

This separates retry deduplication from the rolling window. Deleting failure A from the rolling table cannot make a later redelivery fresh: its receipt still suppresses the duplicate, and even after receipt retention the strict timestamp gate rejects it rather than assigning a new event time.

Half-open completion is idempotent through the durable probe permit. Its first completion stores the stable reported outcome timestamp, immutable `event_at = min(reported_outcome_at, processed_at)`, post-lock processing timestamp, outcome, and `retain_until = max(processed_at, reported_outcome_at, acquisition_started_at) + CircuitCompletionRetention`. A duplicate must match those immutable inputs and returns without another transition; a mismatch returns `idempotency_conflict`. Failure reopens immediately. Success closes only after every already-admitted probe in the generation has completed without failure. Success plus neutral completion closes after the successful generation drains. Neutral-only completion releases probe capacity without closing. Stale-generation outcomes cannot heal or penalize a newer circuit generation.

Every transition that invalidates a circuit generation or backend identity terminalizes all its unfinished permits in the same transaction. This includes an ordinary half-open failure, expiry reopening, backend revision replacement, disabling, and draining. Under the backend state lock and permit locks ordered by UUID, the transition preserves already-terminal outcomes, marks remaining unfinished permits `superseded` (except the triggering failure or expired permit), sets `processed_at = v_now`, and assigns `retain_until = max(v_now, acquisition_started_at) + CircuitCompletionRetention`. No fabricated reported outcome timestamp is stored. Backend mutations lock the affected circuit state and permits before changing identity, and commit invalidation atomically with the configuration change; stale replica reconciliation cannot recreate an older identity. Backend circuit-state rows can act as tombstones while retained permits or failure receipts reference an older revision. Late renewal/acquisition replay returns `lease_lost`, and a completion for a superseded or expired permit is a stale no-op rather than an outcome conflict. A bounded cleanup removes terminal permits only after `retain_until <= v_now`, including permits from old backend revisions and generations. An unfinished permit is never age-deleted. Administrative deletion is out of scope for Production V1; disabling and draining are the supported lifecycle operations.

An expired half-open probe is a conservative circuit failure, not a neutral completion. Expiry reconciliation locks the circuit row, re-checks the permit and generation, selects the earliest expired unfinished permit, marks it `expired`, marks every other unfinished permit from the same generation `superseded`, assigns terminal retention to those permits, transitions the circuit to `open`, clears `half_open_succeeded`, increments generation, and sets `opened_at` to the selected permit's `expires_at`. It emits the ordinary circuit-change notification. This transition applies even when another probe in the generation already succeeded.

Expiry reconciliation runs before every half-open acquire, renewal, and completion and in the periodic circuit refresh worker, so no operation can treat an expired generation as drained successfully. A completion that first discovers its own permit has expired performs the expiry transition and then returns a stale outcome without applying the reported success, failure, or neutral result.

Probe admission resumes only after the new open cooldown measured from that expiry timestamp. If an acquire operation discovers the expiry after this cooldown has already elapsed, it may reconcile the expired generation, advance the now-open circuit to a fresh half-open generation, and issue the requesting attempt a new permit while retaining the same circuit-row lock. A background reconciliation only changes state; it never creates a permit without a requesting attempt. An expired permit cannot be renewed or revived. Any later completion from the expired or superseded generation is a stale no-op and cannot modify failure history or the new generation.

This rule intentionally prefers temporary unavailability over closing a circuit while a probe whose ownership was lost may still be executing upstream.

The distributed implementation must pass the same delayed-failure, failure-wins ordering, multi-probe, neutral, cooldown, and stale-callback cases as the current local breaker.

### 14.3 Cache and notifications

Routing and dashboards read a local cache of distributed circuit snapshots. PostgreSQL emits `NOTIFY llmgw_circuit_changed` after state transitions. Every replica also batch-refreshes circuit state at least once per metrics interval and immediately refreshes after its own acquire or completion changes state.

The authoritative acquire still executes before an upstream attempt. A stale cache may cause the router to consider a backend first, but PostgreSQL can reject the permit; the gateway excludes that backend and selects another. Stale cache cannot exceed global half-open capacity.

Changing a route-affecting backend field increments backend revision and creates a new circuit identity. The configuration transaction terminalizes old unfinished permits as required in section 14.2. Old rolling failure rows become inert and are removed by window cleanup. Completion receipts and terminal probe permits remain until their independent `retain_until` so a delayed retry cannot recreate old state.

## 15. Replica Compatibility

Every PostgreSQL-mode gateway generates a random replica UUID and maintains a row in `coordination_replicas`:

```text
replica_id
started_at
heartbeat_at
heartbeat_expires_at
binary_version
coordination_contract_version
policy_fingerprint
```

The fingerprint covers lease TTL/renewal semantics, circuit thresholds and windows, half-open probe capacity, rate algorithm version, and coordination contract version. Each heartbeat persists its own `heartbeat_expires_at`, so another replica never decides liveness using the joining replica's TTL. Registration and heartbeat both use the same serializable activation transaction and retry transient serialization conflicts; either path rejects activation when another non-expired replica has an incompatible fingerprint.

The first implementation requires a full stop/start without overlap when coordination-critical settings change. This restriction prevents mixed circuit and lease semantics. Ordinary configuration and client-policy changes continue using online revision propagation.

Replica heartbeat and lease renewal share the coordination lifecycle but not an open transaction. Expired replica rows are diagnostic; lease and probe TTLs remain the correctness mechanism.

## 16. PostgreSQL Failure Policy

PostgreSQL coordination errors are classified as timeout, unavailable, stale state, serialization/deadlock retry, constraint/corruption, and permanent incompatibility. Cancellation or deadline expiry of an individual caller is returned to that caller and is not, by itself, evidence that PostgreSQL is unavailable. The coordinator's own timeout and database failures, including failures while consuming query result rows, latch degraded state until the complete recovery barrier succeeds. At most one bounded retry is allowed inside the original coordination timeout, shared between retryable transaction conflicts and ambiguous acquisition results. Admission and circuit acquisition retries preserve their original UUIDs, timestamps, and immutable inputs; they never start another upstream attempt to resolve database uncertainty.

After successful startup, loss of PostgreSQL enters degraded mode:

- admitted responses and streams continue;
- the registry continues serving the in-memory last-known-good snapshot;
- Admin mutations return retryable HTTP 503;
- Normal and Background requests return retryable HTTP 503 before upstream work;
- Critical and High requests may use local emergency admission only;
- last-known open circuits remain unavailable;
- last-known half-open circuits do not admit local probes;
- last-known closed circuits may be made unavailable by a local conservative breaker but cannot heal locally;
- bounded local failure events are replayed after recovery with the exact original attempt IDs, outcomes, and reported timestamps captured by their `sync.Once` completion; replay preserves `event_at` semantics and never substitutes recovery time;
- buffered failures older than `CircuitCompletionRetention` are discarded as observably stale because they are already outside the maximum rolling window and cannot affect current circuit state;
- success observed during the outage never closes distributed circuit state.

Emergency limits are process-wide class caps, additionally bounded by the client's effective local concurrency:

```text
LLMGW_COORDINATION_EMERGENCY_CRITICAL_MAX_INFLIGHT=4
LLMGW_COORDINATION_EMERGENCY_HIGH_MAX_INFLIGHT=2
```

Zero disables emergency admission for that class. Because limits are per replica during the outage, deployment documentation states the aggregate worst case as `replica_count * class_cap`.

Recovery requires a successful database ping, replica-fingerprint check, configuration revision reload, lease reconciliation, local failure replay, and circuit cache refresh. The gateway exits degraded mode only after all mandatory steps succeed. This sequence assumes the database preserved acknowledged commits under section 7.1.

A delayed notification, poll result, or concurrent snapshot load with an older revision is discarded under the ordinary monotonic-publication rule; it is not by itself a database regression. To confirm a suspected regression, capture the highest database revision already observed by this process, then start a fresh independent read of `config_meta.revision` on the writable primary, outside any earlier snapshot transaction. Compare that result with the captured revision, not a higher revision learned while the verification read was in flight. Only a lower revision from this verification is a permanent consistency fault: keep coordination readiness false and reject new inference, including emergency admission, until operator recovery; do not publish the older snapshot or clear the fault after a successful ping. Absence of a confirmed regression does not prove a failover was lossless.

## 17. Readiness and API Errors

PostgreSQL startup does not bind the serving listener until migrations, replica registration, initial snapshot load, and coordinator initialization succeed.

After initialization:

- `/readyz` remains HTTP 200 during a transient PostgreSQL outage and reports `status: degraded` with configuration, coordination, and analytics component states;
- `/inference-readyz` remains HTTP 200 when a healthy eligible backend and enabled emergency capacity can still serve Critical or High traffic;
- `/coordination-readyz` returns HTTP 200 only when PostgreSQL coordination, replica compatibility, config freshness, and circuit refresh are current; otherwise it returns HTTP 503.

Load balancers that carry all priority classes should use `/inference-readyz`. Operators that require strict distributed admission can additionally gate deployments on `/coordination-readyz`.

Concurrency and pool exhaustion retain the bounded OpenAI-shaped HTTP 429 gateway-overloaded response. RPM/TPM exhaustion uses HTTP 429 `rate_limit_exceeded`. PostgreSQL unavailability uses HTTP 503 `gateway_unavailable`. Stale configuration and backend revisions are internal retry reasons and appear to clients only if refresh/retry cannot recover.

## 18. Observability

Add bounded-cardinality Prometheus families for:

- coordination operation count and latency by backend, operation, and outcome;
- PostgreSQL pool acquired, idle, total, and acquisition-failure counts by workload class;
- config notification reconnects and age of last successful revision poll;
- stale configuration and stale backend decisions;
- active distributed leases and local lease handles;
- renew failures, lost leases, and pending completions;
- emergency admissions and rejections by priority class;
- RPM and TPM rejections;
- missing-usage soft-TPM completions;
- circuit cache age, refresh failures, and replay backlog;
- active compatible replicas observed in PostgreSQL.

Existing circuit and pool dashboard fields switch to distributed values in PostgreSQL mode. Structured logs include operation, bounded reason, duration, backend mode, and degraded transitions. DSNs, SQL arguments, request/lease/probe IDs, API keys, prompts, responses, and session identifiers are never metric labels. Sensitive connection information is redacted from errors.

## 19. Testing Strategy

### 19.1 Default CI

The default suite requires no PostgreSQL process and includes:

- every existing SQLite unit, integration, race, vet, and build check;
- SQLite migration tests for rate-policy and revision additions, including pre-existing `MaxConcurrency=10_001` and `MaxGatewayInflight=100_001` rows that remain loadable and unchanged while new/increased above-maximum values are rejected and lowering succeeds;
- local coordinator contract tests for effective/admin limit reduction, unlimited-pool accounting, `0 -> positive` pool transitions, post-lock expiry without resurrection, acquire response loss after commit, duplicate acquire before/after expiry and completion while the receipt is retained, future-skewed nominal retention/cleanup boundaries, bounded terminal-record eviction, completion idempotency, and refill-before-debit TPM completion;
- deterministic circuit-coordinator tests for `success + crashed probe`, `success + expired probe + late failure`, cooldown after expiry, stale-generation no-ops, and `commit -> lost response -> rolling prune -> duplicate completion` with the original event timestamp;
- circuit-acquire contract tests for a lost response with probe capacity one, concurrent identical retries, fingerprint conflicts, retry after completion/expiry/invalidation, and future-skewed acquire timestamps at terminal permit cleanup boundaries;
- circuit permit cleanup tests for `failure + crashed peer`, backend revision replacement, disabling/draining with unfinished probes, stale completion, and eventual removal of superseded permits;
- gateway tests using deterministic fake coordinators;
- emergency-mode and recovery state-machine tests;
- revision-regression confirmation tests: a delayed older snapshot/poll does not trip the consistency fault, while a fresh primary read started after observing a higher revision and returning a lower revision does;
- `LeaseManager` renewal, completion, backlog, cancellation, and shutdown race tests;
- domain and transport boundary tests for every concurrency and rate maximum, including change-aware SQLite grandfathering;
- circuit-cache notification/poll/reconnect tests with fake transports;
- migration manifest tests for unique versions, matched up/down files, and embedded coverage;
- compile-time interface assertions for local and PostgreSQL implementations.

SQL behavior is not claimed as verified by the default suite.

### 19.2 Opt-in PostgreSQL validation

Real PostgreSQL tests require:

```text
LLMGW_POSTGRES_TEST_DSN=postgres://...
make test-postgres
```

`make test-postgres-docker` may provision PostgreSQL 16 for local use, but neither target is called by the default CI configuration.

Transaction-pooler validation is a separate opt-in target, `make test-postgres-pooler`, requiring both `LLMGW_POSTGRES_TEST_DSN` for the direct database and `LLMGW_POSTGRES_POOLER_TEST_DSN` for a transaction-mode PgBouncer connected to that same database with prepared-statement tracking disabled. It must force server-connection reassignment. The ordinary `make test-postgres` does not require PgBouncer and does not claim pooler compatibility coverage.

The opt-in suite covers:

- concurrent migration startup and dirty-state refusal;
- PostgreSQL 16 version enforcement;
- in the pooler target, runtime CRUD, snapshot, analytics, and coordination through transaction-mode PgBouncer with prepared-statement tracking disabled, forcing reassignment between transactions while migrations use a direct connection;
- CRUD, referential integrity, revision increments, and analytics parity;
- LISTEN notification, polling fallback, reconnect, and missed-event recovery;
- aggregate client and pool concurrency across two gateways;
- no over-admission when effective client limits fall below owners acquired under a larger limit;
- unlimited-pool accounting and `0 -> positive` pool-limit transitions with existing inflight;
- administrative client/pool limit reductions with existing inflight;
- conservative no-over-admission under scope-lock contention;
- renewal waiting across lease expiry while a same-client request in another pool acquires the freed capacity;
- acquire response loss after commit followed by the same UUID before expiry, after expiry, and after completion, proving no second RPM debit or lease;
- an acquire timestamp four minutes ahead of PostgreSQL time, terminal receipt cleanup at `max(terminal_at, operation_started_at) + 24h`, and same-UUID replay rejection at the exact boundary;
- long-stream renewal, crash expiry, idempotent completion, and lost-lease detection;
- distributed RPM refill and rejection;
- soft TPM completion normalization/refill/debit ordering, natural overshoot, missing usage, and policy reset;
- distributed circuit rolling window, open, cooldown, half-open capacity, completion ordering, neutral outcomes, and stale generations;
- closed failure commit followed by lost response, event pruning, and duplicate completion, proving the original attempt cannot be inserted with a fresh event timestamp;
- `success + crashed probe` expiry reopening and `success + expired probe + late failure` stale completion;
- half-open acquire `commit -> lost response -> same UUID` at capacity one, proving one permit and no false exhaustion, plus replay after completion, generation change, backend invalidation, and retention cleanup;
- ordinary half-open failure with a crashed peer and backend mutations with unfinished probes, proving atomic superseding, stale replay safety, and bounded terminal cleanup;
- PostgreSQL outage, emergency admission, failure replay, and recovery;
- incompatible replica fingerprint rejection;
- deadlock/serialization behavior under concurrent admission and circuit transitions;
- two in-process gateways using one fake vLLM pool and one PostgreSQL database;
- proxy overhead measurements against the production target of p50 below 5 ms and p99 below 20 ms on the documented reference environment.

Performance results are reported with environment and database topology; they are not treated as portable guarantees.

HA deployments additionally require validation on their actual failover topology: acknowledge a lease/RPM debit and an API-key revocation, fail the primary, and verify the promoted primary retains those records and receipts while the former primary is fenced. Verify that losing synchronous durability blocks commits rather than silently switching to asynchronous writes. A negative recovery test with a regressed configuration revision must keep the gateway fail-closed. A standalone PostgreSQL process and the ordinary outage test do not establish these guarantees. Record the managed-provider guarantee or synchronous replication/promotion settings in acceptance evidence.

## 20. Rollout

This feature does not import SQLite data. A PostgreSQL deployment starts with a new schema and is configured explicitly through the existing Admin API/UI.

Recommended rollout:

1. provision PostgreSQL 16+ and credentials with the durability, synchronous failover, and former-primary fencing guarantees in section 7.1;
2. run migration/status validation with the gateway binary and direct migration URL;
3. start one PostgreSQL-mode gateway replica;
4. configure clients, keys, pools, and backends;
5. run compatibility, streaming, analytics, lease, rate, and circuit smoke tests;
6. start a second compatible gateway replica;
7. run multi-replica concurrency, config-propagation, circuit, outage, pooler (when used), and topology-specific lossless failover tests;
8. place both replicas behind the production load balancer;
9. calibrate coordination timeout, connection pools, emergency caps, and circuit thresholds from observed telemetry.

Rollback to SQLite requires a separately configured SQLite database and returns the deployment to one supported gateway replica. No automatic bidirectional data synchronization exists.

## 21. Documentation Changes

Update the English and Russian READMEs, deployment guide, operations guide, technical specification, acceptance evidence, and real-vLLM runbook to cover:

- profile selection and compatibility matrix;
- PostgreSQL 16+ and pgx requirements;
- migration, direct-connection, runtime query-mode, and transaction-pooler requirements;
- PostgreSQL roles, TLS, backup, synchronous durability, failover fencing, HA ownership, and the separate disaster-recovery procedure for acknowledged-data loss;
- configuration propagation and expected delay;
- distributed lease and circuit semantics;
- RPM and soft TPM behavior;
- outage and emergency-mode behavior;
- readiness endpoint selection;
- opt-in PostgreSQL test commands;
- Redis as an optional future coordination backend rather than a Production V1 requirement.

## 22. Implementation Decomposition

This design is executed through three sequential implementation plans in the same `codex/postgresql` branch:

1. **PostgreSQL persistence foundation** — configuration/analytics interfaces, pgx pools, `golang-migrate`, PostgreSQL repositories, config reload, notifications, client rate fields, and SQLite compatibility.
2. **PostgreSQL admission and rate coordination** — coordination contracts, local backend, PostgreSQL scope locks/lease ledger/rate functions, lease manager, emergency admission, metrics, and contract tests.
3. **Distributed circuit breaker and HA validation** — circuit SQL state machine, local cache, notification/poll refresh, failure replay, readiness, observability, and opt-in multi-replica validation.

Each plan must end in a buildable, testable state and preserve the default SQLite profile. Redis can be added only by implementing the published coordination contracts and passing the same contract suite.
