# PostgreSQL Production Persistence and Distributed Coordination Design

**Date:** 2026-09-14

**Status:** Approved design

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

`AdmissionRequest` contains the request and replica identifiers, immutable configuration revision, authenticated API-key ID, client and pool IDs, the priority-adjusted effective client concurrency limit, pool gateway-inflight limit, client RPM/TPM policy, lease TTL, and the decision timestamp. The local coordinator uses the supplied timestamp for deterministic tests; the PostgreSQL coordinator uses database server time and never trusts the caller timestamp for expiry or refill decisions. `AdmissionDecision` contains either a lease identity or one bounded rejection reason with an optional retry time.

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

The public contracts use typed reasons rather than backend error strings. The required reasons are `concurrency_exhausted`, `rpm_exhausted`, `tpm_exhausted`, `stale_configuration`, `stale_backend`, `circuit_open`, `probe_capacity_exhausted`, `coordination_unavailable`, and `lease_lost`.

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

The same bounds are enforced by SQLite constraints, PostgreSQL constraints, Admin JSON, Admin forms, and coordinator input validation. Concurrency values remain Go `int` values and the selected bounds fit a signed 32-bit integer. Rate values use Go `int64` and fit PostgreSQL `BIGINT`. Validation runs before scope creation, migration backfill, or any loop whose work is proportional to a configured value.

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

URLs are parsed and validated but never emitted in logs, metrics, Admin JSON, or error bodies. Production documentation recommends TLS with certificate verification and a read-write primary endpoint.

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

Startup applies `Up`; `migrate.ErrNoChange` is success. A dirty migration, unsupported database version, migration lock timeout, or schema newer than the binary prevents readiness and startup completion. Production startup never runs `Down` automatically.

The PostgreSQL driver advisory lock prevents concurrent gateway replicas from applying migrations simultaneously. Migrations must use the direct migration URL when the runtime URL passes through a transaction-mode pooler.

Migration files are immutable after merge. The default CI validates unique ordered versions, matched up/down files, and embedded manifest completeness. PostgreSQL itself stores migration version and dirty state; this design does not add a second custom migration-version table.

SQLite retains the current embedded migration runner and `PRAGMA user_version`. A SQLite migration adds rate-policy fields and row revisions while preserving existing data and defaults. Existing SQLite databases do not adopt the PostgreSQL migration mechanism.

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

request_leases
  lease_id       UUID PRIMARY KEY
  request_id     TEXT NOT NULL
  replica_id     UUID NOT NULL
  client_id      BIGINT NOT NULL
  pool_id        BIGINT NOT NULL
  acquired_at    TIMESTAMPTZ NOT NULL
  expires_at     TIMESTAMPTZ NOT NULL
```

`request_leases` has non-unique indexes on `client_id` and `pool_id`; `expires_at` is deliberately not indexed so renewal remains eligible for HOT updates. Lease UUID is the acquire/completion idempotency key. Client-supplied or repeated request IDs are diagnostic values and are not uniqueness keys.

Creating a client or pool creates its corresponding scope row. A client or pool limit update locks that scope row before committing the new policy and global configuration revision. Reducing a limit does not cancel existing owners. It immediately prevents new acquisition while the total active count is at or above the new limit.

Every request lease always records both client and pool identity. Pool limit zero disables only the rejection check; it does not disable lease creation, renewal, distributed pool-inflight accounting, or later transition to a positive limit.

`try_acquire_admission` executes atomically in one PostgreSQL transaction. It captures one PostgreSQL server timestamp for every expiry and refill comparison, then:

1. locks the requested pool scope row and then the requested client scope row;
2. re-reads and checks the global configuration revision, API key, client, pool, enabled policy, and configured limits while those locks are held;
3. counts all unexpired leases for the pool, regardless of the previous or current pool limit;
4. counts all unexpired leases for the client, including leases admitted under a larger administrative or priority-adjusted limit;
5. rejects when a positive pool limit is not greater than total pool inflight;
6. rejects when the effective client limit is not greater than total client inflight;
7. refreshes and checks the soft TPM balance;
8. refreshes and debits one RPM token;
9. inserts exactly one request lease containing both client and pool identity;
10. returns the lease identity and decision metadata.

The supplied effective client limit must be between zero and the current configured client maximum read in step 2. A global revision mismatch returns `stale_configuration`; the gateway reloads and recomputes the effective limit from fresh policy and pool state rather than scaling a stale value inside SQL.

All acquisition, renewal, policy-change, and completion paths lock scopes in the global order `all pool IDs ascending, then all client IDs ascending`. A single-request acquisition therefore locks its pool before its client. Scope locking serializes every membership-changing operation that could affect the two counts. Failure at any step rolls back lease and rate-state changes.

The atomic invariant is:

```text
admit only if active_pool_leases < positive_pool_limit (when enabled)
          and active_client_leases < effective_client_limit
```

Counts include leases created under an older, larger limit. The implementation must not infer inflight from slot ordinals or configured capacity.

Lease identity includes the lease UUID, client ID, pool ID, request ID, replica ID, and expiry. Renewal never resurrects an expired lease: it locks the same pool/client scopes and updates only a matching lease whose existing expiry is still in the future according to the transaction's PostgreSQL server timestamp.

Lease TTL and renewal settings are:

```text
LLMGW_LEASE_TTL=90s
LLMGW_LEASE_RENEW_INTERVAL=30s
```

The renewal interval must be positive and no greater than one third of the TTL. PostgreSQL server time is authoritative for expiry. A crashed gateway stops renewing and its lease stops contributing to active counts after TTL.

`request_leases` uses a reduced fillfactor and aggressive per-table autovacuum settings because expiry is updated frequently. A bounded cleanup worker deletes expired ledger rows in batches; correctness never depends on physical deletion because counts and renewals always test expiry. Distributed pool/client inflight snapshots use a batched `GROUP BY` over unexpired ledger rows and are cached no longer than the metrics interval.

## 12. Lease Manager and Completion

Each gateway process runs one central `LeaseManager`. It owns active distributed lease handles, schedules batch renewal, and records completion without creating one goroutine or one database connection per request.

Completion contains optional actual usage. A PostgreSQL completion function:

1. locks the pool and client scope rows named by the immutable lease identity in the global order;
2. checks whether a completion idempotency record already exists for the lease UUID;
3. when the completion is new, inserts that record and debits the client's soft token balance by `input_tokens + output_tokens`;
4. deletes the request lease only when its lease UUID, client ID, and pool ID match;
5. returns `released`, `already_completed`, or `lease_lost`.

Actual usage is debited once even when the request lease already expired or bounded cleanup removed it: the upstream work still consumed tokens. The immutable lease handle supplies the trusted client and pool identity, while the conditional delete prevents a stale completion from removing any other request. All steps commit or roll back together.

`cache_read_tokens` is a subset of input tokens and is not added again. Missing or invalid usage releases the lease without a token debit and increments a bounded-cardinality unmetered-completion metric.

Completion delivery is asynchronous and idempotent. The manager retries transient failures with bounded exponential backoff. The pending completion backlog defaults to 4096:

```text
LLMGW_COORDINATION_COMPLETION_BACKLOG=4096
```

When the backlog is full, the response remains successful, lease cleanup falls back to TTL, the soft TPM debit may be lost, and a dedicated failure counter increments. This is permitted only because TPM was explicitly selected as a soft limit. Completion idempotency records are retained for 24 hours, longer than the manager's retry horizon, and deleted in bounded batches.

An active stream is never cancelled solely because renewal failed. The manager continues retrying. If the lease expires and another request uses the newly available capacity, a later renewal returns `lease_lost`; the original stream continues and telemetry records temporary overcommit risk.

## 13. Distributed Rate Limits

Client policy supplies two independent limits:

```text
RequestsPerMinute
TokensPerMinute
```

Both use rows in `coordination_rate_state` keyed by client and rate kind. State stores client policy revision, current balance, and the PostgreSQL update timestamp. A policy-revision change starts a new bucket generation with the new full capacity.

RPM is a distributed token bucket. Capacity equals `RequestsPerMinute`, refill is `limit / 60 seconds`, and every successful admission costs one token. The acquisition function serializes changes to one client's RPM row and returns a calculated `retry_at` when exhausted. A rejected concurrency or pool acquisition does not consume an RPM token because the entire statement rolls back.

TPM is intentionally soft. Admission refreshes the balance and requires at least one available token but does not reserve an estimated request cost. Successful completion debits actual `input_tokens + output_tokens`; balance may become negative. Further requests are rejected until refill makes the balance positive. Requests already in flight may overshoot the configured TPM, and the maximum overshoot depends on their concurrency and actual response sizes.

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

backend_circuit_failures
  failure_id             UUID PRIMARY KEY
  backend_id             BIGINT
  backend_revision       BIGINT
  failed_at              TIMESTAMPTZ

backend_circuit_probes
  permit_id              UUID PRIMARY KEY
  backend_id             BIGINT
  backend_revision       BIGINT
  circuit_generation     BIGINT
  replica_id             UUID
  expires_at             TIMESTAMPTZ
  completed_at           TIMESTAMPTZ NULL
  outcome                success | failure | neutral | expired | superseded | NULL
```

### 14.1 Acquire

`acquire_circuit` first verifies that the requested backend revision is still current, enabled, and not draining. An older replica receives `stale_backend` and cannot recreate old state.

A closed-state fast path reads the committed state without taking a write lock. A request racing with a newly opened circuit is treated as already admitted, matching ordinary circuit-breaker semantics. Open-to-half-open transition and half-open probe acquisition lock the backend state row and re-check every condition atomically.

Half-open capacity is global across replicas. Probe permits have a TTL, are scheduled for renewal by the same process-level lease scheduler through `CircuitCoordinator.RenewProbes`, and are counted only for the matching backend revision and circuit generation. Admission-lease completion and circuit-probe completion remain separate idempotent operations because one client request may make more than one backend attempt.

### 14.2 Completion

Closed-state success and neutral outcomes do not write circuit state. A qualifying failure inserts a unique failure event using the attempt ID as its idempotency key, prunes timestamps outside the rolling window, and opens the circuit when the threshold is reached.

Half-open completion is idempotent through the durable probe permit. Failure reopens immediately. Success closes only after every already-admitted probe in the generation has completed without failure. Success plus neutral completion closes after the successful generation drains. Neutral-only completion releases probe capacity without closing. Stale-generation outcomes cannot heal or penalize a newer circuit generation.

An expired half-open probe is a conservative circuit failure, not a neutral completion. Expiry reconciliation locks the circuit row, re-checks the permit and generation, selects the earliest expired unfinished permit, marks it `expired`, marks every other unfinished permit from the same generation `superseded`, transitions the circuit to `open`, clears `half_open_succeeded`, increments generation, and sets `opened_at` to the selected permit's `expires_at`. It emits the ordinary circuit-change notification. This transition applies even when another probe in the generation already succeeded.

Expiry reconciliation runs before every half-open acquire, renewal, and completion and in the periodic circuit refresh worker, so no operation can treat an expired generation as drained successfully. A completion that first discovers its own permit has expired performs the expiry transition and then returns a stale outcome without applying the reported success, failure, or neutral result.

Probe admission resumes only after the new open cooldown measured from that expiry timestamp. If an acquire operation discovers the expiry after this cooldown has already elapsed, it may reconcile the expired generation, advance the now-open circuit to a fresh half-open generation, and issue the requesting attempt a new permit while retaining the same circuit-row lock. A background reconciliation only changes state; it never creates a permit without a requesting attempt. An expired permit cannot be renewed or revived. Any later completion from the expired or superseded generation is a stale no-op and cannot modify failure history or the new generation.

This rule intentionally prefers temporary unavailability over closing a circuit while a probe whose ownership was lost may still be executing upstream.

The distributed implementation must pass the same delayed-failure, failure-wins ordering, multi-probe, neutral, cooldown, and stale-callback cases as the current local breaker.

### 14.3 Cache and notifications

Routing and dashboards read a local cache of distributed circuit snapshots. PostgreSQL emits `NOTIFY llmgw_circuit_changed` after state transitions. Every replica also batch-refreshes circuit state at least once per metrics interval and immediately refreshes after its own acquire or completion changes state.

The authoritative acquire still executes before an upstream attempt. A stale cache may cause the router to consider a backend first, but PostgreSQL can reject the permit; the gateway excludes that backend and selects another. Stale cache cannot exceed global half-open capacity.

Changing a route-affecting backend field increments backend revision and creates a new circuit identity. Old failure and probe rows become inert and are removed by bounded cleanup.

## 15. Replica Compatibility

Every PostgreSQL-mode gateway generates a random replica UUID and maintains a row in `coordination_replicas`:

```text
replica_id
started_at
heartbeat_at
binary_version
coordination_contract_version
policy_fingerprint
```

The fingerprint covers lease TTL/renewal semantics, circuit thresholds and windows, half-open probe capacity, rate algorithm version, and coordination contract version. Registration rejects a replica when another non-expired replica has an incompatible fingerprint.

The first implementation requires a full stop/start without overlap when coordination-critical settings change. This restriction prevents mixed circuit and lease semantics. Ordinary configuration and client-policy changes continue using online revision propagation.

Replica heartbeat and lease renewal share the coordination lifecycle but not an open transaction. Expired replica rows are diagnostic; lease and probe TTLs remain the correctness mechanism.

## 16. PostgreSQL Failure Policy

PostgreSQL coordination errors are classified as timeout, unavailable, stale state, serialization/deadlock retry, constraint/corruption, and permanent incompatibility. At most one bounded retry is allowed for retryable transaction conflicts inside the coordination timeout.

After successful startup, loss of PostgreSQL enters degraded mode:

- admitted responses and streams continue;
- the registry continues serving the in-memory last-known-good snapshot;
- Admin mutations return retryable HTTP 503;
- Normal and Background requests return retryable HTTP 503 before upstream work;
- Critical and High requests may use local emergency admission only;
- last-known open circuits remain unavailable;
- last-known half-open circuits do not admit local probes;
- last-known closed circuits may be made unavailable by a local conservative breaker but cannot heal locally;
- bounded local failure events are replayed with original timestamps and idempotency IDs after recovery;
- success observed during the outage never closes distributed circuit state.

Emergency limits are process-wide class caps, additionally bounded by the client's effective local concurrency:

```text
LLMGW_COORDINATION_EMERGENCY_CRITICAL_MAX_INFLIGHT=4
LLMGW_COORDINATION_EMERGENCY_HIGH_MAX_INFLIGHT=2
```

Zero disables emergency admission for that class. Because limits are per replica during the outage, deployment documentation states the aggregate worst case as `replica_count * class_cap`.

Recovery requires a successful database ping, replica-fingerprint check, configuration revision reload, lease reconciliation, local failure replay, and circuit cache refresh. The gateway exits degraded mode only after all mandatory steps succeed.

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
- SQLite migration tests for rate-policy and revision additions;
- local coordinator contract tests for effective/admin limit reduction, unlimited-pool accounting, `0 -> positive` pool transitions, expiry without resurrection, and completion idempotency;
- deterministic circuit-coordinator tests for `success + crashed probe`, `success + expired probe + late failure`, cooldown after expiry, and stale-generation no-ops;
- gateway tests using deterministic fake coordinators;
- emergency-mode and recovery state-machine tests;
- `LeaseManager` renewal, completion, backlog, cancellation, and shutdown race tests;
- domain and transport boundary tests for every concurrency and rate maximum;
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

The opt-in suite covers:

- concurrent migration startup and dirty-state refusal;
- PostgreSQL 16 version enforcement;
- CRUD, referential integrity, revision increments, and analytics parity;
- LISTEN notification, polling fallback, reconnect, and missed-event recovery;
- aggregate client and pool concurrency across two gateways;
- no over-admission when effective client limits fall below owners acquired under a larger limit;
- unlimited-pool accounting and `0 -> positive` pool-limit transitions with existing inflight;
- administrative client/pool limit reductions with existing inflight;
- conservative no-over-admission under scope-lock contention;
- long-stream renewal, crash expiry, idempotent completion, and lost-lease detection;
- distributed RPM refill and rejection;
- soft TPM completion debit, natural overshoot, missing usage, and policy reset;
- distributed circuit rolling window, open, cooldown, half-open capacity, completion ordering, neutral outcomes, and stale generations;
- `success + crashed probe` expiry reopening and `success + expired probe + late failure` stale completion;
- PostgreSQL outage, emergency admission, failure replay, and recovery;
- incompatible replica fingerprint rejection;
- deadlock/serialization behavior under concurrent admission and circuit transitions;
- two in-process gateways using one fake vLLM pool and one PostgreSQL database;
- proxy overhead measurements against the production target of p50 below 5 ms and p99 below 20 ms on the documented reference environment.

Performance results are reported with environment and database topology; they are not treated as portable guarantees.

## 20. Rollout

This feature does not import SQLite data. A PostgreSQL deployment starts with a new schema and is configured explicitly through the existing Admin API/UI.

Recommended rollout:

1. provision an HA PostgreSQL 16+ primary endpoint and credentials;
2. run migration/status validation with the gateway binary and direct migration URL;
3. start one PostgreSQL-mode gateway replica;
4. configure clients, keys, pools, and backends;
5. run compatibility, streaming, analytics, lease, rate, and circuit smoke tests;
6. start a second compatible gateway replica;
7. run multi-replica concurrency, config-propagation, circuit, and outage tests;
8. place both replicas behind the production load balancer;
9. calibrate coordination timeout, connection pools, emergency caps, and circuit thresholds from observed telemetry.

Rollback to SQLite requires a separately configured SQLite database and returns the deployment to one supported gateway replica. No automatic bidirectional data synchronization exists.

## 21. Documentation Changes

Update the English and Russian READMEs, deployment guide, operations guide, technical specification, acceptance evidence, and real-vLLM runbook to cover:

- profile selection and compatibility matrix;
- PostgreSQL 16+ and pgx requirements;
- migration and direct-connection requirements;
- PostgreSQL roles, TLS, backup, and HA ownership;
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
