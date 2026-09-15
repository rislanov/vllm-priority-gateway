# PostgreSQL production profile

The gateway has two explicit, non-mixable deployment profiles.

| Profile | Persistence | Coordination | Supported replicas |
|---|---|---|---:|
| `sqlite` (default) | SQLite configuration and analytics | in-process admission and circuits | 1 |
| `postgres` | PostgreSQL 16+ configuration and analytics | PostgreSQL leases, RPM/soft-TPM buckets, circuit state and probe permits | 2+ |

Redis/Valkey is a possible future implementation of the backend-neutral coordination interfaces; it is not required or implemented in Production V1. There is no automatic SQLite/PostgreSQL data import or bidirectional synchronization.

## Configuration

Select PostgreSQL explicitly and use a TLS-protected runtime URL:

```bash
LLMGW_DATABASE_DRIVER=postgres
LLMGW_DATABASE_URL='postgres://llmgw_runtime:...@db.internal/llmgw?sslmode=verify-full&default_query_exec_mode=exec'
LLMGW_DATABASE_MIGRATION_URL='postgres://llmgw_migrator:...@db-primary.internal/llmgw?sslmode=verify-full'
```

`LLMGW_DATABASE_MIGRATION_URL` must be a direct PostgreSQL connection when the runtime URL passes through a transaction-mode pooler. Runtime pgx pools force the simple extended-query `exec` mode and do not rely on session-prepared statements. Three independent pools isolate configuration (`LLMGW_POSTGRES_CONFIG_MAX_CONNS`, default 4), analytics (4), and coordination (16). PostgreSQL-specific settings with the SQLite profile, mixed stores, missing URLs, and a non-`exec` query mode are rejected at startup.

Other coordination settings are:

```text
LLMGW_CONFIG_POLL_INTERVAL=5s
LLMGW_COORDINATION_TIMEOUT=50ms
LLMGW_LEASE_TTL=90s
LLMGW_LEASE_RENEW_INTERVAL=30s
LLMGW_COORDINATION_COMPLETION_BACKLOG=4096
LLMGW_COORDINATION_EMERGENCY_CRITICAL_MAX_INFLIGHT=4
LLMGW_COORDINATION_EMERGENCY_HIGH_MAX_INFLIGHT=2
```

Lease renewal must not exceed one third of the lease TTL. Circuit failure windows are limited to 23h55m because durable circuit completion receipts are retained for 24 hours and accept at most five minutes of positive clock skew.

All newly created or changed policies are bounded before persistence:

| Policy field | Maximum | `0` semantics |
|---|---:|---|
| Client `MaxConcurrency` | 10,000 | client admission disabled (requests are rejected) |
| Pool `MaxGatewayInflight` | 100,000 | unlimited pool gateway inflight |
| Client `RequestsPerMinute` | 10,000,000 | RPM disabled |
| Client `TokensPerMinute` | 10,000,000,000,000 | soft TPM disabled |

PostgreSQL applies these maxima to every row. SQLite migration version 4 preserves a pre-existing `MaxConcurrency > 10,000` or `MaxGatewayInflight > 100,000` value only when an update leaves that exact value unchanged. A legacy value may be lowered directly into the supported range; any changed value that remains above the maximum, any increase, and any newly created over-maximum value are rejected. RPM and TPM have no grandfathering because those fields did not exist before version 4. The Admin HTML forms expose the same maxima and warn when editing a grandfathered SQLite concurrency value.

## Database, migration, and security ownership

Use PostgreSQL 16 or newer. Give the migration identity schema ownership and migration DDL privileges; give the runtime identity only the DML, sequence, `LISTEN`, and notification permissions required by the created schema. Do not put credentials in logs or metrics. Restrict both roles by network policy, require server verification, rotate credentials through the deployment secret mechanism, and monitor all three connection pools.

Startup applies embedded, forward-only migrations under the PostgreSQL migration driver's advisory lock. `ErrNoChange` is success. A dirty migration, lock failure, unsupported server, or schema newer than the binary prevents the listener from binding. Production never runs `Down` automatically.

Configuration writes commit with `synchronous_commit=on`, increment one global revision, and notify `llmgw_config_changed`. Notifications are hints: each replica also polls with jitter and checks immediately after reconnect. Older snapshots are discarded. Admission compares the request's snapshot revision and API-key identity with PostgreSQL; a stale result forces one synchronous full reauthentication, authorization, policy-resolution, and admission retry.

Back up configuration and analytics with ordinary PostgreSQL physical or logical tooling appropriate to the selected recovery objectives. Test restoration, migration status, key revocations, and coordination-table cleanup in an isolated environment.

Coordination retention runs in a separate maintenance worker once per second. Each pass drains batches of 256 candidates per category within a 250 ms total budget, alternating admission and circuit cleanup; a full batch triggers another round. This avoids a fixed one-batch-per-minute throughput ceiling while keeping transactions and maintenance time bounded. Expiry and replay safety do not depend on physical deletion. Monitor coordination-table growth and database capacity under the intended request rate; cleanup can lag when the database is overloaded.

## Distributed semantics

Admission locks pool scopes before client scopes and uses one post-lock server timestamp. A request UUID is a durable, fingerprinted receipt: retrying an ambiguous result with identical inputs cannot debit RPM twice or allocate a second lease. Active leases enforce client and pool concurrency globally. A central scheduler renews long streams and durably retries bounded completion batches. RPM is a continuously refilled one-minute client bucket debited at admission. TPM is a soft client bucket checked at admission and debited from upstream-reported input plus output usage at completion; natural overshoot is allowed, and missing usage is observable but uncharged.

Circuit state, rolling failure receipts, generations, and half-open probe permits are global. Every upstream attempt is authorized even though routing reads a local cache. Probe capacity is shared by replicas, expired probes reopen the circuit conservatively, and stale generations cannot heal or penalize the current generation. Backend revision changes, disabling, and draining supersede unfinished permits atomically. The current Admin API has no delete operation; operators must disable or drain topology records rather than assuming a deletion lifecycle exists.

Each process registers a random replica UUID and a fingerprint covering the coordination contract, lease timings, rate algorithm, and circuit policy. Every row carries the expiry calculated from that replica's own TTL. Registration and heartbeat serialize through the same compatibility transaction, so an incompatible live replica prevents both startup and reactivation. Coordination-critical setting changes therefore require a full stop/start without overlap.

PostgreSQL retains terminal coordination receipts for the strict 24-hour replay window. SQLite/local history is process-local and bounded to 65,536 admission receipts and 65,536 circuit attempts; capacity pressure can evict terminal records early, and an evicted UUID can be evaluated again. Choose PostgreSQL when that durable replay guarantee is required.

## Outage and recovery

An admitted response or stream continues during a PostgreSQL outage. The last-known-good configuration remains readable, but Admin writes fail. Normal and Background requests receive retryable `503 gateway_unavailable`. Critical and High may use only their process-local emergency caps, additionally bounded by client effective concurrency. A zero cap disables that class. With `N` replicas, the worst-case outage admission is `N × class_cap`.

Last-known open and half-open circuits admit no local work. A last-known closed circuit may be opened by the local conservative breaker but never healed locally. Failure completions retain their original attempt ID, outcome, and timestamp for replay; events older than the fixed 24-hour receipt window are discarded.

Recovery is complete only after database ping, replica compatibility check, fresh configuration load, lease reconciliation, failure replay, and circuit refresh. Database failures and the coordinator's own timeout latch degraded state at occurrence, including failures while reading query results. Cancellation of an individual caller does not change shared database health. A later successful worker operation cannot clear the latch, and a recovery barrier waits for any active failure replay. A confirmed writable-primary revision lower than the highest revision previously observed is a permanent consistency fault: keep coordination unavailable and reject emergency work until an operator resolves the history.

`synchronous_commit=on` guarantees local WAL durability only unless synchronous standbys are configured. HA operators own synchronous replication, quorum availability, promotion policy, and fencing of the former primary. Validate that losing synchronous durability blocks commits instead of silently weakening them.

If a promotion or restore may have lost acknowledged commits, do **not** use ordinary recovery. Stop ingress and every gateway, fence the old history, reconcile configuration including acknowledged key revocations, and drain or terminate old upstream work before starting a new deployment. Record the provider guarantee or PostgreSQL synchronous-standby and fencing configuration in acceptance evidence.

## Readiness and tests

| Endpoint | Semantics |
|---|---|
| `/readyz` | HTTP 200 after startup; reports `degraded`; `configuration` reflects the coordination/config-freshness result, while `analytics` is evaluated independently |
| `/inference-readyz` | Load-balancer signal; HTTP 200 when an eligible backend and applicable normal or emergency capacity can serve |
| `/coordination-readyz` | Strict distributed-admission gate; HTTP 503 unless coordination, replica compatibility, config freshness, and circuit refresh are current |

The default unit suite requires no PostgreSQL. Real database tests are opt-in and use a dedicated disposable database because they reset gateway tables:

```bash
LLMGW_POSTGRES_TEST_DSN='postgres://...' make test-postgres
make test-postgres-docker
LLMGW_POSTGRES_TEST_DSN='postgres://direct/...' \
LLMGW_POSTGRES_POOLER_TEST_DSN='postgres://pgbouncer/...' make test-postgres-pooler
```

An optional `LLMGW_POSTGRES_PRE16_TEST_DSN` pointing at a disposable PostgreSQL
15-or-older instance exercises the startup version gate. The ordinary PostgreSQL
target also checks concurrent migration startup, dirty-migration refusal, and a
committed `LISTEN/NOTIFY` reload. None of these opt-in tests run in default CI.

The pooler target requires transaction-mode PgBouncer with prepared-statement tracking disabled and verifies server-backend reassignment across transactions, not merely a successful ping. The HA history contracts are additionally selected with `LLMGW_POSTGRES_HA_TEST_DSN` (the writable endpoint under test) and optional `LLMGW_POSTGRES_HA_MIGRATION_DSN` (a direct connection).

Standalone tests do not establish failover safety. Production acceptance must additionally prove acknowledged lease/RPM state and API-key revocations survive the actual promotion topology while the former primary is fenced. The repository’s HA tests verify retained and deliberately regressed database histories at the application boundary; the operator’s failover harness remains responsible for causing promotion and proving fencing.
