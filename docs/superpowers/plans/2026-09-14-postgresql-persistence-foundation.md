# PostgreSQL Persistence Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the complete PostgreSQL persistence profile, configuration propagation, rate-policy fields, and SQLite compatibility while preserving SQLite as the default.

**Architecture:** `internal/store` owns backend-neutral persistence contracts and parameters. SQLite and `internal/store/postgres` implement those contracts independently; PostgreSQL uses three pgx pools, embedded golang-migrate migrations, repeatable-read snapshots, and LISTEN plus polling.

**Tech Stack:** Go 1.27, pgx/v5, pgxpool, golang-migrate v4, SQLite, PostgreSQL 16+

**Spec:** `docs/superpowers/specs/2026-09-14-postgresql-production-coordination-design.md`

## Global Constraints

- `LLMGW_DATABASE_DRIVER` is `sqlite` by default and accepts only `sqlite` or `postgres`.
- PostgreSQL requires `LLMGW_DATABASE_URL`; mixed-profile settings fail startup.
- Runtime pgx connections use `pgx.QueryExecModeExec`; migration and LISTEN connections use the direct migration URL.
- Limits are `10_000`, `100_000`, `10_000_000`, and `10_000_000_000_000` for client concurrency, pool inflight, RPM, and TPM.
- PostgreSQL configuration transactions set `synchronous_commit = on` and publish one revision per mutation.
- Unit tests require no PostgreSQL. Real PostgreSQL tests are opt-in and must not run during this implementation.

---

### Task 1: Domain, configuration, and SQLite compatibility

**Files:**
- Modify: `internal/domain/types.go`, `internal/domain/validate.go`, `internal/config/config.go`
- Create: `internal/store/migrations/004_postgresql_compatibility.sql`
- Modify: `internal/store/{clients,pools,backends,snapshot,models,sqlite}.go`
- Test: `internal/domain/validate_test.go`, `internal/config/config_test.go`, `internal/store/sqlite_test.go`, `internal/store/crud_test.go`

**Interfaces:** Client gains `Revision int64`, `RequestsPerMinute int64`, `TokensPerMinute int64`; pools and backends gain `Revision int64`. Mutation validation accepts the previous concurrency value so an unchanged grandfathered SQLite value remains valid.

- [ ] Write table-driven boundary and mixed-profile tests; run them and confirm failures are caused by missing fields and validation.
- [ ] Add the fields, limits, change-aware validators, all PostgreSQL settings, and cross-setting validation.
- [ ] Add SQLite columns, revision increments, and insert/update triggers that preserve but cannot increase legacy concurrency values.
- [ ] Run `go test -count=1 ./internal/domain ./internal/config ./internal/store` and keep the whole set green.

### Task 2: Persistence interfaces, pools, and migrations

**Files:**
- Create: `internal/store/interfaces.go`
- Create: `internal/store/postgres/{store,pools,migrate,migrations}.go`
- Create: `internal/store/postgres/migrations/000001_configuration.{up,down}.sql`
- Create: `internal/store/postgres/migrations/000002_analytics.{up,down}.sql`
- Create: `internal/store/postgres/migrations/000003_coordination.{up,down}.sql`
- Test: `internal/store/interfaces_test.go`, `internal/store/postgres/{pools,migrations}_test.go`

**Interfaces:** `ConfigurationStore` embeds `registry.Loader`, `AdminStore`, and `KeyUsageStore`; `AnalyticsStore` embeds analytics record/query contracts; `LifecycleStore` exposes `Close() error`. `postgres.Open` returns isolated config, analytics, and coordination repositories sharing one lifecycle.

- [ ] Write compile-time contract, URL-redaction, query-mode, pool-sizing, PostgreSQL-version, and migration-manifest tests and observe the expected failures.
- [ ] Add pgx and golang-migrate dependencies and implement validated pool configs with no DSN exposure.
- [ ] Embed matched migrations containing native types, checks, indexes, scopes, receipt ledgers, rate rows, circuit rows, and replica rows.
- [ ] Implement migration startup with PostgreSQL 16 enforcement, advisory locking, dirty/newer-schema refusal, and `ErrNoChange` success.
- [ ] Run the new unit tests without connecting to PostgreSQL.

### Task 3: PostgreSQL configuration and analytics repositories

**Files:**
- Create: `internal/store/postgres/{configuration,snapshot,clients,keys,pools,backends,analytics}.go`
- Test: `internal/store/postgres/{configuration,analytics}_test.go`
- Create: `tests/integration/postgres_persistence_test.go`

**Interfaces:** PostgreSQL implements the exact `store` parameter and analytics query types. Snapshot reads use one read-only repeatable-read transaction. Mutations increment row/global revisions once, notify before commit, and lock admission/circuit scopes before identity changes.

- [ ] Write unit tests against narrow pgx executor/transaction fakes for transaction options, SQL ordering, revision publication, bounded retention, and analytics parity; verify RED.
- [ ] Implement CRUD, snapshot validation, key usage, batch analytics writes, all analytics queries, and bounded retention using explicit casts and `QueryExecModeExec`.
- [ ] Write opt-in real-PostgreSQL tests behind `LLMGW_POSTGRES_TEST_DSN`; do not run them.
- [ ] Run all repository unit tests and compile the opt-in package with `go test -run '^$' ./tests/integration` only if this does not execute setup; otherwise use `go test -c`.

### Task 4: Reload lifecycle and profile composition

**Files:**
- Create: `internal/store/postgres/watch.go`
- Modify: `internal/registry/registry.go`, `cmd/gateway/main.go`, `Makefile`
- Test: `internal/store/postgres/watch_test.go`, `internal/registry/registry_test.go`, `cmd/gateway/main_test.go`

**Interfaces:** A watcher treats notifications as hints, polls revision with jitter, checks immediately after reconnect, and publishes only newer complete snapshots. Startup completes before the listener is served.

- [ ] Write fake-transport tests for notify, missed notification polling, reconnect, monotonic publication, and startup failure.
- [ ] Implement the watcher and compose one complete SQLite or PostgreSQL profile; reject mixed settings before opening resources.
- [ ] Add `test-postgres`, `test-postgres-docker`, and `test-postgres-pooler` targets without adding them to `test`.
- [ ] Run all non-integration unit packages, `go vet ./...`, and `go build ./cmd/...`.

