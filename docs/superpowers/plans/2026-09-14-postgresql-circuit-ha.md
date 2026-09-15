# Distributed Circuit Breaker and HA Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete distributed circuit coordination, cache and failure replay, replica compatibility, readiness, observability, HA validation tests, and operator documentation.

**Architecture:** `CircuitCoordinator` is backend-neutral. Local coordination delegates to the existing breaker; PostgreSQL persists circuit identity, failure receipts/events, and half-open permits under deterministic locks. Replicas route from a local cache but every upstream attempt is authorized by the authoritative coordinator.

**Tech Stack:** Go 1.27, pgx/v5, PostgreSQL LISTEN/NOTIFY, Prometheus

**Spec:** `docs/superpowers/specs/2026-09-14-postgresql-production-coordination-design.md`

## Global Constraints

- PostgreSQL circuit completion retention is fixed at 24h; failure window is positive and no greater than retention minus 5m. Local terminal attempts have the bounded-eviction exception in design section 5.2, so their nominal 24-hour replay history is best-effort under capacity pressure.
- Acquire/completion retries preserve attempt UUID, acquisition/outcome timestamp, identity, generation, and fingerprint byte-for-byte.
- Failure wins over success for a half-open generation; neutral-only drains without healing; expired probes reopen conservatively.
- Identity changes atomically supersede unfinished permits; stale generations are no-ops and cannot heal or penalize new generations.
- PostgreSQL mode registers only with compatible live replicas and never clears a confirmed revision-regression fault.
- Integration, pooler, and HA tests are written but not run.

---

### Task 1: Circuit contract, local adapter, and contract suite

**Files:**
- Modify: `internal/coordination/types.go`
- Create: `internal/coordination/local/circuit.go`
- Create: `internal/coordination/contracttest/circuit.go`
- Test: `internal/coordination/local/circuit_test.go`, `internal/circuitbreaker/breaker_test.go`

**Interfaces:** Implement `CircuitCoordinator.Reconcile`, `Snapshot`, `Acquire`, `Complete`, `RenewProbes`, `Refresh`, and `Status`; preserve existing breaker outcome precedence and generation rules.

- [ ] Write deterministic shared cases for delayed failures, multi-probe ordering, neutral drains, expiry reopening, idempotency conflict, stale completion, and identity replacement.
- [ ] Implement the local adapter with stable attempts and injectable time while retaining the existing breaker as the state machine.
- [ ] Run local contract and breaker race tests.

### Task 2: PostgreSQL circuit state machine

**Files:**
- Create: `internal/coordination/postgres/{circuit,circuit_queries,circuit_cleanup}.go`
- Modify: `internal/store/postgres/migrations/000003_coordination.up.sql`
- Test: `internal/coordination/postgres/circuit_test.go`
- Create: `tests/integration/postgres_circuit_test.go`

**Interfaces:** Closed failures use durable receipts separate from rolling events. Half-open acquire uses durable permit IDs equal to attempt IDs. Every transition supersedes unfinished peers and retains terminal records to the strict cleanup boundary.

- [ ] Write SQL-executor unit tests for receipt-first dedupe, event timestamp clamping, prune ordering, permit replay, failure-wins, expiry, superseding, and cleanup boundaries.
- [ ] Implement closed/open/half-open acquire, completion, renewal, expiry reconciliation, backend invalidation, and bounded cleanup with post-lock server time.
- [ ] Write opt-in real-PostgreSQL cases for all contract races and lost-response scenarios; do not run them.
- [ ] Run coordinator unit tests and compile integration tests only.

### Task 3: Cache, monitor integration, and failure replay

**Files:**
- Create: `internal/coordination/{circuit_cache,failure_buffer}.go`
- Modify: `internal/monitor/manager.go`, `internal/gateway/service.go`, `cmd/gateway/main.go`
- Test: corresponding unit tests

**Interfaces:** Cache refreshes on notify, polling, and local mutation. During outage, open/half-open snapshots stay unavailable, closed snapshots may only become more conservative, and bounded failures replay with original immutable timestamps.

- [ ] Write fake-transport tests for notification loss/reconnect, cache age, conservative outage behavior, exact replay, stale discard, and recovery ordering.
- [ ] Delegate monitor circuit acquisition/completion to `CircuitCoordinator` while retaining local inflight and health/pressure ownership.
- [ ] Implement cache refresh and bounded failure replay; do not replay success as healing.
- [ ] Run gateway and monitor unit/race tests.

### Task 4: Replica compatibility, readiness, docs, and final verification

**Files:**
- Create: `internal/coordination/postgres/replicas.go`
- Modify: `internal/httpapi/public.go`, `internal/observability/{metrics,log}.go`, `cmd/gateway/main.go`, `Makefile`
- Modify: `README.md`, `README.ru.md`, `docs/{deployment,operations,technical-specification,acceptance-evidence,real-vllm-priority-e2e}.md`
- Create: `tests/integration/postgres_ha_test.go`, `tests/integration/postgres_pooler_test.go`

**Interfaces:** Replica registration fingerprints every coordination-critical setting. `/coordination-readyz` is strict while `/readyz` and `/inference-readyz` reflect degraded emergency capability.

- [ ] Write unit tests for fingerprints, incompatible live replicas, heartbeat expiry, readiness matrices, redaction, and metric label bounds.
- [ ] Implement registration/heartbeat, strict readiness, final metrics, and DSN-safe errors.
- [ ] Write but do not run direct PostgreSQL, PgBouncer reassignment, outage, two-gateway, and HA lossless/regressed-history tests.
- [ ] Update all required English and Russian operator documents with profiles, security, durability, failover, DR, limits, and test commands.
- [ ] Run non-integration unit tests, changed-package race tests, vet, build, migration manifest checks, and inspect the final diff before commit.

