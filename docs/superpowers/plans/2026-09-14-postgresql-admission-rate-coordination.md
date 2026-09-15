# PostgreSQL Admission and Rate Coordination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide backend-neutral admission contracts, local and PostgreSQL implementations, distributed leases and rate limits, a central lease manager, and conservative outage behavior.

**Architecture:** Gateway policy creates stable operation identities and delegates atomic admission to `internal/coordination`. Local coordination is deterministic and contract-tested; PostgreSQL coordination serializes on pool/client scope rows and keeps durable idempotency receipts. One process-level lease manager batches renewals and completion delivery.

**Tech Stack:** Go 1.27, pgx/v5, PostgreSQL functions/transactions, Prometheus

**Spec:** `docs/superpowers/specs/2026-09-14-postgresql-production-coordination-design.md`

## Global Constraints

- All membership-changing locks use pool IDs ascending, then client IDs ascending, then lease and receipt UUIDs.
- Time-dependent decisions capture one `clock_timestamp()` only after correctness-relevant locks.
- PostgreSQL admission receipt replay window is strict `> now-24h` and `<= now+5m`; cleanup uses `retain_until <= now`. Local receipt history has the bounded-eviction exception specified in design section 5.2.
- Lease TTL defaults to 90s; renewal defaults to 30s and must be positive and at most one third of TTL.
- RPM debits once only on successful admission; TPM is soft and debits actual input plus output tokens once on completion.
- Integration tests are written but not run.

---

### Task 1: Coordination contract and local implementation

**Files:**
- Create: `internal/coordination/{types,errors}.go`
- Create: `internal/coordination/local/{admission,status}.go`
- Create: `internal/coordination/contracttest/admission.go`
- Test: `internal/coordination/local/admission_test.go`

**Interfaces:** Implement `AdmissionCoordinator.Acquire`, `Renew`, `Complete`, and `Status` using typed reasons and immutable lease identities. The local coordinator records unlimited-pool leases and applies the same expiry, RPM, soft-TPM, and retained-receipt idempotency semantics with an injectable clock. Local terminal receipts may be evicted at the 65,536-record bound; replay guarantees last only while the receipt is retained, unlike PostgreSQL's strict durable window.

- [ ] Write reusable contract cases for reductions, unlimited transitions, replay conflicts, expiry, completion idempotency, and refill-before-debit; verify RED against a skeleton.
- [ ] Implement the local coordinator by wrapping existing limit semantics and explicit token buckets.
- [ ] Run contract and race tests for the local package.

### Task 2: PostgreSQL admission repository

**Files:**
- Create: `internal/coordination/postgres/{admission,queries,cleanup,status}.go`
- Modify: `internal/store/postgres/migrations/000003_coordination.up.sql`
- Test: `internal/coordination/postgres/admission_test.go`
- Create: `tests/integration/postgres_admission_test.go`

**Interfaces:** PostgreSQL admission performs acquire/renew/complete in synchronous-commit transactions, fingerprints every immutable input, and returns recorded decisions without duplicate rate or lease mutation.

- [ ] Write executor-fake unit tests proving lock/query order, single post-lock clock capture, idempotency branches, no-debit rejection, conditional completion delete, and bounded cleanup.
- [ ] Implement acquisition, batched renewal/completion, grouped inflight snapshots, lease cleanup, and terminal receipt cleanup.
- [ ] Write opt-in contention, two-replica, response-loss, future-skew, rate, and expiry tests; do not run them.
- [ ] Run coordination unit tests and compile the integration test binary only.

### Task 3: Lease manager and outage state machine

**Files:**
- Create: `internal/coordination/{lease_manager,recovery,emergency}.go`
- Test: `internal/coordination/{lease_manager,recovery,emergency}_test.go`

**Interfaces:** `LeaseManager` owns active handles, one renewal loop, a bounded completion queue (default 4096), retry backoff, and shutdown draining. `RecoveryManager` requires ping, fingerprint, revision reload, lease reconciliation, circuit replay, and cache refresh before clearing degraded state.

- [ ] Write deterministic tests for batching, lost leases, backlog overflow, cancellation-neutral streams, shutdown races, emergency caps, and regression confirmation.
- [ ] Implement central scheduling without per-request goroutines or connections; never cancel admitted work solely on renewal loss.
- [ ] Implement permanent regression fault and per-class emergency admission caps.
- [ ] Run package tests with `-race`.

### Task 4: Gateway integration, readiness, and telemetry

**Files:**
- Modify: `internal/gateway/service.go`, `internal/httpapi/{public,errors}.go`, `internal/monitor/manager.go`, `internal/observability/{metrics,log}.go`, `cmd/gateway/main.go`
- Test: corresponding package tests
- Create: `tests/integration/postgres_gateway_admission_test.go`

**Interfaces:** Gateway authenticates and resolves policy, asks the coordinator once, fully restarts the pipeline once on stale configuration, and maps concurrency/rate/unavailable reasons to bounded HTTP responses. Monitor overlays distributed inflight counts.

- [ ] Write fake-coordinator tests for stable IDs, stale-config full reauthentication, rate errors, emergency policy, and completion usage.
- [ ] Replace concrete limiter/pool admission dependencies with the coordinator and lease manager while retaining SQLite behavior through the local backend.
- [ ] Add bounded coordination, pool, lease, rate, and emergency metrics and structured transition logs.
- [ ] Write opt-in two-gateway integration cases; do not run them.
- [ ] Run all non-integration unit packages, vet, build, and race tests for changed packages.

