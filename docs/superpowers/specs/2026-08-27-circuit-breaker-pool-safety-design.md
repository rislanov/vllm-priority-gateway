# Circuit Breaker, Pool Safety, and Inference Readiness Design

**Date:** 2026-08-27
**Status:** Approved for implementation by the user's instruction to proceed without clarification
**Scope:** Single-replica gateway resilience; no distributed coordination or endpoint retry-policy expansion

## Objective

Prevent a backend whose inference endpoint repeatedly fails from continuing to receive ordinary traffic, bound aggregate work admitted to each model pool, and expose an unambiguous readiness signal for inference traffic. Preserve the existing OpenAI API, streaming, cancellation, one pre-first-byte transport retry, priority policy, health checks, and management-plane readiness behavior.

## Design choices

### Circuit breaker ownership

The backend monitor manager owns one in-memory circuit breaker per managed backend. This keeps inference eligibility, Admin runtime state, pool availability, and gateway in-flight accounting in one runtime boundary. Reconciliation retains a breaker when the backend configuration is unchanged and resets it when the backend is replaced, disabled, or reconfigured.

The breaker implementation lives in a focused `internal/circuitbreaker` package. It has no HTTP or gateway dependencies and receives acquisition and completion times explicitly, making delayed outcomes, rolling evidence, and cooldown anchors deterministic in tests.

### Circuit states and defaults

Each backend circuit has three states:

- `closed`: ordinary requests are admitted;
- `open`: requests are rejected until the cooldown expires;
- `half_open`: only a bounded number of inference probes may run.

Global defaults, configurable through `LLMGW_*`, are:

- failure threshold: `5` qualifying failures;
- rolling failure window: `30s`;
- open cooldown: `15s`;
- maximum concurrent half-open probes: `1`.

The threshold and probe limit must be positive. The window and cooldown must be positive durations.

In `closed`, qualifying failures are recorded at attempt completion and retained only inside the rolling window. Reaching the threshold opens the circuit at the latest qualifying completion, so cooldown is also anchored to completed failure time. In `open`, cooldown expiry begins a new `half_open` generation. With multiple probes, a success waits for every already-admitted current-generation outcome; the circuit is unavailable while that successful generation drains, any failure wins and immediately reopens at its completion time, and success closes only after the remaining outcomes are success or neutral. Neutral-only completion releases probe capacity without healing. State transitions invalidate the previous generation so stale callbacks cannot heal or penalize a later state.

### Inference outcome classification

The streaming proxy classifies every backend attempt, including the first attempt of a retry:

- `failure`: connection/DNS/TLS/response-header transport failure not caused by downstream cancellation, upstream `5xx`, or an upstream response-body read failure;
- `success`: a completed upstream HTTP response below `500`, including `4xx` and `429` because those prove the inference endpoint is responsive;
- `neutral`: downstream cancellation, downstream write failure, or another result that must not penalize or heal the backend.

HTTP `5xx` behavior remains conservative: the current response is forwarded and is not retried. The breaker affects subsequent requests. Transport failures remain eligible for the existing single retry before any downstream response byte.

`proxy.Target` carries an attempt-completion callback. The proxy invokes it exactly once for every selected target before asking for an alternate. This lets the monitor timestamp the outcome at callback completion, release the backend in-flight counter and circuit permit, and retain the first failed attempt during a retry.

### Runtime acquisition

The gateway runtime interface replaces the loose backend in-flight increment with an atomic acquisition operation:

```go
AcquireBackend(expected domain.Backend, at time.Time) (complete func(domain.InferenceOutcome), ok bool)
```

Under the same Manager lock used to reserve the breaker permit, acquisition fails when the ID-keyed worker does not exist, its complete immutable `domain.Backend` identity differs from `expected`, or its circuit cannot accept an ordinary request/probe. This prevents a newly published backend incarnation from using an old worker/breaker before reconciliation. A successful acquisition increments gateway in-flight and reserves any half-open probe slot. The idempotent completion callback retains that worker generation, records the outcome at completion time, and decrements only its own in-flight count.

Backend runtime snapshots expose the circuit state, rolling failure count, retry time, half-open probes in flight, and a computed `CircuitAvailable` flag. Existing health and pressure state remains separate from circuit state.

### Pool safety configuration

Each `ModelPool` persists two optional limits:

```text
MaxGatewayInflight int
MaxWaiting         int
```

Both default to `0`, meaning disabled/unlimited, and reject negative values. They are available through SQLite, immutable registry snapshots, Admin JSON, and the embedded Admin UI.

`MaxGatewayInflight` bounds all admitted requests for a pool across clients in this single process. The monitor manager owns an idempotent pool lease:

```go
AcquirePool(poolID int64, maximum int) (release func(), ok bool)
```

The live counter is incremented even when the configured maximum is zero so Admin and Prometheus telemetry remain accurate. A positive maximum rejects acquisition when the current count is already at the maximum. The lease spans client admission, backend selection, retries, and the complete streaming lifecycle.

`MaxWaiting` compares the latest aggregate vLLM waiting-request metric for the pool with the configured positive maximum. Admission rejects when `TotalWaiting >= MaxWaiting`. Every `PoolSnapshot(poolID, at)` overlays waiting from the current stored worker snapshots before the independent pool-observer tick, without advancing cached hysteretic state. Waiting includes enabled, non-draining backends with healthy and fresh monitoring data; circuit state does not erase already queued upstream work.

Pool safety checks occur after authentication/model authorization and pool availability, but before backend selection:

1. reject an unavailable pool with the existing `503` response;
2. reject a reached `MaxWaiting` limit with the existing bounded `429 gateway_overloaded` envelope;
3. acquire the pool lease or return the same `429` envelope;
4. apply the existing priority-adjusted per-client concurrency lease;
5. select and acquire a backend.

### SQLite migration

Introduce ordered embedded schema migrations using SQLite `PRAGMA user_version`:

- version `1`: the existing initial schema;
- version `2`: add the two non-negative pool safety columns with default `0`.

Each version runs in its own transaction and advances `user_version` only after the migration succeeds. Existing databases at version `0` receive both migrations safely; new databases follow the same path. Reopening an up-to-date database is a no-op. A binary with the forward-version guard rejects `user_version` above its latest embedded migration before running migrations or changing the logical version, schema, or data; the version-3 regression verifies preservation of the recorded version, a future-only schema marker, and existing pool data/limits. Connection setup has already applied file permission and WAL pragmas, so no byte-for-byte file immutability is claimed. The immediate pre-versioning `d6787d2` binary does not inspect `user_version`: its idempotent migration 001 and explicit-column CRUD accept and preserve the additive version-2 columns but ignore and do not enforce their values. Only later version-aware binaries with this guard reject future recorded versions.

### Inference readiness

Keep `/readyz` as the existing management-plane readiness endpoint and HTTP `200`, even when inference capacity is zero.

Add unauthenticated `GET /inference-readyz`:

- HTTP `200` with `status: "ready"` when at least one enabled model pool has at least one enabled, non-draining backend that is healthy, metrics-fresh, has any configured upstream secret available, and whose circuit can accept a request or half-open probe;
- HTTP `503` with `status: "unavailable"` otherwise.

The JSON response includes `revision`, `poolAvailability`, and `backendAvailability`. Pool congestion limits do not make the process unready because removing a replica from the load balancer would not create GPU capacity and could cause readiness flapping.

### Observability

Admin runtime JSON/UI displays pool gateway in-flight, total waiting, configured safety limits, and per-backend circuit state.

Prometheus adds bounded-cardinality gauges:

- `llmgw_backend_circuit_state` (`-1=unmanaged/unknown`, `0=closed`, `1=open`, `2=half_open`);
- `llmgw_backend_circuit_failures`;
- `llmgw_pool_gateway_inflight`;
- `llmgw_pool_waiting_requests`;
- `llmgw_pool_available_backends`.

No request IDs, URLs, session IDs, prompts, keys, or content become labels.

## Testing strategy

All implementation tasks follow red-green-refactor TDD.

1. Unit-test the breaker with explicit acquisition/completion timestamps: delayed rolling-window evidence and cooldown, bounded multi-probe generations, failure-wins ordering, successful-generation drain unavailability, stale callback rejection, neutral release, and idempotent completion.
2. Unit-test proxy outcome classification and prove the attempt callback runs for the failed first target before alternate selection.
3. Unit-test Manager backend/pool acquisition and runtime snapshots under concurrency, including exact backend-incarnation matching and latest-waiting overlays before observer ticks; run affected packages with `-race`.
4. Test SQLite migration from the old schema, fresh schema creation, transactional rollback, future-version rejection with logical version/schema/data preservation, pool CRUD/snapshot round trips, validation, Admin API, and Admin UI fields.
5. Add gateway/integration tests for repeated inference `5xx` with healthy `/health` and `/metrics`, circuit exclusion, cooldown/half-open recovery, pool in-flight rejection, waiting rejection, and readiness transitions.
6. Extend the real-vLLM E2E harness to require `/inference-readyz`, expose the new runtime fields, and verify pool safety in the intentional-load mode. Add an isolated resilience scenario that can place a controllable fault proxy in front of one real backend, drain the others, open/recover the circuit, and restore every Admin mutation in cleanup.
7. Verify the unmanaged circuit metric encoding, then run the deterministic suite, race suite, vet, builds, Docker smoke when the daemon is available, and smoke/priority/resilience E2E against at least two local vLLM serving processes.

## Documentation

Update README, technical specification implementation notes, acceptance evidence, real-GPU validation, and the real-vLLM E2E runbook with configuration, API semantics, operating guidance, metrics, and limitations. Remove circuit breaker and pool safety from the “not implemented” list while retaining the single-replica limitation.

## Out of scope

- Redis/Valkey distributed leases or multi-replica correctness;
- PostgreSQL configuration storage;
- retrying HTTP `5xx` responses;
- OIDC/RBAC/audit logging;
- automatic service discovery;
- capacity-weighted or prefix-block-aware routing.
