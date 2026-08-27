# Circuit Breaker, Pool Safety, and Inference Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-backend inference circuit breaking, persisted per-pool safety limits, explicit inference readiness, production telemetry, and real-vLLM resilience coverage.

**Architecture:** A deterministic `internal/circuitbreaker` state machine is owned by `monitor.Manager`; proxy targets complete exactly one classified inference attempt so backend permits and in-flight counters remain correct across retries. Pool configuration is persisted through versioned SQLite migrations, while the runtime manager enforces pool leases and exposes readiness/metrics without changing management readiness.

**Tech Stack:** Go 1.27, `net/http`, SQLite (`modernc.org/sqlite`), Chi, Prometheus client, embedded Go templates, Go tests and race detector.

**Spec:** `docs/superpowers/specs/2026-08-27-circuit-breaker-pool-safety-design.md`

## Global Constraints

- Preserve `GET /v1/models`, the three supported generation routes, byte-immediate streaming, downstream cancellation, and at most one transport retry before downstream response bytes.
- Do not retry upstream HTTP `5xx`; classify it for the breaker and forward the current response.
- Circuit defaults are exactly: threshold `5`, rolling window `30s`, cooldown `15s`, maximum half-open probes `1`.
- `MaxGatewayInflight=0` and `MaxWaiting=0` mean disabled/unlimited; negative values are invalid.
- `/readyz` remains HTTP `200` management readiness; `/inference-readyz` is HTTP `200` with capacity and `503` without capacity.
- Keep metric labels bounded; never add request IDs, URLs, session IDs, API-key material, prompts, or generated content as labels.
- Preserve the single-replica architecture; no Redis, PostgreSQL, or distributed lease claims.
- Follow red-green-refactor TDD and commit each task only after its focused tests pass.

---

### Task 1: Deterministic circuit state machine and configuration contract

**Files:**
- Create: `internal/circuitbreaker/breaker.go`
- Create: `internal/circuitbreaker/breaker_test.go`
- Modify: `internal/domain/types.go`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`

**Interfaces:**
- Produces: `domain.InferenceOutcome` constants `InferenceSuccess`, `InferenceFailure`, `InferenceNeutral`.
- Produces: `domain.CircuitState` constants `CircuitClosed`, `CircuitOpen`, `CircuitHalfOpen`.
- Produces: `circuitbreaker.Options{FailureThreshold int, FailureWindow time.Duration, OpenCooldown time.Duration, HalfOpenMaxProbes int}`.
- Produces: `(*Breaker).Acquire(time.Time) (complete func(domain.InferenceOutcome), ok bool)` and `(*Breaker).Snapshot(time.Time) Snapshot`.
- Produces config fields `CircuitFailureThreshold`, `CircuitFailureWindow`, `CircuitOpenCooldown`, `CircuitHalfOpenMaxProbes` with the exact defaults in Global Constraints.

- [ ] **Step 1: Write failing circuit state-machine tests**

Cover exact scenarios with a fixed `base := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)`:

```go
func TestBreakerOpensWithinRollingWindowAndExpiresOldFailures(t *testing.T)
func TestBreakerCooldownAllowsOnlyOneHalfOpenProbe(t *testing.T)
func TestBreakerHalfOpenSuccessClosesAndFailureReopens(t *testing.T)
func TestBreakerNeutralOutcomeReleasesProbeWithoutHealing(t *testing.T)
func TestBreakerCompletionIsIdempotent(t *testing.T)
func TestOptionsRejectInvalidValues(t *testing.T)
```

Use an options fixture with threshold `3`, window `10*time.Second`, cooldown `5*time.Second`, and one probe. Assert `Snapshot.State`, `FailureCount`, `RetryAt`, `ProbesInFlight`, and `Available` after every transition.

- [ ] **Step 2: Run the breaker tests and verify RED**

Run: `go test ./internal/circuitbreaker -count=1`

Expected: compile failure because the package and interfaces do not exist.

- [ ] **Step 3: Implement the minimal state machine**

Use these public shapes verbatim:

```go
type Options struct {
    FailureThreshold  int
    FailureWindow     time.Duration
    OpenCooldown      time.Duration
    HalfOpenMaxProbes int
}

type Snapshot struct {
    State          domain.CircuitState
    FailureCount   int
    RetryAt        time.Time
    ProbesInFlight int
    Available      bool
}

type Breaker struct {
    mu sync.Mutex
    options Options
    state domain.CircuitState
    failures []time.Time
    openedAt time.Time
    probesInFlight int
}
```

`Acquire` must prune closed-state failures, transition an expired open circuit to half-open, reserve a probe before returning, and return an idempotent completion closure. Completion records only the three explicit outcomes from the spec.

- [ ] **Step 4: Add domain and configuration tests**

Extend `TestLoadUsesDefaults`, `TestLoadReadsOverrides`, and invalid-config coverage for:

```text
LLMGW_CIRCUIT_FAILURE_THRESHOLD
LLMGW_CIRCUIT_FAILURE_WINDOW
LLMGW_CIRCUIT_OPEN_COOLDOWN
LLMGW_CIRCUIT_HALF_OPEN_MAX_PROBES
```

Assert defaults `5`, `30s`, `15s`, `1`; reject zero/negative counts and non-positive durations.

- [ ] **Step 5: Implement domain enums and config parsing**

Add the exact enum values:

```go
const (
    InferenceSuccess InferenceOutcome = "success"
    InferenceFailure InferenceOutcome = "failure"
    InferenceNeutral InferenceOutcome = "neutral"
)
const (
    CircuitClosed CircuitState = "closed"
    CircuitOpen CircuitState = "open"
    CircuitHalfOpen CircuitState = "half_open"
)
```

Parse the four environment variables with existing `intValue` and `durationValue` helpers and validate them in `Config.validate`.

- [ ] **Step 6: Verify Task 1 GREEN and race safety**

Run:

```bash
go test ./internal/circuitbreaker ./internal/config ./internal/domain -count=1
go test -race ./internal/circuitbreaker -count=1
```

Expected: all selected packages pass with zero race reports.

- [ ] **Step 7: Commit Task 1**

```bash
git add internal/circuitbreaker internal/domain/types.go internal/config/config.go internal/config/config_test.go
git commit -m "feat: add inference circuit state machine"
```

---

### Task 2: Classify proxy attempts and enforce backend circuit permits

**Files:**
- Modify: `internal/proxy/proxy.go`
- Modify: `internal/proxy/proxy_test.go`
- Modify: `internal/monitor/manager.go`
- Modify: `internal/monitor/manager_test.go`
- Modify: `internal/monitor/worker.go`
- Modify: `internal/monitor/worker_test.go`
- Modify: `internal/gateway/service.go`
- Modify: `internal/gateway/service_retry_test.go`
- Modify: `internal/httpapi/public_test.go`
- Modify: `tests/integration/streaming_retry_test.go`
- Modify: `cmd/gateway/main.go`

**Interfaces:**
- Consumes: Task 1 breaker, outcome, state, and config interfaces.
- Produces: `proxy.Target.Complete func(domain.InferenceOutcome)` invoked exactly once per attempt.
- Produces: `gateway.Runtime.AcquireBackend(backendID int64, at time.Time) (func(domain.InferenceOutcome), bool)`.
- Produces backend runtime fields `CircuitState`, `CircuitFailures`, `CircuitRetryAt`, `CircuitProbesInFlight`, `CircuitAvailable`.

- [ ] **Step 1: Write failing proxy attempt-classification tests**

Add focused tests proving:

```go
func TestForwardCompletesTransportFailureBeforeSelectingAlternate(t *testing.T)
func TestForwardClassifiesUpstream5xxAsFailureWithoutRetry(t *testing.T)
func TestForwardClassifiesCompleted4xxAsSuccess(t *testing.T)
func TestForwardClassifiesCancellationAndDownstreamWriteFailureAsNeutral(t *testing.T)
func TestForwardCompletesEverySelectedTargetExactlyOnce(t *testing.T)
```

For the retry test, append callback and selector events to a slice and assert the order is `attempt-a:failure`, `select-alternate`, `attempt-b:success`.

- [ ] **Step 2: Run proxy tests and verify RED**

Run: `go test ./internal/proxy -count=1`

Expected: compile failure because `Target.Complete` and outcome classification do not exist.

- [ ] **Step 3: Implement attempt completion in the proxy**

Add `Complete func(domain.InferenceOutcome)` to `Target`. Refactor `forwardOnce` to return both the existing `Result` and one explicit `domain.InferenceOutcome`. Call the target callback exactly once immediately after `forwardOnce` and before retry selection. Preserve all current response, retry-count, byte-count, flush, and cancellation behavior.

- [ ] **Step 4: Write failing monitor acquisition tests**

Add tests with a healthy worker and explicit times:

```go
func TestManagerAcquireBackendTracksInflightAndCircuitOutcome(t *testing.T)
func TestManagerOpenCircuitRejectsUntilHalfOpenProbe(t *testing.T)
func TestManagerBackendCompletionIsIdempotent(t *testing.T)
func TestManagerReconcileKeepsOrResetsCircuitWithWorkerIdentity(t *testing.T)
```

Open the breaker with the configured threshold while health and metrics remain healthy. Assert `CircuitAvailable=false` while open, a single completion callback after cooldown, close on success, and immediate reopen on half-open failure.

- [ ] **Step 5: Integrate breakers into monitor manager**

Add `breaker *circuitbreaker.Breaker` to `managedWorker`. Construct it from `monitor.Options.Circuit`. Replace `IncrementInflight` with:

```go
func (m *Manager) AcquireBackend(backendID int64, at time.Time) (func(domain.InferenceOutcome), bool)
```

The returned closure is idempotent, completes the breaker permit, and decrements worker in-flight. `Snapshot` combines worker state with breaker snapshot fields. `observePoolLocked` excludes a backend when `CircuitAvailable` is false.

- [ ] **Step 6: Write failing gateway retry/circuit tests**

Update runtime test doubles to expose `AcquireBackend`. Assert:

- initial selection skips a circuit-open backend;
- a failed first attempt completes backend A before selecting backend B;
- an HTTP `5xx` records failure but is still forwarded and not retried;
- cancellation records neutral and releases the permit;
- per-attempt observer backend in-flight increments/decrements stay balanced.

- [ ] **Step 7: Integrate atomic backend acquisition into gateway**

In `selectTarget`, router selection remains pressure/affinity based. After choosing a candidate, call `AcquireBackend`; when acquisition races and fails, add that backend to a local exclusion set and select again. Attach the returned completion closure to `proxy.Target.Complete` together with the backend observer decrement. Remove the old manual retry release path and preserve final request-event backend attribution.

Wire Task 1 circuit config into `monitor.Options` in `cmd/gateway/main.go`.

- [ ] **Step 8: Verify Task 2 GREEN and race safety**

Run:

```bash
go test ./internal/proxy ./internal/monitor ./internal/gateway ./internal/httpapi ./tests/integration -count=1
go test -race ./internal/proxy ./internal/monitor ./internal/gateway -count=1
```

Expected: all selected packages pass; retry and streaming integration tests remain green.

- [ ] **Step 9: Commit Task 2**

```bash
git add internal/proxy internal/monitor internal/gateway internal/httpapi/public_test.go tests/integration/streaming_retry_test.go cmd/gateway/main.go
git commit -m "feat: isolate failing inference backends"
```

---

### Task 3: Persist and administer model-pool safety limits

**Files:**
- Create: `internal/store/migrations/002_pool_safety.sql`
- Modify: `internal/store/sqlite.go`
- Modify: `internal/store/sqlite_test.go`
- Modify: `internal/store/models.go`
- Modify: `internal/store/pools.go`
- Modify: `internal/store/snapshot.go`
- Modify: `internal/store/crud_test.go`
- Modify: `internal/domain/types.go`
- Modify: `internal/domain/validate.go`
- Modify: `internal/domain/validate_test.go`
- Modify: `internal/httpapi/admin.go`
- Modify: `internal/httpapi/admin_test.go`
- Modify: `internal/web/web.go`
- Modify: `internal/web/templates/backends.html`
- Modify: `internal/web/templates/dashboard.html`
- Modify: `internal/web/web_test.go`
- Modify: `tests/integration/harness_test.go`

**Interfaces:**
- Produces `domain.ModelPool.MaxGatewayInflight int` and `domain.ModelPool.MaxWaiting int`.
- Produces matching `CreatePoolParams`, `PoolInput`, `AdminPool`, JSON fields `maxGatewayInflight` and `maxWaiting`, and HTML number inputs `min="0"`.
- Produces schema `user_version=2` through ordered embedded migrations.

- [ ] **Step 1: Write failing migration tests**

Create a version-1 database by applying the current `001_initial.sql`, insert a pool, close it, and open it through `store.Open`. Assert:

```go
func TestSQLiteMigratesVersionOnePoolSafetyDefaults(t *testing.T)
```

The existing row must survive with both limits equal to zero, `PRAGMA user_version` must equal `2`, and reopening must not reapply `ALTER TABLE`.

- [ ] **Step 2: Run store tests and verify RED**

Run: `go test ./internal/store -run 'TestSQLiteMigratesVersionOnePoolSafetyDefaults|TestSQLiteMigratesAndReopens' -count=1`

Expected: failure because versioned migrations and new columns do not exist.

- [ ] **Step 3: Implement ordered migrations**

Embed `migrations/*.sql`, define ordered entries for versions 1 and 2, read `PRAGMA user_version`, and run each missing migration in its own transaction. Use trusted integer formatting for `PRAGMA user_version = N` and advance only inside the successful transaction.

`002_pool_safety.sql` must add:

```sql
ALTER TABLE model_pools ADD COLUMN max_gateway_inflight INTEGER NOT NULL DEFAULT 0 CHECK (max_gateway_inflight >= 0);
ALTER TABLE model_pools ADD COLUMN max_waiting INTEGER NOT NULL DEFAULT 0 CHECK (max_waiting >= 0);
```

- [ ] **Step 4: Write failing domain/store round-trip tests**

Extend validation and CRUD/snapshot tests with non-zero values (`MaxGatewayInflight: 17`, `MaxWaiting: 9`), zero defaults, negative rejection, update persistence, and immutable snapshot publication.

- [ ] **Step 5: Implement domain and SQLite CRUD fields**

Add both fields to all pool SELECT, INSERT, UPDATE, scan, and snapshot paths in the same order. `ModelPool.Validate` rejects either negative value and otherwise preserves zero.

- [ ] **Step 6: Write failing Admin API/UI tests**

Extend create/update JSON tests and rendered page tests to assert:

```json
{"maxGatewayInflight":17,"maxWaiting":9}
```

and form controls:

```html
<input name="max_gateway_inflight" type="number" min="0">
<input name="max_waiting" type="number" min="0">
```

Assert edits are prefilled and negative values return controlled validation errors.

- [ ] **Step 7: Implement Admin API/UI plumbing**

Add the two fields to `PoolInput`, create/update service methods, `AdminPool`, form parsing, templates, and integration-harness pool creation helpers. Keep existing JSON field names stable and additive.

- [ ] **Step 8: Verify Task 3 GREEN**

Run:

```bash
go test ./internal/domain ./internal/store ./internal/httpapi ./internal/web ./tests/integration -count=1
go test -race ./internal/store ./internal/httpapi -count=1
```

Expected: migration, CRUD, API, UI, and existing Admin behavior all pass.

- [ ] **Step 9: Commit Task 3**

```bash
git add internal/store internal/domain internal/httpapi internal/web tests/integration/harness_test.go
git commit -m "feat: persist model pool safety limits"
```

---

### Task 4: Enforce pool safety, expose inference readiness, and add metrics

**Files:**
- Modify: `internal/domain/types.go`
- Modify: `internal/monitor/manager.go`
- Modify: `internal/monitor/manager_test.go`
- Modify: `internal/gateway/service.go`
- Modify: `internal/httpapi/public.go`
- Modify: `internal/httpapi/public_test.go`
- Modify: `internal/observability/metrics.go`
- Modify: `internal/observability/metrics_test.go`
- Modify: `internal/httpapi/admin.go`
- Modify: `internal/web/templates/dashboard.html`
- Modify: `cmd/gateway/main.go`
- Modify: `cmd/gateway/main_test.go`
- Modify: `tests/integration/admission_priority_test.go`
- Modify: `tests/integration/routing_health_test.go`

**Interfaces:**
- Consumes Task 2 backend acquisition and Task 3 pool limits.
- Produces `gateway.Runtime.AcquirePool(poolID int64, maximum int) (func(), bool)`.
- Produces `PoolRuntime.GatewayInflight int` and `PoolRuntime.TotalWaiting float64`.
- Produces `Service.InferenceReadiness() InferenceReadiness` with `Revision`, `PoolAvailability`, and `BackendAvailability`.
- Produces `GET /inference-readyz` response status semantics from Global Constraints.

- [ ] **Step 1: Write failing manager pool lease and aggregation tests**

Add:

```go
func TestManagerAcquirePoolEnforcesPositiveLimitAndTracksUnlimited(t *testing.T)
func TestManagerPoolReleaseIsIdempotent(t *testing.T)
func TestManagerPoolSnapshotAggregatesWaitingAndCircuitAvailability(t *testing.T)
```

Assert limit `2` accepts exactly two concurrent leases, zero accepts without a cap while still counting, releases return to zero, waiting sums healthy/fresh non-draining workers, and open circuits reduce `AvailableBackends` without erasing `TotalWaiting`.

- [ ] **Step 2: Run monitor tests and verify RED**

Run: `go test ./internal/monitor -run 'TestManagerAcquirePool|TestManagerPool' -count=1`

Expected: compile failure because the pool runtime/acquisition fields do not exist.

- [ ] **Step 3: Implement pool runtime acquisition and aggregation**

Store per-pool counters under `Manager.mu`. `AcquirePool` checks a positive maximum atomically, increments for both limited and unlimited pools, and returns an idempotent release. `PoolSnapshot` overlays the current counter. `observePoolLocked` calculates total waiting independently from circuit eligibility.

- [ ] **Step 4: Write failing gateway admission tests**

Add tests that hold real leases through a blocking forwarder and assert:

- the second request receives `429 gateway_overloaded` at `MaxGatewayInflight=1`;
- `TotalWaiting >= MaxWaiting` receives the same bounded `429` and positive `Retry-After`;
- zero limits preserve current behavior;
- pool lease release occurs after normal completion, stream cancellation, upstream error, and selection failure;
- client priority cannot bypass pool safety.

- [ ] **Step 5: Implement pool safety in `Service.Forward`**

After pool state availability, compare positive `MaxWaiting`, acquire the pool lease, defer its release, then apply the existing priority-adjusted client limiter. Do not change authentication/model error precedence or API envelopes.

- [ ] **Step 6: Write failing inference-readiness tests**

Test `Service.InferenceReadiness` and the composed HTTP route for these cases:

| Condition | Expected |
|---|---|
| healthy/fresh/closed backend with available secret | `200 ready` |
| all circuits open | `503 unavailable` |
| metrics stale, unhealthy, disabled, or draining | `503 unavailable` |
| configured upstream secret missing | `503 unavailable` |
| half-open probe capacity available | `200 ready` |
| pool max in-flight or waiting reached | still `200 ready` |

Assert JSON keys `status`, `revision`, `poolAvailability`, and `backendAvailability`. Assert `/readyz` remains HTTP `200` with zero backend availability.

- [ ] **Step 7: Implement service readiness and route**

Add a read-only gateway method that uses the immutable registry snapshot, runtime snapshots, and upstream-secret resolution. Register `/inference-readyz` next to `/readyz` in `cmd/gateway/main.go`; write `503` only when both availability counts are zero.

- [ ] **Step 8: Add failing Prometheus tests**

Extend the metrics family test to require:

```text
llmgw_backend_circuit_state
llmgw_backend_circuit_failures
llmgw_pool_gateway_inflight
llmgw_pool_waiting_requests
llmgw_pool_available_backends
```

Assert the circuit state numeric mapping `closed=0`, `open=1`, `half_open=2` and no new high-cardinality labels.

- [ ] **Step 9: Implement metrics and Admin runtime display**

Extend `Metrics.SetBackend` and add `Metrics.SetPool(model string, runtime domain.PoolRuntime)`. Update the periodic metrics publisher to set backend and pool gauges. Render pool in-flight/waiting/availability and backend circuit state in the dashboard without JavaScript-only content.

- [ ] **Step 10: Verify Task 4 GREEN and integration behavior**

Run:

```bash
go test ./cmd/gateway ./internal/monitor ./internal/gateway ./internal/httpapi ./internal/observability ./tests/integration -count=1
go test -race ./cmd/gateway ./internal/monitor ./internal/gateway ./tests/integration -count=1
```

Expected: pool limits, readiness transitions, metrics, priority isolation, routing health, and lifecycle tests pass.

- [ ] **Step 11: Commit Task 4**

```bash
git add internal/domain/types.go internal/monitor internal/gateway internal/httpapi internal/observability internal/web/templates/dashboard.html cmd/gateway tests/integration
git commit -m "feat: enforce pool safety and inference readiness"
```

---

### Task 5: Real-vLLM resilience E2E and operating documentation

**Files:**
- Modify: `tests/e2e/harness_test.go`
- Modify: `tests/e2e/harness_unit_test.go`
- Modify: `tests/e2e/real_vllm_test.go`
- Modify: `Makefile`
- Modify: `README.md`
- Modify: `docs/technical-specification.md`
- Modify: `docs/acceptance-evidence.md`
- Modify: `docs/real-gpu-testing.md`
- Modify: `docs/real-vllm-priority-e2e.md`

**Interfaces:**
- Consumes all prior tasks.
- Produces E2E modes `smoke`, `priority`, and `resilience`.
- Produces environment variables `LLMGW_E2E_CIRCUIT_BACKEND_ID` and `LLMGW_E2E_CIRCUIT_FAILURE_COUNT` for the isolated resilience scenario.
- Produces cleanup that restores backend URL, drain state, and pool safety values even after an assertion failure.

- [ ] **Step 1: Write failing E2E harness unit tests**

Add unit coverage for:

```go
func TestFaultProxyPassesHealthMetricsAndCanToggleInference5xx(t *testing.T)
func TestAdminMutationCleanupRestoresPoolAndBackends(t *testing.T)
func TestLoadE2EConfigAcceptsResilienceAndRejectsMissingCircuitBackend(t *testing.T)
func TestInferenceReadinessHonorsContextDeadline(t *testing.T)
```

The fault proxy must forward `/health`, `/metrics`, and successful inference byte-for-byte, but return a deterministic `503` OpenAI-shaped body while faulting.

- [ ] **Step 2: Run E2E unit tests and verify RED**

Run: `go test ./tests/e2e -run 'TestFaultProxy|TestAdminMutationCleanup|TestLoadE2EConfigAcceptsResilience|TestInferenceReadinessHonors' -count=1`

Expected: compile failure because resilience helpers and mode do not exist.

- [ ] **Step 3: Implement E2E harness extensions**

Add typed readiness/runtime fields, Admin update helpers, a loopback reverse fault proxy, polling for backend circuit state, and a cleanup object that records original values before any mutation. Never log secrets or full API keys.

- [ ] **Step 4: Extend smoke and priority assertions**

Smoke must require `/inference-readyz` HTTP `200` and all new Prometheus families. Priority mode must observe non-zero pool gateway in-flight and waiting during load, temporarily set a positive pool limit, prove even Critical receives the bounded pool-safety `429`, restore the limit, and prove Critical streaming continuity again.

- [ ] **Step 5: Add the isolated resilience E2E**

`TestCircuitBreakerRecoveryWithRealVLLM` performs this exact sequence:

1. capture the target backend, pool, and every sibling drain state;
2. start a local fault proxy forwarding to the target real-vLLM URL;
3. update the target backend URL to the proxy and drain siblings;
4. enable inference `503` and send the configured failure count using distinct request IDs;
5. wait for Admin runtime circuit state `open` while `/health` and `/metrics` remain healthy/fresh;
6. assert `/inference-readyz` is `503 unavailable`;
7. disable the fault, wait through cooldown, send one streaming probe, and wait for circuit `closed`;
8. assert `/inference-readyz` is `200 ready`;
9. restore target URL, all drain states, and pool limits in cleanup.

The test runs only in `LLMGW_E2E_MODE=resilience` and refuses a non-loopback gateway URL because its fault proxy must be reachable by the gateway on the same host.

- [ ] **Step 6: Verify deterministic E2E package GREEN**

Run: `go test ./tests/e2e -count=1`

Expected: unit tests pass and external modes skip when `LLMGW_E2E_MODE` is unset.

- [ ] **Step 7: Update documentation**

Document exact circuit defaults/env vars, outcome behavior, persisted pool fields, migration, `/readyz` versus `/inference-readyz`, metrics, Admin fields, three E2E modes, cleanup guarantees, and single-replica limitation. Update acceptance mappings to named tests and remove only the implemented gaps from “Not implemented”.

- [ ] **Step 8: Run real local multi-vLLM E2E**

With two local vLLM serving processes configured as in `docs/real-vllm-priority-e2e.md`, run:

```bash
LLMGW_E2E_MODE=smoke make test-real-vllm
LLMGW_E2E_MODE=priority make test-real-vllm
LLMGW_E2E_MODE=resilience make test-real-vllm
```

Capture vLLM versions, backend count, circuit transition evidence, pool safety rejection, and recovery timings in the implementation report without recording secrets.

- [ ] **Step 9: Run full verification**

Run fresh, complete commands:

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/...
make build-linux-amd64
make build-e2e-linux-amd64
make container-smoke
git diff --check
```

If Docker is unavailable, record the exact daemon error; all other commands remain required.

- [ ] **Step 10: Commit Task 5**

```bash
git add tests/e2e Makefile README.md docs
git commit -m "test: cover gateway resilience with real vllm"
```
