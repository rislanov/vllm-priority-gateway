# Lightweight vLLM Priority Gateway MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the complete Go MVP described by the source requirements: a tested priority-aware, load-aware, streaming vLLM gateway with embedded Admin UI, SQLite, Fake vLLM, Load Generator, English documentation, and Linux amd64 delivery artifacts.

**Architecture:** Build a modular Go monolith whose pure policy packages implement pressure, hysteresis, admission, concurrency, and routing. A SQLite-backed immutable configuration registry feeds independent backend monitors and an HTTP orchestration layer; the gateway streams upstream bodies directly and embeds a server-rendered Admin UI. Fake vLLM and Load Generator are separate commands in the same module.

**Tech Stack:** Go 1.27, `net/http`, chi v5, `database/sql`, pure-Go modernc SQLite, Prometheus Go client/common, `log/slog`, `html/template`, embedded static assets.

**Spec:** `docs/superpowers/specs/2026-08-26-vllm-priority-gateway-mvp-design.md`

## Global Constraints

- Deployment target: Linux x86_64 (`linux/amd64`); development and all fake-backend tests run natively on macOS without Docker or NVIDIA hardware.
- One gateway process, one SQLite database, and no Redis, PostgreSQL, Kafka, Kubernetes dependency, Node, or React.
- Supported public routes are exactly `GET /v1/models`, `POST /v1/chat/completions`, `POST /v1/completions`, and `POST /v1/responses`.
- Client API keys use `llmgw_` plus 32 random bytes encoded as unpadded base64url; only prefix plus HMAC-SHA-256 digest is stored.
- vLLM priority is always server-controlled; remove client `X-Vllm-Priority` and JSON `priority`, then set the configured integer header.
- Default health interval is 2 seconds, metrics interval 1 second, metrics staleness 5 seconds, unhealthy after 3 failures, and recovered after 2 successes.
- Pressure weights are queue 0.55, KV 0.30, running 0.15; queue soft limit 2; KV soft/hard limits 0.80/0.95; EWMA window 4 seconds.
- Overload entry persists 3 seconds; recovery persists 10 seconds; thresholds are busy 0.70, saturated 1.00, emergency 1.40, with recovery boundaries 0.55, 0.85, and 1.20.
- Admission percentages are the exact normal/busy/saturated/emergency table in the design spec and apply to `floor(MaxConcurrency * percentage)`.
- The concurrency lease spans the full response body/SSE lifecycle and is released exactly once on EOF, error, cancellation, or shutdown.
- At most one retry is allowed on a different backend, only for transport failure before any downstream byte; never retry an HTTP response or a started stream.
- Prompts, generated content, raw authorization headers, full API keys, and upstream secrets are never logged or exposed as metric labels.
- Every behavior change follows red-green-refactor: write a test, run it and observe the expected failure, add the minimum implementation, then run the focused and affected suites.
- The source requirement file `ТЗ_ Lightweight vLLM Priority Gateway.md` is user-owned and must not be modified or staged unless the user explicitly requests it.

---

## File map

| Area | Responsibility |
|---|---|
| `internal/domain` | Stable domain types and validation shared by policy packages |
| `internal/config` | Environment parsing, safe defaults, and startup validation |
| `internal/apikey` | Cryptographic key generation and constant-time verification |
| `internal/store` | SQLite migrations, CRUD, and snapshot loading |
| `internal/registry` | Atomic immutable configuration snapshots |
| `internal/pressure` | Pressure formula, EWMA, and pool hysteresis |
| `internal/admission` | Priority policy and full-lifecycle local leases |
| `internal/routing` | Candidate filtering and least-pressure selection |
| `internal/fakevllm` | Deterministic upstream simulator implementation |
| `internal/monitor` | Per-backend health/metrics workers and runtime snapshots |
| `internal/proxy` | Header filtering, upstream transport, retry, streaming, cancellation |
| `internal/gateway` | Public-request orchestration independent of HTTP rendering |
| `internal/httpapi` | Public/Admin routes, middleware, errors, readiness |
| `internal/observability` | Prometheus instruments and structured completion logs |
| `internal/web` | Embedded templates, CSS, and progressive JavaScript |
| `internal/loadgen` | Workload generation and percentile aggregation |
| `cmd/*` | Process wiring for gateway, fake backend, and load generator |
| `tests/integration` | Black-box multi-component acceptance tests |

---

### Task 1: Go module, domain types, and validated configuration

**Files:**
- Create: `go.mod`
- Create: `.gitignore`
- Create: `internal/domain/types.go`
- Create: `internal/domain/validate.go`
- Create: `internal/domain/validate_test.go`
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`

**Interfaces:**
- Produces: `domain.PriorityClass`, `domain.PoolState`, `domain.Client`, `domain.ModelPool`, `domain.Backend`, `domain.BackendRuntime`, and validation methods.
- Produces: `config.Load(lookup func(string) (string, bool)) (Config, error)` with all defaults and required secret validation.
- Consumes: only the Go standard library.

- [ ] **Step 1: Initialize the module and ignore local/runtime artifacts**

```go
module github.com/rislanov/vllm-priority-gateway

go 1.27.0
```

`.gitignore` must contain `.tools/`, `data/`, `dist/`, `coverage.out`, and macOS `.DS_Store`.

- [ ] **Step 2: Write failing table tests for domain validation and config defaults**

```go
func TestBackendValidateRejectsUnsafeBaseURL(t *testing.T) {
    backend := domain.Backend{Name: "gpu-1", BaseURL: "http://user:pass@host:8000?x=1", RunningSoftLimit: 8}
    if err := backend.Validate(); err == nil {
        t.Fatal("expected unsafe backend URL to be rejected")
    }
}

func TestLoadUsesMVPDefaults(t *testing.T) {
    env := map[string]string{
        "LLMGW_ADMIN_USERNAME": "admin",
        "LLMGW_ADMIN_PASSWORD": "correct horse battery staple",
        "LLMGW_API_KEY_HMAC_SECRET": strings.Repeat("s", 32),
    }
    cfg, err := config.Load(func(key string) (string, bool) { value, ok := env[key]; return value, ok })
    if err != nil { t.Fatal(err) }
    if cfg.ListenAddress != ":8080" || cfg.MetricsInterval != time.Second || cfg.MetricsStaleAfter != 5*time.Second {
        t.Fatalf("unexpected defaults: %+v", cfg)
    }
}
```

- [ ] **Step 3: Run focused tests and confirm failure because packages/types do not exist**

Run: `go test ./internal/domain ./internal/config`

Expected: compile failure naming missing domain/config declarations.

- [ ] **Step 4: Add exact domain enums, structs, validation, and config parser**

```go
type PriorityClass string
const (
    PriorityCritical PriorityClass = "critical"
    PriorityHigh PriorityClass = "high"
    PriorityNormal PriorityClass = "normal"
    PriorityBackground PriorityClass = "background"
)

type PoolState string
const (
    PoolNormal PoolState = "normal"
    PoolBusy PoolState = "busy"
    PoolSaturated PoolState = "saturated"
    PoolEmergency PoolState = "emergency"
    PoolUnavailable PoolState = "unavailable"
)
```

`Backend.Validate` must accept only absolute HTTP/HTTPS URLs with host, no userinfo/query/fragment, positive `RunningSoftLimit`, and positive `CapacityHint`; normalize a trailing slash. `Config` must expose every global default in the design and reject missing/short credentials, invalid durations, invalid threshold ordering, and request-body limits below 1 KiB.

- [ ] **Step 5: Run tests and formatting**

Run: `gofmt -w internal/domain internal/config && go test ./internal/domain ./internal/config`

Expected: PASS.

- [ ] **Step 6: Commit the foundation**

```sh
git add go.mod .gitignore internal/domain internal/config
git commit -m "feat: establish gateway domain and configuration"
```

---

### Task 2: API-key cryptography

**Files:**
- Create: `internal/apikey/apikey.go`
- Create: `internal/apikey/apikey_test.go`

**Interfaces:**
- Consumes: server HMAC secret from `config.Config`.
- Produces: `apikey.Generate(io.Reader) (Plaintext, error)`, `apikey.Digest(secret []byte, raw string) [32]byte`, and `apikey.Verify(secret []byte, raw string, expected [32]byte) bool`.
- `Plaintext` fields are `Value string` and `Prefix string`; it has no JSON/string formatter that can accidentally log the secret.

- [ ] **Step 1: Write failing tests for format, entropy input, digest, and rejection**

```go
func TestGenerateProducesExpectedFormatAndPrefix(t *testing.T) {
    raw := bytes.Repeat([]byte{0x42}, 32)
    key, err := apikey.Generate(bytes.NewReader(raw))
    if err != nil { t.Fatal(err) }
    if key.Value != "llmgw_QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI" { t.Fatalf("value=%q", key.Value) }
    if key.Prefix != key.Value[:12] { t.Fatalf("prefix=%q", key.Prefix) }
}

func TestVerifyRejectsDifferentKey(t *testing.T) {
    secret := []byte("01234567890123456789012345678901")
    digest := apikey.Digest(secret, "llmgw_original")
    if apikey.Verify(secret, "llmgw_attacker", digest) { t.Fatal("accepted different key") }
}
```

- [ ] **Step 2: Run the test and observe missing package failure**

Run: `go test ./internal/apikey`

Expected: compile failure for missing functions.

- [ ] **Step 3: Implement generation and constant-time HMAC verification**

```go
func Digest(secret []byte, raw string) [32]byte {
    mac := hmac.New(sha256.New, secret)
    _, _ = io.WriteString(mac, raw)
    var out [32]byte
    copy(out[:], mac.Sum(nil))
    return out
}

func Verify(secret []byte, raw string, expected [32]byte) bool {
    actual := Digest(secret, raw)
    return subtle.ConstantTimeCompare(actual[:], expected[:]) == 1
}
```

`Generate` must use exactly 32 bytes from the injected reader, prefix with `llmgw_`, and use `base64.RawURLEncoding`.

- [ ] **Step 4: Run focused tests**

Run: `gofmt -w internal/apikey && go test ./internal/apikey`

Expected: PASS.

- [ ] **Step 5: Commit**

```sh
git add internal/apikey
git commit -m "feat: add secure client API keys"
```

---

### Task 3: SQLite persistence and atomic registry

**Files:**
- Create: `internal/store/migrations/001_initial.sql`
- Create: `internal/store/sqlite.go`
- Create: `internal/store/models.go`
- Create: `internal/store/clients.go`
- Create: `internal/store/pools.go`
- Create: `internal/store/backends.go`
- Create: `internal/store/snapshot.go`
- Create: `internal/store/sqlite_test.go`
- Create: `internal/registry/registry.go`
- Create: `internal/registry/registry_test.go`

**Interfaces:**
- Produces: `store.Open(ctx, path) (*SQLite, error)`, `Migrate`, CRUD methods matching Admin API resources, `LoadSnapshot`, and `TouchKeyLastUsed`.
- Produces: `registry.New(loader Loader)`, `Reload(ctx) error`, and `Snapshot() *Snapshot` backed by `atomic.Pointer`.
- `registry.Snapshot` contains clients by ID, key candidates by prefix, enabled pools by public name, explicit access pairs, and backends by pool ID.
- Consumes: `domain` and `apikey` digest bytes; no plaintext client or upstream secret is accepted by persistence APIs.

- [ ] **Step 1: Write failing real-SQLite tests for migration, CRUD, and secret absence**

```go
func TestSQLiteRoundTripAndNoPlaintextKey(t *testing.T) {
    db := openTestDB(t)
    client, err := db.CreateClient(context.Background(), store.CreateClientParams{
        Name: "codex-ci", Enabled: true, PriorityClass: domain.PriorityBackground,
        VLLMPriority: 100, MaxConcurrency: 20,
    })
    if err != nil { t.Fatal(err) }
    digest := sha256.Sum256([]byte("not-the-real-production-digest"))
    _, err = db.CreateAPIKey(context.Background(), client.ID, "llmgw_abcd", digest[:], nil)
    if err != nil { t.Fatal(err) }
    bytesOnDisk, err := os.ReadFile(db.Path())
    if err != nil { t.Fatal(err) }
    if bytes.Contains(bytesOnDisk, []byte("llmgw_secret-value")) { t.Fatal("plaintext secret found") }
}
```

Also test expiry/revocation, foreign-key conflicts, duplicate names, backend URL persistence, access replacement, migration idempotence, and reopen behavior.

- [ ] **Step 2: Run and confirm missing implementation failure**

Run: `go test ./internal/store ./internal/registry`

Expected: compile failure for missing store and registry.

- [ ] **Step 3: Add the exact schema and migration transaction**

Run: `go get modernc.org/sqlite@latest`

Create the five tables from the design with `CHECK` constraints, foreign keys, unique names, UTC RFC3339Nano timestamps, and indexes on API-key prefix/backend pool. Open SQLite with `_pragma=journal_mode(WAL)`, `_pragma=foreign_keys(1)`, and `_pragma=busy_timeout(5000)`; cap open connections to four.

- [ ] **Step 4: Implement CRUD and immutable snapshot loading**

```go
type Loader interface {
    LoadSnapshot(context.Context) (Snapshot, error)
}

type Registry struct {
    loader Loader
    current atomic.Pointer[Snapshot]
}

func (r *Registry) Snapshot() *Snapshot {
    return r.current.Load()
}
```

`Reload` must build a complete new snapshot, validate cross-references, and publish it only after successful loading. Copy slices/maps before publication so callers cannot mutate shared state.

- [ ] **Step 5: Add a registry test proving failed reload preserves the last good snapshot**

```go
func TestReloadFailurePreservesPublishedSnapshot(t *testing.T) {
    loader := &sequenceLoader{results: []result{{snapshot: registry.Snapshot{Revision: 1}}, {err: errors.New("db down")}}}
    reg := registry.New(loader)
    if err := reg.Reload(context.Background()); err != nil { t.Fatal(err) }
    if err := reg.Reload(context.Background()); err == nil { t.Fatal("expected reload error") }
    if got := reg.Snapshot().Revision; got != 1 { t.Fatalf("revision=%d", got) }
}
```

- [ ] **Step 6: Run persistence suites, including concurrent reads/writes**

Run: `gofmt -w internal/store internal/registry && go test ./internal/store ./internal/registry`

Expected: PASS.

- [ ] **Step 7: Commit**

```sh
git add internal/store internal/registry
git commit -m "feat: persist gateway configuration in SQLite"
```

---

### Task 4: Pressure, EWMA, and hysteretic pool state

**Files:**
- Create: `internal/pressure/pressure.go`
- Create: `internal/pressure/ewma.go`
- Create: `internal/pressure/state.go`
- Create: `internal/pressure/pressure_test.go`
- Create: `internal/pressure/state_test.go`

**Interfaces:**
- Produces: `pressure.Calculate(Sample, Limits) float64`.
- Produces: `(*EWMA).Add(at time.Time, value float64) float64` with injected timestamps.
- Produces: `(*PoolMachine).Observe(at time.Time, best float64, allWaiting bool, available bool) domain.PoolState`.
- Consumes: pressure thresholds/durations from `config.Config` and `domain.PoolState`.

- [ ] **Step 1: Write literal table tests for normalization and weights**

```go
func TestCalculateUsesSpecifiedWeights(t *testing.T) {
    got := pressure.Calculate(
        pressure.Sample{Running: 8, Waiting: 2, KVUsage: 0.875},
        pressure.Limits{QueueSoft: 2, KVSoft: .80, KVHard: .95, RunningSoft: 8},
    )
    const want = 0.85 // .55*1 + .30*.5 + .15*1
    if math.Abs(got-want) > 1e-9 { t.Fatalf("got=%v want=%v", got, want) }
}
```

Include clamp-at-two, zero/invalid-limit rejection, and time-aware EWMA literals.

- [ ] **Step 2: Write fake-clock state tests for enter and recovery windows**

```go
func TestPoolMachineIgnoresShortBusySpike(t *testing.T) {
    m := pressure.NewPoolMachine(testThresholds())
    start := time.Unix(100, 0)
    if got := m.Observe(start, .8, false, true); got != domain.PoolNormal { t.Fatal(got) }
    if got := m.Observe(start.Add(2*time.Second), .8, false, true); got != domain.PoolNormal { t.Fatal(got) }
    if got := m.Observe(start.Add(2500*time.Millisecond), .4, false, true); got != domain.PoolNormal { t.Fatal(got) }
}
```

Cover all-waiting saturation, emergency, one-state-at-a-time recovery, unavailable immediately, and backend recovery.

- [ ] **Step 3: Run and observe missing package failure**

Run: `go test ./internal/pressure`

Expected: compile failure.

- [ ] **Step 4: Implement pure pressure functions and state machine**

Use `alpha = 1 - exp(-elapsed/window)` for EWMA after the first sample. The pool machine tracks candidate state and `candidateSince`; interrupted persistence resets the candidate timer. `available=false` always returns unavailable immediately.

- [ ] **Step 5: Run focused tests**

Run: `gofmt -w internal/pressure && go test ./internal/pressure`

Expected: PASS.

- [ ] **Step 6: Commit**

```sh
git add internal/pressure
git commit -m "feat: calculate backend and pool pressure"
```

---

### Task 5: Admission, full-lifecycle leases, and routing

**Files:**
- Create: `internal/admission/policy.go`
- Create: `internal/admission/limiter.go`
- Create: `internal/admission/policy_test.go`
- Create: `internal/admission/limiter_test.go`
- Create: `internal/routing/router.go`
- Create: `internal/routing/router_test.go`

**Interfaces:**
- Produces: `admission.EffectiveLimit(class, state, max) int`.
- Produces: `(*Limiter).Acquire(clientID int64, limit int) (*Lease, bool)`; `Lease.Release()` is idempotent.
- Produces: `routing.Router.Select(candidates []routing.Candidate, exclude map[int64]struct{}) (routing.Candidate, error)`.
- `routing.Candidate` contains `domain.Backend`, EWMA pressure, gateway in-flight count, and monitor eligibility.

- [ ] **Step 1: Write failing policy boundary tests with hand-derived limits**

```go
func TestEffectiveLimit(t *testing.T) {
    cases := []struct{ class domain.PriorityClass; state domain.PoolState; max, want int }{
        {domain.PriorityBackground, domain.PoolBusy, 21, 10},
        {domain.PriorityNormal, domain.PoolSaturated, 3, 1},
        {domain.PriorityHigh, domain.PoolEmergency, 1, 0},
        {domain.PriorityCritical, domain.PoolEmergency, 20, 20},
    }
    for _, tc := range cases {
        if got := admission.EffectiveLimit(tc.class, tc.state, tc.max); got != tc.want { t.Fatalf("%+v got=%d", tc, got) }
    }
}
```

- [ ] **Step 2: Write a concurrent limiter test that catches over-admission and double release**

Start 100 goroutines against limit 7, hold acquired leases behind a barrier, assert exactly seven acquisitions, release each twice, and assert `InFlight(clientID)==0`.

- [ ] **Step 3: Write routing tests for pressure, epsilon, in-flight tie, random tie, and exclusions**

```go
func TestSelectChoosesLeastPressure(t *testing.T) {
    r := routing.New(.02, routing.FixedSource(0))
    got, err := r.Select([]routing.Candidate{
        candidate(1, .30, 1), candidate(2, 1.10, 0),
    }, nil)
    if err != nil { t.Fatal(err) }
    if got.Backend.ID != 1 { t.Fatalf("backend=%d", got.Backend.ID) }
}
```

- [ ] **Step 4: Run and verify expected missing-code failures**

Run: `go test ./internal/admission ./internal/routing`

Expected: compile failures.

- [ ] **Step 5: Implement the table, CAS/mutex-safe lease accounting, and router ordering**

The limiter must decide and increment under one lock. `Release` uses `sync.Once`. Router first filters ineligible/excluded candidates, finds minimum pressure, retains epsilon ties, finds minimum in-flight, and draws uniformly from final ties using the injected source.

- [ ] **Step 6: Run focused and race tests**

Run: `gofmt -w internal/admission internal/routing && go test -race ./internal/admission ./internal/routing`

Expected: PASS with no race report.

- [ ] **Step 7: Commit**

```sh
git add internal/admission internal/routing
git commit -m "feat: enforce admission and least-pressure routing"
```

---

### Task 6: Deterministic Fake vLLM

**Files:**
- Create: `internal/fakevllm/server.go`
- Create: `internal/fakevllm/state.go`
- Create: `internal/fakevllm/server_test.go`
- Create: `cmd/fake-vllm/main.go`

**Interfaces:**
- Produces: `fakevllm.New() *Server`, `Handler() http.Handler`, and thread-safe state/control records.
- Control API: `GET /admin/state`, `PUT /admin/state` using the exact JSON state fields in the design.
- Request record API exposes received route/model/request ID/priority plus active/cancelled counters.

- [ ] **Step 1: Write failing HTTP tests for health, metrics, ordinary completion, and SSE flush timing**

```go
func TestStreamingFlushesFramesWithDelay(t *testing.T) {
    fake := fakevllm.New()
    fake.SetState(fakevllm.State{TokenDelay: 25 * time.Millisecond, Tokens: []string{"one", "two"}})
    srv := httptest.NewServer(fake.Handler())
    defer srv.Close()
    req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(`{"model":"upstream","stream":true}`))
    resp, err := http.DefaultClient.Do(req)
    if err != nil { t.Fatal(err) }
    reader := bufio.NewReader(resp.Body)
    first, err := reader.ReadString('\n')
    if err != nil { t.Fatal(err) }
    if !strings.HasPrefix(first, "data: ") { t.Fatalf("frame=%q", first) }
}
```

Also assert current metric names, legacy metric option, received priority, request cancellation, configured 5xx, and reset-before/after-byte modes.

- [ ] **Step 2: Run and confirm missing implementation failure**

Run: `go test ./internal/fakevllm`

Expected: compile failure.

- [ ] **Step 3: Implement thread-safe simulator and command flags**

The handler must implement only the routes listed in the design. SSE writes deterministic `data:` JSON frames, blank lines, flushes each frame, and ends with `data: [DONE]`. Reset modes use `http.Hijacker` only when supported and otherwise return a deterministic test error.

- [ ] **Step 4: Run tests and a command smoke build**

Run: `gofmt -w internal/fakevllm cmd/fake-vllm && go test ./internal/fakevllm && go build ./cmd/fake-vllm`

Expected: PASS/build success.

- [ ] **Step 5: Commit**

```sh
git add internal/fakevllm cmd/fake-vllm
git commit -m "feat: add deterministic fake vLLM server"
```

---

### Task 7: Backend monitoring and runtime snapshots

**Files:**
- Create: `internal/monitor/metrics.go`
- Create: `internal/monitor/worker.go`
- Create: `internal/monitor/manager.go`
- Create: `internal/monitor/metrics_test.go`
- Create: `internal/monitor/worker_test.go`
- Create: `internal/monitor/manager_test.go`

**Interfaces:**
- Consumes: registry backend snapshots, `pressure.Calculate`, configuration intervals, and an injected `http.Client`/clock.
- Produces: `monitor.Manager.Reconcile(ctx, backends []domain.Backend)`, `Snapshot(backendID)`, `PoolSnapshot(poolID)`, `IncrementInflight`, and `Shutdown`.
- Runtime snapshot includes health counters, freshness, running/waiting/KV, raw/EWMA pressure, derived backend state, and gateway in-flight.

- [ ] **Step 1: Write literal metrics parser tests**

Use a fixture containing multiple model labels and assert sums of `vllm:num_requests_running`, `vllm:num_requests_waiting`, and `vllm:kv_cache_usage_perc`. Add a fixture where only `vllm:gpu_cache_usage_perc` exists and a fixture missing waiting metrics that must return an error.

- [ ] **Step 2: Write fake-clock worker tests for failure/recovery/staleness**

Drive three failing health responses to unhealthy, two successes to recovered, then advance past five seconds without a valid metrics sample and assert routing eligibility is false.

- [ ] **Step 3: Write manager reconciliation test**

Publish backends `[1,2]`, reconcile, then publish `[2,3]`; assert worker 1 stopped, worker 2 stayed, and worker 3 started exactly once.

- [ ] **Step 4: Run and verify missing-code failures**

Run: `go test ./internal/monitor`

Expected: compile failure.

- [ ] **Step 5: Implement parser, workers, and copy-on-read manager snapshots**

Run: `go get github.com/prometheus/common@latest`

Use Prometheus `expfmt.TextParser`. Worker loops must use injected tick channels in tests and real tickers in production. Every network request has its own timeout context. EWMA updates only on valid complete metric samples. Manager locking must never cover network I/O.

- [ ] **Step 6: Run focused and race suites**

Run: `gofmt -w internal/monitor && go test -race ./internal/monitor`

Expected: PASS.

- [ ] **Step 7: Commit**

```sh
git add internal/monitor
git commit -m "feat: monitor vLLM backend pressure and health"
```

---

### Task 8: Streaming upstream proxy and conservative retry

**Files:**
- Create: `internal/proxy/headers.go`
- Create: `internal/proxy/proxy.go`
- Create: `internal/proxy/headers_test.go`
- Create: `internal/proxy/proxy_test.go`

**Interfaces:**
- Consumes: selected `domain.Backend`, optional environment-resolved upstream API key, request context, rewritten JSON bytes, and an alternate-selector callback.
- Produces: `proxy.Forward(ctx, downstream, Request) Result`; `Result` includes backend ID, status, bytes sent, first-byte duration, retry count, and cancellation/transport error.
- Alternate callback signature: `func(exclude map[int64]struct{}) (domain.Backend, error)`.

- [ ] **Step 1: Write failing header-sanitization tests**

```go
func TestPrepareUpstreamHeadersOverwritesPriorityAndRequestID(t *testing.T) {
    in := http.Header{"X-Vllm-Priority": {"-999"}, "X-Request-Id": {"client"}, "Connection": {"keep-alive"}}
    got := proxy.PrepareUpstreamHeaders(in, "gateway-id", -10, "upstream-secret")
    if got.Get("X-Vllm-Priority") != "-10" || got.Get("X-Request-Id") != "gateway-id" { t.Fatalf("headers=%v", got) }
    if got.Get("Connection") != "" || got.Get("Authorization") != "Bearer upstream-secret" { t.Fatalf("headers=%v", got) }
}
```

- [ ] **Step 2: Write streaming tests against Fake vLLM**

Assert first SSE frame reaches a flushing recorder before the fake sends its second frame; assert the complete output bytes are unchanged; cancel the inbound context and observe fake cancellation.

- [ ] **Step 3: Write retry tests**

Backend A resets before body and B succeeds: assert one retry and B's response. A writes one SSE frame then resets: assert no retry callback invocation. A returns HTTP 503: assert status/body forwarded and no retry.

- [ ] **Step 4: Run and observe missing implementation failures**

Run: `go test ./internal/proxy`

Expected: compile failure.

- [ ] **Step 5: Implement shared transport, delayed commit, immediate flush, and idempotent cleanup**

Read the first upstream chunk before writing downstream headers. On a pre-byte transport read error, close the body, select one alternate, and repeat once. Strip standard hop-by-hop headers plus tokens named by `Connection`. After commit, copy read chunks directly and flush each non-empty write.

- [ ] **Step 6: Run focused and race suites**

Run: `gofmt -w internal/proxy && go test -race ./internal/proxy`

Expected: PASS.

- [ ] **Step 7: Commit**

```sh
git add internal/proxy
git commit -m "feat: stream and retry vLLM requests safely"
```

---

### Task 9: Public gateway orchestration and OpenAI HTTP API

**Files:**
- Create: `internal/gateway/service.go`
- Create: `internal/gateway/service_test.go`
- Create: `internal/httpapi/errors.go`
- Create: `internal/httpapi/middleware.go`
- Create: `internal/httpapi/public.go`
- Create: `internal/httpapi/public_test.go`

**Interfaces:**
- Consumes: registry snapshots, HMAC secret, limiter, monitor snapshots, router, proxy, and bounded last-used updater.
- Produces: `gateway.Service.Models(auth)` and `gateway.Service.Forward(ctx, Request, http.ResponseWriter)`.
- Produces: `httpapi.NewPublicHandler(service, bodyLimit)` with only the supported routes.
- Public request model: method, path, headers, raw body, generated request ID, optional parent request ID.

- [ ] **Step 1: Write failing authentication/model tests**

Use a real registry snapshot with literal key digest. Assert unknown/revoked/expired/disabled keys all return the same `401` envelope; a valid client sees only explicitly allowed enabled pools; forbidden model returns `403 model_not_allowed`.

- [ ] **Step 2: Write failing rewrite/admission/lifecycle tests**

Send JSON containing public model, client `priority`, and client priority header. Assert upstream receives the upstream model and configured header. Hold an SSE response open, send a second request at max concurrency one, assert `429`, close the stream, then assert the next request succeeds.

- [ ] **Step 3: Write route/error tests**

Assert malformed/oversized JSON is `400`, unsupported `/v1/embeddings` is controlled `404`, no eligible backend is `503`, overload is `429` plus `Retry-After: 2`, and actual upstream response bytes/status are unchanged.

- [ ] **Step 4: Run and verify missing-code failures**

Run: `go test ./internal/gateway ./internal/httpapi`

Expected: compile failure.

- [ ] **Step 5: Implement authentication, JSON preservation/rewrite, and the exact pipeline**

Run: `go get github.com/go-chi/chi/v5@latest`

Parse a `map[string]json.RawMessage`, validate `model` as a non-empty string, replace its encoded value, delete `priority`, and marshal the map. Acquire the client lease before routing and `defer` its idempotent release around proxy completion. Increment/decrement backend in-flight on every attempt.

- [ ] **Step 6: Implement OpenAI errors and middleware request IDs**

```go
type ErrorEnvelope struct {
    Error APIError `json:"error"`
}
type APIError struct {
    Message string `json:"message"`
    Type string `json:"type"`
    Code string `json:"code"`
}
```

Generate gateway IDs from 16 crypto-random bytes encoded as lowercase hex. Accept a bounded printable client ID only as parent metadata; never reuse it as the gateway ID.

- [ ] **Step 7: Run focused and race suites**

Run: `gofmt -w internal/gateway internal/httpapi && go test -race ./internal/gateway ./internal/httpapi`

Expected: PASS.

- [ ] **Step 8: Commit**

```sh
git add internal/gateway internal/httpapi
git commit -m "feat: expose authenticated OpenAI gateway API"
```

---

### Task 10: Prometheus telemetry and structured logs

**Files:**
- Create: `internal/observability/metrics.go`
- Create: `internal/observability/log.go`
- Create: `internal/observability/metrics_test.go`
- Create: `internal/observability/log_test.go`
- Modify: `internal/gateway/service.go`
- Modify: `internal/httpapi/public.go`

**Interfaces:**
- Produces: a dedicated Prometheus registry with the complete required `llmgw_*` instruments and `Handler()`.
- Produces: `observability.RequestRecord` and `Logger.Complete(record)` using `slog`.
- Consumes bounded enums/identifiers from gateway results; never raw bodies/headers.

- [ ] **Step 1: Write failing metric exposition test**

Perform one successful request and one admission rejection, scrape the handler, parse exposition, and assert the required metric families exist with bounded labels. Explicitly assert the output does not contain request ID or API-key prefix.

- [ ] **Step 2: Write failing structured-log redaction test**

Capture JSON `slog` output for a request whose prompt and Authorization contain unique sentinel strings. Assert required fields exist and neither sentinel appears.

- [ ] **Step 3: Run and observe missing-code failures**

Run: `go test ./internal/observability ./internal/gateway ./internal/httpapi`

Expected: compile failure.

- [ ] **Step 4: Implement all required instruments and wire lifecycle updates**

Run: `go get github.com/prometheus/client_golang@latest`

Create counters/gauges/histograms for request totals/inflight/rejections, client/backend inflight, backend pressure/running/waiting/KV, duration/TTFT, stream disconnects, backend failures, and retries. Register them in a non-global registry to keep tests isolated.

- [ ] **Step 5: Implement one completion record per public request**

The record includes gateway/parent IDs, client/model/priority, backend, pool state/pressure, status, duration, TTFT, disconnect, retry, and usage only when parsed from an already-buffered ordinary small metadata response. Do not buffer streaming to obtain usage.

- [ ] **Step 6: Run tests**

Run: `gofmt -w internal/observability internal/gateway internal/httpapi && go test ./internal/observability ./internal/gateway ./internal/httpapi`

Expected: PASS.

- [ ] **Step 7: Commit**

```sh
git add internal/observability internal/gateway internal/httpapi
git commit -m "feat: expose gateway telemetry and request logs"
```

---

### Task 11: Admin API and embedded server-rendered UI

**Files:**
- Create: `internal/httpapi/admin_auth.go`
- Create: `internal/httpapi/admin.go`
- Create: `internal/httpapi/admin_test.go`
- Create: `internal/web/embed.go`
- Create: `internal/web/templates/layout.html`
- Create: `internal/web/templates/dashboard.html`
- Create: `internal/web/templates/clients.html`
- Create: `internal/web/templates/keys.html`
- Create: `internal/web/templates/backends.html`
- Create: `internal/web/static/app.css`
- Create: `internal/web/static/app.js`
- Create: `internal/web/web.go`
- Create: `internal/web/web_test.go`

**Interfaces:**
- Consumes: SQLite CRUD, registry reload, monitor status, API-key generator, and admin credentials.
- Produces: the exact Admin API endpoints in the design plus `/admin`, `/admin/clients`, `/admin/keys`, and `/admin/backends` pages.
- Produces: Basic-auth middleware and double-submit CSRF validation for every state-changing admin request.

- [ ] **Step 1: Write failing auth/security-header/CSRF tests**

Assert no credentials and wrong credentials return `401` with `WWW-Authenticate`; correct credentials render HTML; mutating JSON/form requests without matching CSRF cookie/header/form value return `403`; successful admin responses include no-store, frame denial, content-type protection, referrer policy, and a restrictive CSP.

- [ ] **Step 2: Write failing Admin CRUD tests against real temporary SQLite**

Create/update/disable a client, replace model access, create a pool/backend, drain/resume, generate/revoke a key, and fetch aggregate status. Assert every successful write increments/publishes a registry revision. Assert the key secret appears in exactly the create response and never in subsequent list output.

- [ ] **Step 3: Write failing rendered-page semantic tests**

Run: `go get golang.org/x/net@latest`

Parse HTML with `golang.org/x/net/html` and assert one navigation landmark, labelled forms, table headers for every required screen, status text, and one-time secret region. Do not test exact CSS strings.

- [ ] **Step 4: Run and verify expected failures**

Run: `go test ./internal/httpapi ./internal/web`

Expected: compile/template failure because Admin implementation and assets are absent.

- [ ] **Step 5: Implement Basic auth, CSRF, validation, CRUD, and registry reconciliation**

Use constant-time comparison of SHA-256 credential digests. Issue a random CSRF cookie after successful Basic auth; accept the matching `X-CSRF-Token` header or hidden `csrf_token` form field. Return typed JSON conflicts/validation errors. After each committed mutation call registry reload and monitor reconcile; if reload fails, return `503` while leaving the durable write for the next reload attempt and show degraded status.

- [ ] **Step 6: Implement embedded templates and accessible minimal styling**

Use one shared layout, semantic tables/forms, visible focus, responsive overflow, state badges, no CDN, and progressive JavaScript only for dashboard refresh, confirmations, and clipboard. Forms remain functional with JavaScript disabled.

- [ ] **Step 7: Run focused tests**

Run: `gofmt -w internal/httpapi internal/web && go test ./internal/httpapi ./internal/web`

Expected: PASS.

- [ ] **Step 8: Commit**

```sh
git add internal/httpapi internal/web
git commit -m "feat: add embedded gateway administration UI"
```

---

### Task 12: Process wiring, load generator, deployment, and English documentation

**Files:**
- Create: `cmd/gateway/main.go`
- Create: `cmd/gateway/main_test.go`
- Create: `internal/loadgen/loadgen.go`
- Create: `internal/loadgen/stats.go`
- Create: `internal/loadgen/loadgen_test.go`
- Create: `cmd/loadgen/main.go`
- Create: `Dockerfile`
- Create: `Makefile`
- Create: `docs/real-gpu-testing.md`
- Create: `README.md`

**Interfaces:**
- Gateway command wires config, store/migrations, registry, monitors, metrics, public/admin/web handlers, HTTP server, and graceful shutdown.
- Load generator accepts URL, one key or class-key map, parallelism, request count, prompt size, max tokens, stream, and validated traffic mix; returns deterministic summary stats.
- Deployment produces all three commands for `linux/amd64` with `CGO_ENABLED=0`.

- [ ] **Step 1: Write failing process-wiring test**

Call an exported `run(ctx, getenv, listener, stdout, stderr) error` with temporary SQLite and valid credentials, wait for `/healthz`, cancel context, and assert graceful return. A second test omits the HMAC secret and asserts startup fails before listening.

- [ ] **Step 2: Write failing load-stat and traffic-mix tests**

```go
func TestPercentilesUseNearestRank(t *testing.T) {
    got := loadgen.Summarize([]time.Duration{time.Millisecond, 2*time.Millisecond, 3*time.Millisecond, 100*time.Millisecond})
    if got.P50 != 2*time.Millisecond || got.P95 != 100*time.Millisecond || got.P99 != 100*time.Millisecond {
        t.Fatalf("summary=%+v", got)
    }
}
```

Also assert a non-zero traffic class without a mapped key is rejected, streaming TTFT is first response byte, `429` is counted separately, and a transport failure affects exit status.

- [ ] **Step 3: Run and observe missing-code failures**

Run: `go test ./cmd/gateway ./internal/loadgen ./cmd/loadgen`

Expected: compile failure.

- [ ] **Step 4: Implement gateway composition and graceful shutdown**

Mount `/metrics`, `/healthz`, `/readyz`, public routes, Admin API, and embedded pages in one chi router. Start monitor reconciliation before readiness. On SIGINT/SIGTERM, call `http.Server.Shutdown` with the configured grace period, then stop monitors and close SQLite.

- [ ] **Step 5: Implement load generator and CLI**

Use a fixed worker pool and precomputed weighted identity sequence seeded from a CLI flag for reproducibility. For streaming, consume to EOF while recording first body byte separately. Print JSON or aligned text containing totals and p50/p95/p99 TTFT/latency.

- [ ] **Step 6: Write the Dockerfile and Make targets**

The Dockerfile builds with a pinned `golang:1.27` builder, runs as a numeric non-root user in a minimal runtime image, exposes 8080, and declares `/data`. Make targets: `test`, `test-race`, `vet`, `build`, `build-linux-amd64`, `fake-vllm`, and `loadgen`.

- [ ] **Step 7: Write the English README and real-GPU guide**

README sections must be: purpose, architecture, features, requirements, quick start, initial configuration, Admin UI/API, client API, vLLM setup flags, Fake vLLM, load generation, metrics/logging, security, macOS development, Linux amd64 build, Docker, testing, real-GPU validation, limitations, and production evolution. The GPU guide provides exact commands for compatibility, cancellation, queue, priority isolation, and hysteretic recovery tests.

- [ ] **Step 8: Run command tests and host builds**

Run: `gofmt -w cmd internal/loadgen && go test ./cmd/... ./internal/loadgen && go build ./cmd/...`

Expected: PASS/build success.

- [ ] **Step 9: Cross-build every command**

Run: `mkdir -p dist && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dist/gateway-linux-amd64 ./cmd/gateway && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dist/fake-vllm-linux-amd64 ./cmd/fake-vllm && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dist/loadgen-linux-amd64 ./cmd/loadgen`

Expected: three Linux x86-64 executables; `file dist/*` reports ELF 64-bit x86-64.

- [ ] **Step 10: Commit**

```sh
git add cmd internal/loadgen Dockerfile Makefile README.md docs/real-gpu-testing.md
git commit -m "feat: ship gateway commands and deployment docs"
```

---

### Task 13: End-to-end acceptance suite and performance smoke

**Files:**
- Create: `tests/integration/harness_test.go`
- Create: `tests/integration/auth_models_test.go`
- Create: `tests/integration/routing_health_test.go`
- Create: `tests/integration/admission_priority_test.go`
- Create: `tests/integration/streaming_retry_test.go`
- Create: `tests/integration/admin_test.go`
- Create: `tests/integration/performance_test.go`
- Create: `docs/acceptance-evidence.md`
- Modify: any production file only through a failing regression test when acceptance exposes a defect.

**Interfaces:**
- Harness starts a real temporary SQLite store, gateway HTTP server, and multiple real Fake vLLM `httptest.Server` instances.
- Acceptance evidence maps every source MVP criterion to a named test/command or the explicit external real-GPU procedure.

- [ ] **Step 1: Build a deterministic black-box harness**

The harness creates admin credentials, pools/backends/clients/keys through the Admin API rather than internal inserts, waits for monitor recovery with bounded polling, and exposes cleanup that fails on leaked active requests.

- [ ] **Step 2: Write and run authentication/model acceptance tests**

Run: `go test ./tests/integration -run 'Test(Authentication|Model)' -count=1`

Expected before any required fix: tests either expose missing behavior or pass because earlier focused tests already drove it.

- [ ] **Step 3: Write and run 100-request least-pressure plus health/recovery tests**

Configure A pressure 0.3 and B pressure 1.1, send 100 requests, assert B receives zero while A is eligible. Then fail A health three times, assert exclusion within the configured timeout, recover twice, and assert automatic return.

Run: `go test ./tests/integration -run 'Test(Routing|Health|Recovery)' -count=1`

- [ ] **Step 4: Write and run overload/priority/hysteresis tests**

Drive fake clock or shortened configured windows through normal, busy, saturated, emergency. Assert Background rejects before Normal/High/Critical and every admitted request carries configured priority. Assert a sub-enter-window spike does not change state and recovery requires the exit window.

Run: `go test ./tests/integration -run 'Test(Admission|Priority|Hysteresis)' -count=1`

- [ ] **Step 5: Write and run streaming/cancellation/retry tests**

Assert immediate first frame, byte-for-byte SSE, held lease, disconnect cancellation, retry only before first byte, and no retry after first chunk.

Run: `go test ./tests/integration -run 'Test(Streaming|Cancellation|Retry)' -count=1`

- [ ] **Step 6: Write and run Admin UI/API acceptance tests**

Assert all four screens, Basic auth, CSRF, CRUD, one-time key, drain/resume, live status, and no plaintext secret in DB/API/HTML after creation.

Run: `go test ./tests/integration -run 'TestAdmin' -count=1`

- [ ] **Step 7: Add opt-in local performance smoke**

Gate the timing assertion behind `LLMGW_RUN_PERF=1`. Against an immediate fake backend, warm up connections, run moderate concurrency, report gateway-added p50/p99, and fail the opt-in run above 5ms/20ms on a quiet supported host. Default CI still runs functional loadgen smoke without brittle wall-clock limits.

- [ ] **Step 8: Map acceptance evidence**

For every MVP criterion in the source requirements, list the exact test name and command. Mark the five real-GPU scenarios as externally pending in this macOS environment but provide the exact documented Linux procedure; do not claim they ran locally.

- [ ] **Step 9: Run the entire pre-review verification suite**

Run: `go test ./...`

Run: `go test -race ./...`

Run: `go vet ./...`

Run: `make build-linux-amd64`

Run: `git diff --check`

Expected: all commands succeed with no warnings/races; Linux binaries identify as ELF x86-64.

- [ ] **Step 10: Commit acceptance evidence**

```sh
git add tests/integration docs/acceptance-evidence.md
git commit -m "test: verify gateway MVP acceptance criteria"
```

---

### Task 14: Independent subagent review, remediation, and final verification

**Files:**
- Modify: only files implicated by validated review findings.
- Create/modify: regression tests for every behavior/security defect that is fixed.

**Interfaces:**
- Consumes: complete implementation, source requirements, design spec, implementation plan, test output, and acceptance evidence.
- Produces: independent reviews for requirements coverage, correctness/security, and tests/operability; validated fixes; final clean verification evidence.

- [ ] **Step 1: Refresh the codebase graph and coverage before delegation**

Index the completed repository, record project generation, inspect changed symbols/blast radius, and check coverage for every changed code path plus root scope. Read/grep any partial/skipped ranges before making claims. Pass this evidence to every reviewer.

- [ ] **Step 2: Dispatch three independent review subagents in parallel**

Reviewer A checks every source MVP requirement against code/tests/docs. Reviewer B audits concurrency, cancellation, streaming, retry, authentication, CSRF, SSRF, secret handling, and shutdown correctness. Reviewer C audits test honesty, race coverage, deployment portability, README commands, Fake vLLM, and Load Generator behavior. Reviewers return only actionable findings with severity, file/line, evidence, and a proposed verification.

- [ ] **Step 3: Validate every finding locally**

Reproduce each plausible defect or prove the cited path cannot occur. Reject opinion-only scope expansions and production-only features excluded by the approved design.

- [ ] **Step 4: Fix validated findings through TDD**

For each defect, add a focused regression test, run it to observe the expected failure, implement the minimal fix, run the focused suite, then run affected integration/race tests.

- [ ] **Step 5: Ask original reviewers to re-check remediated areas**

Provide exact commits/diffs and test output. Continue until no reviewer has an unresolved P0/P1/P2 correctness, security, or requirements-coverage finding.

- [ ] **Step 6: Run final completion audit and verification**

Run: `go test ./...`

Run: `go test -race ./...`

Run: `go vet ./...`

Run: `make build-linux-amd64`

Run: `git diff --check`

Run: `git status --short --branch`

Inspect `docs/acceptance-evidence.md` requirement by requirement against current test names and outputs. Confirm user-owned source requirements remain unmodified/uncommitted.

- [ ] **Step 7: Commit review fixes and hand off**

```sh
git add -u
git add cmd internal tests README.md docs Dockerfile Makefile go.mod go.sum
git commit -m "fix: address independent gateway review findings"
```

If no files changed, do not create an empty commit. Report verified commands, review outcome, Linux artifact paths, documentation paths, and the explicit limitation that real RTX/vLLM tests were prepared but not executed on macOS.
