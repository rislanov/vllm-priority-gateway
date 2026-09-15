# Behavior-Preserving Go Project Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the complete Go project around its proven complexity hotspots, restore Windows SQLite portability, and preserve every public contract and operator workflow.

**Architecture:** Keep the current modular-monolith packages and stable interfaces. Decompose oversized orchestration functions into package-private lifecycle objects and focused helpers, split files by responsibility, and use the existing characterization suite as the behavioral boundary. Add no runtime dependency and make no schema, route, metric, UI, or configuration contract change.

**Tech Stack:** Go 1.27, `net/http`, chi, modernc SQLite, Prometheus client, embedded templates, Docker/Compose, `go test`, race detector, `go vet`, `gopls`.

**Spec:** `docs/superpowers/specs/2026-08-31-go-project-refactor-design.md`

## Global Constraints

- Base commit is `d4e7357af7ec141aa6f12b834b7919f9373cd5e1`; implementation branch is `codex/go-project-refactor`.
- No new runtime dependency and no dependency version change.
- Preserve all public and Admin routes, methods, status codes, JSON/error envelopes, headers, authentication, CSRF, and `Retry-After` behavior.
- Preserve streaming bytes, flush timing, cancellation, first-byte retry boundary, usage inspection, and terminal accounting.
- Preserve SQLite schema/migrations/data, Prometheus names/labels, log fields, Admin UI markup/copy/forms, environment variables/defaults, commands, Docker entrypoint, and Compose contracts.
- Every behavior-sensitive extraction runs its existing characterization tests before and after the edit; new behavior begins with a failing regression test.
- The worktree graph project `C-Projects-vllm-priority-gateway-go-project-refactor` indexed 2,246 nodes and 12,128 edges. Its transport later closed; structural candidates and traces gathered before closure are evidence, while exact implementation work uses direct source reads. Known graph gaps are the SQL/template parser ranges recorded in the spec audit and the deliberately excluded `deploy/` subtree, which must be checked directly.
- After Task 1 restores the host suite, measure the touched functions with `-coverpkg=./...`; assign High/Medium/Low safety-net tiers from `golang-refactoring`, and add characterization tests before any extraction whose touched branches are not already exercised.

---

## File Structure

**Create:**

- `internal/store/sqlite_uri_test.go` — package-internal cross-platform file-URI regression test.
- `internal/observability/metrics_internal_test.go` — package-internal label-set construction test.
- `internal/gateway/forward.go` — public forwarding orchestration and private request/target lifecycle helpers moved out of `service.go`.
- `internal/proxy/json_validator.go` — incremental JSON validator states and transitions moved out of `usage.go`.
- `cmd/gateway/application.go` — application dependency ownership, handler construction, and ordered cleanup.
- `cmd/gateway/server.go` — HTTP listener/server lifecycle and graceful shutdown.
- `internal/web/resources.go` — Clients, API Keys, pools, and backends form dispatch and edit lookup.

**Modify:**

- `internal/store/sqlite.go`, existing `internal/store/sqlite_test.go` behavior tests — portable SQLite file URI and public regression coverage.
- `internal/config/config.go`, `internal/config/config_test.go` — split defaults/typed loading/validation without changing values or errors.
- `internal/loadgen/loadgen.go`, `internal/loadgen/loadgen_test.go` — separate worker execution and aggregation.
- `internal/web/web.go`, `internal/web/web_test.go` — retain routing/rendering/analytics and verify resource handler compatibility after file split.
- `internal/observability/metrics.go`, existing `internal/observability/metrics_test.go` — separate label-set construction/reconciliation/publication.
- `internal/proxy/proxy.go`, `internal/proxy/proxy_test.go`, `internal/proxy/usage.go`, `internal/proxy/usage_test.go` — response copy lifecycle and parser decomposition.
- `internal/gateway/service.go`, existing `internal/gateway/*_test.go` — retain constructors/models/auth/payload helpers and test extracted forwarding lifecycle.
- `cmd/gateway/main.go`, `cmd/gateway/main_test.go` — leave CLI entry and narrow `run`; test lifecycle equivalence.

**Inspect directly, modify only for a concrete finding:**

- `internal/admission`, `analytics`, `apikey`, `circuitbreaker`, `domain`, `fakevllm`, `httpapi`, `monitor`, `pressure`, `registry`, `routing`, and the remaining `store` files.
- `Dockerfile`, `Makefile`, `compose*.yaml`, `.dockerignore`, `scripts/container-smoke.sh`, `.github/workflows/ci.yml`, and `deploy/observability/**`.

---

### Task 1: Restore Portable SQLite File URIs

**Files:**

- Modify: `internal/store/sqlite.go:36-78`
- Create: `internal/store/sqlite_uri_test.go`
- Existing behavior coverage: `internal/store/sqlite_test.go:TestSQLiteMigratesAndReopens`

**Interfaces:**

- Produces: `func sqliteDSN(path string) (string, error)`.
- Consumes: `filepath.Abs`, `filepath.VolumeName`, `filepath.ToSlash`, and `url.URL.String`.
- `Open(context.Context, string) (*SQLite, error)` remains unchanged.

- [ ] **Step 1: Add a direct file-URI regression test**

```go
package store

import (
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestSQLiteDSNUsesAbsoluteFileURL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db with space.sqlite")
	dsn, err := sqliteDSN(path)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme != "file" || parsed.Host != "" {
		t.Fatalf("SQLite DSN = %q, scheme=%q host=%q", dsn, parsed.Scheme, parsed.Host)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	wantPath := filepath.ToSlash(absolute)
	if filepath.VolumeName(absolute) != "" && !strings.HasPrefix(wantPath, "/") {
		wantPath = "/" + wantPath
	}
	if parsed.Path != wantPath {
		t.Fatalf("SQLite URI path = %q, want %q", parsed.Path, wantPath)
	}
	for _, pragma := range []string{"journal_mode(WAL)", "foreign_keys(1)", "busy_timeout(5000)"} {
		if !slices.Contains(parsed.Query()["_pragma"], pragma) {
			t.Errorf("SQLite DSN lacks pragma %q: %q", pragma, dsn)
		}
	}
}
```

- [ ] **Step 2: Run the focused test and confirm the missing helper fails compilation**

Run: `go test -count=1 -run TestSQLiteDSNUsesAbsoluteFileURL -v ./internal/store`

Expected: FAIL with `undefined: sqliteDSN`.

- [ ] **Step 3: Implement the portable DSN helper and delegate from `Open`**

```go
const sqlitePragmas = "?_pragma=journal_mode%28WAL%29&_pragma=foreign_keys%281%29&_pragma=busy_timeout%285000%29"

func sqliteDSN(path string) (string, error) {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve SQLite path: %w", err)
	}
	urlPath := filepath.ToSlash(absolutePath)
	if filepath.VolumeName(absolutePath) != "" && !strings.HasPrefix(urlPath, "/") {
		urlPath = "/" + urlPath
	}
	return (&url.URL{Scheme: "file", Path: urlPath}).String() + sqlitePragmas, nil
}
```

Replace the inline `filepath.Abs`/URL block in `Open` with `dsn, err := sqliteDSN(path)` and return the helper error unchanged.

- [ ] **Step 4: Verify the store regression and all previously blocked Windows tests**

Run:

```powershell
go test -count=1 -run 'TestSQLiteDSNUsesAbsoluteFileURL|TestSQLiteMigratesAndReopens' -v ./internal/store
go test -count=1 ./internal/store ./internal/httpapi ./internal/web ./tests/integration ./cmd/gateway
```

Expected: PASS; no `invalid uri authority: C:` failures.

- [ ] **Step 5: Establish the coverage-adaptive safety net for later tasks**

```powershell
$coverage = Join-Path $env:TEMP 'llmgw-go-project-refactor-cover.out'
go test -covermode=atomic -coverpkg=./... -coverprofile=$coverage ./...
go tool cover -func=$coverage
Remove-Item -LiteralPath $coverage
```

Record the coverage of `config.Load`, `loadgen.Run`, `web.(*Handler).clients/keys/backends`, `observability.(*Metrics).PublishRuntime`, `proxy.(*Proxy).forwardOnce`, `proxy.(*incrementalJSONValidator).stepByte`, `gateway.(*Service).Forward`, and `cmd/gateway.run`. Inspect the touched branches, not only the percentage. Add characterization coverage before extraction for every Medium/Low target.

- [ ] **Step 6: Format and commit**

```powershell
gofmt -w internal/store/sqlite.go internal/store/sqlite_uri_test.go
git add internal/store/sqlite.go internal/store/sqlite_uri_test.go
git commit -m "fix: build portable SQLite file URI"
```

### Task 2: Decompose Configuration Loading

**Files:**

- Modify: `internal/config/config.go:54-200`
- Modify only if characterization is missing: `internal/config/config_test.go`

**Interfaces:**

- Produces: `func defaultConfig() Config`, `func (c *Config) loadHealth(LookupFunc) error`, `loadCircuit`, `loadPressure`, `loadRequestPolicy`, and `loadTransport`.
- `Load(LookupFunc) (Config, error)`, environment names, defaults, parse errors, and validation errors remain unchanged.

- [ ] **Step 1: Capture the complete configuration contract before extraction**

Run: `go test -count=1 -run TestLoad -v ./internal/config`

Expected: PASS for defaults, overrides, secrets, threshold order, finite values, circuit settings, analytics retention, and session-affinity pressure.

- [ ] **Step 2: Extract the literal defaults without changing a value**

```go
func defaultConfig() Config {
	return Config{
		ListenAddress:              ":8080",
		DatabasePath:               "./data/llmgw.db",
		HealthInterval:             2 * time.Second,
		HealthTimeout:              time.Second,
		MetricsInterval:            time.Second,
		MetricsTimeout:             time.Second,
		MetricsStaleAfter:          5 * time.Second,
		UnhealthyAfter:             3,
		RecoveryAfter:              2,
		CircuitFailureThreshold:    5,
		CircuitFailureWindow:       30 * time.Second,
		CircuitOpenCooldown:        15 * time.Second,
		CircuitHalfOpenMaxProbes:   1,
		QueueSoftLimit:             2,
		KVSoftLimit:                .80,
		KVHardLimit:                .95,
		EWMAWindow:                 4 * time.Second,
		BusyThreshold:              .70,
		SaturatedThreshold:         1.00,
		EmergencyThreshold:         1.40,
		BusyRecoveryThreshold:      .55,
		SaturatedRecoveryThreshold: .85,
		EmergencyRecoveryThreshold: 1.20,
		OverloadEnterWindow:        3 * time.Second,
		OverloadRecoveryWindow:     10 * time.Second,
		RequestBodyLimit:           16 << 20,
		RetryAfter:                 2 * time.Second,
		RoutingPressureEpsilon:     .02,
		SessionAffinityMaxPressure: 1.00,
		DialTimeout:                3 * time.Second,
		TLSHandshakeTimeout:        3 * time.Second,
		ResponseHeaderTimeout:      30 * time.Second,
		ShutdownGracePeriod:        30 * time.Second,
		AnalyticsRetention:         2160 * time.Hour,
	}
}
```

`Load` begins with `cfg := defaultConfig()` and applies the five existing string/secret reads before typed groups.

- [ ] **Step 3: Extract typed load groups with exact assignment order**

```go
func (c *Config) loadHealth(lookup LookupFunc) error
func (c *Config) loadCircuit(lookup LookupFunc) error
func (c *Config) loadPressure(lookup LookupFunc) error
func (c *Config) loadRequestPolicy(lookup LookupFunc) error
func (c *Config) loadTransport(lookup LookupFunc) error
```

Keep the helpers as contiguous slices of the current load order so multi-error precedence cannot change: Health owns `LLMGW_HEALTH_INTERVAL` through `LLMGW_RECOVERY_AFTER`; Circuit owns `LLMGW_CIRCUIT_FAILURE_THRESHOLD` through `LLMGW_CIRCUIT_HALF_OPEN_MAX_PROBES`; Pressure owns `LLMGW_QUEUE_SOFT_LIMIT` through `LLMGW_OVERLOAD_RECOVERY_WINDOW`; RequestPolicy owns `LLMGW_REQUEST_BODY_LIMIT` through `LLMGW_SESSION_AFFINITY_MAX_PRESSURE`; Transport owns `LLMGW_DIAL_TIMEOUT` through `LLMGW_ANALYTICS_RETENTION`. Each helper returns the first existing parse error. Do not introduce reflection, a generic registry, or map iteration.

- [ ] **Step 4: Run configuration and integration characterization**

Run:

```powershell
gofmt -w internal/config/config.go
go test -count=1 ./internal/config ./cmd/gateway
```

Expected: PASS with unchanged error strings.

- [ ] **Step 5: Commit**

```powershell
git add internal/config/config.go internal/config/config_test.go
git commit -m "refactor: separate configuration loading stages"
```

### Task 3: Separate Load-Generator Execution from Aggregation

**Files:**

- Modify: `internal/loadgen/loadgen.go:99-219`
- Modify: `internal/loadgen/loadgen_test.go`

**Interfaces:**

- Produces: `func observe(ctx context.Context, config Config, identities []identity) <-chan observation`, `type resultAccumulator`, `func (*resultAccumulator) add(observation)`, and `func (*resultAccumulator) finish(context.Context) (Result, error)`.
- `Run(context.Context, Config) (Result, error)` and every result counter/percentile remain unchanged.

- [ ] **Step 1: Run the load-generator suite before extraction**

Run: `go test -count=1 -run 'TestRun|TestSmallTraffic|TestTransport' -v ./internal/loadgen`

Expected: PASS, including the new public-behavior characterization.

- [ ] **Step 2: Move bounded worker/channel ownership into `observe`**

```go
func observe(ctx context.Context, config Config, identities []identity) <-chan observation {
	jobs := make(chan identity)
	observations := make(chan observation, config.Requests)
	workers := min(config.Parallelism, config.Requests)
	var group sync.WaitGroup
	for range workers {
		group.Go(func() {
			for identity := range jobs {
				observations <- execute(ctx, config, identity)
			}
		})
	}
	go func() {
		defer close(jobs)
		for _, identity := range identities {
			select {
			case jobs <- identity:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		group.Wait()
		close(observations)
	}()
	return observations
}
```

- [ ] **Step 3: Move counter and percentile state into `resultAccumulator`**

```go
type resultAccumulator struct {
	summary      Result
	ttft         []time.Duration
	latency      []time.Duration
	classTTFT    map[domain.PriorityClass][]time.Duration
	classLatency map[domain.PriorityClass][]time.Duration
}
```

Construct the accumulator with non-nil `summary.ByClass`, `classTTFT`, and `classLatency` maps. `add` increments global totals first, updates class totals only for a valid priority, handles transport errors before the status switch, records latency only for 2xx responses, and writes the class result back only for a valid priority. `finish` computes the global and per-class summaries, converts an empty `ByClass` map to nil, returns the existing transport-failure error before `ctx.Err()`, and otherwise returns the accumulated result.

- [ ] **Step 4: Verify and commit**

```powershell
gofmt -w internal/loadgen/loadgen.go internal/loadgen/loadgen_test.go
go test -count=1 ./internal/loadgen ./cmd/loadgen ./tests/integration
git add internal/loadgen/loadgen.go internal/loadgen/loadgen_test.go
git commit -m "refactor: separate load generation and aggregation"
```

### Task 4: Split Admin Resource Handlers from Web Rendering

**Files:**

- Create: `internal/web/resources.go`
- Modify: `internal/web/web.go`
- Modify only for missing characterization: `internal/web/web_test.go`

**Interfaces:**

- `Handler.ServeHTTP`, route paths, templates, form names/actions, redirects, status codes, and error text remain unchanged.
- Produces private helpers `mutateClient`, `mutateKey`, `mutateBackend`, `editClient`, `editPool`, and `editBackend`.

- [ ] **Step 1: Run the complete rendered UX contract before moving code**

Run: `go test -count=1 -v ./internal/web`

Expected: all fifteen Web tests PASS.

- [ ] **Step 2: Move resource-only symbols to `resources.go` without edits**

Move `clients`, `keys`, `backends`, `clientInput`, `poolInput`, `backendInput`, `optionalFormInt`, `positiveID`, `errorText`, and `valueError`. Keep `Handler`, `ServeHTTP`, analytics, secret-flash ownership, rendering, and shared presentation helpers in `web.go`.

- [ ] **Step 3: Extract form mutation helpers**

```go
func (h *Handler) mutateClient(request *http.Request) string
func (h *Handler) mutateKey(request *http.Request) (redirect string, errorText string)
func (h *Handler) mutateBackend(request *http.Request) string
```

The key helper returns the exact `/admin/keys?flash=<nonce>` redirect only after successful creation. The handlers still own `ParseForm`, method validation, response status selection, and rendering.

- [ ] **Step 4: Extract typed edit lookups from one `AdminView` snapshot per request**

```go
func editClient(view httpapi.AdminView, rawID string) *httpapi.AdminClient
func editPool(view httpapi.AdminView, rawID string) *httpapi.AdminPool
func editBackend(view httpapi.AdminView, rawID string) *httpapi.AdminBackend
```

Invalid or absent IDs still silently yield nil; copy the selected value before returning its address.

- [ ] **Step 5: Verify byte-visible behavior and commit**

```powershell
gofmt -w internal/web/web.go internal/web/resources.go
go test -count=1 ./internal/web ./cmd/gateway ./tests/integration
git add internal/web/web.go internal/web/resources.go internal/web/web_test.go
git commit -m "refactor: isolate Admin resource form handlers"
```

### Task 5: Decompose Runtime Metric Label Reconciliation

**Files:**

- Modify: `internal/observability/metrics.go:299-397`
- Create: `internal/observability/metrics_internal_test.go`
- Existing public behavior coverage: `internal/observability/metrics_test.go`

**Interfaces:**

- Produces `type runtimeLabelSets`, `func makeRuntimeLabelSets([]PoolRuntimeMetric, []BackendRuntimeMetric, []InflightRuntimeLabels) runtimeLabelSets`, and package-private reconciliation/publication helpers.
- `Metrics.PublishRuntime` remains the only public topology publication method; every metric family and label lifecycle remains unchanged.

- [ ] **Step 1: Add a focused label-set construction test**

```go
package observability

import (
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestRuntimeLabelSetsDeduplicateTopology(t *testing.T) {
	sets := makeRuntimeLabelSets(
		[]PoolRuntimeMetric{{Model: "model"}, {Model: "model"}},
		[]BackendRuntimeMetric{{Model: "model", Backend: "backend"}, {Model: "model", Backend: "backend"}},
		[]InflightRuntimeLabels{{Client: "client", Model: "model", PriorityClass: domain.PriorityNormal}},
	)
	if len(sets.pools) != 1 || len(sets.backends) != 1 || len(sets.requests) != 1 || len(sets.clients) != 1 {
		t.Fatalf("runtime label cardinalities = %d/%d/%d/%d", len(sets.pools), len(sets.backends), len(sets.requests), len(sets.clients))
	}
}
```

- [ ] **Step 2: Run the new test and confirm the helper is missing**

Run: `go test -count=1 -run TestRuntimeLabelSetsDeduplicateTopology -v ./internal/observability`

Expected: FAIL with `undefined: makeRuntimeLabelSets`.

- [ ] **Step 3: Implement label-set construction and locked reconciliation**

```go
type runtimeLabelSets struct {
	pools    map[string]struct{}
	backends map[backendMetricLabels]struct{}
	requests map[requestMetricLabels]struct{}
	clients  map[clientMetricLabels]struct{}
}

func (m *Metrics) reconcileRuntimeLabels(current runtimeLabelSets)
func (m *Metrics) publishRuntimeValues(pools []PoolRuntimeMetric, backends []BackendRuntimeMetric, current runtimeLabelSets)
```

`PublishRuntime` builds sets before taking `runtimeMu`, then holds the existing lock across reconciliation, known-set replacement, and gauge publication. Do not weaken synchronization.

- [ ] **Step 4: Verify topology lifecycle and commit**

```powershell
gofmt -w internal/observability/metrics.go internal/observability/metrics_internal_test.go
go test -count=1 ./internal/observability ./cmd/gateway
git add internal/observability/metrics.go internal/observability/metrics_internal_test.go
git commit -m "refactor: isolate runtime metric reconciliation"
```

### Task 6: Consolidate Proxy Response Copy Lifecycle

**Files:**

- Modify: `internal/proxy/proxy.go:90-209`
- Modify only for missing characterization: `internal/proxy/proxy_test.go`

**Interfaces:**

- Produces private `responseCopier`, `commit`, `write`, and `copy` helpers.
- `Proxy.Forward`, `Proxy.forwardOnce`, `Result`, retry selection, backend completion, and outcome classifications remain unchanged.

- [ ] **Step 1: Run the complete proxy response contract before extraction**

Run: `go test -count=1 -v ./internal/proxy`

Expected: all streaming, usage, cancellation, retry, classification, redirect, and error-status tests PASS.

- [ ] **Step 2: Introduce a response-copy owner**

```go
type responseCopier struct {
	ctx        context.Context
	downstream http.ResponseWriter
	response   *http.Response
	inspector  *usageInspector
	started    time.Time
	result     *Result
}

func (c *responseCopier) commit()
func (c *responseCopier) write(data []byte, provenReadFailure bool) (domain.InferenceOutcome, bool)
func (c *responseCopier) copy() (retryable bool, outcome domain.InferenceOutcome)
```

The `write` boolean means terminal. It preserves short-write conversion, byte count, flush-after-write, inspection of only delivered bytes, cancellation observation, and neutral/failure classification. `copy` keeps an explicit `firstRead` flag so only a zero-byte, non-EOF read failure on a successful response can be retryable; after any committed byte it always returns `retryable=false`.

- [ ] **Step 3: Replace duplicate first-read/loop branches with one read loop**

Keep the special rules: non-2xx headers commit before body read; a successful response may retry only if the first read fails before any byte; empty EOF commits; usage finalizes only on EOF; started responses never retry.

- [ ] **Step 4: Stress the retry/stream boundary and commit**

```powershell
gofmt -w internal/proxy/proxy.go
go test -count=20 -run 'TestForward(Flushes|Retries|DoesNotRetry|Classifies|Commits|Preserves|Completes)' ./internal/proxy
go test -race -count=1 ./internal/proxy
git add internal/proxy/proxy.go internal/proxy/proxy_test.go
git commit -m "refactor: centralize proxy response copying"
```

### Task 7: Isolate Incremental JSON Validation

**Files:**

- Create: `internal/proxy/json_validator.go`
- Modify: `internal/proxy/usage.go:366-735`
- Modify only for missing characterization: `internal/proxy/usage_test.go`

**Interfaces:**

- Moves unexported `jsonScanOp`, `jsonScanState`, `jsonParseState`, and `incrementalJSONValidator` unchanged into the focused file.
- Produces small transition helpers `stepString`, `stepNumber`, and `stepLiteral` used by `stepByte`.
- `usageInspector` memory limits, authoritative usage selection, normalization, and failure formats remain unchanged.

- [ ] **Step 1: Run every split-boundary parser characterization**

Run: `go test -count=1 -run TestUsageInspector -v ./internal/proxy`

Expected: all fifteen usage-inspector tests PASS.

- [ ] **Step 2: Move validator symbols mechanically to `json_validator.go`**

Run `gofmt`, then rerun `TestUsageInspector` before any transition rewrite. Expected: PASS with no semantic edit.

- [ ] **Step 3: Group string, number, and literal transitions**

```go
func (v *incrementalJSONValidator) stepString(current byte) (jsonScanOp, bool)
func (v *incrementalJSONValidator) stepNumber(current byte) (jsonScanOp, bool)
func (v *incrementalJSONValidator) stepLiteral(current byte) (jsonScanOp, bool)
```

The boolean reports whether the helper handled the current state. Preserve every state transition and continue/error/end-object result. Structural object/array transitions remain in `stepByte` so stack ownership stays visible.

- [ ] **Step 4: Run exhaustive boundary repetition and commit**

```powershell
gofmt -w internal/proxy/usage.go internal/proxy/json_validator.go
go test -count=20 -run TestUsageInspector ./internal/proxy
go test -race -count=1 ./internal/proxy
git add internal/proxy/usage.go internal/proxy/json_validator.go internal/proxy/usage_test.go
git commit -m "refactor: isolate incremental JSON validation"
```

### Task 8: Make Gateway Forwarding Stages Explicit

**Files:**

- Create: `internal/gateway/forward.go`
- Modify: `internal/gateway/service.go:99-117,203-451`
- Modify existing `internal/gateway/service_pool_test.go`, `service_retry_test.go`, and `usage_test.go` only if an extracted invariant lacks direct coverage.

**Interfaces:**

- `Service.Forward` and `ForwardRequest` signatures stay unchanged.
- Produces private `forwardLifecycle`, `resolvedForwardRequest`, and `targetSelector` types.
- `ResponseCompleteReservation`, `Runtime`, `Forwarder`, `Observer`, and all package boundaries remain unchanged.

- [ ] **Step 1: Run all forwarding lifecycle tests before extraction**

Run: `go test -count=1 -v ./internal/gateway`

Expected: pool lease, retry, reservation, event, usage, cancellation, readiness, and accounting tests PASS.

- [ ] **Step 2: Move `ForwardRequest` and `Service.Forward` mechanically to `forward.go`**

Run `gofmt` and `go test -count=1 ./internal/gateway`. Expected: PASS before structural edits.

- [ ] **Step 3: Introduce terminal lifecycle ownership**

```go
type forwardLifecycle struct {
	service             *Service
	started             time.Time
	admissionStarted    time.Time
	event               RequestEvent
	reservationRollback func()
}

func (l *forwardLifecycle) finish(result proxy.Result, reservation ResponseCompleteReservation, apiErr *APIError)
func (l *forwardLifecycle) rollbackOnPanic()
```

Register `defer lifecycle.rollbackOnPanic()` before the deferred call to `finish`, preserving the current LIFO behavior when observer finalization panics. `finish` copies `FirstByte`, cancellation, retry count, usage, parse failure, status/reason/decision, and rejected queue timing into the event; stages the reserved lifecycle; then calls the observer. `rollbackOnPanic` invokes the acquired rollback only while unwinding a panic and re-panics with the same value. Successful terminal staging keeps the rollback reserved for panic only.

- [ ] **Step 4: Extract stable identity and payload resolution**

```go
type resolvedForwardRequest struct {
	client       domain.Client
	pool         domain.ModelPool
	snapshot     *registry.Snapshot
	payload      []byte
	publicModel  string
	sessionID    string
	affinityKey  string
}

func (s *Service) resolveForwardRequest(request ForwardRequest, lifecycle *forwardLifecycle) (resolvedForwardRequest, *APIError)
func (s *Service) prepareForwardPayload(request ForwardRequest, resolved resolvedForwardRequest) (resolvedForwardRequest, *APIError)
```

`resolveForwardRequest` performs authenticate, client event identity, session-ID validation, payload/model parsing, the registry snapshot lookup, and stable pool event identity. `Forward` then performs the existing response-completion reservation inline so cancellation and unavailable-recorder returns stay visible. `prepareForwardPayload` next enforces pool/access policy, derives the affinity key, forces streaming usage, and replaces the upstream model. This keeps the observable ordering unchanged without hiding the reservation's three-way result inside a helper.

- [ ] **Step 5: Extract backend selection with exactly-once completion**

```go
type targetSelector struct {
	service   *Service
	client    domain.Client
	pool      domain.ModelPool
	affinity  string
	inflight  InflightEvent
	lifecycle *forwardLifecycle
}

func (s targetSelector) select(exclude map[int64]struct{}) (proxy.Target, error)
```

Keep current-registry revalidation, runtime/secret eligibility, routing, acquisition races, copied exclusions, event backend/pressure/queue fields, and `sync.Once` balancing of backend inflight and runtime completion.

- [ ] **Step 6: Reassemble the narrow orchestration method**

`Forward` owns: lifecycle defers, resolution, pool/client leases, observer client inflight, selector construction, proxy request header scrubbing, first target, alternate selector, proxy call, and final error mapping. Do not hide lease acquisition behind a helper that can obscure `defer` ordering.

- [ ] **Step 7: Stress lifecycle and commit**

```powershell
gofmt -w internal/gateway/service.go internal/gateway/forward.go
go test -count=20 ./internal/gateway
go test -race -count=1 ./internal/gateway ./internal/httpapi ./tests/integration
git add internal/gateway/service.go internal/gateway/forward.go internal/gateway/*_test.go
git commit -m "refactor: expose gateway forwarding lifecycle"
```

### Task 9: Separate Application Construction and Server Shutdown

**Files:**

- Create: `cmd/gateway/application.go`
- Create: `cmd/gateway/server.go`
- Modify: `cmd/gateway/main.go:71-246`
- Modify: `cmd/gateway/main_test.go`

**Interfaces:**

- `main`, `checkGatewayHealth`, and `run(context.Context, config.LookupFunc, net.Listener, io.Writer, io.Writer) error` stay unchanged.
- Produces `gatewayApplication`, `newGatewayApplication`, `Handler`, `Close`, and `serveGateway`.

- [ ] **Step 1: Run the complete command lifecycle contract**

Run: `go test -count=1 -v ./cmd/gateway`

Expected: health, Admin rendering, analytics, graceful stream completion, forced shutdown, missing-secret rejection, metrics, and request-ID tests PASS.

- [ ] **Step 2: Move existing support code by responsibility**

Move recorder/store ownership and API-key usage recorder to `application.go`. Move runtime metric publication/update and HTTP server lifecycle to `server.go`. Keep CLI argument handling, `checkGatewayHealth`, `run`, and `writeJSON` in `main.go`.

- [ ] **Step 3: Build an application owner**

```go
type gatewayApplication struct {
	handler         http.Handler
	database        *store.SQLite
	requestRecorder *analytics.Recorder
	apiKeyUsage     *apiKeyUsageRecorder
	manager         *monitor.Manager
	transport       *http.Transport
	closeOnce       sync.Once
	closeErr        error
}

func newGatewayApplication(ctx context.Context, cfg config.Config, getenv config.LookupFunc, stderr io.Writer) (*gatewayApplication, error)
func (a *gatewayApplication) Handler() http.Handler
func (a *gatewayApplication) Close(ctx context.Context) error
```

Construction transfers database ownership to the analytics recorder only after recorder creation. On every intermediate error, close already-owned resources in reverse order. `Close` stops key usage, drains the request recorder, closes SQLite only after recorder completion, shuts down monitoring, and closes idle upstream connections exactly once; it stores the joined result in `closeErr` so repeated calls return the same outcome.

- [ ] **Step 4: Extract condition-based server lifecycle**

```go
func serveGateway(ctx context.Context, listener net.Listener, handler http.Handler, grace time.Duration, stdout io.Writer) error
```

Use the current buffered serve-error channel. On context cancellation, call `Shutdown` with a fresh timeout context, fall back to `Close`, always consume the serve result, and join shutdown/serve errors. Do not add sleeps.

- [ ] **Step 5: Reduce `run` to ordered ownership**

```go
func run(ctx context.Context, getenv config.LookupFunc, listener net.Listener, stdout, stderr io.Writer) (runErr error) {
	cfg, err := config.Load(getenv)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	app, err := newGatewayApplication(ctx, cfg, getenv, stderr)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
		defer cancel()
		if err := app.Close(closeCtx); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close gateway application: %w", err))
		}
	}()
	if listener == nil {
		listener, err = net.Listen("tcp", cfg.ListenAddress)
		if err != nil {
			return fmt.Errorf("listen on %s: %w", cfg.ListenAddress, err)
		}
	}
	return serveGateway(ctx, listener, app.Handler(), cfg.ShutdownGracePeriod, stdout)
}
```

- [ ] **Step 6: Repeat graceful and forced shutdown tests and commit**

```powershell
gofmt -w cmd/gateway/main.go cmd/gateway/application.go cmd/gateway/server.go cmd/gateway/main_test.go
go test -count=20 -run 'TestRun|TestAdminDashboard|TestPublishRuntime|TestCloseRecorder' ./cmd/gateway
go test -race -count=1 ./cmd/gateway ./tests/integration
git add cmd/gateway/main.go cmd/gateway/application.go cmd/gateway/server.go cmd/gateway/main_test.go
git commit -m "refactor: separate gateway application lifecycle"
```

### Task 10: Complete the Whole-Project Go Audit

**Files:**

- Inspect every tracked `*.go` file and `go.mod`/`go.sum`.
- Modify only files with a concrete correctness, ownership, error-handling, API-design, or duplication finding supported by a test.

**Interfaces:**

- No new exported symbol unless required by an existing package consumer.
- No module requirement changes.

- [ ] **Step 1: Run semantic and static diagnostics over every package**

Run:

```powershell
gofmt -l .
go vet ./...
go test -count=1 ./...
go list -deps -test ./... | Out-Null
$changedGo = @(git diff --name-only origin/main...HEAD -- '*.go')
if ($changedGo.Count -gt 0) {
	go run golang.org/x/tools/gopls@v0.23.0 check @changedGo
}
```

`gopls` is not installed on this host, so use the pinned one-shot command above; it populates only the Go download/build cache and does not edit `go.mod` or `go.sum`. The repository has no golangci-lint configuration, so do not introduce a new lint policy during this behavior-preserving refactor. Record each diagnostic before editing.

- [ ] **Step 2: Apply the Go skill checklists package by package**

Review in dependency order: `domain`; `admission`, `apikey`, `circuitbreaker`, `pressure`, `routing`; `registry`, `store`; `monitor`, `proxy`, `analytics`, `observability`; `gateway`, `httpapi`, `web`; `fakevllm`, `loadgen`; `cmd/*`; `tests/*`.

For each package check: dependency direction, naming/API surface, nil/collection safety, error context, context propagation, goroutine ownership, channel closure, lock/I/O boundaries, SQL parameterization/rows closure/transactions, bounded telemetry, hot-loop allocation, secret handling, dependency necessity, build/test portability, Go 1.27-safe standard-library modernization, and test determinism. Keep structural extraction and any behavioral/correctness fix in separate commits.

- [ ] **Step 3: Handle findings one at a time**

For each material finding: add a focused failing test, run it to prove the gap, make the smallest fix, run the package and impacted integration tests, then commit with `fix:` or `refactor:`. If no finding exists in a package, make no edit.

- [ ] **Step 4: Confirm dependency and diff discipline**

Run:

```powershell
go mod verify
go mod tidy -diff
go list -m all
go mod graph
go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...
git diff origin/main...HEAD -- go.mod go.sum
git diff --check origin/main...HEAD
git status --short
```

Expected: verified modules, no reachable untriaged vulnerability, no `go.mod`/`go.sum` diff, no whitespace errors, and no untracked build artifacts. Treat scanner output as evidence: confirm symbol reachability and deployed-build conditions before changing a version.

### Task 11: Full Host and Docker Verification

**Files:**

- No source edits unless a verification failure is root-caused and fixed with a regression test.

**Interfaces:**

- Produces authoritative command output proving host, Linux-container, image, and Compose behavior.

- [ ] **Step 1: Run final host gates from a clean worktree**

```powershell
gofmt -l .
go test -count=1 ./...
go test -shuffle=on -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go build ./cmd/...
go test -count=10 ./internal/gateway ./internal/proxy ./internal/analytics ./cmd/gateway
```

Expected: `gofmt -l` prints nothing; every command exits 0.

- [ ] **Step 2: Build static Linux commands**

Run the PowerShell equivalents of the Make targets with task-specific output paths under `dist/`, then remove only those generated artifacts after recording success:

```powershell
$previousCGO = [Environment]::GetEnvironmentVariable('CGO_ENABLED', 'Process')
$previousGOOS = [Environment]::GetEnvironmentVariable('GOOS', 'Process')
$previousGOARCH = [Environment]::GetEnvironmentVariable('GOARCH', 'Process')
$artifacts = @('dist/gateway-linux-amd64', 'dist/fake-vllm-linux-amd64', 'dist/loadgen-linux-amd64')
try {
	$env:CGO_ENABLED = '0'
	$env:GOOS = 'linux'
	$env:GOARCH = 'amd64'
	go build -trimpath -o $artifacts[0] ./cmd/gateway
	go build -trimpath -o $artifacts[1] ./cmd/fake-vllm
	go build -trimpath -o $artifacts[2] ./cmd/loadgen
} finally {
	Remove-Item -LiteralPath $artifacts -ErrorAction SilentlyContinue
	[Environment]::SetEnvironmentVariable('CGO_ENABLED', $previousCGO, 'Process')
	[Environment]::SetEnvironmentVariable('GOOS', $previousGOOS, 'Process')
	[Environment]::SetEnvironmentVariable('GOARCH', $previousGOARCH, 'Process')
}
```

- [ ] **Step 3: Start and verify Docker Desktop Linux engine**

```powershell
$dockerDesktop = 'C:\Program Files\Docker\Docker\Docker Desktop.exe'
if ((docker info --format '{{.OSType}}' 2>$null) -ne 'linux') {
	if (-not (Test-Path -LiteralPath $dockerDesktop)) {
		throw "Docker Desktop executable not found: $dockerDesktop"
	}
	Start-Process -FilePath $dockerDesktop -WindowStyle Hidden
	for ($attempt = 0; $attempt -lt 30; $attempt++) {
		if ((docker info --format '{{.OSType}}' 2>$null) -eq 'linux') { break }
		Start-Sleep -Seconds 2
	}
}
docker version
if ((docker info --format '{{.OSType}}') -ne 'linux') { throw 'Docker Linux engine is unavailable' }
```

The bounded startup attempt lasts at most 60 seconds. If it does not connect, preserve the diagnostic and retry in a later bounded call rather than hiding the failure.

- [ ] **Step 4: Run the full suite in a clean Go 1.27 Linux container**

```powershell
$checkout = (Get-Location).Path
$moduleVolume = 'llmgw-go-refactor-modcache'
$buildVolume = 'llmgw-go-refactor-buildcache'
docker volume create $moduleVolume | Out-Null
docker volume create $buildVolume | Out-Null
try {
	docker run --rm --name llmgw-go-refactor-tests `
		--mount "type=bind,source=$checkout,target=/src,readonly" `
		--mount "type=volume,source=$moduleVolume,target=/go/pkg/mod" `
		--mount "type=volume,source=$buildVolume,target=/root/.cache/go-build" `
		--workdir /src golang:1.27 go test -count=1 ./...
	if ($LASTEXITCODE -ne 0) { throw 'Linux container test suite failed' }
} finally {
	docker rm -f llmgw-go-refactor-tests 2>$null | Out-Null
	docker volume rm $moduleVolume $buildVolume 2>$null | Out-Null
}
```

- [ ] **Step 5: Run Compose contracts, production image build, and smoke test**

```powershell
go test -count=1 -v ./tests/compose
docker build --platform linux/amd64 --tag llmgw-go-refactor-verification .
```

Run this PowerShell equivalent of `scripts/container-smoke.sh` after the image build so the task-specific object names and cleanup are deterministic:

```powershell
$smokeContainer = 'llmgw-go-refactor-smoke'
$smokeVolume = 'llmgw-go-refactor-data'
docker volume create $smokeVolume | Out-Null
try {
	docker run -d --name $smokeContainer -p 127.0.0.1::8080 `
		--mount "type=volume,source=$smokeVolume,target=/data" `
		-e LLMGW_ADMIN_USERNAME=operator `
		-e LLMGW_ADMIN_PASSWORD='correct horse battery staple' `
		-e LLMGW_API_KEY_HMAC_SECRET='01234567890123456789012345678901' `
		llmgw-go-refactor-verification | Out-Null
	if ((docker inspect --format '{{.Config.User}}' $smokeContainer) -ne '65532:65532') {
		throw 'Production image does not run as UID/GID 65532'
	}
	$published = docker port $smokeContainer 8080/tcp | Select-Object -First 1
	$port = $published.Substring($published.LastIndexOf(':') + 1)
	$healthy = $false
	for ($attempt = 0; $attempt -lt 50; $attempt++) {
		try {
			$response = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$port/healthz" -TimeoutSec 1
			if ($response.StatusCode -eq 200) { $healthy = $true; break }
		} catch {}
		Start-Sleep -Milliseconds 100
	}
	if (-not $healthy) {
		docker logs $smokeContainer
		throw 'Production image health smoke failed'
	}
	$mount = docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Type}}:{{.Name}}{{end}}{{end}}' $smokeContainer
	if ($mount -ne "volume:$smokeVolume") { throw "Unexpected /data mount: $mount" }
} finally {
	docker rm -f $smokeContainer 2>$null | Out-Null
	docker volume rm $smokeVolume 2>$null | Out-Null
	docker image rm llmgw-go-refactor-verification 2>$null | Out-Null
}
```

The scratch image intentionally has no shell, so data-volume writeability is proven by a healthy gateway after SQLite creation plus the inspected named `/data` mount. The full real-vLLM Compose stack remains an optional hardware acceptance test because it requires NVIDIA GPUs and large model downloads; the deterministic Compose render tests, Linux-container suite, production build, UID check, volume check, and live `/healthz` smoke are mandatory.

- [ ] **Step 6: Diagnose any failure before changing code**

Use `superpowers:systematic-debugging` and `golang-troubleshooting`; preserve logs, isolate the first incorrect state, add a regression test, fix once, then rerun all affected and final gates.

### Task 12: Independent Subagent Review and Final Reverification

**Files:**

- Review: complete `origin/main...HEAD` diff.
- Modify: only files implicated by accepted findings.

**Interfaces:**

- Produces an independent review covering correctness, compatibility, concurrency, security, performance, tests, and documentation.

- [ ] **Step 1: Refresh graph/diff evidence for the reviewer**

If codebase-memory is available, call `detect_changes` against `origin/main`, trace both directions for changed high-fan-in symbols, and call `check_index_coverage` for every changed path plus root scope. Read every reported missed range directly. If the transport remains closed, state that limitation and provide direct-source/diff evidence instead.

- [ ] **Step 2: Dispatch a fresh review subagent**

Pass: spec, plan, base/head SHAs, exact changed files, graph project/generation/coverage evidence, source fallbacks, command outputs, Docker evidence, and the requirement to report only actionable findings with severity, file/line, invariant violated, and a concrete reproduction.

- [ ] **Step 3: Triage every finding skeptically**

For each finding, reproduce or prove it from source. Accept valid findings; reject false positives with exact evidence. Use `superpowers:receiving-code-review` before editing.

- [ ] **Step 4: Fix accepted findings with TDD and request a second review pass**

Use the original implementer/reviewer agent via follow-up when possible. Run focused tests after each fix and repeat the relevant host/Docker gates.

- [ ] **Step 5: Run the complete final verification again**

Repeat Task 11 Steps 1, 4, and 5 after the last review fix. Require a clean `git status`, no dependency diff, no contract/doc drift, and all commands exiting 0.

- [ ] **Step 6: Commit final review fixes and prepare handoff**

Use focused commit messages. Summarize commits, changed responsibilities, tests, Docker evidence, review findings/fixes, remaining hardware-only acceptance limits, and the branch/worktree path. Do not merge or push unless explicitly requested.
