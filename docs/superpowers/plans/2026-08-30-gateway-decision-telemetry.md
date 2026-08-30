# Gateway Decision Telemetry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose bounded, decision-specific gateway telemetry and a provisioned Grafana dashboard that makes GPU-backed overload, Low shedding, and High latency continuity visible in one timeline.

**Architecture:** Extend the existing `gateway.RequestEvent` and Prometheus observer rather than adding an event bus. The request service assigns bounded decision reasons and one admission-to-selection sample, the runtime publisher adds pool pressure/state, and the existing backend in-flight callback counts actual backend leases. An optional Compose overlay provisions Prometheus 3.14.0 and Grafana 13.2.0 with a checked-in dashboard.

**Tech Stack:** Go 1.27, Prometheus client_golang, Prometheus 3.14.0, Grafana 13.2.0, Docker Compose, JSON/YAML provisioning, real-vLLM E2E tests.

**Spec:** `docs/superpowers/specs/2026-08-30-gateway-decision-telemetry-design.md`

## Global Constraints

- Preserve the public OpenAI-compatible response status, body, error code, and `Retry-After` behavior.
- Keep all existing `llmgw_*` family names and existing label names intact.
- Use only configured client/model/backend names and bounded enums as labels; never expose request IDs, keys, URLs, prompts, generated text, session IDs, or upstream-provided free text.
- `llmgw_queue_wait_seconds` means gateway admission-to-first-backend-lease or admission-to-terminal-rejection time, not vLLM internal queue residency.
- Retries add backend selection counts but never add a second queue-wait sample for one request.
- The Prometheus/Grafana overlay is optional and binds both UIs to loopback by default.
- Production code is written only after its behavior test has failed for the expected missing-feature reason.
- Native Windows SQLite tests are a known baseline limitation (`file://C:` URI authority); authoritative full-suite verification runs in the pinned Linux Go 1.27 container.

---

### Task 1: Attach bounded decision reasons and queue timing to request events

**Files:**
- Modify: `internal/gateway/observer.go:20-41`
- Modify: `internal/gateway/service.go:99-105,202-429,553-585`
- Modify: `internal/gateway/service_pool_test.go:24-307`
- Modify: `internal/gateway/service_retry_test.go:214-308,513-543`
- Modify: `internal/gateway/usage_test.go:99-304`
- Modify: `internal/observability/log.go:23-44`
- Modify: `internal/observability/log_test.go`

**Interfaces:**
- Produces: `type DecisionReason string` and exported constants `DecisionPoolWaitingLimit`, `DecisionPoolInflightLimit`, `DecisionPriorityConcurrencyLimit`, `DecisionPoolUnavailable`, `DecisionNoEligibleBackend`, `DecisionGatewayBackpressure`, `DecisionModelNotAllowed`, `DecisionInvalidRequest`, `DecisionInvalidAPIKey`, and `DecisionUpstreamFailure`.
- Produces: `type QueueOutcome string` with `QueueSelected` and `QueueRejected`.
- Produces: `RequestEvent.DecisionReason DecisionReason`, `RequestEvent.QueueWait time.Duration`, and `RequestEvent.QueueOutcome QueueOutcome`.
- Produces: `APIError.DecisionReason DecisionReason`; it is internal metadata and is not serialized by `writeGatewayError`.
- Consumes later: the metrics observer reads these fields without changing the `gateway.Observer` method set.

- [ ] **Step 1: Extend existing pool-safety tests with exact decision reasons**

Add literal assertions to the three distinct overload branches. The waiting test must contain:

~~~go
if apiErr.DecisionReason != gateway.DecisionPoolWaitingLimit {
	t.Fatalf("waiting rejection reason = %q, want %q", apiErr.DecisionReason, gateway.DecisionPoolWaitingLimit)
}
~~~

The pool-inflight test asserts `gateway.DecisionPoolInflightLimit`, and `TestServiceReleasesPoolLeaseWhenClientLimiterRejects` asserts `gateway.DecisionPriorityConcurrencyLimit`. Keep the existing status/code/Retry-After assertions so telemetry cannot alter the API contract.

- [ ] **Step 2: Run the focused gateway tests and verify RED**

Run:

~~~powershell
go test ./internal/gateway -run 'TestServicePoolMaxWaitingRejectsBeforePoolAndClientAcquisition|TestServicePoolMaxInflightRejectsSecondAndReleasesAfterCompletion|TestServiceReleasesPoolLeaseWhenClientLimiterRejects' -count=1
~~~

Expected: compile failure because `DecisionReason` and the decision constants do not exist.

- [ ] **Step 3: Add the bounded types and request/API fields**

Add to `observer.go`:

~~~go
type DecisionReason string

const (
	DecisionPoolWaitingLimit         DecisionReason = "pool_waiting_limit"
	DecisionPoolInflightLimit        DecisionReason = "pool_inflight_limit"
	DecisionPriorityConcurrencyLimit DecisionReason = "priority_concurrency_limit"
	DecisionPoolUnavailable          DecisionReason = "pool_unavailable"
	DecisionNoEligibleBackend        DecisionReason = "no_eligible_backend"
	DecisionGatewayBackpressure      DecisionReason = "gateway_backpressure"
	DecisionModelNotAllowed          DecisionReason = "model_not_allowed"
	DecisionInvalidRequest           DecisionReason = "invalid_request"
	DecisionInvalidAPIKey            DecisionReason = "invalid_api_key"
	DecisionUpstreamFailure          DecisionReason = "upstream_failure"
)

type QueueOutcome string

const (
	QueueSelected QueueOutcome = "selected"
	QueueRejected QueueOutcome = "rejected"
)
~~~

Append the three event fields to `RequestEvent` and `DecisionReason DecisionReason` to `APIError`.

- [ ] **Step 4: Assign a reason at every gateway-owned error factory and overload branch**

Make overload construction explicit:

~~~go
func overloaded(retryAfter time.Duration, reason DecisionReason) *APIError {
	return &APIError{
		HTTPStatus: http.StatusTooManyRequests, Message: "Gateway is overloaded",
		Type: "rate_limit_error", Code: "gateway_overloaded",
		RetryAfter: retryAfter, DecisionReason: reason,
	}
}
~~~

Use `DecisionPoolWaitingLimit` when `MaxWaiting` is reached, `DecisionPoolInflightLimit` when `AcquirePool` refuses the lease, and `DecisionPriorityConcurrencyLimit` when `Limiter.Acquire` refuses. Give `backendUnavailable`, `gatewayUnavailable`, `modelNotAllowed`, `invalidRequest`, `invalidAPIKey`, and `upstreamError` their corresponding constants while keeping public fields byte-for-byte compatible.

- [ ] **Step 5: Re-run the focused gateway tests and verify GREEN**

Run the Step 2 command. Expected: PASS.

- [ ] **Step 6: Add failing request-event tests for one queue sample and precise terminal reasons**

Use the existing real `backendRecordingObserver` and retry forwarder. Add `TestForwardRecordsAdmissionWaitOnceAcrossRetry` with:

~~~go
event := observer.Event()
if event.QueueOutcome != gateway.QueueSelected || event.QueueWait < 0 {
	t.Fatalf("selected queue telemetry = outcome %q wait %s", event.QueueOutcome, event.QueueWait)
}
if event.DecisionReason != "" {
	t.Fatalf("successful request decision reason = %q, want empty", event.DecisionReason)
}
~~~

Extend the retry case to assert a single terminal event still has one `QueueSelected` outcome after two backend leases. Add an optional `observer gateway.Observer` to `poolServiceOptions`, pass it through `newPoolService`, and make the waiting-limit test assert `QueueRejected` plus `DecisionPoolWaitingLimit` on the recorded event. Extend the known-model policy denial test to assert `DecisionModelNotAllowed` and an empty queue outcome because the request did not enter pool admission.

- [ ] **Step 7: Run the event tests and verify RED**

Run:

~~~powershell
go test ./internal/gateway -run 'TestForwardRecordsAdmissionWaitOnceAcrossRetry|TestServicePoolMaxWaitingRejectsBeforePoolAndClientAcquisition|TestRequestEventUsageRecordsIdentityForKnownModelPolicyDenials' -count=1
~~~

Expected: failure because `Forward` does not populate queue outcome/wait or copy the API error decision reason.

- [ ] **Step 8: Instrument the request lifecycle once**

After stable model resolution and before `acquirePool`, set `admissionStarted := time.Now()`. In the existing completion defer, copy `apiErr.DecisionReason` and, only when an API rejection ends an admission with no selected outcome, record:

~~~go
if !admissionStarted.IsZero() && event.QueueOutcome == "" && apiErr != nil {
	event.QueueWait = time.Since(admissionStarted)
	event.QueueOutcome = QueueRejected
}
~~~

Immediately after the first successful `AcquireBackend`:

~~~go
if event.QueueOutcome == "" {
	event.QueueWait = time.Since(admissionStarted)
	event.QueueOutcome = QueueSelected
}
~~~

Do not modify the outcome during alternate selection. Cancellation without an API error remains unclassified.

- [ ] **Step 9: Re-run gateway tests and verify GREEN**

Run: `go test ./internal/gateway -count=1`. Expected: PASS.

- [ ] **Step 10: Add a failing structured-log test**

Complete an event with `DecisionPoolWaitingLimit`, `QueueRejected`, and `12*time.Millisecond`; decode the JSON record and assert:

~~~go
if record["decisionReason"] != "pool_waiting_limit" || record["queueOutcome"] != "rejected" {
	t.Fatalf("decision log fields = %+v", record)
}
if record["queueWaitMs"] != float64(12) {
	t.Fatalf("queueWaitMs = %#v, want 12", record["queueWaitMs"])
}
~~~

- [ ] **Step 11: Run log test RED, implement fields, and verify GREEN**

Run `go test ./internal/observability -run TestLoggerIncludesDecisionTelemetry -count=1`. Expected first: FAIL because fields are absent. Then append:

~~~go
slog.String("decisionReason", string(event.DecisionReason)),
slog.String("queueOutcome", string(event.QueueOutcome)),
slog.Float64("queueWaitMs", float64(event.QueueWait.Microseconds())/1000),
~~~

Rerun for PASS.

- [ ] **Step 12: Verify Task 1 and commit**

~~~powershell
gofmt -w internal/gateway/observer.go internal/gateway/service.go internal/gateway/service_pool_test.go internal/gateway/service_retry_test.go internal/gateway/usage_test.go internal/observability/log.go internal/observability/log_test.go
go test ./internal/gateway ./internal/observability -count=1
git diff --check
git add internal/gateway internal/observability/log.go internal/observability/log_test.go
git commit -m "feat: classify gateway admission decisions"
~~~

Expected: tests PASS, diff clean, commit created.

---

### Task 2: Export pool pressure, pool state, backend selections, and queue wait

**Files:**
- Modify: `internal/observability/metrics.go:16-425`
- Modify: `internal/observability/metrics_test.go:16-152`
- Modify: `cmd/gateway/main_test.go:98-277`

**Interfaces:**
- Consumes: Task 1 event fields and positive-delta `BackendInflight` lifecycle.
- Produces: `llmgw_pool_pressure{model}`.
- Produces: `llmgw_pool_state{model,state}` with five one-hot state values.
- Produces: `llmgw_backend_selected_total{model,backend}`.
- Produces: `llmgw_queue_wait_seconds{model,priority_class,outcome}`.

- [ ] **Step 1: Extend the metrics contract test first**

Add two positive backend assignments and releases, one rejected event with `DecisionPoolWaitingLimit`/`QueueRejected`/12ms, one selected event with `QueueSelected`/4ms, and:

~~~go
metrics.SetPool("qwen", domain.PoolRuntime{
	State: domain.PoolSaturated, BestBackendPressure: 1.25,
	GatewayInflight: 3, TotalWaiting: 2.5, AvailableBackends: 2,
})
~~~

Require these literal samples:

~~~text
llmgw_backend_selected_total{backend="gpu-1",model="qwen"} 2
llmgw_pool_pressure{model="qwen"} 1.25
llmgw_pool_state{model="qwen",state="saturated"} 1
llmgw_pool_state{model="qwen",state="normal"} 0
llmgw_requests_rejected_total{client="client-a",model="qwen",priority_class="high",reason="pool_waiting_limit"} 1
llmgw_queue_wait_seconds_count{model="qwen",outcome="rejected",priority_class="high"} 1
llmgw_queue_wait_seconds_count{model="qwen",outcome="selected",priority_class="high"} 1
~~~

- [ ] **Step 2: Run the metrics test and verify RED**

Run `go test ./internal/observability -run TestMetricsExposeRequiredFamiliesWithoutHighCardinalityLabels -count=1`.

Expected: FAIL because new families are absent and rejection still uses the public API code.

- [ ] **Step 3: Define and register the collectors**

~~~go
m.poolPressure = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{Name: "llmgw_pool_pressure", Help: "Best eligible backend pressure used for pool admission."},
	[]string{"model"},
)
m.poolState = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{Name: "llmgw_pool_state", Help: "One-hot gateway pool admission state."},
	[]string{"model", "state"},
)
m.backendSelected = prometheus.NewCounterVec(
	prometheus.CounterOpts{Name: "llmgw_backend_selected_total", Help: "Backend leases selected by the gateway, including retries."},
	[]string{"model", "backend"},
)
m.queueWait = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{Name: "llmgw_queue_wait_seconds", Help: "Gateway admission-to-selection or admission-to-rejection time.", Buckets: prometheus.DefBuckets},
	[]string{"model", "priority_class", "outcome"},
)
~~~

Add fields to `Metrics` and register all four in its private registry.

- [ ] **Step 4: Connect collectors to lifecycle callbacks**

In `BackendInflight` increment `backendSelected` only for `delta > 0`. In `Complete` prefer the explicit reason and observe queue wait only for a non-empty outcome:

~~~go
reason := event.Reason
if event.DecisionReason != "" {
	reason = string(event.DecisionReason)
}
if reason != "" {
	m.rejected.WithLabelValues(client, model, priority, value(reason)).Inc()
}
if event.QueueOutcome != "" {
	m.queueWait.WithLabelValues(model, priority, string(event.QueueOutcome)).Observe(event.QueueWait.Seconds())
}
~~~

- [ ] **Step 5: Publish one-hot pool state and pressure with cleanup**

~~~go
var poolMetricStates = []domain.PoolState{
	domain.PoolNormal, domain.PoolBusy, domain.PoolSaturated,
	domain.PoolEmergency, domain.PoolUnavailable,
}
~~~

`setPool` sets pressure and all five state series, with exactly the active state equal to 1. `PublishRuntime` deletes `poolPressure` and every `poolState` label pair for removed/renamed pools together with existing pool gauges.

- [ ] **Step 6: Re-run metrics tests and verify GREEN**

Run `go test ./internal/observability -count=1`. Expected: PASS.

- [ ] **Step 7: Extend runtime publisher tests**

Set `runtime.pool.State = domain.PoolEmergency` and `BestBackendPressure = 1.6`. Require:

~~~text
llmgw_pool_pressure{model="qwen"} 1.6
llmgw_pool_state{model="qwen",state="emergency"} 1
llmgw_pool_state{model="qwen",state="busy"} 0
~~~

Extend topology removal to reject old-model pressure/state samples.

- [ ] **Step 8: Run publisher tests RED then GREEN**

Run:

~~~powershell
go test ./cmd/gateway -run 'TestPublishRuntimeMetricsSetsBackendAndPoolGauges|TestPublishRuntimeMetricsRemovesRenamedAndDeletedTopologySeries' -count=1
~~~

Expected before cleanup: FAIL on new/stale samples. Expected after Step 5: PASS.

- [ ] **Step 9: Verify Task 2 and commit**

~~~powershell
gofmt -w internal/observability/metrics.go internal/observability/metrics_test.go cmd/gateway/main_test.go
go test ./internal/observability ./cmd/gateway -count=1
git diff --check
git add internal/observability/metrics.go internal/observability/metrics_test.go cmd/gateway/main_test.go
git commit -m "feat: expose gateway decision metrics"
~~~

---

### Task 3: Prove the metric contract in integration and real-vLLM tests

**Files:**
- Modify: `tests/integration/admission_priority_test.go:13-89`
- Modify: `tests/integration/harness_test.go`
- Modify: `tests/e2e/harness_test.go:497-650`
- Modify: `tests/e2e/real_vllm_test.go:71-186`

**Interfaces:**
- Consumes: Task 2 metric families.
- Produces test helper: `metricsResponse.value(name string, labels map[string]string) (float64, bool)` and `metricsResponse.histogramCount(name string, labels map[string]string) uint64`.
- Produces real-vLLM priority evidence for pressure, Low rejection reasons, High duration, backend selections, and queue wait.

- [ ] **Step 1: Add a failing integration assertion around emergency admission**

Scrape `/metrics` before and after Critical/High/Normal/Background requests. Parse real exposition with `prometheus/common/expfmt.TextParser` and assert:

~~~go
if got := metricCounterDelta(before, after, "llmgw_requests_rejected_total", map[string]string{"reason": "priority_concurrency_limit"}); got != 2 {
	t.Fatalf("priority rejection delta = %v, want 2", got)
}
if got := metricCounterDelta(before, after, "llmgw_backend_selected_total", map[string]string{"backend": "gpu-a"}); got != 2 {
	t.Fatalf("backend selection delta = %v, want 2", got)
}
~~~

- [ ] **Step 2: Run integration RED and complete only missing wiring**

Run `go test ./tests/integration -run TestAdmissionPriorityAndHysteresisAcceptance -count=1` (Linux container on Windows if SQLite blocks native execution).

Expected: metric assertion failure until the real HTTP path supplies Task 1/2 fields. Update existing event finalization, not a second observer. If runtime gauges need publication in the harness, call production `Metrics.SetPool`/`SetBackend` after observing manager state; never duplicate pressure calculations.

- [ ] **Step 3: Re-run integration and verify GREEN**

Run Step 2 command. Expected: PASS with exact deltas.

- [ ] **Step 4: Add failing real-metrics parser tests**

~~~go
fixture := metricsResponse("llmgw_pool_pressure{model=\"qwen\"} 1.4\n" +
	"llmgw_queue_wait_seconds_count{model=\"qwen\",priority_class=\"high\",outcome=\"selected\"} 3\n")
if got, ok := fixture.value("llmgw_pool_pressure", map[string]string{"model": "qwen"}); !ok || got != 1.4 {
	t.Fatalf("pool pressure = %v, %t", got, ok)
}
~~~

- [ ] **Step 5: Run parser RED, implement, then verify GREEN**

Run `go test ./tests/e2e -run TestMetricsResponseLookup -count=1`. Expected first: compile failure. Implement using `expfmt.TextParser` and DTO counter/gauge/histogram fields, then rerun for PASS.

- [ ] **Step 6: Extend the real-vLLM priority scenario**

Before sustained load, issue one High streaming baseline and capture `baselineHigh.FirstByte` and `beforeMetrics`. After saturation and Low/High probes, require:

~~~go
requireMetricIncrease(t, beforeMetrics, loadedMetrics, "llmgw_requests_rejected_total", map[string]string{
	"model": cfg.model, "priority_class": "background", "reason": "priority_concurrency_limit",
}, 3)
requireMetricIncrease(t, beforeMetrics, loadedMetrics, "llmgw_backend_selected_total", map[string]string{"model": cfg.model}, 1)
requireHistogramIncrease(t, beforeMetrics, loadedMetrics, "llmgw_request_duration_seconds", map[string]string{
	"model": cfg.model, "priority_class": "high", "status_class": "2xx",
}, 1)
requireHistogramIncrease(t, beforeMetrics, loadedMetrics, "llmgw_queue_wait_seconds", map[string]string{
	"model": cfg.model, "priority_class": "high", "outcome": "selected",
}, 1)
~~~

Assert current pool pressure is positive. Log baseline/loaded High first byte and their ratio; do not hide a universal flaky threshold in the test.

- [ ] **Step 7: Compile E2E, verify Task 3, and commit**

~~~powershell
gofmt -w tests/integration/admission_priority_test.go tests/integration/harness_test.go tests/e2e/harness_test.go tests/e2e/real_vllm_test.go
go test ./tests/e2e -count=1
git diff --check
git add tests/integration tests/e2e
git commit -m "test: verify overload decision telemetry"
~~~

Expected: parser tests PASS; real E2E compiles and skips without `LLMGW_E2E_MODE`.

---

### Task 4: Provision Prometheus and the Gateway Decisions dashboard

**Files:**
- Create: `compose.observability.yaml`
- Create: `deploy/observability/prometheus.yml`
- Create: `deploy/observability/grafana/provisioning/datasources/prometheus.yml`
- Create: `deploy/observability/grafana/provisioning/dashboards/dashboards.yml`
- Create: `deploy/observability/grafana/dashboards/gateway-decisions.json`
- Create: `tests/compose/observability_test.go`

**Interfaces:**
- Produces loopback Prometheus on `${LLMGW_PROMETHEUS_PORT:-9090}` and Grafana on `${LLMGW_GRAFANA_PORT:-3000}`.
- Produces datasource UID `llmgw-prometheus` and dashboard UID `llmgw-gateway-decisions`.
- Consumes all Task 2 families plus existing running/waiting/TTFT/circuit/inflight metrics.

- [ ] **Step 1: Write the failing overlay/dashboard contract test**

Execute:

~~~go
command := exec.Command("docker", "compose", "-f", "compose.yaml", "-f", "compose.observability.yaml", "config", "--format", "json")
command.Dir = repositoryRoot(t)
~~~

Decode services and require exact images `prom/prometheus:v3.14.0` and `grafana/grafana:13.2.0`, loopback ports, read-only config mounts, and named data volumes. Decode dashboard JSON and require UID, datasource UID, variables `model`, `backend`, `client`, `priority`, and these causal queries:

~~~promql
max by (model) (llmgw_pool_pressure{model=~"$model"})
sum by (reason) (rate(llmgw_requests_rejected_total{model=~"$model",priority_class="background"}[$__rate_interval]))
histogram_quantile(0.95, sum by (le) (rate(llmgw_request_duration_seconds_bucket{model=~"$model",priority_class="high",status_class="2xx"}[$__rate_interval])))
histogram_quantile(0.95, sum by (le) (rate(llmgw_queue_wait_seconds_bucket{model=~"$model",priority_class="high",outcome="selected"}[$__rate_interval])))
~~~

- [ ] **Step 2: Run test and verify RED**

Run `go test ./tests/compose -run TestObservabilityOverlayAndDashboardContract -count=1`.

Expected: FAIL because files do not exist.

- [ ] **Step 3: Add Prometheus configuration and Compose overlay**

`prometheus.yml`:

~~~yaml
global:
  scrape_interval: 1s
  evaluation_interval: 1s

scrape_configs:
  - job_name: vllm-priority-gateway
    static_configs:
      - targets: [gateway:8080]
~~~

The overlay defines Prometheus/Grafana, named data volumes, loopback ports, read-only provisioning mounts, `GF_SECURITY_ADMIN_USER`, `GF_SECURITY_ADMIN_PASSWORD`, and `GF_USERS_ALLOW_SIGN_UP=false`. Grafana depends on Prometheus; Prometheus depends on healthy gateway.

- [ ] **Step 4: Add datasource and dashboard provisioning**

Datasource URL is `http://prometheus:9090`, access `proxy`, UID `llmgw-prometheus`, default/read-only. Dashboard provisioning reads `/var/lib/grafana/dashboards`, disables UI deletion, and permits checked-in updates.

- [ ] **Step 5: Build aligned dashboard panels**

Use a Grafana 13.2-compatible schema, 5-second refresh, browser timezone, and top panels:

1. Pool pressure, thresholds .55/.9/1.2/1.5.
2. Low 429 decisions/sec split by reason.
3. High p95 request duration and TTFT.
4. High gateway queue-wait p95.

Add pool-state timeline, request rate by priority/status, client/request inflight, backend pressure/running/waiting, backend selection rate, and circuit state. Every target uses datasource UID `llmgw-prometheus` and variables where applicable.

- [ ] **Step 6: Run contract and consumer validators**

~~~powershell
go test ./tests/compose -run TestObservabilityOverlayAndDashboardContract -count=1
$sourcePath = (Get-Location).Path
docker run --rm --mount "type=bind,source=$sourcePath/deploy/observability,target=/etc/llmgw-observability,readonly" prom/prometheus:v3.14.0 promtool check config /etc/llmgw-observability/prometheus.yml
docker compose -f compose.yaml -f compose.observability.yaml config --quiet
~~~

Expected: exit 0.

- [ ] **Step 7: Commit Task 4**

~~~powershell
git diff --check
git add compose.observability.yaml deploy/observability tests/compose/observability_test.go
git commit -m "feat: provision gateway decisions dashboard"
~~~

---

### Task 5: Document metric and operator contracts

**Files:**
- Modify: `README.md`
- Modify: `README.ru.md`
- Modify: `docs/operations.md`
- Modify: `docs/technical-specification.md`
- Modify: `docs/real-gpu-testing.md`
- Modify: `docs/real-vllm-priority-e2e.md`
- Modify: `docs/acceptance-evidence.md`

**Interfaces:**
- Consumes exact Task 1/2 names/reasons and Task 4 paths/ports.
- Produces copy-paste local commands and PromQL for the overload sequence.

- [ ] **Step 1: Update both READMEs**

Document:

~~~bash
docker compose -f compose.yaml -f compose.observability.yaml up -d --build
~~~

Grafana: `http://127.0.0.1:3000/d/llmgw-gateway-decisions`. Prometheus: `http://127.0.0.1:9090`. State that local defaults must be overridden outside loopback.

- [ ] **Step 2: Update operations and technical specification**

Add exact metric/label tables, bounded reason vocabulary, timer boundaries, retry behavior, one-hot pool state, backend lease counting, topology cleanup, cardinality rules, and compatibility note: queries matching `reason="gateway_overloaded"` migrate to three precise overload reasons. Include the four top-row PromQL expressions.

- [ ] **Step 3: Update GPU/E2E runbooks and initial acceptance mapping**

Add overlay startup, Prometheus target check, dashboard URL, required retained evidence, and E2E metric-delta criteria. Add named deterministic tests immediately; measured GPU values are added only after Task 6.

- [ ] **Step 4: Verify references and commit**

~~~powershell
rg -n 'llmgw_pool_pressure|llmgw_backend_selected_total|llmgw_queue_wait_seconds|priority_concurrency_limit|compose.observability.yaml' README.md README.ru.md docs
git diff --check
git add README.md README.ru.md docs
git commit -m "docs: explain gateway decision telemetry"
~~~

---

### Task 6: Full validation, local GPU evidence, and independent review

**Files:**
- Modify after measurement: `docs/acceptance-evidence.md`
- Modify after measurement: `docs/real-gpu-testing.md`
- Modify for review fixes only: paths named by validated findings.

**Interfaces:**
- Consumes complete implementation and local secret/GPU setup.
- Produces fresh verification, running dashboard evidence, real-vLLM measurements, reviewer findings, and final audit.

- [ ] **Step 1: Run static, Linux suite, and artifact checks**

~~~powershell
gofmt -w (rg --files -g '*.go' cmd internal tests)
go vet ./...
$sourcePath = (Get-Location).Path
docker run --rm --mount "type=bind,source=$sourcePath,target=/src" -w /src golang:1.27-bookworm sh -c 'go test ./...'
docker compose -f compose.yaml -f compose.observability.yaml config --quiet
docker run --rm --mount "type=bind,source=$sourcePath/deploy/observability,target=/etc/llmgw-observability,readonly" prom/prometheus:v3.14.0 promtool check config /etc/llmgw-observability/prometheus.yml
git diff --check
~~~

Expected: all exit 0.

- [ ] **Step 2: Start local GPU and observability stack**

Use the existing ignored `.env` from the main checkout without copying or printing secrets:

~~~powershell
$mainCheckout = 'C:\Projects\vllm-priority-gateway'
docker compose --env-file "$mainCheckout\.env" -f compose.yaml -f compose.observability.yaml up -d --build --wait
nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader
~~~

Wait on health conditions. Confirm Prometheus `/api/v1/targets` and Grafana `/api/dashboards/uid/llmgw-gateway-decisions` using local credentials.

- [ ] **Step 3: Run real-vLLM priority scenario**

Set runbook variables without printing secret values:

~~~powershell
$env:LLMGW_E2E_MODE = 'priority'
$env:LLMGW_E2E_BASE_URL = 'http://127.0.0.1:8080'
$env:LLMGW_E2E_MODEL = 'qwen'
go test -count=1 -v -timeout 10m ./tests/e2e -run TestPriorityIsolationWithRealVLLM
~~~

Expected: pressure/waiting saturation, three Low 429s with `priority_concurrency_limit`, High/Critical completion, metric deltas, and recovery to normal.

- [ ] **Step 4: Capture evidence and update measured docs**

Query the four dashboard expressions over the test window via Prometheus `/api/v1/query_range`. Record only commit, GPU model, vLLM image/model, timestamps, pressure peak, Low rejection delta, baseline/loaded High first byte and ratio, and test result. Capture a Grafana screenshot of the aligned causal row without credentials.

- [ ] **Step 5: Commit measured evidence**

~~~powershell
git add docs/acceptance-evidence.md docs/real-gpu-testing.md
git commit -m "docs: record decision telemetry GPU validation"
~~~

- [ ] **Step 6: Dispatch required review subagent**

Use base SHA `d905f00` and current HEAD. Provide spec, plan, test/GPU outputs, graph project `C-Projects-vllm-priority-gateway-gateway-decision-telemetry` and current generation, changed paths, trace/coverage evidence, and ask for correctness, concurrency, cardinality/lifecycle, PromQL/Grafana semantics, compatibility, test gaps, and docs accuracy.

- [ ] **Step 7: Resolve findings through TDD**

For every Critical/Important finding, reproduce with a failing test/validator, watch RED, implement the smallest fix, rerun GREEN, and return the diff to the same reviewer. Rebut invalid findings with source/test evidence.

- [ ] **Step 8: Repeat complete verification**

Repeat Step 1 and rerun real-vLLM priority if review touched request flow, metrics, Compose, E2E, or dashboard queries. Inspect `git status --short`, `git diff d905f00...HEAD --stat`, and committed log.

- [ ] **Step 9: Complete requirement audit**

Prove requests, rejections, duration, inflight, pool pressure, backend pressure, backend selections, circuit state, and queue wait are exported; Grafana causal row is provisioned/queryable; local GPU evidence exists; docs match; and review has no unresolved Critical/Important findings.
