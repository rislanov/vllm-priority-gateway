# Gateway Decision Telemetry Design

Date: 2026-08-30

## Context

The gateway already exposes Prometheus counters, gauges, and histograms for completed requests, aggregate rejections, in-flight work, backend pressure, request duration, TTFT, retries, circuit breakers, and pool safety. Those measurements are useful but do not yet make the gateway's own decisions explicit enough to explain an overload incident end to end.

Operators need to see one causal sequence without reconstructing it from logs:

> GPU-backed vLLM capacity overloaded, pressure rose, Low requests began receiving 429 responses, and High latency remained stable.

The implementation must preserve the public OpenAI-compatible error contract, use bounded Prometheus labels, keep request content and secrets out of telemetry, and remain useful with the current single-replica runtime.

## Goals

- Attribute every gateway rejection that reaches a stable client/model identity to a bounded internal decision reason.
- Expose pool pressure and backend selection decisions alongside the existing backend and circuit metrics.
- Measure how long admission and backend selection take inside the gateway.
- Ship a provisioned Prometheus and Grafana setup with a dashboard that presents the overload sequence directly.
- Extend deterministic tests and the opt-in real-vLLM priority scenario so the metric contract is executable.
- Validate the completed change against a local NVIDIA GPU and retain reproducible evidence.

## Non-goals

- Do not add a request queue or change admission, routing, retry, circuit-breaker, or vLLM priority behavior.
- Do not infer exact per-request vLLM queue time. The OpenAI-compatible response does not provide a stable, version-independent value for it.
- Do not add request IDs, API-key material, URLs, prompts, generated text, session IDs, or arbitrary upstream values as metric labels.
- Do not require Grafana or Prometheus for the gateway process to start or serve traffic.
- Do not make DCGM exporter data a prerequisite. External GPU metrics may be added to the same Prometheus later, while gateway pool/backend pressure remains the portable capacity signal.

## Metric Contract

The project keeps the existing `llmgw_` namespace and existing label names for compatibility. The requested conceptual metrics map to the following concrete series.

| Concept | Prometheus series | Change |
| --- | --- | --- |
| Requests | `llmgw_requests_total{client,model,priority_class,status_class}` | Existing; retained |
| Rejections | `llmgw_requests_rejected_total{client,model,priority_class,reason}` | Existing family; `reason` becomes an explicit bounded gateway decision reason |
| Request latency | `llmgw_request_duration_seconds{model,backend,priority_class,status_class}` | Existing; retained |
| Gateway in-flight | `llmgw_client_inflight{client,model,priority_class}` and `llmgw_requests_inflight{model,priority_class}` | Existing; retained |
| Pool pressure | `llmgw_pool_pressure{model}` | New gauge using `PoolRuntime.BestBackendPressure` |
| Pool state | `llmgw_pool_state{model,state}` | New one-hot gauge for `normal`, `busy`, `saturated`, `emergency`, and `unavailable` |
| Backend pressure | `llmgw_backend_pressure{model,backend}` | Existing; retained |
| Backend selection | `llmgw_backend_selected_total{model,backend}` | New counter incremented for every successfully leased backend, including retry selections |
| Circuit breaker | `llmgw_backend_circuit_state{model,backend}` | Existing numeric state gauge; retained |
| Gateway queue wait | `llmgw_queue_wait_seconds{model,priority_class,outcome}` | New histogram for admission-to-selection or admission-to-rejection time |

`status_class` remains a bounded `2xx`/`4xx`/`5xx` style label rather than an unbounded upstream status string. Grafana identifies gateway-generated 429s through the bounded rejection reasons below; each overload reason maps to the unchanged HTTP 429 API envelope.

## Rejection Reasons

The client-visible error `code` remains unchanged. A separate internal `DecisionReason` is attached to the request event and used by metrics and structured logs. The initial bounded vocabulary is:

| Decision reason | Client-visible result | Meaning |
| --- | --- | --- |
| `pool_waiting_limit` | `429 gateway_overloaded` | Configured aggregate upstream waiting limit reached |
| `pool_inflight_limit` | `429 gateway_overloaded` | Pool gateway-in-flight lease could not be acquired |
| `priority_concurrency_limit` | `429 gateway_overloaded` | Effective per-client limit for the current pool state was reached |
| `pool_unavailable` | `503 backend_unavailable` | Pool runtime or configuration is unavailable |
| `no_eligible_backend` | `503 backend_unavailable` | Routing found no healthy, fresh, secret-ready, circuit-available backend |
| `gateway_backpressure` | `503 gateway_unavailable` | The bounded analytics lifecycle could not reserve capacity |
| `model_not_allowed` | Existing 404/403-compatible model error | Model missing, disabled, or not granted to the client |
| `invalid_request` | Existing 400 error | Request validation or rewriting failed |
| `invalid_api_key` | Existing 401 error | Authentication failed before a stable client identity |
| `upstream_failure` | Existing 502/5xx path | Selected upstream failed before a response could start |

The metrics observer falls back to the existing API error code only for legacy or pre-identity paths that do not carry a more precise decision reason. Values are constants owned by the gateway package, not strings copied from upstream responses.

## Queue-wait Semantics

The gateway deliberately does not hold an application request queue. `llmgw_queue_wait_seconds` therefore measures gateway pre-dispatch delay, not vLLM's internal per-request queue time.

The timer starts after authentication and public-model resolution, immediately before pool admission. It stops at the first of:

- a backend lease is successfully acquired (`outcome="selected"`), or
- pool admission, priority admission, or backend selection returns a terminal rejection (`outcome="rejected"`).

Only requests with a stable model and priority identity are observed. Retries do not restart or add another queue-wait sample; `llmgw_backend_selected_total` records each retry selection separately. This definition lets operators distinguish gateway decision delay from end-to-end request duration and TTFT without pretending to know upstream queue residency.

## Runtime Data Flow

1. The request service resolves the client and public model and starts the admission timer.
2. Existing pool safety and priority admission branches attach a bounded `DecisionReason` when they reject.
3. The first successful backend lease records queue-wait outcome `selected`; a terminal admission/selection failure records `rejected`.
4. The existing backend in-flight observer increments `llmgw_backend_selected_total` only on a positive assignment delta, so every actual leased attempt is counted and releases remain balanced.
5. The periodic runtime publisher writes pool pressure/state and the existing backend/circuit gauges from the same runtime snapshot.
6. Request completion records the unchanged request and duration metrics plus the explicit rejection reason.

No new event bus is introduced. The existing gateway observer remains the integration boundary, with additive request-event fields and metrics behavior.

## Grafana and Prometheus

An optional `compose.observability.yaml` overlay adds Prometheus and Grafana without changing the default quick start. Both UIs bind to loopback by default. Provisioned files live under `deploy/observability/` and include:

- a one-second Prometheus scrape of `gateway:8080/metrics`;
- a Grafana Prometheus datasource;
- a provisioned **Gateway Decisions** dashboard;
- persistent local volumes for Prometheus and Grafana data.

The dashboard has a top **Overload causality** row with aligned time-series panels:

1. pool pressure and pool state;
2. Low 429 rate split by gateway decision reason;
3. High p95 request duration and TTFT;
4. High gateway queue-wait p95.

Supporting rows show request rate/status by priority, gateway/client in-flight work, backend pressure and waiting work, backend selection rate, circuit state, and rejection reasons for all priorities. Dashboard variables select model, backend, client, and priority while retaining an all-value view.

The intended operator reading is direct: pressure crosses the configured operating region, `priority_concurrency_limit` begins rising for Low, High duration/TTFT remains within its normal band, and gateway queue wait shows whether the gateway itself introduced delay.

## Cardinality and Lifecycle

- `client`, `model`, and `backend` come only from committed gateway configuration.
- `priority_class`, `status_class`, `state`, `outcome`, and `reason` are bounded enums.
- Historical counters and histograms remain available until process restart, matching current behavior.
- Runtime gauges remove series for renamed or deleted clients, pools, and backends through the existing topology publication path.
- The dashboard never depends on an individual request ID or raw API-key identity.

## Testing Strategy

Implementation follows test-driven development.

### Unit and service tests

- Extend the metrics family test with exact new descriptors, labels, and samples.
- Prove pool pressure/state publication and stale topology label removal.
- Prove a positive backend assignment increments selection exactly once and a release does not increment it.
- Prove queue-wait records one selected or rejected sample per request and never duplicates on retry.
- Exercise each overload branch and assert the internal reason while preserving the existing HTTP status, body, and `Retry-After` contract.
- Parse the checked-in Grafana dashboard JSON and assert the required PromQL families and variables are present.
- Validate the Compose overlay and Prometheus configuration in the existing container/compose test area.

### Integration tests

Drive the real gateway with deterministic fake vLLM backends and assert:

- pressure/state gauges match runtime state;
- Low overload increments the precise rejection reason;
- High completion increments request and duration histograms;
- backend selection and queue-wait samples are emitted;
- metrics contain no request IDs, keys, prompts, or arbitrary upstream labels.

### Local GPU validation

Run the opt-in real-vLLM priority scenario against the local NVIDIA GPU using the base Compose file plus the observability overlay. The scenario captures a High baseline before saturation, drives sustained load until pool pressure/state and upstream waiting rise, proves Low 429 shedding, completes High streaming traffic, and captures loaded High first-byte/end-to-end latency.

The real-vLLM test additionally checks metric deltas for pool pressure, Low rejection reason, High request/duration, backend selections, and queue wait. The validation record includes GPU model, gateway commit, vLLM image/model, timestamps, baseline and loaded High latency, Prometheus query results, and the Grafana dashboard URL or screenshot. Any stability threshold used for the hardware run is stated explicitly with the evidence instead of being hidden in a universal flaky test default.

## Documentation

Update the English and Russian READMEs, operations guide, technical specification, real-GPU guide, real-vLLM E2E runbook, and acceptance evidence. Documentation must define every new metric and reason, explain queue-wait semantics, provide the Compose/Grafana commands and PromQL, and distinguish gateway pressure from optional external GPU telemetry.

## Compatibility and Rollout

- The public request/response API and admission behavior do not change.
- Existing metric family names and existing labels remain intact.
- `llmgw_requests_rejected_total.reason` becomes more specific for gateway decisions; dashboards that match only `gateway_overloaded` must migrate to the new bounded overload reason set. The release notes and operations guide call this out.
- The observability overlay is optional and can be removed without affecting gateway state or traffic.
- Operators should deploy the metric-producing gateway before importing queries that depend on the new series.

## Review and Completion Gates

Completion requires:

1. the full Linux Go test suite and `go vet ./...` passing;
2. Compose, Prometheus, dashboard JSON, and provisioning validation passing;
3. the local GPU priority scenario producing retained telemetry evidence;
4. documentation matching the implemented metric contract;
5. an independent subagent code review with every critical and important finding resolved or technically rebutted;
6. a final diff and requirement-by-requirement verification against this design.
