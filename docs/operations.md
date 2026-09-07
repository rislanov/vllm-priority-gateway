# Operations guide

This guide covers the runtime behavior that operators need after deployment. For installation and release verification, see the [production deployment guide](deployment.md).

## Readiness endpoints

The gateway exposes three intentionally different health signals:

| Endpoint | Meaning |
|---|---|
| `/healthz` | The process is alive. |
| `/readyz` | SQLite and the in-memory configuration registry are ready. Admin access remains available even if inference capacity is zero. |
| `/inference-readyz` | At least one enabled pool and backend are eligible for inference. Returns HTTP `503` when no inference capacity is available. |

Use `/inference-readyz` as the load-balancer signal for client traffic. Removing a gateway from service cannot create GPU capacity, so transient pool congestion does not make this endpoint fail.

## Backend monitoring and routing

Each enabled backend has independent health and Prometheus scrapes. The gateway derives EWMA pressure from running requests, waiting requests, and KV-cache utilization, applies hysteresis to pool state, and routes to the least-pressured eligible backend.

Send a stable opaque `X-LLM-Session-Id` for consecutive requests from one agent or conversation. Rendezvous hashing improves prefix-cache locality while health, freshness, drain state, retry exclusions, and live pressure keep precedence. The identifier is limited to 256 bytes, is not logged or used as a metric label, and is removed before forwarding to vLLM.

This is locality-aware routing, not a distributed KV-block index. A session can move when its preferred backend becomes unavailable or overloaded.

The first nonempty header in this order supplies the session ID (header names are case-insensitive):

| Priority | Header | Client / purpose |
|---|---|---|
| 1 | `X-LLM-Session-Id` | Explicit gateway override |
| 2 | `X-OpenCode-Session` | OpenCode |
| 3 | `X-Claude-Code-Session-Id` | Claude Code |
| 4 | `Session-Id` | Codex and Pi Codex transport |
| 5 | `Session_id` | Older Codex and Pi OpenAI transport |
| 6 | `X-Session-Affinity` | Pi completions transport |
| 7 | `X-Session-Id` | Pi OpenRouter transport |
| 8 | `X-Client-Request-Id` | Pi-only fallback when `User-Agent` starts with `pi (` |

PiCode here refers to the Pi coding agent. Header emission depends on the client version, provider and cache settings. Because `X-Client-Request-Id` identifies a single request in many other clients, the gateway treats it as a session ID and removes it only when Pi's `User-Agent` is present. Other clients keep that header and can supply `X-LLM-Session-Id` as an explicit stable conversation ID. If the client sends none of these headers, routing remains least-pressure. These aliases apply to the gateway's existing OpenAI-compatible endpoints; recognizing Claude's header does not add the Anthropic Messages API.

Values are trimmed and scoped to the authenticated client and model pool. Every applicable alias is checked against the 256-byte limit, including lower-priority aliases. Repeated values for one header must agree after trimming; conflicting duplicates return HTTP `400` with `invalid_request_error`. Different header names may have different values; precedence resolves them. Applicable session headers are removed before forwarding, including unused aliases, and their values are never recorded in gateway logs or metric labels.

Client references: [OpenCode request headers](https://github.com/anomalyco/opencode/blob/e207624c48159b03dbe17dbc8e51bbcf23e72df5/packages/opencode/src/session/llm/request.ts), [Claude Code changelog](https://github.com/anthropics/claude-code/blob/ab9b2cf7bb9e4f98ff264c07a22e46d83c29c558/CHANGELOG.md), [Codex session headers](https://github.com/openai/codex/blob/455318c2020d75ae7d66d6ccf19defda97edec34/codex-rs/codex-api/src/requests/headers.rs), [Pi responses](https://github.com/earendil-works/pi/blob/9767ba275f3e9a5ee0f5c5342249b629ab1b2282/packages/ai/src/api/openai-responses.ts), [Pi completions](https://github.com/earendil-works/pi/blob/9767ba275f3e9a5ee0f5c5342249b629ab1b2282/packages/ai/src/api/openai-completions.ts).

## Circuit breaker and pool safety

Each managed backend has a process-local inference circuit. With the default configuration, five qualifying failures in 30 seconds open the circuit for 15 seconds; one half-open probe is then allowed. Connection, DNS, TLS, response-header, upstream `5xx`, and upstream response-body failures count against the circuit. Downstream cancellation and write failure are neutral.

An upstream `5xx` is forwarded and is not retried. The gateway performs only one conservative retry for transport failures that occur before response headers are received.

Each model pool has two optional guards:

- `MaxGatewayInflight` atomically bounds admitted requests across all clients in the pool.
- `MaxWaiting` rejects new work when the latest healthy and fresh aggregate vLLM waiting count reaches the configured limit.

Both guards return the bounded `429 gateway_overloaded` envelope before per-client priority can bypass capacity protection. Zero disables the corresponding limit; calibrate non-zero values against the selected model, GPU, and vLLM configuration.

Circuit and pool leases are process-local and are correct only for the documented single-gateway topology.

## Draining a backend

Before planned vLLM maintenance:

1. Open **Admin → Backends**.
2. Select **Drain** for the backend.
3. Wait for existing streams and gateway in-flight requests to finish.
4. Stop or upgrade vLLM.
5. Restore vLLM and wait for health and metrics to become fresh.
6. Select **Resume**.

The gateway reloads every committed Admin change without a process restart.

## Usage analytics and retention

The Analytics page provides UTC presets and exact ranges, client/model/usage filters, summary counters, request and token charts, a newest-first request table, and CSV export.

Analytics is metadata-only. It stores timestamps, generated request IDs, configured client/model/backend identifiers, HTTP status, duration, TTFT, retry/disconnect fields, and nullable token counts. It does **not** store prompts, messages, generated text, request or response bodies, authorization headers, or API-key secrets.

`LLMGW_ANALYTICS_RETENTION` defaults to `2160h` (90 days). Set it to `0` to disable automatic deletion. Size the state volume for request rate times retention. Deletes do not necessarily shrink SQLite immediately, and WAL files can grow until a checkpoint, so monitor both `llmgw.db` and `llmgw.db-wal` and preserve free-space headroom.

Cache-read tokens are a subset of input tokens. Missing cache detail means the upstream did not report it; it is unknown, not zero. Cache totals and hit ratios therefore use only the cache-known subset.

## Metrics and logs

`GET /metrics` exposes request, decision, in-flight, pool/backend pressure, duration, queue wait, TTFT, circuit, failure/retry, and token-usage series under the `llmgw_*` namespace. The decision-focused contract is:

| Metric | Labels | Meaning |
|---|---|---|
| `llmgw_requests_total` | `client`, `model`, `priority_class`, `status_class` | Completed public requests. |
| `llmgw_requests_rejected_total` | `client`, `model`, `priority_class`, `reason` | Gateway-owned terminal rejections using the bounded decision vocabulary below. |
| `llmgw_request_duration_seconds` | `model`, `backend`, `priority_class`, `status_class` | End-to-end public request duration histogram. |
| `llmgw_requests_inflight` | `model`, `priority_class` | Requests admitted and currently in flight. |
| `llmgw_client_inflight` | `client`, `model`, `priority_class` | The same admitted work split by configured client. |
| `llmgw_pool_pressure` | `model` | Best eligible backend pressure used for pool admission. |
| `llmgw_pool_state` | `model`, `state` | One-hot `normal`, `busy`, `saturated`, `emergency`, or `unavailable` pool state. |
| `llmgw_backend_pressure` | `model`, `backend` | Smoothed backend pressure. |
| `llmgw_backend_selected_total` | `model`, `backend` | Backend leases selected by the gateway; an alternate retry lease counts again. |
| `llmgw_backend_circuit_state` | `model`, `backend` | Circuit encoding: unmanaged/unknown `-1`, closed `0`, open `1`, half-open `2`. |
| `llmgw_queue_wait_seconds` | `model`, `priority_class`, `outcome` | Gateway admission-to-first-selection or admission-to-terminal-rejection histogram. |

`llmgw_queue_wait_seconds` starts only after authentication, stable model resolution, policy/access validation, and payload rewriting. It ends when the first backend lease is selected (`outcome="selected"`) or admission/selection returns a gateway API error (`outcome="rejected"`). Pre-admission authentication, request-shape, and model-policy failures have no queue sample. A retry does not emit a second queue observation.

Rejection `reason` is deliberately independent of the public API error code:

| Decision reason | Public result |
|---|---|
| `pool_waiting_limit` | `429 gateway_overloaded` |
| `pool_inflight_limit` | `429 gateway_overloaded` |
| `priority_concurrency_limit` | `429 gateway_overloaded` |
| `pool_unavailable` | `503 backend_unavailable` |
| `no_eligible_backend` | `503 backend_unavailable` |
| `gateway_backpressure` | `503 gateway_unavailable` |
| `model_not_allowed` | `403 model_not_allowed` |
| `invalid_request` | `400 invalid_request_error` |
| `invalid_api_key` | `401 invalid_api_key` |
| `upstream_failure` | `502 upstream_error` |
| `internal_error` | `500 internal_error` before a forwarding lifecycle can be established |

The public error envelopes remain compatible. Existing PromQL matching `reason="gateway_overloaded"` must migrate to `reason=~"pool_waiting_limit|pool_inflight_limit|priority_concurrency_limit"`.

Important capacity signals include:

- `llmgw_pool_pressure`
- `llmgw_pool_state`
- `llmgw_backend_pressure`
- `llmgw_backend_running_requests`
- `llmgw_backend_waiting_requests`
- `llmgw_backend_kv_cache_usage`
- `llmgw_backend_selected_total`
- `llmgw_backend_circuit_state`
- `llmgw_pool_gateway_inflight`
- `llmgw_pool_waiting_requests`
- `llmgw_pool_available_backends`
- `llmgw_queue_wait_seconds`

The five query expressions behind the four causal dashboard panels are:

```promql
max by (model) (llmgw_pool_pressure{model=~"$model"})
sum by (model, reason) (rate(llmgw_requests_rejected_total{model=~"$model",priority_class="background",reason=~"pool_waiting_limit|pool_inflight_limit|priority_concurrency_limit"}[$__rate_interval]))
histogram_quantile(0.95, sum by (model, le) (rate(llmgw_request_duration_seconds_bucket{model=~"$model",priority_class="high",status_class="2xx"}[$__rate_interval])))
histogram_quantile(0.95, sum by (model, le) (rate(llmgw_ttft_seconds_bucket{model=~"$model",priority_class="high"}[$__rate_interval])))
histogram_quantile(0.95, sum by (model, le) (rate(llmgw_queue_wait_seconds_bucket{model=~"$model",priority_class="high",outcome="selected"}[$__rate_interval])))
```

The rejection panel intentionally allowlists only gateway overload decisions that return HTTP 429. Every causal query retains `model`, so selecting `All` shows separate pool series instead of correlating pressure from one model with decisions or latency from another.

For a local GPU stack, add `compose.observability.yaml`, then open `http://127.0.0.1:3000/d/llmgw-gateway-decisions`. Prometheus listens on `127.0.0.1:9090`; both ports are configurable. The overlay defaults Grafana to `admin` / `admin`, which is acceptable only for a loopback development host and must be overridden elsewhere.

The gateway writes one JSON record per completed inference request to stderr. It includes correlation IDs, configured client/model policy, selected backend, pressure/state, status, `decisionReason`, `queueOutcome`, `queueWaitMs`, duration, TTFT, disconnect, and retry count. Bodies, prompts, generated text, authorization headers, and API-key secrets are never logged.

Metric labels are bounded to configured names and enums. Request IDs, key prefixes, URLs, prompts, and generated text are not labels. Current-topology gauges are removed when configured pools, backends, clients, or label identities disappear; historical counters and histograms remain queryable.

## Security boundary

- Keep vLLM endpoints on a private network; the gateway is the client-facing policy boundary.
- Terminate TLS and apply network allowlists at a trusted reverse proxy.
- Restrict Admin, metrics, and readiness endpoints to operator infrastructure.
- Admin credentials come from required environment variables; state-changing Admin requests also require CSRF protection.
- Client keys are stored only as a lookup prefix and `HMAC-SHA-256(server secret, full key)` digest.
- Upstream API keys are read only from named gateway environment variables and are never stored in SQLite.

The current release does not implement TLS, OIDC, RBAC, an audit log, or a secret manager. Those controls must be supplied by the deployment environment.
