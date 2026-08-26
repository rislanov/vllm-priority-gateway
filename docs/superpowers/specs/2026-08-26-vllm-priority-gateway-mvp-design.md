# Lightweight vLLM Priority Gateway MVP Design

**Date:** 2026-08-26

**Status:** Approved in chat

**Source requirements:** `ТЗ_ Lightweight vLLM Priority Gateway.md`

## 1. Goal

Build a lightweight, single-instance Go gateway in front of one or more vLLM OpenAI-compatible servers. The MVP must authenticate clients, enforce per-client model and concurrency policies, protect higher-priority traffic during overload, route requests to the least-pressured healthy backend, propagate the server-controlled vLLM priority, stream responses without full buffering, expose an embedded administration UI, and provide deterministic test tooling.

The deployable target is Linux x86_64 (`linux/amd64`). Development and the complete fake-backend test suite must work on macOS without Docker or NVIDIA hardware. Real-vLLM validation remains an explicit acceptance layer to run on Linux with an RTX 4070 Ti or another supported NVIDIA GPU.

## 2. Scope

### MVP capabilities

- One gateway process and one SQLite database.
- OpenAI-compatible `GET /v1/models`.
- Transparent proxying of:
  - `POST /v1/chat/completions`
  - `POST /v1/completions`
  - `POST /v1/responses`
- Client authentication with high-entropy API keys that are never stored in plaintext.
- Client enable/disable, priority class, integer vLLM priority, maximum concurrency, and model-pool access.
- Model pools with public model names, upstream model names, and multiple static backends.
- Independent backend health and Prometheus metric polling.
- EWMA pressure calculation, hysteretic backend/pool states, least-pressure routing, and unhealthy/draining exclusion.
- Priority-aware admission control and fast OpenAI-shaped `429` responses.
- Unbuffered response streaming, disconnect cancellation, and one conservative pre-first-byte transport retry.
- Prometheus metrics and structured request logging without prompts or generated content.
- Embedded server-rendered Admin UI and JSON Admin API.
- Fake vLLM simulator and load generator.
- Unit, persistence, HTTP integration, streaming, cancellation, monitoring, routing, admission, and load-smoke tests.

### Deliberate non-goals

The MVP does not implement Kubernetes discovery, Redis/Valkey, PostgreSQL, multiple gateway replicas, distributed leases, billing, token/rate budgets, autoscaling, prefix-aware routing, GPU/NVML scheduling, cancellation of admitted low-priority generations, OIDC/RBAC, audit logs, OpenTelemetry, or a production circuit breaker. The package boundaries must leave room for the production implementations named in the source requirements without implementing them now.

## 3. Chosen approach

Use a modular Go monolith with a server-rendered Admin UI embedded into the gateway binary. This keeps deployment to one executable plus a writable SQLite file and avoids a Node/React build chain. Fake vLLM and the load generator are separate commands in the same Go module, not runtime services inside the gateway.

The design favors focused packages and explicit interfaces over a single large handler, while avoiding a framework-heavy or microservice architecture.

## 4. Technology and portability

- Go 1.27.
- Standard `net/http` server and client, with `github.com/go-chi/chi/v5` for routing and middleware composition.
- `database/sql` with `modernc.org/sqlite`, so local tests and Linux cross-compilation do not require CGO.
- `github.com/prometheus/client_golang` for gateway metrics.
- `github.com/prometheus/common/expfmt` for parsing vLLM Prometheus text exposition.
- Standard `log/slog` JSON logging.
- Standard `html/template`, `embed`, CSS, and small progressive-enhancement JavaScript for the Admin UI.
- Standard `testing`, `httptest`, and package-local fakes; no assertion framework is required.

The repository must provide host-native commands for macOS development and a reproducible static target build:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./cmd/gateway
```

A multi-stage Dockerfile must build and run the same Linux amd64-compatible gateway. Tests must not require Docker.

## 5. Repository layout

```text
cmd/
  gateway/          application wiring and process lifecycle
  fake-vllm/        deterministic upstream simulator
  loadgen/          configurable traffic generator

internal/
  admission/        pure admission decisions and local concurrency leases
  apikey/           key generation, parsing, hashing, and verification
  config/           environment configuration and validation
  domain/           clients, pools, backends, priority and state types
  gateway/          request orchestration use cases
  httpapi/          public API, Admin API, middleware, and error shapes
  monitor/          health checks, metrics scraping, EWMA, and state machines
  observability/    Prometheus instruments and structured request records
  proxy/            upstream request and streaming response transport
  routing/          candidate filtering and least-pressure selection
  store/            repository interfaces, SQLite implementation, migrations
  web/              embedded templates and static assets

tests/
  integration/      black-box gateway plus fake-upstream tests

deploy/
  docker/           container entrypoint material if needed

docs/
  real-gpu-testing.md

Dockerfile
Makefile
README.md
```

Pure policy packages (`admission`, `domain`, and `routing`) must not depend on HTTP, SQLite, Prometheus, or the concrete monitor implementation.

## 6. Domain model and persistence

SQLite owns durable configuration. Runtime measurements and active leases stay in memory.

### Tables

`clients`

- `id` integer primary key
- `name` unique text
- `enabled` boolean
- `priority_class` one of `critical`, `high`, `normal`, `background`
- `vllm_priority` integer
- `max_concurrency` non-negative integer
- `created_at`, `updated_at` UTC timestamps

`api_keys`

- `id` integer primary key
- `client_id` foreign key
- `prefix` indexed text
- `secret_hash` fixed-size bytes/hex
- `created_at`, optional `expires_at`, optional `revoked_at`, optional `last_used_at`

`model_pools`

- `id` integer primary key
- `public_model_name` unique text
- `upstream_model_name` non-empty text
- `enabled` boolean
- `created_at`, `updated_at`

`client_model_access`

- composite primary key (`client_id`, `model_pool_id`)
- `enabled` boolean

`backends`

- `id` integer primary key
- `model_pool_id` foreign key
- `name` unique text
- normalized absolute `base_url`
- `enabled`, `draining` booleans
- `capacity_hint` positive real, recorded for forward compatibility
- `running_soft_limit` positive real
- optional `upstream_api_key_env` containing an environment-variable name, not a secret
- `created_at`, `updated_at`

Schema migrations are embedded and run transactionally at startup. SQLite uses WAL mode, foreign keys, a busy timeout, and a bounded connection pool. The database file is created with owner-only permissions where the host filesystem permits it.

Configuration writes publish an in-process revision event. Monitors reconcile backend workers without restarting the gateway. Public requests read immutable registry snapshots, so configuration changes do not hold database locks in the hot path.

## 7. Configuration

Environment variables configure process-level settings. Invalid or unsafe settings fail startup with a precise error.

Required settings:

- `LLMGW_ADMIN_USERNAME`
- `LLMGW_ADMIN_PASSWORD`
- `LLMGW_API_KEY_HMAC_SECRET` with at least 32 bytes of entropy

Important defaults:

- listen address: `:8080`
- SQLite path: `./data/llmgw.db`
- health interval: 2 seconds
- metrics interval: 1 second
- metrics stale after: 5 seconds
- unhealthy after: 3 consecutive health failures
- recovered after: 2 consecutive health successes
- queue soft limit: 2
- KV soft/hard limits: 0.80/0.95
- EWMA window: 4 seconds
- overload enter window: 3 seconds
- overload recovery window: 10 seconds
- request body limit: 16 MiB
- retry count: one alternate backend
- overload `Retry-After`: 2 seconds

Backend upstream credentials are resolved from the environment variable named by `upstream_api_key_env`. They are never returned by the Admin API or rendered in the UI.

## 8. API-key security

Keys have the form `llmgw_` followed by 32 random bytes encoded with unpadded URL-safe base64. Generation uses `crypto/rand`.

The database stores a display prefix and `HMAC-SHA-256(server_secret, full_key)`. Authentication derives the HMAC from the presented key and compares it with `subtle.ConstantTimeCompare`. The complete key is returned exactly once by the create-key Admin API response and is not logged. A prefix narrows lookup candidates but is not treated as authentication proof.

Unknown, malformed, expired, revoked, and disabled-client keys all produce the same OpenAI-shaped `401` response. `LastUsedAt` updates are rate-limited/asynchronous enough not to serialize the request hot path.

## 9. Public request pipeline

For each public request the gateway performs:

1. Generate a gateway request ID. Preserve a valid client `X-Request-Id` as `parentRequestId` in logs, but send the gateway ID upstream as `X-Request-Id`.
2. Authenticate `Authorization: Bearer <key>` and load the immutable client policy.
3. For `GET /v1/models`, synthesize an OpenAI model list from enabled pools explicitly allowed for that client.
4. For supported POST routes, read a size-limited JSON body, require a string `model`, and preserve all other fields.
5. Resolve the public model pool and verify explicit client access. Replace the public model with the pool's upstream model.
6. Remove any client `X-Vllm-Priority` header and any JSON `priority` field. Set `X-Vllm-Priority` to the configured client policy value.
7. Read the current pool snapshot and run admission control.
8. Atomically acquire a local client concurrency lease at the current effective limit.
9. Select the least-pressured eligible backend and increment its local in-flight observation.
10. Forward the request and stream the upstream response.
11. On completion, error, or cancellation, release both client and backend in-flight state exactly once and record metrics/logs.

Unsupported `/v1/*` routes return a controlled OpenAI-shaped `404` with code `unsupported_endpoint`. Admin, health, and metrics routes never accept client bearer keys as administrator credentials.

## 10. OpenAI-compatible errors

Errors use:

```json
{
  "error": {
    "message": "...",
    "type": "...",
    "code": "..."
  }
}
```

Stable gateway codes include:

- `invalid_api_key` (`401`)
- `invalid_request_error` (`400`)
- `model_not_allowed` (`403`)
- `unsupported_endpoint` (`404`)
- `gateway_overloaded` (`429`, with `Retry-After`)
- `backend_unavailable` (`503`, with `Retry-After`)
- `upstream_error` (`502` only when no upstream response can be forwarded)

The gateway forwards an actual upstream HTTP status and body unchanged once an upstream response has been selected for delivery.

## 11. Monitoring and pressure

Each enabled backend has an independent monitor worker. Configuration reconciliation starts, updates, or stops workers. Disabling or deleting a backend prevents new routing immediately. Draining is operator state: existing streams continue and new requests are excluded.

### Health

`GET /health` is sampled every 2 seconds. Three consecutive failures mark the backend unhealthy. Two consecutive successes recover it. A successful status is any `2xx` response completed within the configured health timeout.

### Metrics

`GET /metrics` is sampled every second and parsed as Prometheus exposition. The monitor reads and sums samples for:

- `vllm:num_requests_running`
- `vllm:num_requests_waiting`
- `vllm:kv_cache_usage_perc`

For compatibility with older vLLM versions it may fall back to `vllm:gpu_cache_usage_perc` only when the current KV metric is absent. Missing required metrics, parse errors, or samples older than five seconds make metrics stale; stale backends are excluded from routing even if `/health` still succeeds.

### Pressure

The raw pressure is:

```text
queuePressure   = clamp(waiting / queueSoftLimit, 0, 2)
kvPressure      = clamp((kv - kvSoft) / (kvHard - kvSoft), 0, 2)
runningPressure = clamp(running / runningSoftLimit, 0, 2)

pressure = 0.55*queuePressure + 0.30*kvPressure + 0.15*runningPressure
```

Each backend applies a time-aware EWMA with a default four-second window. Runtime local upstream in-flight counts are used only as a routing tie-break, not merged into scraped vLLM pressure.

### State and hysteresis

An available backend is displayed as healthy below 0.70, busy at or above 0.70, and saturated at or above 1.00. Draining and unhealthy/stale override pressure display states.

Pool pressure is the minimum EWMA pressure of all enabled, non-draining, healthy, fresh backends. With no eligible backend the pool is `unavailable` immediately.

The pool enters a higher overload state only after the threshold persists for three seconds:

- `busy`: best pressure at least 0.70
- `saturated`: best pressure at least 1.00, or every eligible backend has waiting requests
- `emergency`: best pressure at least 1.40

Recovery must persist for ten seconds and descends at most one state per evaluation:

- `emergency` to `saturated` below 1.20
- `saturated` to `busy` below 0.85 and not all backends waiting
- `busy` to `normal` below 0.55

All thresholds and windows are configuration values. An unavailable pool derives its overload state from fresh measurements as soon as a backend satisfies the configured recovery health count.

## 12. Admission and concurrency

Admission uses the fixed four priority classes and the source-requirement policy table:

| Pool state | Critical | High | Normal | Background |
|---|---:|---:|---:|---:|
| normal | 100% | 100% | 100% | 100% |
| busy | 100% | 100% | 100% | 50% |
| saturated | 100% | 100% | 50% | 0% |
| emergency | 100% | 50% | 0% | 0% |

`effectiveConcurrency = floor(MaxConcurrency * percentage)`, clamped to `[0, MaxConcurrency]`. A zero configured or effective limit rejects new requests. A state change never cancels an existing request; it only changes whether subsequent lease acquisitions succeed.

The local limiter owns one atomic in-memory counter per client. A lease is acquired before routing and remains held through the entire non-streaming body or SSE lifecycle. It is released on normal EOF, upstream error, client disconnect, or server shutdown through an idempotent release function.

An unavailable pool returns `503 backend_unavailable`. Policy or concurrency rejection returns `429 gateway_overloaded` with `Retry-After: 2`.

## 13. Routing

Candidates must belong to the resolved model pool and be enabled, non-draining, health-recovered, and metrics-fresh.

Selection order is:

1. Lowest EWMA pressure, with values within a configurable epsilon considered tied.
2. Lowest gateway-observed upstream in-flight count.
3. Random tie-break using an injected concurrency-safe source.

`capacity_hint` is persisted and displayed but does not influence MVP routing. Tests inject deterministic tie-breaking. A failed attempt is excluded when selecting the one retry backend.

## 14. Proxy, streaming, cancellation, and retry

The proxy uses a shared tuned `http.Transport` and no short whole-request timeout. Dial, TLS handshake, response-header, and idle-connection bounds are configured separately. The inbound request context is attached to every upstream request, so a client disconnect cancels the upstream exchange.

Hop-by-hop headers are stripped in both directions. End-to-end headers, status, and body bytes are otherwise forwarded. SSE and ordinary response bodies use the same streaming copy loop; after every non-empty read the gateway writes and flushes immediately when the downstream writer supports flushing. The gateway never accumulates a completion body.

To support the only allowed retry, downstream headers are not committed until the first upstream body bytes are available. A transport failure before any downstream byte causes one attempt on a different eligible backend. HTTP error statuses are upstream responses, not transport failures, and are not retried. Once any header/body has been committed downstream, no retry occurs; a later upstream failure terminates the client stream.

An empty successful response is committed on clean EOF. A cancelled stream increments the disconnect/cancellation telemetry and releases leases. Server shutdown cancels monitor workers, stops accepting requests, waits for an explicit grace period, and then cancels remaining upstream requests.

## 15. Admin API and UI

`/admin` and `/admin/api/*` use HTTP Basic authentication backed only by required environment credentials. Comparisons are constant-time. The UI applies CSRF protection to state-changing form/API requests by issuing an `HttpOnly`, `SameSite=Strict` session/CSRF cookie and requiring a matching token. Admin responses set restrictive security headers and are never cached.

The JSON API implements the required endpoints:

- clients: list, create, update
- client keys: create and revoke
- model pools: list, create, update
- backends: list, create, update, drain, and resume
- aggregate status

Resource validation includes unique names, valid priority class, bounded integer priorities, non-negative concurrency, explicit model-access IDs, HTTP(S) backend URLs without userinfo/query/fragment, positive pressure limits, and referential-integrity conflict responses.

The embedded UI provides four responsive screens:

- Dashboard: pool states and per-backend live measurements.
- Clients: create/edit/disable, priorities, limits, and model access.
- API Keys: create, one-time secret display, revoke, expiry/status, and last use.
- Backends: pool assignment, URL, health/pressure, enable/disable, drain/resume.

HTML forms work without JavaScript. Small embedded JavaScript adds confirmation prompts, periodic dashboard refresh, and one-time secret copy affordances. No external CDN asset is required.

## 16. Gateway observability

`GET /metrics` exposes the required `llmgw_*` counters, gauges, and histograms. Labels are limited to stable client ID/name, public model name, backend name, priority class, HTTP status class, and a bounded reason enum. Request IDs, key prefixes, URLs, prompts, and generated text are never metric labels.

Every completed request emits one JSON log record with request IDs, client ID, public model, priority class/value, selected backend, pool state, backend pressure, HTTP status, duration, time to first byte, optional token usage parsed only from ordinary response metadata when cheaply available, disconnect, and retry flags. Raw request/response bodies and authorization headers are never logged.

Gateway liveness and readiness endpoints are separate from vLLM routes:

- `/healthz`: process is alive.
- `/readyz`: database initialized and registry loaded; backend availability is reported in the body but does not make the management plane unready.

## 17. Fake vLLM simulator

`fake-vllm` implements `/health`, `/metrics`, `/v1/models`, and the three required POST endpoints. A test-only `GET/PUT /admin/state` controls:

- running, waiting, and KV cache usage
- time-to-first-byte and per-token delay
- normal versus streaming responses
- response status/body
- connection reset before headers, before body, or after N chunks
- health failure count/mode

It records request count, active requests, cancellation, model, request ID, and received priority so integration tests can assert observable behavior without mocks. Streaming responses are deterministic SSE frames ending in `[DONE]`.

## 18. Load generator

`loadgen` accepts gateway URL, one API key or class-to-key mappings, parallelism, request count, prompt size, maximum tokens, streaming mode, and an optional critical/high/normal/background traffic mix. A traffic mix requires a key mapping for every class with non-zero weight, because gateway priority cannot be client-controlled.

It reports requests, successes, `429`, `5xx`, other failures, and p50/p95/p99 time-to-first-byte and end-to-end latency. Exit status is non-zero for configuration errors or transport-level run failure, not merely because overload testing intentionally produces `429`.

## 19. Testing strategy

Implementation follows red-green-refactor. Each production behavior begins with a test that fails for the expected missing behavior.

### Unit tests

- HMAC key generation/verification and indistinguishable invalid-key cases.
- Pressure normalization, EWMA, threshold boundaries, and hysteresis timing.
- Admission percentages and effective concurrency rounding.
- Concurrent lease acquisition/release and no slot leak.
- Candidate filtering, least-pressure choice, in-flight tie-break, and deterministic random tie.
- Header sanitization, priority overwrite, model rewrite, and OpenAI error encoding.

### Persistence tests

- Fresh migration and restart against temporary real SQLite files.
- CRUD validation, foreign keys, model access, revoke/expiry, and absence of plaintext secrets.
- Concurrent read/config-write behavior under WAL mode.

### Integration tests

- Unknown/revoked/valid API key behavior.
- Per-client model listing and forbidden model behavior.
- All three POST endpoints, ordinary and SSE responses.
- Streaming first-byte delivery without full-response buffering.
- Client cancellation reaches the fake backend and releases the slot.
- Priority/body/header escalation attempts are overwritten.
- Lowest-pressure routing and no routing to draining, unhealthy, or stale backends.
- Health failure exclusion and automatic recovery within configured windows.
- Normal/busy/saturated/emergency admission order and `429` contract.
- Hysteresis rejects short spikes and eventually recovers.
- One alternate-backend retry before first byte and no retry after stream start.
- Admin authentication, CSRF, CRUD, key one-time disclosure, drain, and resume.
- Prometheus endpoint presence and bounded labels.
- Graceful shutdown with an active stream.

### Load and platform checks

- Fake-backend smoke load verifies no unexpected failures and produces latency percentiles.
- A non-contractual local benchmark records proxy overhead against an immediate fake backend.
- CI runs tests and race detection on supported host runners, then cross-builds `cmd/gateway`, `cmd/fake-vllm`, and `cmd/loadgen` for `linux/amd64` with CGO disabled.
- The real-GPU guide covers OpenAI compatibility, cancellation, queue detection, priority isolation, and recovery against vLLM started with priority scheduling and request-ID headers.

## 20. Delivery and documentation

The English `README.md` must explain architecture, quick start, bootstrap configuration, Admin UI/API, client setup, vLLM flags, supported routes, security model, monitoring, Fake vLLM, load generation, testing, macOS development, Linux amd64 builds, Docker deployment, operational limitations, and the real-GPU validation procedure.

The final delivery is complete only after:

1. all host tests, integration tests, race tests, vet/static checks, and Linux amd64 cross-builds pass;
2. acceptance requirements are mapped to evidence;
3. independent subagents review requirements coverage, correctness/security, and test quality;
4. actionable findings are fixed with regression tests where appropriate; and
5. the full verification suite passes again after review fixes.

## 21. Design decisions reserved for Production V1

Interfaces must permit replacing SQLite with PostgreSQL and the local limiter with a distributed lease store, but the MVP will not ship unused implementations. Capacity-aware routing, soft session affinity, circuit breaking, distributed configuration revisions, OIDC/RBAC, audit logging, secret managers, Redis failure policy, and OpenTelemetry remain later design work.
