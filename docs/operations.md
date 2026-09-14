# Operations guide

This guide covers the runtime behavior that operators need after deployment. For installation and release verification, see the [production deployment guide](deployment.md).

## Readiness endpoints

The gateway exposes four intentionally different health signals:

| Endpoint | Meaning |
|---|---|
| `/healthz` | The process is alive. |
| `/readyz` | Selected persistence profile and in-memory registry initialized; component states report PostgreSQL degradation. Admin access remains available if inference capacity is zero. |
| `/inference-readyz` | At least one enabled pool and backend are eligible for inference. Returns HTTP `503` when no inference capacity is available. |
| `/coordination-readyz` | Strict PostgreSQL coordination, compatibility, configuration-freshness, and circuit-refresh gate. Returns HTTP `503` while degraded. |

Use `/inference-readyz` as the load-balancer signal for client traffic. Removing a gateway from service cannot create GPU capacity, so transient pool congestion does not make this endpoint fail.

## Backend monitoring and routing

Each enabled backend has independent health and Prometheus scrapes. The gateway derives EWMA pressure from running requests, waiting requests, and KV-cache utilization, applies hysteresis to pool state, and routes to the least-pressured eligible backend.

Send a stable opaque `X-LLM-Session-Id` for consecutive requests from one agent or conversation. Rendezvous hashing improves prefix-cache locality while health, freshness, drain state, retry exclusions, and live pressure keep precedence. The identifier is limited to 256 bytes, is not logged or used as a metric label, and is removed before forwarding to vLLM.

This is locality-aware routing, not a distributed KV-block index. A session can move when its preferred backend becomes unavailable or overloaded.

## Circuit breaker and pool safety

Each managed backend has a circuit: process-local in SQLite mode and PostgreSQL-coordinated in PostgreSQL mode. With the default configuration, five qualifying failures in 30 seconds open it for 15 seconds; one half-open probe is then allowed. Connection, DNS, TLS, response-header, upstream `5xx`, and upstream response-body failures count against the circuit. Downstream cancellation and write failure are neutral.

An upstream `5xx` is forwarded and is not retried. The gateway performs only one conservative retry for transport failures that occur before response headers are received.

Each model pool has two optional guards:

- `MaxGatewayInflight` atomically bounds admitted requests across all clients in the pool.
- `MaxWaiting` rejects new work when the latest healthy and fresh aggregate vLLM waiting count reaches the configured limit.

Both guards return the bounded `429 gateway_overloaded` envelope before per-client priority can bypass capacity protection. Zero disables the corresponding limit; calibrate non-zero values against the selected model, GPU, and vLLM configuration.

Circuit and pool leases are process-local in the SQLite profile. PostgreSQL mode distributes both across compatible gateway replicas.

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

`GET /metrics` exposes request, rejection, in-flight, backend pressure/running/waiting/KV, duration, TTFT, disconnect, failure, retry, circuit, pool-safety, and token-usage series under the `llmgw_*` namespace.

Important capacity signals include:

- `llmgw_backend_pressure`
- `llmgw_backend_running_requests`
- `llmgw_backend_waiting_requests`
- `llmgw_backend_kv_cache_usage`
- `llmgw_backend_circuit_state`
- `llmgw_pool_gateway_inflight`
- `llmgw_pool_waiting_requests`
- `llmgw_pool_available_backends`

The gateway writes one JSON record per completed inference request to stderr. It includes correlation IDs, configured client/model policy, selected backend, pressure/state, status, duration, TTFT, disconnect, and retry count. Bodies, prompts, generated text, authorization headers, and API-key secrets are never logged.

Metric labels are bounded to configured names and enums. Request IDs, key prefixes, URLs, prompts, and generated text are not labels.

## Security boundary

- Keep vLLM endpoints on a private network; the gateway is the client-facing policy boundary.
- Terminate TLS and apply network allowlists at a trusted reverse proxy.
- Restrict Admin, metrics, and readiness endpoints to operator infrastructure.
- Admin credentials come from required environment variables; state-changing Admin requests also require CSRF protection.
- Client keys are stored only as a lookup prefix and `HMAC-SHA-256(server secret, full key)` digest.
- Upstream API keys are read only from named gateway environment variables and are never stored in SQLite.

The current release does not implement TLS, OIDC, RBAC, an audit log, or a secret manager. Those controls must be supplied by the deployment environment.

## PostgreSQL coordination operations

In PostgreSQL mode, client/pool inflight, RPM, soft TPM, circuit generations, and half-open permits are global rather than process-local. `/readyz` remains HTTP 200 and reports degraded components through a transient database outage; `/coordination-readyz` is the strict database/compatibility/config/circuit gate. Normal and Background fail closed during an outage, while Critical and High use only configured per-replica emergency caps. Alert on coordination operation failures/latency, PostgreSQL pool saturation, renewal loss, pending completions, circuit-cache age/replay backlog, and compatible replica count. The full recovery sequence, failover durability boundary, and separate acknowledged-data-loss disaster-recovery procedure are in [PostgreSQL production profile](postgresql-production.md).
