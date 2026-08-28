[English](README.md) | [Русский](README.ru.md)

# Lightweight vLLM Priority Gateway

## Contents

- [What it is](#what-it-is)
- [Key capabilities](#key-capabilities)
- [Production quick start](#production-quick-start)
- [Admin UI](#admin-ui)
- [Client API behavior](#client-api-behavior)
- [Operations and deployment](#operations-and-deployment)
- [Admin API](#admin-api)
- [Development and testing](#development-and-testing)
- [Current scope and limitations](#current-scope-and-limitations)
- [Documentation](#documentation)

## What it is

Lightweight vLLM Priority Gateway is a small control and routing layer for a static pool of [vLLM](https://docs.vllm.ai/) OpenAI-compatible servers. It lets application teams use ordinary OpenAI clients while operators centrally own priority, concurrency, model access, routing, and overload policy.

Without the gateway, every client can reach vLLM directly and potentially choose its own priority. With the gateway, clients receive scoped API keys, request a public model name, and cannot raise their own priority. The gateway rewrites the model and priority, admits the request according to policy, selects a healthy backend using live GPU pressure, and streams the response back immediately.

```text
OpenAI client
    │ Bearer llmgw_* key
    ▼
┌──────────────────────────────────────────────────────────┐
│ Lightweight vLLM Priority Gateway                       │
│ auth → model access → admission → affinity/live routing │
│ streaming proxy │ health/pressure │ Admin UI/API        │
│ analytics       │ Prometheus      │ SQLite registry     │
└───────────────────────────┬──────────────────────────────┘
                            │ controlled X-Vllm-Priority
                   ┌────────┴────────┐
                   ▼                 ▼
              vLLM backend A    vLLM backend B
```

The deployment footprint is one static Go binary and one SQLite state directory. The current release intentionally targets one gateway replica and a small, operator-managed backend pool; it does not require Redis, PostgreSQL, a message broker, a Kubernetes controller, or a frontend build chain.

## Key capabilities

- OpenAI-compatible `GET /v1/models`, `POST /v1/chat/completions`, `POST /v1/completions`, and `POST /v1/responses`.
- High-entropy client keys stored as HMAC-SHA-256 digests, never as plaintext.
- Per-client enablement, priority class, integer vLLM priority, concurrency limit, and explicit model access.
- Public-to-upstream model rewriting and static multi-backend model pools.
- Independent backend health and Prometheus polling with EWMA pressure and hysteretic pool states.
- Priority-aware admission, bounded `429` errors, least-pressure routing, soft session affinity, draining, and one conservative pre-first-byte retry.
- Per-backend circuit breakers and per-pool gateway-inflight/waiting safety limits.
- Immediate streaming and downstream cancellation propagation.
- Metadata-only request/token analytics with charts, filters, request detail, and CSV export.
- Embedded Basic-authenticated, CSRF-protected Admin UI and JSON Admin API.
- Prometheus metrics and structured JSON completion logs.

## Production quick start

This path connects the gateway to a real vLLM server and produces the first authenticated inference request. It uses Docker for the gateway; see the [deployment guide](docs/deployment.md) for native systemd installation, reverse-proxy requirements, production verification, and backup/restore.

### Prerequisites

- A Linux host with Docker for the gateway.
- A production vLLM endpoint reachable over the private network.
- A trusted TLS reverse proxy before exposing the gateway to client networks.
- `curl`; `jq` is useful for the verification commands.

The examples use:

- upstream model: `Qwen/Qwen2.5-7B-Instruct`;
- public model exposed to clients: `qwen`;
- private vLLM URL reachable from the gateway container: `http://vllm.internal:8000`;
- gateway listener on the Docker host: `http://127.0.0.1:8080`.

Replace all four values for your environment. In particular, `127.0.0.1` inside the gateway container refers to the gateway container itself, not to vLLM on the Docker host.

### 1. Start vLLM with priority scheduling

If vLLM is already managed by your inference platform, verify the equivalent flags and skip to the next step.

```bash
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --host 0.0.0.0 \
  --port 8000 \
  --scheduling-policy priority \
  --enable-prefix-caching \
  --enable-prompt-tokens-details \
  --enable-request-id-headers
```

vLLM handles lower integer priority values earlier. `--enable-prompt-tokens-details` allows cache-read analytics when the model/backend reports them. Check the current [vLLM serve CLI](https://docs.vllm.ai/en/latest/cli/serve/) and [OpenAI-compatible server documentation](https://docs.vllm.ai/en/latest/serving/online_serving/openai_compatible_server/) when pinning or upgrading vLLM.

Keep the vLLM port private. Clients should reach only the gateway.

### 2. Build and start the gateway

```bash
git clone https://github.com/rislanov/vllm-priority-gateway.git
cd vllm-priority-gateway

umask 077
export LLMGW_ADMIN_PASSWORD="$(openssl rand -base64 24)"
export LLMGW_API_KEY_HMAC_SECRET="$(openssl rand -base64 48)"
# Save both generated values in the approved secret store now.

docker build --platform linux/amd64 -t vllm-priority-gateway:local .
docker volume create llmgw-data
docker run -d --name llmgw --restart unless-stopped \
  -p 127.0.0.1:8080:8080 \
  -v llmgw-data:/data \
  -e LLMGW_ADMIN_USERNAME=operator \
  -e LLMGW_ADMIN_PASSWORD \
  -e LLMGW_API_KEY_HMAC_SECRET \
  vllm-priority-gateway:local
```

Verify process and registry readiness:

```bash
curl -fsS http://127.0.0.1:8080/healthz | jq
curl -fsS http://127.0.0.1:8080/readyz | jq
```

`/readyz` confirms management-plane readiness. It deliberately remains healthy when inference capacity is zero; `/inference-readyz` becomes ready after the pool and backend are configured and monitored successfully.

### 3. Configure the gateway in Admin

Open `http://127.0.0.1:8080/admin` through an operator-only path and authenticate as `operator` with `LLMGW_ADMIN_PASSWORD`.

Create resources in this order:

1. Open **Backends → Create model pool**:
   - **Public model name:** `qwen`
   - **Upstream model name:** `Qwen/Qwen2.5-7B-Instruct`
   - **Max gateway inflight:** `32`
   - **Max waiting:** `8`
   - **Enabled:** checked
2. On the same page, create a backend:
   - **Name:** `gpu-a`
   - **Model pool:** `qwen`
   - **URL:** `http://vllm.internal:8000`
   - **Capacity hint:** `1`
   - **Running soft limit:** `16`
   - **Enabled:** checked
3. Open **Clients → Create client**:
   - **Name:** `production-app`
   - **Priority class:** `normal`
   - **vLLM priority:** `0`
   - **Max concurrency:** `8`
   - **Model access:** check `qwen`
   - **Enabled:** checked
4. Open **API Keys**, select `production-app`, optionally set an expiry date, and choose **Generate API key**. Copy the full `llmgw_*` value immediately; it is shown only once.

The numeric limits above are illustrative values for validating the workflow, not universal production sizing. Calibrate them against the selected model, GPU, vLLM `--max-num-seqs`, latency objective, and saturation tests before enabling client traffic. Zero disables the corresponding pool limit.

If vLLM uses `--api-key`, enter only the gateway environment-variable name, such as `VLLM_GPU_A_KEY`, in **Upstream API key environment variable** and inject that variable into the gateway container. Never paste the upstream secret into Admin.

### 4. Verify capacity and make real requests

Wait for backend health and metrics polling, then require inference readiness:

```bash
curl -fsS http://127.0.0.1:8080/inference-readyz \
  | jq -e '.status == "ready" and .backendAvailability > 0'
```

Export the one-time client key:

```bash
export LLMGW_URL='http://127.0.0.1:8080'
export LLMGW_CLIENT_KEY='llmgw_copy-the-one-time-value-here'
```

List the models available to this client:

```bash
curl -fsS "$LLMGW_URL/v1/models" \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" | jq
```

Send a streaming Chat Completions request:

```bash
curl -fsS -N "$LLMGW_URL/v1/chat/completions" \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" \
  -H 'X-LLM-Session-Id: production-agent-42' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "qwen",
    "messages": [{"role": "user", "content": "Explain priority scheduling in one sentence."}],
    "max_tokens": 64,
    "stream": true
  }'
```

The client sends the public model name `qwen`. The gateway authenticates the key, enforces the stored client policy, rewrites the model to `Qwen/Qwen2.5-7B-Instruct`, applies the server-controlled vLLM priority, selects an eligible backend, and streams the upstream response.

## Admin UI

The embedded Admin UI is served by the gateway; there is no separate frontend deployment. The screenshots below use synthetic sample traffic and contain no production credentials.

### Gateway and backend status

The Dashboard shows configuration readiness, pool pressure and capacity guards, and backend health, circuit, running/waiting, and KV-cache state.

![Gateway dashboard with one healthy pool and backend](docs/images/admin-dashboard.jpg)

### Issue and revoke API keys

Create the client and grant model access first. Then open **API Keys**, choose the client and optional expiry, and select **Generate API key**. The full secret appears once; the table retains only its prefix, status, expiry, and last-use time. Revocation takes effect immediately.

![API key list and generation form](docs/images/admin-api-keys.jpg)

### View usage analytics

After inference requests complete, open **Analytics**. Use UTC presets or an exact range, filter by client/model/usage availability, inspect request and token charts, review per-request metadata, or download the same filtered dataset as CSV.

![Usage analytics request, token, and cache charts](docs/images/admin-analytics.jpg)

Analytics stores operational metadata and token counts only. Prompts, messages, generated text, bodies, authorization headers, and API-key secrets are not stored.

## Client API behavior

Supported client routes:

```text
GET  /v1/models
POST /v1/chat/completions
POST /v1/completions
POST /v1/responses
```

Clients cannot raise their priority: the gateway removes inbound `X-Vllm-Priority` and JSON `priority`, rewrites the public model name, and applies policy from SQLite. Unsupported `/v1/*` routes and gateway failures use an OpenAI-shaped error envelope.

For prefix-cache locality, send the same opaque `X-LLM-Session-Id` on consecutive requests from one agent or conversation. The value is limited to 256 bytes, never logged or used as a metric label, and stripped before forwarding to vLLM. Health, drain state, metrics freshness, circuit state, and live pressure always take precedence over affinity.

## Operations and deployment

- [Production deployment](docs/deployment.md): Docker and native systemd installation, reverse-proxy requirements, post-deployment verification, backup, and restore.
- [Operations guide](docs/operations.md): readiness semantics, routing, drain/resume, circuit breakers, pool safety, analytics retention, metrics, logs, and the security boundary.
- [Real-vLLM E2E runbook](docs/real-vllm-priority-e2e.md): production-safe smoke mode plus isolated priority, saturation, drain, and resilience tests.
- [Real-GPU test plan](docs/real-gpu-testing.md): final hardware/model compatibility and threshold calibration.

Common runtime endpoints:

| Endpoint | Purpose |
|---|---|
| `/healthz` | Process liveness |
| `/readyz` | SQLite and registry readiness |
| `/inference-readyz` | Usable inference capacity; HTTP `503` when unavailable |
| `/metrics` | Prometheus telemetry |
| `/admin` | Operator UI |

The gateway reloads committed Admin changes without a process restart. Drain a backend before maintenance and resume it only after health and metrics are fresh.

## Admin API

All Admin routes require Basic auth. Every state-changing request also requires the matching double-submit CSRF cookie/header or cookie/form token.

```text
GET    /admin/api/status
GET    /admin/api/clients
POST   /admin/api/clients
PUT    /admin/api/clients/{id}
POST   /admin/api/clients/{id}/keys
DELETE /admin/api/keys/{id}
GET    /admin/api/pools
POST   /admin/api/pools
PUT    /admin/api/pools/{id}
GET    /admin/api/backends
POST   /admin/api/backends
PUT    /admin/api/backends/{id}
POST   /admin/api/backends/{id}/drain
POST   /admin/api/backends/{id}/resume
GET    /admin/api/analytics
GET    /admin/api/analytics/requests
GET    /admin/api/analytics/export.csv
```

## Development and testing

Requirements: Go 1.27 and macOS or Linux. The SQLite driver is pure Go; builds do not require CGO.

```bash
make test
make test-race
make vet
make build
make build-linux-amd64
make build-e2e-linux-amd64
make container-smoke  # requires a running Docker daemon
```

The repository also contains a deterministic fake vLLM and load generator for tests; they are development tools and are not part of the production quick start.

The implementation and deterministic acceptance suite are code-complete. The opt-in real-vLLM smoke, priority/pool-safety, and circuit-resilience modes passed on Apple M4 with vLLM-Metal on 2026-08-27. Repeat compatibility, saturation, and threshold calibration on the selected production model and GPU before sign-off.

## Current scope and limitations

- Exactly one gateway replica; admission leases and backend runtime state are process-local.
- Static operator-managed backend registration; no service discovery or autoscaling.
- No distributed rate limits, token budgets, billing, or GPU/NVML scheduling.
- Priority admission rejects new lower-priority work but does not preempt an admitted generation.
- Soft session affinity improves locality without inspecting KV blocks or prefix contents.
- Basic auth is the current management credential. TLS, OIDC/RBAC, audit trails, and a secret manager are external responsibilities.
- Capacity hints are persisted for forward compatibility but do not currently weight routing.

See the [technical specification](docs/technical-specification.md) for the broader Production V1 target.

## Documentation

- [Production deployment](docs/deployment.md)
- [Operations guide](docs/operations.md)
- [Technical specification](docs/technical-specification.md)
- [Automated real-vLLM E2E](docs/real-vllm-priority-e2e.md)
- [Real-GPU test plan](docs/real-gpu-testing.md)
- [Acceptance evidence](docs/acceptance-evidence.md)
- [Russian README](README.ru.md)
