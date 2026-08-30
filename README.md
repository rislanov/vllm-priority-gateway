[English](README.md) | [Русский](README.ru.md)

# Lightweight vLLM Priority Gateway

## Contents

- [What it is](#what-it-is)
- [Key capabilities](#key-capabilities)
- [Docker quick start](#docker-quick-start)
- [Admin UI](#admin-ui)
- [Client API behavior](#client-api-behavior)
- [Operations and deployment](#operations-and-deployment)
- [Admin API](#admin-api)
- [Development and testing](#development-and-testing)
- [CI and releases](#ci-and-releases)
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

## Docker quick start

This is the canonical first-run path. The same `compose.yaml` launches the gateway and two real `Qwen/Qwen3-0.6B` vLLM servers on Linux Docker Engine and Docker Desktop. Compose creates the private network, service DNS, SQLite volume, and shared model cache; only the gateway is published, on `127.0.0.1:8080`.

The tested defaults deliberately run two small-capacity vLLM processes on one NVIDIA GPU with at least 12 GiB VRAM. They are suitable for validating routing and priority behavior, not production sizing. See the [deployment guide](docs/deployment.md) before exposing the gateway outside the Docker host.

### Prerequisites

- Docker Engine with Docker Compose v2, or Docker Desktop using Linux containers.
- An NVIDIA GPU visible to Docker and a Docker daemon configured for [Compose GPU reservations](https://docs.docker.com/compose/how-tos/gpu-support/).
- Approximately 25 GiB free disk space for the pinned vLLM image, model weights, and caches.
- Access to Hugging Face for the public Qwen model. `HF_TOKEN` is optional but avoids anonymous download rate limits.

### 1. Set local secrets and validate Docker

Clone the repository and open its directory. Create `.env` next to `compose.yaml` from [`.env.example`](.env.example), then fill both blank secrets with independently generated random values:

```dotenv
LLMGW_ADMIN_USERNAME=operator
LLMGW_ADMIN_PASSWORD=replace-with-at-least-16-random-bytes
LLMGW_API_KEY_HMAC_SECRET=replace-with-at-least-32-random-bytes
LLMGW_PORT=8080
```

Keep `.env` readable only by the operator account and never commit it; the repository already ignores it. The remaining values in `.env.example` pin the tested vLLM image, Qwen model, memory fraction, and compatibility runner. Change capacity settings only after the first successful run.

These commands are identical in Bash and PowerShell:

```console
docker compose config --quiet
docker compose run --rm --no-deps --entrypoint nvidia-smi vllm-a
```

The first command must finish silently. The second must show the intended NVIDIA GPU from inside the pinned vLLM container.

### 2. Build and start the complete stack

```console
docker compose up -d --build --wait --wait-timeout 900
docker compose ps
```

`vllm-a` downloads and compiles the model first; `vllm-b` starts after A is healthy and reuses the named Hugging Face cache. The gateway starts only after both inference servers pass `/health`. On a cold machine the image pull and first model initialization can take several minutes.

The optional `probe` service provides a pinned curl client inside the private Compose network, so host curl, jq, shell variables, and platform-specific quoting are not required:

```console
docker compose run --rm --no-deps probe -fsS http://gateway:8080/healthz
docker compose run --rm --no-deps probe -fsS http://gateway:8080/readyz
```

`/readyz` confirms management-plane readiness. It deliberately remains healthy before model pools are configured.

### 3. Configure the gateway in Admin

Open `http://127.0.0.1:8080/admin` and authenticate with the Admin username and password from `.env`.

Create resources in this order:

1. Open **Backends → Create model pool**:
   - **Public model name:** `qwen`
   - **Upstream model name:** `qwen-test`
   - **Max gateway inflight:** `32`
   - **Max waiting:** `8`
   - **Enabled:** checked
2. Create two backends in the same pool:
   - **Name:** `vllm-a`; **URL:** `http://vllm-a:8000`
   - **Name:** `vllm-b`; **URL:** `http://vllm-b:8000`
   - For both: **Capacity hint:** `1`; **Running soft limit:** `1`; **Enabled:** checked
3. Open **Clients → Create client**:
   - **Name:** `production-app`
   - **Priority class:** `normal`
   - **vLLM priority:** `0`
   - **Max concurrency:** `8`
   - **Model access:** check `qwen`
   - **Enabled:** checked
4. Open **API Keys**, select `production-app`, optionally set an expiry date, and choose **Generate API key**. Copy the full `llmgw_*` value immediately; it is shown only once.

The service names above are Compose DNS records, not host aliases. vLLM has no published host port and remains reachable only by other services in this stack.

### 4. Verify real inference

After the monitor has polled both backends, inference readiness must return HTTP 200 and report two available backends:

```console
docker compose run --rm --no-deps probe -fsS http://gateway:8080/inference-readyz
```

Replace the placeholder below with the one-time client key and list the public models:

```console
docker compose run --rm --no-deps probe -fsS http://gateway:8080/v1/models -H "Authorization: Bearer llmgw_replace-with-the-generated-key"
```

Send the checked-in streaming request body from [`examples/quickstart-chat.json`](examples/quickstart-chat.json):

```console
docker compose run --rm --no-deps probe -fsS -N http://gateway:8080/v1/chat/completions -H "Authorization: Bearer llmgw_replace-with-the-generated-key" -H "Content-Type: application/json" --data-binary "@/requests/chat.json"
```

The stream must end with `data: [DONE]`. The client uses public model `qwen`; the gateway authenticates it, applies the stored priority, rewrites the upstream model to `qwen-test`, and selects one of the two healthy vLLM services.

Useful lifecycle commands are also host-independent:

```console
docker compose logs -f gateway
docker compose stop
docker compose down
```

`docker compose down` preserves the SQLite and cache volumes. Use `docker compose down --volumes` only when intentionally deleting all Quick Start state and cached model data.

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

The repository also contains a deterministic fake vLLM and load generator for tests; they are development tools and are not part of the Docker quick start.

The implementation and deterministic acceptance suite are code-complete. The canonical Docker Quick Start and the real-vLLM smoke, priority/pool-safety, and circuit-resilience modes passed on an RTX 4070 Ti with two `Qwen/Qwen3-0.6B` services on 2026-08-28. Repeat compatibility, saturation, and threshold calibration with the selected production model and topology before sign-off.

## CI and releases

Pull requests and pushes to `main` automatically run the unit tests, `go vet`, a gateway binary build, and a Docker image build. They never publish artifacts or images.

Releases are manual and use the newest existing stable SemVer tag reachable from `main`:

```bash
git tag -a v1.2.3 -m "v1.2.3"
git push origin v1.2.3
```

In GitHub, open **Actions → Release → Run workflow**, enter the tag, and start the run. Releases are serialized, and an older tag is rejected so it cannot move `latest`, major, or minor image aliases backwards. The workflow publishes Linux `amd64` and `arm64` gateway archives plus `SHA256SUMS` to a GitHub Release. It also publishes the multi-platform image as `ghcr.io/rislanov/vllm-priority-gateway:1.2.3`, `:1.2`, `:1`, and `:latest`. Version numbers are derived exclusively from the selected tag.

If a run is interrupted after creating its draft release, run the same tag again; the workflow validates and refreshes the matching draft before resuming. Configure a repository ruleset that prevents updates and deletion of stable `v*` tags. The workflow verifies the remote tag immediately before publishing the image and again before publishing the draft, but the ruleset is the durable protection against a tag changing during a release.

GitHub Container Registry packages can be private after their first publication. If anonymous pulls are required, change the package visibility to public once in the package settings.

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
