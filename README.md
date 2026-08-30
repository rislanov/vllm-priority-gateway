[English](README.md) | [Русский](README.ru.md)

# vLLM Priority Gateway

**Protect high-priority inference workloads on shared vLLM GPU clusters.**

vLLM Priority Gateway is a lightweight policy and routing layer that sits **in front of your existing [vLLM](https://docs.vllm.ai/) servers**. Applications keep using the standard OpenAI API: change the API base URL, use a gateway-issued key, and leave model deployment and GPU scheduling where they already are.

The gateway then:

- assigns priority from server-side client policy;
- enforces per-client concurrency and model access;
- monitors every vLLM backend through health and Prometheus metrics;
- routes requests to the least-pressured healthy backend;
- keeps related agent or conversation traffic on the same backend when possible;
- sheds lower-priority work when shared GPU capacity is overloaded.

## Where it sits in your vLLM cluster

```text
                    Existing inference infrastructure

 User-facing API ───────┐
                        │  llmgw_prod_*     priority: high
 Coding agents ─────────┼──────────────────────────────────┐
                        │  llmgw_agents_*   priority: normal
 Batch / eval jobs ─────┘  llmgw_batch_*    priority: background
                                                            │
                                                            ▼
                                              ┌───────────────────────┐
                                              │ vLLM Priority Gateway │
                                              │                       │
                                              │ auth / policy         │
                                              │ admission control     │
                                              │ pressure-aware route  │
                                              │ session affinity      │
                                              │ circuit breakers      │
                                              └───────────┬───────────┘
                                                          │
                                      ┌───────────────────┼───────────────────┐
                                      ▼                   ▼                   ▼
                               vLLM GPU node A     vLLM GPU node B     vLLM GPU node C
                               :8000               :8000               :8000
```

The gateway does **not** deploy models, schedule GPUs, or replace Kubernetes, Slurm, or your existing vLLM lifecycle tooling. It controls **who gets shared inference capacity and which vLLM instance receives each request**.

The deployment footprint is one static Go binary and one SQLite state directory. The current release intentionally targets one gateway replica and a small, operator-managed backend pool; it does not require Redis, PostgreSQL, a message broker, a Kubernetes controller, or a frontend build chain.

## Contents

- [Where it sits in your vLLM cluster](#where-it-sits-in-your-vllm-cluster)
- [Connect an existing vLLM cluster](#connect-an-existing-vllm-cluster)
- [Why not just use Nginx or a Kubernetes Service?](#why-not-just-use-nginx-or-a-kubernetes-service)
- [Key capabilities](#key-capabilities)
- [Local demo with real vLLM](#local-demo-with-real-vllm)
- [Admin UI](#admin-ui)
- [Client API behavior](#client-api-behavior)
- [Operations and deployment](#operations-and-deployment)
- [Admin API](#admin-api)
- [Development and testing](#development-and-testing)
- [CI and releases](#ci-and-releases)
- [Current scope and limitations](#current-scope-and-limitations)
- [Documentation](#documentation)

## Connect an existing vLLM cluster

Suppose you already run three vLLM servers:

```text
http://vllm-a.internal:8000
http://vllm-b.internal:8000
http://vllm-c.internal:8000
```

The gateway must be able to reach `/health`, `/metrics`, and the OpenAI-compatible `/v1/*` routes on each server.

### 1. Start the gateway

Create `.env` with independently generated secrets:

```dotenv
LLMGW_ADMIN_USERNAME=operator
LLMGW_ADMIN_PASSWORD=replace-with-at-least-16-random-bytes
LLMGW_API_KEY_HMAC_SECRET=replace-with-at-least-32-random-bytes
```

Keep this file readable only by the operator account and out of version control; the repository already ignores `.env`. Pull the published image and start the gateway on a network that can reach your vLLM endpoints:

```console
docker pull ghcr.io/rislanov/vllm-priority-gateway:0.2.0
docker volume create llmgw-data
docker run -d --name vllm-priority-gateway --restart unless-stopped -p 127.0.0.1:8080:8080 --env-file .env -v llmgw-data:/data ghcr.io/rislanov/vllm-priority-gateway:0.2.0
```

If the vLLM servers live on a Docker network, add the corresponding `--network` option. In production, normally place your existing TLS reverse proxy or ingress in front of the gateway:

```text
clients
   │
   ▼
https://llm.company.internal
   │
   ▼
vLLM Priority Gateway
   │
   ├── vllm-a.internal:8000
   ├── vllm-b.internal:8000
   └── vllm-c.internal:8000
```

See the [production deployment guide](docs/deployment.md) for reverse-proxy, secret-management, backup, and systemd requirements.

### 2. Register the existing vLLM servers

Open `http://127.0.0.1:8080/admin` and create a model pool, for example:

```text
Public model:      qwen-coder
Upstream model:    Qwen/Qwen3-Coder-Next
```

Then register every vLLM instance in that pool:

```text
vllm-a  → http://vllm-a.internal:8000
vllm-b  → http://vllm-b.internal:8000
vllm-c  → http://vllm-c.internal:8000
```

The gateway continuously polls each registered URL. It combines health, metrics freshness, running and waiting requests, KV-cache usage, circuit state, and gateway inflight counts when deciding which backend is eligible and least pressured.

> **For pressure-aware routing, register individual vLLM instances or stable per-instance endpoints whenever possible.**
>
> Avoid placing an opaque round-robin load balancer between the gateway and multiple vLLM replicas. If metrics are read from one replica but the inference request is sent to another, the gateway cannot make a reliable backend-level routing decision.

On Kubernetes, this usually means stable per-instance Services, StatefulSet DNS such as `vllm-0.vllm-headless`, or another topology in which the gateway can address each serving instance directly. Model deployment and GPU scheduling remain unchanged; backend registration is static in the current release.

### 3. Create clients and priorities

Create one gateway client for each workload class, grant only the required public models, and set its maximum concurrency. For example:

```text
production-api
  priority class: high
  max concurrency: 32
  models: qwen-coder

developer-agents
  priority class: normal
  max concurrency: 16
  models: qwen-coder

nightly-evals
  priority class: background
  max concurrency: 8
  models: qwen-coder
```

Assign the corresponding integer vLLM priority and generate a gateway API key for each client. Clients cannot promote themselves: inbound priority values are removed and replaced with policy stored by the gateway.

### 4. Point existing OpenAI clients at the gateway

Before:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://vllm-a.internal:8000/v1",
    api_key="token",
)
```

After:

```python
from openai import OpenAI

client = OpenAI(
    base_url="https://llm.company.internal/v1",
    api_key="llmgw_...",
)
```

Application code otherwise stays the same and requests the public model name:

```python
response = client.chat.completions.create(
    model="qwen-coder",
    messages=[{"role": "user", "content": "Review this function."}],
)
```

For every request, the gateway performs this path:

```text
authenticate client
        ↓
check model access
        ↓
apply server-side priority and overload policy
        ↓
choose a healthy vLLM backend
        ↓
rewrite public → upstream model name
        ↓
stream the response back
```

## Why not just use Nginx or a Kubernetes Service?

A normal HTTP load balancer can distribute requests. It usually does not know:

- which client is allowed to consume shared GPU capacity;
- which request is high-priority versus background work;
- how many requests are running or waiting inside each vLLM scheduler;
- how much KV cache each engine is using;
- when lower-priority traffic should be rejected to protect interactive traffic;
- which backend should receive the next request for prefix-cache locality.

vLLM Priority Gateway makes these decisions using **application policy plus live inference-server state**:

```text
shared GPU cluster

production requests  ── high ────────┐
interactive agents   ── normal ──────┼──► same vLLM capacity
nightly / eval jobs  ── background ──┘

                         GPU pressure rises

background traffic ──► throttled / 429
production traffic ──► remains admitted
```

That is the problem the gateway is designed to solve.

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

## Local demo with real vLLM

Don't have an existing cluster available? This self-contained demo uses two real `Qwen/Qwen3-0.6B` vLLM servers and one published gateway artifact: either the multi-platform image from GHCR or the checksum-verified Linux binary from the GitHub Release. The gateway is never built from the checkout in either path; the checkout supplies only the pinned vLLM topology and the sample request.

The tested defaults deliberately run two small-capacity vLLM processes on one NVIDIA GPU with at least 12 GiB VRAM. This environment validates routing, priority, overload behavior, metrics, release artifacts, and the Admin UI; it is not the recommended production topology or production sizing. The Docker gateway path works with Linux Docker Engine and Docker Desktop. The native path requires Linux `amd64` or `arm64`; its local vLLM containers publish loopback ports only. See the [deployment guide](docs/deployment.md) before exposing the gateway outside the host.

### Prerequisites

- Docker Engine or Docker Desktop using Linux containers, with Docker Compose 2.24.4 or newer.
- An NVIDIA GPU visible to Docker and a Docker daemon configured for [Compose GPU reservations](https://docs.docker.com/compose/how-tos/gpu-support/).
- Approximately 25 GiB free disk space for the pinned vLLM image, model weights, and caches.
- Access to Hugging Face for the public Qwen model. `HF_TOKEN` is optional but avoids anonymous download rate limits.
- For the native Linux path: `curl`, `tar`, and GNU `sha256sum`.

### 1. Set local secrets and validate Docker

Clone the repository and open its directory. Create `.env` next to `compose.yaml` from [`.env.example`](.env.example), then fill both blank secrets with independently generated random values:

```dotenv
LLMGW_VERSION=0.2.0
LLMGW_ADMIN_USERNAME=operator
LLMGW_ADMIN_PASSWORD=replace-with-at-least-16-random-bytes
LLMGW_API_KEY_HMAC_SECRET=replace-with-at-least-32-random-bytes
LLMGW_PORT=8080
```

`LLMGW_VERSION` is the image/archive version without the leading `v`; the corresponding Git tag is `v0.2.0`. Keep `.env` readable only by the operator account and never commit it; the repository already ignores it. The remaining values in `.env.example` pin the tested vLLM image, Qwen model, memory fraction, and compatibility runner. Change capacity settings only after the first successful run.

These commands are identical in Bash and PowerShell:

```console
docker compose config --quiet
docker compose run --rm --no-deps --entrypoint nvidia-smi vllm-a
```

The first command must finish silently. The second must show the intended NVIDIA GPU from inside the pinned vLLM container.

### 2. Choose one release artifact

Do not run both gateway options at the same time: both listen on `127.0.0.1:8080`. Their SQLite state is independent: Docker uses its named volume, while the native path uses `data/release-linux`.

#### Option A: Docker image from GHCR

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml config --quiet
docker compose --env-file .env -f compose.yaml -f compose.release.yaml pull gateway
docker compose --env-file .env -f compose.yaml -f compose.release.yaml up -d --no-build --wait --wait-timeout 900
docker compose --env-file .env -f compose.yaml -f compose.release.yaml ps
```

`compose.release.yaml` removes the local Docker build and selects `ghcr.io/rislanov/vllm-priority-gateway:${LLMGW_VERSION}`. `vllm-a` downloads and compiles the model first; `vllm-b` starts after A is healthy and reuses the named Hugging Face cache. The release gateway starts only after both inference servers pass `/health`. On a cold machine the image pull and first model initialization can take several minutes.

The optional `probe` service provides a pinned curl client inside the private Compose network, so host curl, jq, shell variables, and platform-specific quoting are not required:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/healthz
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/readyz
```

`/readyz` confirms management-plane readiness. It deliberately remains healthy before model pools are configured.

#### Option B: native Linux binary from GitHub Release

Start only the two local inference servers. `compose.native.yaml` publishes them on loopback so the host process can reach them without exposing vLLM to the LAN:

```console
docker compose --env-file .env -f compose.yaml -f compose.native.yaml config --quiet
docker compose --env-file .env -f compose.yaml -f compose.native.yaml up -d --wait --wait-timeout 900 vllm-a vllm-b
docker compose --env-file .env -f compose.yaml -f compose.native.yaml ps
```

Download the archive for the current Linux architecture and verify it against the release manifest before extraction:

```bash
VERSION=0.2.0
case "$(uname -m)" in
  x86_64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

ASSET="vllm-priority-gateway_${VERSION}_linux_${ARCH}.tar.gz"
BASE_URL="https://github.com/rislanov/vllm-priority-gateway/releases/download/v${VERSION}"
RELEASE_DIR="$PWD/dist/release-${VERSION}"
mkdir -p "$RELEASE_DIR" &&
curl --fail --location --remove-on-error --output "$RELEASE_DIR/$ASSET" "$BASE_URL/$ASSET" &&
curl --fail --location --remove-on-error --output "$RELEASE_DIR/SHA256SUMS" "$BASE_URL/SHA256SUMS" &&
(cd "$RELEASE_DIR" && sha256sum --check --ignore-missing SHA256SUMS) &&
tar --extract --gzip --file "$RELEASE_DIR/$ASSET" --directory "$RELEASE_DIR"
```

The checksum command must print `./$ASSET: OK`. Start the binary in the foreground with a separate SQLite directory; keep this terminal open and use a second terminal for the remaining steps:

```bash
install -d -m 0750 "$PWD/data/release-linux"
set -a
. ./.env
set +a
export LLMGW_LISTEN_ADDRESS=127.0.0.1:8080
export LLMGW_DATABASE_PATH="$PWD/data/release-linux/llmgw.db"
"$RELEASE_DIR/gateway"
```

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
   - Docker gateway: **Name:** `vllm-a`; **URL:** `http://vllm-a:8000`, and **Name:** `vllm-b`; **URL:** `http://vllm-b:8000`
   - Native Linux gateway: **Name:** `vllm-a`; **URL:** `http://127.0.0.1:8001`, and **Name:** `vllm-b`; **URL:** `http://127.0.0.1:8002`
   - For both: **Capacity hint:** `1`; **Running soft limit:** `1`; **Enabled:** checked
3. Open **Clients → Create client**:
   - **Name:** `production-app`
   - **Priority class:** `normal`
   - **vLLM priority:** `0`
   - **Max concurrency:** `8`
   - **Model access:** check `qwen`
   - **Enabled:** checked
4. Open **API Keys**, select `production-app`, optionally set an expiry date, and choose **Generate API key**. Copy the full `llmgw_*` value immediately; it is shown only once.

The Docker gateway uses private Compose DNS. The native gateway uses the loopback-only ports from `compose.native.yaml`; neither path exposes unauthenticated vLLM endpoints to the LAN.

### 4. Verify real inference

After the monitor has polled both backends, inference readiness must return HTTP 200 and report two available backends:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/inference-readyz
```

For native Linux, run `curl -fsS http://127.0.0.1:8080/inference-readyz` instead. Wait until the response reports `"backendAvailability":2`; allow one more monitor interval before sending automated traffic.

Replace the placeholder below with the one-time client key and list the public models:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/v1/models -H "Authorization: Bearer llmgw_replace-with-the-generated-key"
```

For native Linux:

```bash
export LLMGW_CLIENT_KEY='llmgw_replace-with-the-generated-key'
curl -fsS http://127.0.0.1:8080/v1/models -H "Authorization: Bearer $LLMGW_CLIENT_KEY"
```

Send the checked-in streaming request body from [`examples/quickstart-chat.json`](examples/quickstart-chat.json):

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS -N http://gateway:8080/v1/chat/completions -H "Authorization: Bearer llmgw_replace-with-the-generated-key" -H "Content-Type: application/json" --data-binary "@/requests/chat.json"
```

For native Linux:

```bash
curl -fsS -N http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" \
  -H "Content-Type: application/json" \
  --data-binary "@examples/quickstart-chat.json"
```

The stream must end with `data: [DONE]`. The client uses public model `qwen`; the gateway authenticates it, applies the stored priority, rewrites the upstream model to `qwen-test`, and selects one of the two healthy vLLM services.

For the Docker gateway path:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml logs -f gateway
docker compose --env-file .env -f compose.yaml -f compose.release.yaml stop
docker compose --env-file .env -f compose.yaml -f compose.release.yaml down
```

For native Linux, stop the foreground gateway with `Ctrl-C`, then stop the local backends with `docker compose --env-file .env -f compose.yaml -f compose.native.yaml down`. The SQLite file remains under `data/release-linux`; Docker's named Hugging Face cache also remains. Add `--volumes` only when intentionally deleting the Docker gateway state or cached model data.

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

### Optional local Prometheus and Grafana

The checked-in observability overlay starts Prometheus and provisions the **Gateway Decisions** dashboard in Grafana:

```console
docker compose --env-file .env -f compose.yaml -f compose.observability.yaml up -d --build --wait --wait-timeout 900
```

Open Grafana at `http://127.0.0.1:3000/d/llmgw-gateway-decisions` and Prometheus at `http://127.0.0.1:9090`. The dashboard's first row is deliberately causal: pool pressure → Low (`background`) 429 decisions by precise reason → High successful-request latency/TTFT → High gateway queue wait. Filters cover model, backend, client, and priority.

The local defaults are Grafana `admin` / `admin`. Override `LLMGW_GRAFANA_ADMIN_USER` and `LLMGW_GRAFANA_ADMIN_PASSWORD` before any use beyond a loopback-only development host. `LLMGW_PROMETHEUS_PORT` and `LLMGW_GRAFANA_PORT` override the loopback ports. When using the published gateway image, include `compose.release.yaml` before `compose.observability.yaml` and retain `--no-build`.

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

The repository also contains a deterministic fake vLLM and load generator for tests; they are development tools and are not part of the local demo.

The implementation and deterministic acceptance suite are code-complete. The local-build Docker topology and the real-vLLM smoke, priority/pool-safety, and circuit-resilience modes passed on an RTX 4070 Ti with two `Qwen/Qwen3-0.6B` services on 2026-08-28. On 2026-08-30, both published v0.1.0 paths—the GHCR image and the checksum-verified Linux `amd64` archive—independently completed real streaming inference through the same two vLLM services and preserved their separate SQLite state across a gateway restart. Repeat compatibility, saturation, and threshold calibration with the selected production model and topology before sign-off.

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
