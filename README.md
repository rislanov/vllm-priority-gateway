# Lightweight vLLM Priority Gateway

## Purpose

Lightweight vLLM Priority Gateway is a single-process Go gateway for a small, static pool of vLLM OpenAI-compatible servers. It authenticates clients, enforces centrally configured priority and concurrency policy, routes to the least-pressured healthy backend, and sheds lower-priority traffic before critical traffic when inference capacity is scarce.

The repository also ships a deterministic fake vLLM server and a load generator. The MVP is intentionally operationally small: one gateway binary, one SQLite file, and no Redis, PostgreSQL, message broker, Kubernetes controller, or frontend build chain.

Status: the implementation and deterministic acceptance suite are code-complete. Real-vLLM scheduling, cancellation, and threshold calibration still require the documented GPU sign-off before production use.

## Architecture

```text
OpenAI client
    │ Bearer llmgw_* key
    ▼
┌──────────────────────────────────────────────────────────┐
│ Go gateway                                               │
│ auth → model access → admission → least-pressure routing │
│ streaming proxy │ monitor workers │ Admin API/UI         │
│ Prometheus      │ JSON logs       │ SQLite registry      │
└───────────────────────────┬──────────────────────────────┘
                            │ X-Vllm-Priority + request ID
                   ┌────────┴────────┐
                   ▼                 ▼
              vLLM backend A    vLLM backend B
```

SQLite owns durable configuration. Immutable indexed registry snapshots, health/pressure state, admission leases, and gateway in-flight counts live in memory. Fake vLLM and the load generator are separate commands in the same Go module.

## Features

- OpenAI-compatible `GET /v1/models`, `POST /v1/chat/completions`, `POST /v1/completions`, and `POST /v1/responses`.
- High-entropy client keys stored as an HMAC-SHA-256 digest, never as plaintext.
- Client enablement, four priority classes, server-controlled integer vLLM priority, maximum concurrency, and explicit model access.
- Public-to-upstream model rewriting and static multi-backend model pools.
- Independent health and Prometheus scraping with EWMA pressure and hysteretic pool states.
- Priority-aware admission, bounded `429` errors, least-pressure routing, draining exclusion, and one conservative pre-first-byte retry.
- Immediate response streaming, downstream cancellation propagation, Prometheus telemetry, and safe JSON completion logs.
- Basic-authenticated, CSRF-protected JSON Admin API and embedded server-rendered Admin UI.
- Deterministic fake vLLM controls and repeatable mixed-priority load generation.

## Requirements

- Go 1.27 for local builds.
- macOS or Linux for development; the delivery target is static Linux x86-64 (`linux/amd64`).
- A writable directory for SQLite.
- One or more reachable vLLM endpoints, or the bundled fake server.
- `curl` for the examples; `jq` is useful for Admin API and validation commands.

## Quick start

```bash
export LLMGW_ADMIN_USERNAME=operator
export LLMGW_ADMIN_PASSWORD='replace-with-at-least-16-bytes'
export LLMGW_API_KEY_HMAC_SECRET="$(openssl rand -base64 48)"
export LLMGW_DATABASE_PATH="$PWD/data/llmgw.db"

go run ./cmd/gateway
```

Open `http://127.0.0.1:8080/admin`, authenticate with the configured administrator credential, then create a model pool, backend, client, and API key. The full client key is shown once.

Required environment variables:

| Variable | Meaning |
|---|---|
| `LLMGW_ADMIN_USERNAME` | Admin Basic-auth username |
| `LLMGW_ADMIN_PASSWORD` | Admin password, at least 16 bytes |
| `LLMGW_API_KEY_HMAC_SECRET` | Server-side key hashing secret, at least 32 bytes |

Common optional variables:

| Variable | Default |
|---|---:|
| `LLMGW_LISTEN_ADDRESS` | `:8080` |
| `LLMGW_DATABASE_PATH` | `./data/llmgw.db` |
| `LLMGW_HEALTH_INTERVAL` / `LLMGW_HEALTH_TIMEOUT` | `2s` / `1s` |
| `LLMGW_METRICS_INTERVAL` / `LLMGW_METRICS_TIMEOUT` | `1s` / `1s` |
| `LLMGW_METRICS_STALE_AFTER` | `5s` |
| `LLMGW_UNHEALTHY_AFTER` / `LLMGW_RECOVERY_AFTER` | `3` / `2` |
| `LLMGW_QUEUE_SOFT_LIMIT` | `2` |
| `LLMGW_KV_SOFT_LIMIT` / `LLMGW_KV_HARD_LIMIT` | `0.80` / `0.95` |
| `LLMGW_EWMA_WINDOW` | `4s` |
| `LLMGW_OVERLOAD_ENTER_WINDOW` / `LLMGW_OVERLOAD_RECOVERY_WINDOW` | `3s` / `10s` |
| `LLMGW_REQUEST_BODY_LIMIT` | `16777216` |
| `LLMGW_SHUTDOWN_GRACE_PERIOD` | `30s` |

Threshold, recovery, dial, TLS, response-header, retry, and routing-epsilon values are also configurable with the `LLMGW_*` variables defined in `internal/config/config.go`. Startup rejects incomplete secrets and inconsistent threshold ordering.

## Initial configuration

The Admin UI is the simplest bootstrap path. Create resources in this order:

1. A model pool with a public client-facing name and the exact upstream vLLM model name.
2. At least one enabled backend using an absolute `http://` or `https://` base URL.
3. A client with priority class, integer vLLM priority, concurrency limit, and explicit pool access.
4. An API key for that client; copy the returned secret immediately.

For JSON automation, first obtain the double-submit CSRF cookie:

```bash
curl -sS -u "$LLMGW_ADMIN_USERNAME:$LLMGW_ADMIN_PASSWORD" \
  -c /tmp/llmgw-cookies http://127.0.0.1:8080/admin/api/status | jq
CSRF_TOKEN="$(awk '$6 == "llmgw_csrf" {print $7}' /tmp/llmgw-cookies)"
```

Then send both the cookie and `X-CSRF-Token` header:

```bash
curl -sS -u "$LLMGW_ADMIN_USERNAME:$LLMGW_ADMIN_PASSWORD" \
  -b /tmp/llmgw-cookies -H "X-CSRF-Token: $CSRF_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"publicModelName":"qwen","upstreamModelName":"Qwen/Qwen2.5-7B-Instruct","enabled":true}' \
  http://127.0.0.1:8080/admin/api/pools | jq
```

Every committed mutation increments the SQLite configuration revision, publishes a new registry snapshot, and reconciles backend monitor workers.

## Admin UI and API

The embedded UI has four screens:

- `/admin`: pool and backend runtime status.
- `/admin/clients`: client policy and model access.
- `/admin/keys`: one-time key generation, expiry/status, last use, and revocation.
- `/admin/backends`: model pools, endpoints, enablement, pressure, drain, and resume.

The JSON API exposes:

```text
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
GET    /admin/api/status
```

All Admin routes require Basic auth. Every state-changing request additionally needs the matching CSRF cookie/header or cookie/form token. Responses are non-cacheable and include restrictive CSP, frame, MIME-sniffing, and referrer headers.

## Client API

Use the one-time client key as a Bearer credential:

```bash
curl -sS http://127.0.0.1:8080/v1/models \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" | jq

curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen","messages":[{"role":"user","content":"Hello"}],"stream":true}'
```

Clients cannot raise their own priority: the gateway removes an inbound `X-Vllm-Priority` header and JSON `priority`, rewrites the model, and applies policy from SQLite. Unsupported `/v1/*` routes and gateway failures use an OpenAI-shaped error envelope.

## vLLM setup flags

Start each vLLM server with priority scheduling. Request-ID headers are recommended for end-to-end correlation:

```bash
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --host 0.0.0.0 \
  --port 8000 \
  --scheduling-policy priority \
  --enable-request-id-headers
```

In current vLLM documentation, lower integer priority values run earlier, and `X-Vllm-Priority` is supported by Chat Completions, Completions, and Responses. Check the [official OpenAI-compatible server documentation](https://docs.vllm.ai/en/latest/serving/online_serving/openai_compatible_server/) when upgrading vLLM.

If an upstream uses `--api-key`, configure only the environment-variable name in the backend record, for example `VLLM_GPU_A_KEY`, and set that variable in the gateway process. The secret is never stored in SQLite or returned by the Admin API.

## Fake vLLM

```bash
go run ./cmd/fake-vllm -listen 127.0.0.1:8002
```

Point a backend at `http://127.0.0.1:8002`. Inspect or modify deterministic state through the simulator-only endpoint:

```bash
curl -sS http://127.0.0.1:8002/admin/state | jq
curl -sS -X PUT http://127.0.0.1:8002/admin/state \
  -H 'Content-Type: application/json' \
  -d '{"running":12,"waiting":3,"kvCacheUsage":0.88,"ttftMs":40,"tokenDelayMs":10,"tokens":["one","two"]}' | jq
```

The control surface can also set HTTP failures, health failures, legacy KV metrics, and connection reset modes before headers, before body, or after a selected number of chunks.

## Load generation

Single client:

```bash
go run ./cmd/loadgen -- \
  -url http://127.0.0.1:8080 -key "$LLMGW_CLIENT_KEY" -model qwen \
  -parallelism 16 -requests 500 -prompt-size 512 -max-tokens 64 -stream
```

Deterministic mixed priority traffic:

```bash
go run ./cmd/loadgen -- \
  -url http://127.0.0.1:8080 -model qwen -parallelism 32 -requests 1000 -seed 42 \
  -class-keys "critical=$CRITICAL_KEY,high=$HIGH_KEY,normal=$NORMAL_KEY,background=$BACKGROUND_KEY" \
  -mix critical=10,high=20,normal=40,background=30 -json
```

The report separates successes, intentional `429` overload responses, `5xx`, other HTTP failures, and transport failures. TTFT and latency percentiles include successful generations only and are reported both in aggregate and by priority class, so fast rejections cannot make inference latency look better. A transport failure returns a non-zero process status; expected `429` responses do not.

## Metrics and logging

`GET /metrics` exposes the `llmgw_*` request, rejection, in-flight, backend pressure/running/waiting/KV, duration, TTFT, disconnect, backend-failure, and retry families. Labels are bounded to configured names and enums; request IDs, key prefixes, URLs, prompts, and generated text are not labels.

The gateway writes one JSON record per completed inference request to stderr. Records include correlation IDs, client/model policy, selected backend, pressure/state, status, duration, TTFT, disconnect, and retry count. Bodies and authorization headers are never logged.

`GET /healthz` reports process liveness. `GET /readyz` reports database/registry readiness and current backend availability without making the management plane unavailable when all inference backends are down.

## Security

- Client keys contain 32 random bytes after the `llmgw_` prefix. SQLite stores a short lookup prefix and `HMAC-SHA-256(server secret, full key)` only.
- Invalid, revoked, expired, and disabled-client keys share the same public `401` response.
- Admin credentials come only from required environment variables and are compared in constant time.
- State-changing Admin requests require double-submit CSRF protection; cookies are `HttpOnly` and `SameSite=Strict`.
- The gateway strips client authorization and hop-by-hop headers before proxying. An upstream authorization header is constructed only from the named gateway environment variable.
- Keep vLLM endpoints on a private network. The gateway is the client-facing policy boundary.
- Terminate TLS and apply network allowlists at a trusted reverse proxy. The MVP does not implement TLS, OIDC, RBAC, or an audit log.

## macOS development

Install Go 1.27 for Apple Silicon or Intel macOS, then run:

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/...
```

The SQLite driver is pure Go, so local development and Linux cross-compilation do not need Xcode command-line C tooling or CGO.

## Linux amd64 build

```bash
make build-linux-amd64
file dist/*
```

This produces static `gateway-linux-amd64`, `fake-vllm-linux-amd64`, and `loadgen-linux-amd64` executables. The equivalent direct command is:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o dist/gateway-linux-amd64 ./cmd/gateway
```

## Docker

```bash
docker build --platform linux/amd64 -t vllm-priority-gateway:local .
docker volume create llmgw-data
docker run --rm -p 8080:8080 -v llmgw-data:/data \
  -e LLMGW_ADMIN_USERNAME=operator \
  -e LLMGW_ADMIN_PASSWORD='replace-with-at-least-16-bytes' \
  -e LLMGW_API_KEY_HMAC_SECRET="$LLMGW_API_KEY_HMAC_SECRET" \
  vllm-priority-gateway:local
```

The runtime image is `scratch`, contains only CA certificates and the static gateway, and runs as numeric UID/GID `65532`.
The image initializes `/data` with that ownership, so a fresh named or anonymous volume is writable. For a Linux bind mount, create the directory and grant UID/GID `65532` write access before starting the container; Docker Desktop for macOS applies its own host-file sharing rules.

With a running Docker daemon, exercise the exact image, fresh-volume, non-root, SQLite, and health path with:

```bash
make container-smoke
```

## Testing

```bash
make test
make test-race
make vet
make build
make build-linux-amd64
make container-smoke  # requires a running Docker daemon
```

Tests cover domain validation, key security, SQLite migrations and CRUD, registry snapshots, pressure/EWMA/hysteresis, admission, routing, monitoring, fake-vLLM controls, proxy streaming/cancellation/retry, public and Admin HTTP contracts, UI semantics, telemetry, process lifecycle, load statistics, and black-box acceptance behavior.

## Real-GPU validation

Follow [`docs/real-gpu-testing.md`](docs/real-gpu-testing.md) for compatibility, cancellation, queue pressure, priority isolation, and hysteretic recovery runs against actual vLLM GPUs. Fake-backend tests prove gateway mechanics but cannot prove GPU scheduler behavior.

## Limitations

- One gateway replica; concurrency leases and backend runtime state are process-local.
- Static backend configuration and SQLite persistence only.
- No distributed rate limits, token budgets, billing, circuit breaker, autoscaling, discovery, prefix-aware routing, or GPU/NVML scheduling.
- Priority admission rejects new lower-priority requests; it does not preempt an already admitted generation.
- Basic auth is the MVP management credential. TLS, OIDC/RBAC, audit trails, and a secret manager are external or future responsibilities.
- Capacity hints are persisted for forward compatibility but do not yet weight routing.

## Production evolution

The package boundaries allow SQLite to be replaced with PostgreSQL, local leases with a distributed store, and static backend configuration with discovery. A production revision should add multi-replica configuration revisions, OIDC/RBAC, audit logging, secret-manager integration, circuit breaking, capacity-aware and affinity-aware routing, distributed budgets, OpenTelemetry, and controlled failure policy for its coordination store.
