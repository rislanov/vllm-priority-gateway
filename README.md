[English](README.md) | [Русский](README.ru.md)

# vLLM Priority Gateway

[![CI](https://github.com/rislanov/vllm-priority-gateway/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/rislanov/vllm-priority-gateway/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/rislanov/vllm-priority-gateway)](https://github.com/rislanov/vllm-priority-gateway/releases/latest)
[![Go 1.27](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go)](go.mod)
[![License: Unlicense](https://img.shields.io/badge/license-Unlicense-blue.svg)](LICENSE)

**Protect high-priority inference workloads on shared vLLM GPU clusters.**

vLLM Priority Gateway is a lightweight policy, admission, and routing layer for **existing [vLLM](https://docs.vllm.ai/) servers**. Applications keep the standard OpenAI API: change the base URL, use a gateway-issued key, and leave model deployment and GPU scheduling where they already are.

**Validated against real vLLM on NVIDIA hardware**, including saturation, priority isolation, backend failure and recovery, streaming, and restart persistence. [Evidence](docs/acceptance-evidence.md).

## What problem it solves

A normal HTTP load balancer can distribute requests, but it usually does not know:

- which client may consume shared GPU capacity;
- which request is production traffic versus background work;
- how many requests are running or waiting inside each vLLM scheduler;
- how much KV cache each engine is using;
- when lower-priority traffic should be shed;
- which backend best preserves prefix-cache locality.

The gateway combines server-side client policy with live vLLM health and Prometheus metrics:

```text
production requests  ── high ────────┐
interactive agents   ── normal ──────┼──► shared vLLM capacity
nightly / eval jobs  ── background ──┘

                         GPU pressure rises

background traffic ──► throttled / 429
production traffic ──► remains admitted
```

## Where it sits

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
```

The gateway does **not** deploy models, schedule GPUs, or replace Kubernetes, Slurm, or existing vLLM lifecycle tooling. It controls **who receives shared inference capacity and which vLLM instance receives each request**.

The deployment footprint is one static Go binary and one SQLite state directory. The current release intentionally targets one gateway replica and a small operator-managed backend pool.

## Engineering highlights

- Server-side priority: inbound priority headers and JSON fields are removed and replaced with stored client policy.
- Explicit model ACLs and per-client concurrency limits.
- Independent backend health and metrics polling with EWMA pressure and hysteretic pool states.
- Least-pressure routing, soft session affinity, backend drain, and one conservative pre-first-byte retry.
- Pool-wide inflight/waiting safety limits and per-backend circuit breakers.
- Immediate streaming with downstream cancellation propagation.
- Metadata-only request/token analytics; prompts and generated text are never stored.
- Embedded Basic-authenticated, CSRF-protected Admin UI and JSON Admin API.
- Prometheus metrics, structured completion logs, and a provisioned causal Grafana dashboard.

## 5-minute gateway quick start

This path connects the published gateway to vLLM servers you already operate. The gateway must reach `/health`, `/metrics`, and `/v1/*` on each registered server.

1. Create `.env` with independent random secrets:

   ```dotenv
   LLMGW_ADMIN_USERNAME=operator
   LLMGW_ADMIN_PASSWORD=replace-with-at-least-16-random-bytes
   LLMGW_API_KEY_HMAC_SECRET=replace-with-at-least-32-random-bytes
   ```

2. Start the released image with persistent SQLite state:

   ```console
   docker pull ghcr.io/rislanov/vllm-priority-gateway:0.3.0
   docker volume create llmgw-data
   docker run -d --name vllm-priority-gateway --restart unless-stopped -p 127.0.0.1:8080:8080 --env-file .env -v llmgw-data:/data ghcr.io/rislanov/vllm-priority-gateway:0.3.0
   ```

3. Open `http://127.0.0.1:8080/admin`, then create:

   - one model pool with a public and upstream model name;
   - each vLLM instance as a separate backend;
   - one client with priority, concurrency, and model access;
   - one client API key. Copy it immediately; it is shown only once.

4. Point an OpenAI client at the gateway:

   ```python
   from openai import OpenAI

   client = OpenAI(
       base_url="http://127.0.0.1:8080/v1",
       api_key="llmgw_...",
   )

   response = client.chat.completions.create(
       model="your-public-model",
       messages=[{"role": "user", "content": "Review this function."}],
   )
   ```

For a self-contained GPU environment with two real vLLM instances, release checksum verification, exact Admin values, inference probes, and cleanup, use the [complete real-vLLM local demo](docs/local-demo.md).

## Admin UI

The UI is embedded in the gateway; there is no separate frontend deployment.

### Gateway and backend status

![Gateway dashboard with one healthy pool and backend](docs/images/admin-dashboard.jpg)

### Issue and revoke API keys

![API key list and issue form](docs/images/admin-api-keys.jpg)

### Inspect metadata-only usage analytics

![Request, token, and cache-usage charts](docs/images/admin-analytics.jpg)

## Client API

```text
GET  /v1/models
POST /v1/chat/completions
POST /v1/completions
POST /v1/responses
```

For prefix-cache locality, send the same opaque `X-LLM-Session-Id` on consecutive requests from one agent or conversation. The value is bounded, never logged or used as a metric label, and stripped before forwarding. Health, drain state, metrics freshness, circuit state, and pressure always take precedence over affinity.

## Production and operations

- [Production deployment](docs/deployment.md): Docker and systemd installation, TLS reverse proxy, secrets, backup, and restore.
- [Operations guide](docs/operations.md): readiness, routing, drain/resume, circuits, pool safety, metrics, logs, and recovery.
- [Real-vLLM E2E runbook](docs/real-vllm-priority-e2e.md): smoke, priority, saturation, drain, and resilience modes.
- [Real-GPU validation](docs/real-gpu-testing.md): compatibility and threshold calibration on target hardware.

| Endpoint | Purpose |
|---|---|
| `/healthz` | Process liveness |
| `/readyz` | SQLite and registry readiness |
| `/inference-readyz` | Usable inference capacity; HTTP `503` when unavailable |
| `/metrics` | Prometheus telemetry |
| `/admin` | Operator UI |

The optional observability overlay provisions Prometheus and the **Gateway Decisions** dashboard:

```console
docker compose --env-file .env -f compose.yaml -f compose.observability.yaml up -d --build --wait --wait-timeout 900
```

The dashboard shows pool pressure → precise Low 429 reasons → whether High traffic remains admitted → High gateway queue wait. It does not present saturated GPU latency as stable.

## Validation and real-vLLM evidence

The release and overload behavior were exercised on real inference infrastructure:

- NVIDIA RTX 4070 Ti with 12 GB VRAM;
- two `vllm/vllm-openai:v0.28.0` instances serving `Qwen/Qwen3-0.6B`;
- saturation at `GatewayInflight=16` and `TotalWaiting=14`;
- lower-priority probes rejected while High and Critical traffic remained admitted;
- backend drain, pool safety limits, circuit opening, and recovery verified;
- streaming, gateway restart, and SQLite persistence verified on real vLLM;
- downstream cancellation propagation covered by deterministic integration tests;
- a separate Prometheus/Grafana decision-telemetry scenario reproduced the pressure → shed → admission chain.

Under the recorded telemetry load, the protected High probe remained admitted while first-byte latency increased from about `187ms` to `561ms`. The evidence demonstrates admission isolation, not a claim that GPU latency stays unchanged under saturation.

- [Acceptance evidence](docs/acceptance-evidence.md) maps product claims to automated tests and recorded hardware runs.
- [Automated real-vLLM E2E](docs/real-vllm-priority-e2e.md) contains opt-in deployed tests.
- [Real-GPU validation](docs/real-gpu-testing.md) defines the retained evidence and production calibration procedure.

```bash
make test
make test-race
make vet
make build
make container-smoke  # requires Docker
```

## CI and releases

Pull requests and pushes to `main` run unit tests, the Go race detector, `go vet`, a gateway binary build, and a Docker image build in parallel. The aggregate **Unit tests and builds** status succeeds only when all five jobs pass and is the required branch check. CI never publishes artifacts.

Releases are manual. The Release workflow validates the selected stable SemVer tag, publishes checksum-protected Linux `amd64` and `arm64` archives, and publishes a multi-platform GHCR image. See [Production deployment](docs/deployment.md) for artifact use and [the workflow](.github/workflows/release.yml) for the exact release contract.

## Current scope and limitations

- Exactly one gateway replica; admission leases and backend runtime state are process-local.
- Static operator-managed backend registration; no service discovery or autoscaling.
- No distributed rate limits, token budgets, billing, or GPU/NVML scheduling.
- Priority admission rejects new lower-priority work but does not preempt an admitted generation.
- Soft session affinity improves locality without inspecting KV blocks or prefix contents.
- Basic auth is the current management credential. TLS, OIDC/RBAC, audit trails, and a secret manager are external responsibilities.
- Capacity hints are persisted for forward compatibility but do not currently weight routing.

See the [technical specification](docs/technical-specification.md) for the broader Production V1 target.

## Development

Requirements: Go 1.27 and macOS or Linux. The SQLite driver is pure Go; builds do not require CGO.

```bash
make test
make test-race
make vet
make build
make build-linux-amd64
make build-e2e-linux-amd64
```

## Documentation

- [Complete real-vLLM local demo](docs/local-demo.md)
- [Production deployment](docs/deployment.md)
- [Operations guide](docs/operations.md)
- [Technical specification](docs/technical-specification.md)
- [Acceptance evidence](docs/acceptance-evidence.md)
- [Russian README](README.ru.md)

## License

[The Unlicense](LICENSE).
