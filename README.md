# Lightweight vLLM Priority Gateway

## Purpose

Lightweight vLLM Priority Gateway is a single-process Go gateway for a small, static pool of vLLM OpenAI-compatible servers. It authenticates clients, enforces centrally configured priority and concurrency policy, combines soft session affinity with live-pressure routing, and sheds lower-priority traffic before critical traffic when inference capacity is scarce.

The repository also ships a deterministic fake vLLM server and a load generator. The MVP is intentionally operationally small: one gateway binary, one SQLite file, and no Redis, PostgreSQL, message broker, Kubernetes controller, or frontend build chain.

Status: the implementation and deterministic acceptance suite are code-complete. An opt-in real-vLLM black-box suite automates production smoke, priority isolation and pool safety, drain, circuit-breaker recovery, and hysteretic recovery checks. Threshold calibration and final sign-off still need to be repeated on the selected production models and GPU hardware.

## Architecture

```text
OpenAI client
    │ Bearer llmgw_* key
    ▼
┌──────────────────────────────────────────────────────────┐
│ Go gateway                                               │
│ auth → model access → admission → affinity/live routing │
│ streaming proxy │ monitor workers │ Admin API/UI         │
│ Prometheus      │ JSON logs       │ SQLite registry      │
└───────────────────────────┬──────────────────────────────┘
                            │ X-Vllm-Priority + request ID
                   ┌────────┴────────┐
                   ▼                 ▼
              vLLM backend A    vLLM backend B
```

SQLite owns durable configuration. Immutable indexed registry snapshots, health/pressure state, admission leases, and gateway in-flight counts live in memory. Fake vLLM and the load generator are separate commands in the same Go module.

## Documentation

- [Technical specification (English)](docs/technical-specification.md) — complete MVP and Production V1 requirements translated from the original project brief.
- [Real-GPU test plan](docs/real-gpu-testing.md) — hardware validation procedure for vLLM compatibility, cancellation, priority, pressure, and recovery.
- [Automated real-vLLM E2E](docs/real-vllm-priority-e2e.md) — reproducible Mac M4/Linux setup and opt-in smoke, saturation, pool-safety, drain, and circuit-recovery suite.
- [Acceptance evidence](docs/acceptance-evidence.md) — mapping from MVP acceptance criteria to automated or manual evidence.

## Implemented in the MVP

- OpenAI-compatible `GET /v1/models`, `POST /v1/chat/completions`, `POST /v1/completions`, and `POST /v1/responses`.
- High-entropy client keys stored as an HMAC-SHA-256 digest, never as plaintext.
- Client enablement, four priority classes, server-controlled integer vLLM priority, maximum concurrency, and explicit model access.
- Public-to-upstream model rewriting and static multi-backend model pools.
- Independent health and Prometheus scraping with EWMA pressure and hysteretic pool states.
- Priority-aware admission, bounded `429` errors, least-pressure routing, draining exclusion, and one conservative pre-first-byte retry.
- Per-backend inference circuit breakers and per-pool gateway-inflight/waiting safety limits, with explicit inference readiness.
- Soft session affinity through `X-LLM-Session-Id`: rendezvous hashing improves prefix-cache locality while live pressure, health, freshness, drain state, and retry exclusions retain precedence.
- Immediate response streaming, downstream cancellation propagation, Prometheus telemetry, and safe JSON completion logs.
- Basic-authenticated, CSRF-protected JSON Admin API and embedded server-rendered Admin UI.
- Deterministic fake vLLM controls and repeatable mixed-priority load generation.

## Requirements

- Go 1.27 for local builds.
- macOS or Linux for development; the delivery target is static Linux x86-64 (`linux/amd64`).
- A writable directory for SQLite.
- One or more reachable vLLM endpoints, or the bundled fake server.
- `curl` for the examples; `jq` is useful for Admin API and validation commands.

## Quick start (local development)

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
| `LLMGW_SESSION_AFFINITY_MAX_PRESSURE` | `1.0` |
| `LLMGW_CIRCUIT_FAILURE_THRESHOLD` | `5` |
| `LLMGW_CIRCUIT_FAILURE_WINDOW` | `30s` |
| `LLMGW_CIRCUIT_OPEN_COOLDOWN` | `15s` |
| `LLMGW_CIRCUIT_HALF_OPEN_MAX_PROBES` | `1` |
| `LLMGW_SHUTDOWN_GRACE_PERIOD` | `30s` |

Threshold, recovery, dial, TLS, response-header, retry, and routing-epsilon values are also configurable with the `LLMGW_*` variables defined in `internal/config/config.go`. Startup rejects incomplete secrets and inconsistent threshold ordering.

## Usage: bootstrap and first request

The Admin UI is the simplest bootstrap path. Create resources in this order:

1. A model pool with a public client-facing name, the exact upstream vLLM model name, and optional non-negative `MaxGatewayInflight` and `MaxWaiting` limits. Zero means unlimited/disabled.
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
  -d '{"publicModelName":"qwen","upstreamModelName":"Qwen/Qwen2.5-7B-Instruct","enabled":true,"maxGatewayInflight":0,"maxWaiting":0}' \
  http://127.0.0.1:8080/admin/api/pools | jq
```

Every committed mutation increments the SQLite configuration revision, publishes a new registry snapshot, and reconciles backend monitor workers.

After creating the API key, export the one-time secret and verify the client path:

```bash
export LLMGW_CLIENT_KEY='llmgw_copy-the-one-time-value-here'

curl -sS http://127.0.0.1:8080/v1/models \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" | jq

curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" \
  -H 'X-LLM-Session-Id: coding-agent-run-8f0d' \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen","messages":[{"role":"user","content":"Hello"}],"stream":true}'
```

For routine operation, watch `/metrics`, drain a backend before maintenance, and resume it after the backend is healthy. `/readyz` is management-plane readiness and deliberately remains HTTP 200 when inference capacity is zero. `/inference-readyz` is the load-balancer signal: it returns HTTP 200 with `status: "ready"` when at least one pool/backend is eligible for inference, or HTTP 503 with `status: "unavailable"` otherwise. The gateway reloads committed Admin changes without a process restart.

### Circuit breaker, pool safety, and readiness

Each managed backend has a process-local inference circuit. Five qualifying failures inside the rolling 30-second window open it for 15 seconds; after cooldown, one half-open probe may run. A completed response below HTTP 500, including `4xx` and `429`, is a success: it closes and clears a half-open circuit, but in closed state it does not erase retained failures, which age out of the rolling window. Connection, DNS, TLS, response-header, upstream `5xx`, and upstream response-body failures count against it. Downstream cancellation/write failure is neutral. An upstream `5xx` is forwarded and is not retried; only the existing pre-first-byte transport retry remains.

`MaxGatewayInflight` atomically bounds admitted requests across all clients in one model pool. `MaxWaiting` rejects when the latest healthy/fresh, enabled, non-draining aggregate vLLM waiting count is at or above the configured limit. Both return the same bounded `429 gateway_overloaded` envelope before per-client priority can bypass the pool guard. Zero disables the corresponding limit while gateway in-flight telemetry is still counted. These fields are persisted by SQLite schema migration version 2; reopening a current database is a no-op.

Circuit availability contributes to `/inference-readyz`, but transient pool congestion does not: removing a gateway from service cannot create GPU capacity. `/readyz` remains independent so operators retain Admin access during a total inference outage. Circuit and pool leases are in memory and correct only for the documented single gateway replica.

## Admin UI and API

The embedded UI has four screens:

- `/admin`: pool gateway in-flight/waiting/available counts and per-backend health, pressure, and circuit state.
- `/admin/clients`: client policy and model access.
- `/admin/keys`: one-time key generation, expiry/status, last use, and revocation.
- `/admin/backends`: model-pool `MaxGatewayInflight`/`MaxWaiting`, endpoints, enablement, pressure, drain, and resume.

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

### Soft session affinity and KV cache locality

Send the same opaque `X-LLM-Session-Id` value on consecutive requests from one agent or conversation. The gateway rendezvous-hashes the authenticated client ID, model-pool ID, and session ID, so unrelated clients or models do not share an affinity key. The rendezvous winner is computed over backends that are enabled, non-draining, healthy, metrics-fresh, have their configured upstream secret, and are not excluded by a retry. If that winner's pressure is below `LLMGW_SESSION_AFFINITY_MAX_PRESSURE` (default `1.0`), it is preferred; otherwise the request falls back to normal least-pressure routing.

The identifier is trimmed, must be at most 256 bytes, is never logged or used as a metrics label, and is stripped before the request reaches vLLM. Omitting it preserves the original least-pressure behavior. Use stable opaque IDs rather than prompts or user data.

This is locality-aware routing, not a distributed KV block index: the gateway does not inspect cached token blocks, cache hits, eviction, or prefix contents. vLLM KV-cache utilization remains an aggregate pressure input. Rendezvous hashing minimizes remapping when the eligible backend set changes: an ineligible winner rehashes to the next eligible backend, while overload deliberately switches to least-pressure routing. Either case can move a session and warm a different replica.

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

`GET /metrics` exposes the `llmgw_*` request, rejection, in-flight, backend pressure/running/waiting/KV, duration, TTFT, disconnect, backend-failure, and retry families. Resilience gauges are `llmgw_backend_circuit_state` (`closed=0`, `open=1`, `half_open=2`), `llmgw_backend_circuit_failures`, `llmgw_pool_gateway_inflight`, `llmgw_pool_waiting_requests`, and `llmgw_pool_available_backends`. Labels are bounded to configured model/backend names and enums; request IDs, key prefixes, URLs, prompts, and generated text are not labels.

The gateway writes one JSON record per completed inference request to stderr. Records include correlation IDs, client/model policy, selected backend, pressure/state, status, duration, TTFT, disconnect, and retry count. Bodies and authorization headers are never logged.

`GET /healthz` reports process liveness. `GET /readyz` reports database/registry management readiness without becoming unavailable when inference capacity is zero. `GET /inference-readyz` returns HTTP 200/503 for usable inference capacity and includes `revision`, `poolAvailability`, and `backendAvailability`.

## Security

- Client keys contain 32 random bytes after the `llmgw_` prefix. SQLite stores a short lookup prefix and `HMAC-SHA-256(server secret, full key)` only.
- Invalid, revoked, expired, and disabled-client keys share the same public `401` response.
- Admin credentials come only from required environment variables and are compared in constant time.
- State-changing Admin requests require double-submit CSRF protection; cookies are `HttpOnly` and `SameSite=Strict`.
- The gateway strips client authorization and hop-by-hop headers before proxying. An upstream authorization header is constructed only from the named gateway environment variable.
- Session-affinity IDs are bounded, excluded from logs and metric labels, and removed before upstream forwarding.
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

## Deployment

The MVP is a single-replica service. Run exactly one gateway process against its local SQLite state directory. Put client and Admin traffic behind a trusted TLS reverse proxy and keep vLLM backends on a private network.

The reverse proxy must terminate TLS, disable response buffering for streaming routes, use read/write timeouts longer than the longest permitted generation, propagate client disconnects, and avoid retrying inference `POST` requests. Expose only `/v1/*` to client networks. Restrict `/admin/*`, `/metrics`, `/healthz`, `/readyz`, and `/inference-readyz` to an operator, monitoring, or load-balancer network; metrics contain configured client, model, pool, and backend labels.

### Native Linux x86-64

```bash
make build-linux-amd64
file dist/*
```

This produces static `gateway-linux-amd64`, `fake-vllm-linux-amd64`, and `loadgen-linux-amd64` executables. The equivalent direct command is:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o dist/gateway-linux-amd64 ./cmd/gateway
```

Copy the gateway to the Linux host and create a dedicated service account and state directory:

```bash
sudo useradd --system --home-dir /var/lib/llmgw --shell /usr/sbin/nologin llmgw
sudo install -o root -g root -m 0755 dist/gateway-linux-amd64 /usr/local/bin/llmgw
sudo install -d -o llmgw -g llmgw -m 0750 /var/lib/llmgw
sudo install -d -o root -g llmgw -m 0750 /etc/llmgw
```

Generate separate random values for the Admin password and HMAC secret, save them in an approved secret store, then create `/etc/llmgw/llmgw.env`. Never deploy the placeholders below. Protect the file with `root:llmgw` ownership and mode `0640`:

```dotenv
LLMGW_LISTEN_ADDRESS=127.0.0.1:8080
LLMGW_DATABASE_PATH=/var/lib/llmgw/llmgw.db
LLMGW_ADMIN_USERNAME=operator
LLMGW_ADMIN_PASSWORD=replace-with-at-least-16-bytes
LLMGW_API_KEY_HMAC_SECRET=replace-with-at-least-32-random-bytes
LLMGW_SESSION_AFFINITY_MAX_PRESSURE=1.0
```

```bash
sudo chown root:llmgw /etc/llmgw/llmgw.env
sudo chmod 0640 /etc/llmgw/llmgw.env
```

Install `/etc/systemd/system/llmgw.service`:

```ini
[Unit]
Description=Lightweight vLLM Priority Gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=llmgw
Group=llmgw
EnvironmentFile=/etc/llmgw/llmgw.env
ExecStart=/usr/local/bin/llmgw
Restart=on-failure
RestartSec=2s
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
ReadWritePaths=/var/lib/llmgw

[Install]
WantedBy=multi-user.target
```

Start the service and verify process/registry readiness:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now llmgw
sudo systemctl status llmgw
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz | jq
```

After bootstrapping a pool, backend, client, and key, require inference capacity and make an authenticated client-path check before enabling proxy traffic:

```bash
curl -fsS http://127.0.0.1:8080/inference-readyz | jq -e '.status == "ready" and .backendAvailability > 0'
curl -fsS http://127.0.0.1:8080/v1/models \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" | jq
```

### Post-deployment production verification

The two checks above are necessary but not sufficient: `/readyz` deliberately remains HTTP 200 when inference capacity is zero, and `/v1/models` proves authorization/configuration without proving that a model can generate. Before enabling client traffic after a new deployment, restart, configuration change, or restore, run the following from an operator network against the same ingress path clients will use.

Set the public model and gateway URL without placing credentials in shell history or source control:

```bash
export LLMGW_VERIFY_URL='https://gateway.example.internal'
export LLMGW_VERIFY_MODEL='qwen'
export LLMGW_VERIFY_EXPECTED_BACKENDS=2
export LLMGW_VERIFY_REQUEST_ID="deployment-verify-$(date -u +%Y%m%dT%H%M%SZ)-$$"
# Inject LLMGW_ADMIN_USERNAME, LLMGW_ADMIN_PASSWORD, and LLMGW_CLIENT_KEY
# from the deployment secret mechanism.
```

1. Verify liveness, management readiness, a loaded registry revision, and non-zero inference capacity:

```bash
curl -fsS "$LLMGW_VERIFY_URL/healthz" | jq -e '.status == "alive"'
curl -fsS "$LLMGW_VERIFY_URL/readyz" | jq -e '.status == "ready" and .revision > 0'
curl -fsS "$LLMGW_VERIFY_URL/inference-readyz" \
  | jq -e --argjson expected "$LLMGW_VERIFY_EXPECTED_BACKENDS" \
      '.status == "ready" and .revision > 0 and .poolAvailability > 0 and .backendAvailability >= $expected'
```

2. Verify that enabled, non-draining backends are healthy and their vLLM metrics are fresh:

The manual command prompts for the Admin password so it is not exposed in the process argument list. Do not append the password to `--user`.

```bash
curl -fsS --user "$LLMGW_ADMIN_USERNAME" \
  "$LLMGW_VERIFY_URL/admin/api/status" \
  | jq -e --argjson expected "$LLMGW_VERIFY_EXPECTED_BACKENDS" '
      [.backends[] | select(.enabled and (.draining | not))] as $eligible
      | ($eligible | length) >= $expected
        and all($eligible[]; .runtime.Healthy and .runtime.MetricsFresh)
        and any(.pools[]; .enabled and .runtime.AvailableBackends >= $expected)'
```

3. Verify model visibility and one complete streaming inference. Consuming the entire stream confirms the first-byte path, continued streaming, and the terminal `[DONE]` event:

```bash
curl -fsS "$LLMGW_VERIFY_URL/v1/models" \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" \
  | jq -e --arg model "$LLMGW_VERIFY_MODEL" 'any(.data[]; .id == $model)'

curl -fsS -N "$LLMGW_VERIFY_URL/v1/completions" \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" \
  -H "X-Request-Id: $LLMGW_VERIFY_REQUEST_ID" \
  -H 'Content-Type: application/json' \
  -d "{\"model\":\"$LLMGW_VERIFY_MODEL\",\"prompt\":\"Production verification.\",\"max_tokens\":4,\"stream\":true}" \
  | awk '{ sub(/\r$/, ""); if ($0 == "data: [DONE]") done=1 } END { exit(done ? 0 : 1) }'
```

4. Verify telemetry and correlate the probe with a successful completion log:

```bash
(
  set -e
  LLMGW_VERIFY_METRICS="$(curl -fsS "$LLMGW_VERIFY_URL/metrics")"
  for family in llmgw_requests_total llmgw_backend_pressure llmgw_backend_running_requests \
    llmgw_backend_circuit_state llmgw_backend_circuit_failures llmgw_pool_gateway_inflight \
    llmgw_pool_waiting_requests llmgw_pool_available_backends; do
    printf '%s\n' "$LLMGW_VERIFY_METRICS" | grep -qE "^(# HELP )?$family([ {])"
  done

  ssh gateway-host "sudo journalctl -u llmgw --since='-5 minutes' -o cat" \
    | jq -Rse --arg request_id "$LLMGW_VERIFY_REQUEST_ID" '
      [split("\n")[] | fromjson?
       | select(.msg == "request completed"
           and .parentRequestId == $request_id
           and .status == 200
           and (.backend // "") != "")]
      | if length > 0 then .
        else error("no successful completion log for the verification request")
        end'
)
```

The example reads logs over SSH from the systemd gateway host. If the shell is already on that host, replace the `ssh ...` command with `sudo journalctl -u llmgw --since '-5 minutes' -o cat`. With centralized logging, query the same exact `parentRequestId`, require `status=200`, and require a non-empty `backend` field. The `jq` expression exits non-zero if no matching completion exists.

For a repeatable assertion-based version, run the safe E2E smoke test from an operator workstation:

```bash
LLMGW_E2E_MODE=smoke \
LLMGW_E2E_GATEWAY_URL="$LLMGW_VERIFY_URL" \
LLMGW_E2E_ADMIN_USERNAME="$LLMGW_ADMIN_USERNAME" \
LLMGW_E2E_ADMIN_PASSWORD="$LLMGW_ADMIN_PASSWORD" \
LLMGW_E2E_MODEL="$LLMGW_VERIFY_MODEL" \
LLMGW_E2E_HIGH_KEY="$LLMGW_CLIENT_KEY" \
LLMGW_E2E_EXPECTED_BACKENDS="$LLMGW_VERIFY_EXPECTED_BACKENDS" \
go test -count=1 -v -timeout 2m ./tests/e2e -run TestProductionSmoke
```

Pass criteria: all commands exit zero; Admin status shows the expected healthy/fresh capacity; the stream reaches `[DONE]`; Prometheus exposes gateway/backend series; and the matching completion log records status `200` and a selected backend. Do not run the intentional saturation test against live production traffic. Follow [the real-vLLM priority E2E runbook](docs/real-vllm-priority-e2e.md) only in an isolated environment or an approved capacity-test window.

SQLite runs in WAL mode. Do not copy only `llmgw.db` while the gateway is running: committed data may still be in `llmgw.db-wal`. Use a quiesced backup of the complete state directory, for example:

```bash
sudo systemctl stop llmgw
sudo tar -C /var/lib/llmgw -czf /secure-backups/llmgw-state.tgz .
sudo systemctl start llmgw
```

Keep `/etc/llmgw/llmgw.env` out of source control and escrow it securely with the state backup. Losing or changing `LLMGW_API_KEY_HMAC_SECRET` immediately invalidates every existing client key; the MVP has no dual-secret migration path. HMAC-secret rotation therefore requires a planned cutover that regenerates and redistributes every client key. Test restore into an isolated instance and verify Admin login, configuration, `backendAvailability`, and an authenticated `/v1/models` request before relying on a backup.

### Docker

```bash
umask 077
export LLMGW_ADMIN_PASSWORD="$(openssl rand -base64 24)"
export LLMGW_API_KEY_HMAC_SECRET="$(openssl rand -base64 48)"
# Persist both values in an approved secret store before starting the container.

docker build --platform linux/amd64 -t vllm-priority-gateway:local .
docker volume create llmgw-data
docker run -d --name llmgw --restart unless-stopped \
  -p 127.0.0.1:8080:8080 -v llmgw-data:/data \
  -e LLMGW_ADMIN_USERNAME=operator \
  -e LLMGW_ADMIN_PASSWORD \
  -e LLMGW_API_KEY_HMAC_SECRET \
  vllm-priority-gateway:local

curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz | jq
```

The runtime image is `scratch`, contains only CA certificates and the static gateway, and runs as numeric UID/GID `65532`.
The image initializes `/data` with that ownership, so a fresh named or anonymous volume is writable. For a Linux bind mount, create the directory and grant UID/GID `65532` write access before starting the container; Docker Desktop for macOS applies its own host-file sharing rules.
Treat `llmgw-data` and the exact HMAC secret as one recoverable state set. Stop the container before snapshotting or restoring the complete volume, restart it afterward, and perform the same restore verification described for the native deployment. Privileged Docker users can inspect container environment variables; use the deployment platform's secret injection mechanism when stronger isolation is required.

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
make build-e2e-linux-amd64
make container-smoke  # requires a running Docker daemon
```

Tests cover domain validation, key security, SQLite migrations and CRUD, registry snapshots, pressure/EWMA/hysteresis, admission, least-pressure and rendezvous routing, session-affinity fallback/exclusion/header handling, monitoring, fake-vLLM controls, proxy streaming/cancellation/retry, public and Admin HTTP contracts, UI semantics, telemetry, process lifecycle, load statistics, and black-box acceptance behavior.

## Real-GPU validation

Run `make test-real-vllm` with `LLMGW_E2E_MODE=smoke`, `priority`, or `resilience` and the environment documented in [`docs/real-vllm-priority-e2e.md`](docs/real-vllm-priority-e2e.md). The modes cover production-safe inference readiness/streaming, intentional priority and pool-safety saturation, and isolated real-vLLM circuit recovery respectively. Follow [`docs/real-gpu-testing.md`](docs/real-gpu-testing.md) for broader compatibility, cancellation, routing, and hardware-calibration evidence. Fake-backend tests prove gateway mechanics but cannot prove the scheduler behavior of a selected real model/hardware combination.

## Not implemented in the MVP

- One gateway replica; concurrency leases and backend runtime state are process-local.
- Operator-managed backend registration and SQLite persistence only; there is no automatic service discovery.
- No distributed rate limits, token budgets, billing, autoscaling, discovery, KV-block/prefix-index routing, or GPU/NVML scheduling. Circuit and pool-safety state is process-local; soft session affinity is implemented without inspecting cache contents.
- Priority admission rejects new lower-priority requests; it does not preempt an already admitted generation.
- Basic auth is the MVP management credential. TLS, OIDC/RBAC, audit trails, and a secret manager are external or future responsibilities.
- Capacity hints are persisted for forward compatibility but do not yet weight routing.
- Automated fake-backend acceptance and the opt-in real-vLLM test harness are complete, but the real-vLLM suite and threshold calibration must still pass on the selected production model/GPU combination before production use.

## Remaining work for Production V1

The package boundaries allow SQLite to be replaced with PostgreSQL, local leases with a distributed store, and static backend configuration with discovery. Production V1 still needs:

- Real-GPU validation and calibrated pressure/admission thresholds for the selected models and hardware.
- Multi-replica gateway HA with coordinated configuration revisions and distributed concurrency/rate/token budgets.
- PostgreSQL for durable configuration and a defined degraded-mode policy for the distributed coordination store.
- Orchestrated drain-aware rolling shutdown, capacity-weighted routing, and optional KV-block/prefix-aware routing beyond the current soft affinity.
- OIDC-based Admin authentication, RBAC, audit logging, secret-manager integration, and managed TLS/network policy.
- OpenTelemetry traces, production dashboards and alerts, configuration migration/versioning, and Kubernetes deployment/discovery where required.
- Load, failure, upgrade, backup/restore, and security validation against the Production V1 acceptance criteria in the technical specification.
