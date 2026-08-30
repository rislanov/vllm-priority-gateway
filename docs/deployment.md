# Production deployment

This guide contains the detailed deployment and post-deployment procedures for Lightweight vLLM Priority Gateway. For the tested first run with two real vLLM servers, start with the canonical [release quick start](../README.md#release-quick-start).

## Supported topology

The current release is a single-replica service:

```text
client network -> TLS reverse proxy -> one gateway process -> private vLLM endpoints
                                         |
                                         +-> local SQLite state directory
```

Run exactly one gateway process against one SQLite state directory. Admission leases, circuit state, backend pressure, and in-flight counters are process-local and are not coordinated between replicas.

The reverse proxy must:

- terminate TLS;
- disable response buffering for streaming routes;
- use read and write timeouts longer than the longest permitted generation;
- propagate client disconnects;
- never retry inference `POST` requests;
- expose `/v1/*` to client networks;
- restrict `/admin/*`, `/metrics`, `/healthz`, `/readyz`, and `/inference-readyz` to operator, monitoring, and load-balancer networks.

## Docker deployment

Use the repository's canonical [`compose.yaml`](../compose.yaml) with [`compose.release.yaml`](../compose.release.yaml) for release deployment. This tested topology works with Linux Docker Engine and Docker Desktop: Compose pulls the selected gateway image from GHCR, starts two pinned vLLM services, creates their private DNS/network, and owns the SQLite and model-cache volumes. Docker Compose 2.24.4 or newer is required for the override file's [reset merge tag](https://docs.docker.com/reference/compose-file/merge/#reset-value).

Create `.env` from [`.env.example`](../.env.example), replace both blank secrets, and escrow them before starting the stack. The HMAC secret is part of durable credential state: changing or losing it invalidates every existing client key.

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml config --quiet
docker compose --env-file .env -f compose.yaml -f compose.release.yaml pull gateway
docker compose --env-file .env -f compose.yaml -f compose.release.yaml up -d --no-build --wait --wait-timeout 900
docker compose --env-file .env -f compose.yaml -f compose.release.yaml ps
```

The gateway runtime image is `scratch`, contains only CA certificates and the static binary, runs as numeric UID/GID `65532`, and stores SQLite under `/data`. The named `gateway-data` volume is initialized with the correct ownership. The Compose stack publishes only `127.0.0.1:8080`; vLLM remains private on the Compose-managed network at `http://vllm-a:8000` and `http://vllm-b:8000`.

For an externally managed vLLM pool, replace the Compose vLLM services with private DNS endpoints reachable from the gateway service. Do not publish unauthenticated vLLM ports to client networks.

If vLLM requires an upstream API key, add its value to the deployment secret mechanism, map that variable into the gateway service's `environment`, and store only the variable name in the backend Admin record. Do not put the upstream secret itself in SQLite or in the Admin form.

Verify the image, fresh-volume, non-root, SQLite, and health path separately with a running Docker daemon:

```bash
make container-smoke
```

## Native Linux x86-64 deployment

Build the static binaries:

```bash
make build-linux-amd64
file dist/*
```

Install the gateway and create a dedicated service account and state directory:

```bash
sudo useradd --system --home-dir /var/lib/llmgw --shell /usr/sbin/nologin llmgw
sudo install -o root -g root -m 0755 dist/gateway-linux-amd64 /usr/local/bin/llmgw
sudo install -d -o llmgw -g llmgw -m 0750 /var/lib/llmgw
sudo install -d -o root -g llmgw -m 0750 /etc/llmgw
```

Create `/etc/llmgw/llmgw.env` from values supplied by the deployment secret mechanism. Never deploy the placeholders below:

```dotenv
LLMGW_LISTEN_ADDRESS=127.0.0.1:8080
LLMGW_DATABASE_PATH=/var/lib/llmgw/llmgw.db
LLMGW_ADMIN_USERNAME=operator
LLMGW_ADMIN_PASSWORD=replace-with-at-least-16-bytes
LLMGW_API_KEY_HMAC_SECRET=replace-with-at-least-32-random-bytes
LLMGW_SESSION_AFFINITY_MAX_PRESSURE=1.0
LLMGW_ANALYTICS_RETENTION=2160h
```

Protect the environment file:

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

Validate the installed unit before enabling it:

```bash
sudo systemd-analyze verify /etc/systemd/system/llmgw.service
```

Start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now llmgw
sudo systemctl status llmgw
```

## Configure gateway state

A fresh database starts with revision `0`. At that point `/healthz` and management readiness work, but `/inference-readyz` correctly remains HTTP 503 until an enabled model pool and at least one eligible backend have been created. A usable inference request additionally requires a client and its API key.

Backends may run on the gateway host or on remote serving hosts. Before registering them, verify every upstream from the gateway host itself, using the same addresses that will be stored in Admin:

```bash
export VLLM_A_URL='http://inference-a.internal:8000'
export VLLM_B_URL='http://inference-b.internal:8000'

for upstream in "$VLLM_A_URL" "$VLLM_B_URL"; do
  curl -fsS "$upstream/health" >/dev/null
  curl -fsS "$upstream/v1/models" | jq -e '.data | length > 0'
  curl -fsS "$upstream/metrics" \
    | grep -E 'vllm:(num_requests_running|num_requests_waiting|kv_cache_usage_perc)' >/dev/null
done
```

Add the configured upstream authorization header to these probes when vLLM API-key authentication is enabled.

Restrict remote vLLM ingress to the gateway hosts at the network boundary; do not publish unauthenticated serving ports broadly. Then use the Admin UI through the intended authenticated ingress, or an SSH tunnel to the loopback listener, to create:

1. an enabled public model pool mapped to the exact upstream served-model name;
2. every backend with its gateway-reachable base URL and an appropriate `runningSoftLimit`;
3. each client with its allowed pool, class, vLLM priority, and concurrency limit;
4. a distinct API key for each client, storing the returned secret when it is shown once.

The Admin UI performs CSRF handling automatically. Scripts using the JSON Admin API must first make an authenticated GET to receive the `llmgw_csrf` cookie, then send both that cookie and the matching `X-CSRF-Token` header on every state-changing request. Basic authentication alone is intentionally rejected with HTTP 403. This example creates one model pool; apply the same session pattern to backend, client, and key requests:

```bash
export LLMGW_ADMIN_URL='http://127.0.0.1:8080'
export LLMGW_ADMIN_USERNAME='operator'

admin_cookie_jar=$(mktemp)
trap 'rm -f "$admin_cookie_jar"' EXIT

curl -fsS --user "$LLMGW_ADMIN_USERNAME" \
  -c "$admin_cookie_jar" \
  "$LLMGW_ADMIN_URL/admin/api/status" >/dev/null

csrf_token=$(awk '$6 == "llmgw_csrf" { print $7 }' "$admin_cookie_jar")
test -n "$csrf_token"

curl -fsS --user "$LLMGW_ADMIN_USERNAME" \
  -b "$admin_cookie_jar" \
  -H "X-CSRF-Token: $csrf_token" \
  -H 'Content-Type: application/json' \
  -d '{"publicModelName":"qwen","upstreamModelName":"qwen-test","enabled":true,"maxGatewayInflight":0,"maxWaiting":0}' \
  "$LLMGW_ADMIN_URL/admin/api/pools" | jq .
```

Do not continue to deployment verification until Admin shows the intended inventory and every enabled, non-draining backend is healthy with fresh metrics.

## Post-deployment verification

Run the checks from the operator network through the same ingress path that clients will use.

```bash
export LLMGW_VERIFY_URL='https://gateway.example.internal'
export LLMGW_VERIFY_MODEL='qwen'
export LLMGW_VERIFY_EXPECTED_BACKENDS=2
export LLMGW_VERIFY_REQUEST_ID="deployment-verify-$(date -u +%Y%m%dT%H%M%SZ)-$$"
# Inject LLMGW_ADMIN_USERNAME, LLMGW_ADMIN_PASSWORD, and LLMGW_CLIENT_KEY
# from the deployment secret mechanism.
```

Verify liveness, loaded configuration, and inference capacity:

```bash
curl -fsS "$LLMGW_VERIFY_URL/healthz" | jq -e '.status == "alive"'
curl -fsS "$LLMGW_VERIFY_URL/readyz" | jq -e '.status == "ready" and .revision > 0'
curl -fsS "$LLMGW_VERIFY_URL/inference-readyz" \
  | jq -e --argjson expected "$LLMGW_VERIFY_EXPECTED_BACKENDS" \
      '.status == "ready" and .revision > 0 and .poolAvailability > 0 and .backendAvailability >= $expected'
```

Verify that enabled, non-draining backends are healthy and their vLLM metrics are fresh. Omit the Admin password from the command line so `curl` prompts for it:

```bash
curl -fsS --user "$LLMGW_ADMIN_USERNAME" \
  "$LLMGW_VERIFY_URL/admin/api/status" \
  | jq -e --argjson expected "$LLMGW_VERIFY_EXPECTED_BACKENDS" '
      [.backends[] | select(.enabled and (.draining | not))] as $eligible
      | ($eligible | length) >= $expected
        and all($eligible[]; .runtime.Healthy and .runtime.MetricsFresh)
        and any(.pools[]; .enabled and .runtime.AvailableBackends >= $expected)'
```

Verify model visibility and consume one complete streaming generation:

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

Finally, confirm that `/metrics` exposes the expected `llmgw_*` families and that the completion log contains the exact `parentRequestId`, status `200`, and a non-empty backend. For an assertion-based production-safe smoke test:

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

Do not run the intentional saturation modes against live production traffic. See the [real-vLLM E2E runbook](real-vllm-priority-e2e.md) for isolated capacity and resilience testing.

## Backup and restore

SQLite runs in WAL mode. Do not copy only `llmgw.db` while the gateway is running: committed data may still be in `llmgw.db-wal`. Back up the complete, quiesced state directory:

```bash
sudo systemctl stop llmgw
sudo tar -C /var/lib/llmgw -czf /secure-backups/llmgw-state.tgz .
sudo systemctl start llmgw
```

For Docker, stop the container before snapshotting or restoring the complete `llmgw-data` volume, then restart it and repeat the post-deployment verification.

Escrow the exact `LLMGW_API_KEY_HMAC_SECRET` with the state backup. The current release has no dual-secret migration path; rotating this secret requires regenerating and redistributing every client key. Test restores in an isolated instance before relying on a backup.
