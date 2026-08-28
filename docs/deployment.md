# Production deployment

This guide contains the detailed deployment and post-deployment procedures for Lightweight vLLM Priority Gateway. For the shortest path from a running vLLM server to the first inference request, start with the [production quick start](../README.md#production-quick-start).

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

Generate and escrow the administrator password and HMAC secret before starting the container. The HMAC secret is part of the durable credential state: changing or losing it invalidates every existing client key.

```bash
umask 077
export LLMGW_ADMIN_PASSWORD="$(openssl rand -base64 24)"
export LLMGW_API_KEY_HMAC_SECRET="$(openssl rand -base64 48)"
# Persist both values in the approved secret store now.

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

The runtime image is `scratch`, contains only CA certificates and the static gateway, runs as numeric UID/GID `65532`, and stores SQLite state under `/data`. A fresh named volume is initialized with the correct ownership. For a Linux bind mount, create the directory and grant UID/GID `65532` write access before starting the container.

The backend URL entered in Admin must be reachable **from the gateway container**. `127.0.0.1` inside the container refers to the gateway container itself, not to a vLLM process on the Docker host. Prefer private DNS or a shared container network, for example `http://vllm.internal:8000`.

If vLLM requires an upstream API key, store only its environment-variable name in the backend record and inject the value into the gateway container:

```bash
export VLLM_GPU_A_KEY='secret-from-your-secret-store'
docker run ... -e VLLM_GPU_A_KEY ...
```

Do not put the upstream secret itself in SQLite or in the Admin form.

Verify the image, fresh-volume, non-root, SQLite, and health path with a running Docker daemon:

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

Start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now llmgw
sudo systemctl status llmgw
```

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
