[English](local-demo.md) | [Русский](local-demo.ru.md)

# Local demo with two real vLLM instances

This reproducible demo runs a released vLLM Priority Gateway artifact against two real `vllm/vllm-openai:v0.28.0` services serving `Qwen/Qwen3-0.6B`. Choose either the GHCR image or a checksum-verified Linux release binary. The gateway is not built from the checkout.

The tested defaults run both small vLLM processes on one NVIDIA GPU with at least 12 GiB VRAM. This validates routing, priority, overload behavior, metrics, release artifacts, and the Admin UI; it is not a recommended production topology or a production latency calibration.

## Prerequisites

- Docker Engine or Docker Desktop using Linux containers.
- Docker Compose 2.24.4 or newer.
- An NVIDIA GPU visible to Docker with [Compose GPU reservations](https://docs.docker.com/compose/how-tos/gpu-support/).
- Approximately 25 GiB free disk space for the vLLM image, model weights, and caches.
- Access to Hugging Face for the public Qwen model. `HF_TOKEN` is optional.
- For the native Linux path: Linux `amd64` or `arm64`, `curl`, `tar`, and GNU `sha256sum`.

## 1. Prepare secrets and validate Docker

Clone the repository and create `.env` from [`.env.example`](../.env.example). Fill both blank secrets with independently generated random values:

```dotenv
LLMGW_VERSION=0.2.0
LLMGW_ADMIN_USERNAME=operator
LLMGW_ADMIN_PASSWORD=replace-with-at-least-16-random-bytes
LLMGW_API_KEY_HMAC_SECRET=replace-with-at-least-32-random-bytes
LLMGW_PORT=8080
```

`LLMGW_VERSION` is the image/archive version without the leading `v`. Keep `.env` readable only by the operator account and never commit it. The remaining defaults pin the tested vLLM image, model, memory fraction, and compatibility runner.

Validate the configuration and GPU:

```console
docker compose config --quiet
docker compose run --rm --no-deps --entrypoint nvidia-smi vllm-a
```

The first command must finish silently. The second must show the intended NVIDIA GPU from inside the pinned vLLM container.

## 2. Choose one gateway artifact

Do not run both options simultaneously: both listen on `127.0.0.1:8080`. Their SQLite state is independent.

### Option A: GHCR image

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml config --quiet
docker compose --env-file .env -f compose.yaml -f compose.release.yaml pull gateway
docker compose --env-file .env -f compose.yaml -f compose.release.yaml up -d --no-build --wait --wait-timeout 900
docker compose --env-file .env -f compose.yaml -f compose.release.yaml ps
```

`compose.release.yaml` selects `ghcr.io/rislanov/vllm-priority-gateway:${LLMGW_VERSION}` and removes the local gateway build. On a cold host, the image pull and first model initialization can take several minutes.

The `probe` service provides curl inside the private Compose network:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/healthz
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/readyz
```

`/readyz` checks the management plane and deliberately remains healthy before a model pool is configured.

### Option B: checksum-verified native Linux binary

Start only the two inference servers. `compose.native.yaml` publishes their ports on host loopback:

```console
docker compose --env-file .env -f compose.yaml -f compose.native.yaml config --quiet
docker compose --env-file .env -f compose.yaml -f compose.native.yaml up -d --wait --wait-timeout 900 vllm-a vllm-b
docker compose --env-file .env -f compose.yaml -f compose.native.yaml ps
```

Download the archive for the current architecture and verify it before extraction:

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

The checksum command must print `./$ASSET: OK`. Start the gateway in the foreground and keep the terminal open:

```bash
install -d -m 0750 "$PWD/data/release-linux"
set -a
. ./.env
set +a
export LLMGW_LISTEN_ADDRESS=127.0.0.1:8080
export LLMGW_DATABASE_PATH="$PWD/data/release-linux/llmgw.db"
"$RELEASE_DIR/gateway"
```

## 3. Configure the gateway

Open `http://127.0.0.1:8080/admin` and authenticate with the Admin credentials from `.env`.

Create resources in this order:

1. **Backends → Create model pool**
   - Public model name: `qwen`
   - Upstream model name: `qwen-test`
   - Max gateway inflight: `32`
   - Max waiting: `8`
   - Enabled: checked
2. Create two backends in the pool:
   - GHCR gateway: `vllm-a` → `http://vllm-a:8000`; `vllm-b` → `http://vllm-b:8000`
   - Native gateway: `vllm-a` → `http://127.0.0.1:8001`; `vllm-b` → `http://127.0.0.1:8002`
   - Capacity hint: `1`; running soft limit: `1`; enabled: checked
3. **Clients → Create client**
   - Name: `production-app`
   - Priority class: `normal`
   - vLLM priority: `0`
   - Max concurrency: `8`
   - Model access: `qwen`
4. **API Keys → Generate API key**
   - Select `production-app`
   - Copy the complete `llmgw_*` secret immediately; it is displayed once.

## 4. Verify real inference

Wait for two healthy and metrics-fresh backends. For the GHCR path:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/inference-readyz
```

For native Linux:

```console
curl -fsS http://127.0.0.1:8080/inference-readyz
```

Continue when the response reports `"backendAvailability":2`. Replace the placeholder with the one-time client key and list models:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/v1/models -H "Authorization: Bearer llmgw_replace-with-the-generated-key"
```

Send the checked-in streaming request:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS -N http://gateway:8080/v1/chat/completions -H "Authorization: Bearer llmgw_replace-with-the-generated-key" -H "Content-Type: application/json" --data-binary "@/requests/chat.json"
```

For native Linux:

```bash
export LLMGW_CLIENT_KEY='llmgw_replace-with-the-generated-key'
curl -fsS -N http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" \
  -H "Content-Type: application/json" \
  --data-binary "@examples/quickstart-chat.json"
```

The stream must end with `data: [DONE]`. The gateway authenticates the public model `qwen`, applies stored priority, rewrites it to `qwen-test`, and selects one healthy vLLM service.

## 5. Optional decision telemetry

Add the observability overlay to provision Prometheus and Grafana:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml -f compose.observability.yaml up -d --no-build --wait --wait-timeout 900
```

Open Grafana at `http://127.0.0.1:3000/d/llmgw-gateway-decisions`. Local credentials default to `admin` / `admin`; override them before any non-loopback use. The dashboard distinguishes admission from latency: it shows whether High traffic remains admitted without claiming saturated GPU latency stays unchanged.

## 6. Restart and cleanup

Restart only the gateway and verify that Admin configuration and inference readiness return:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml restart gateway
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/inference-readyz
```

For the GHCR path:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml logs -f gateway
docker compose --env-file .env -f compose.yaml -f compose.release.yaml down
```

For native Linux, stop the foreground gateway with `Ctrl-C`, then run:

```console
docker compose --env-file .env -f compose.yaml -f compose.native.yaml down
```

The native SQLite file remains under `data/release-linux`; Docker volumes retain gateway state and the model cache. Add `--volumes` only when intentionally deleting them.

## Evidence and production use

The recorded RTX 4070 Ti results are in [acceptance-evidence.md](acceptance-evidence.md). For broader compatibility, cancellation, priority, resilience, and retained-artifact procedures, use [real-gpu-testing.md](real-gpu-testing.md) and [real-vllm-priority-e2e.md](real-vllm-priority-e2e.md).

This demo is a development-sized validation environment. Read [deployment.md](deployment.md) and [operations.md](operations.md) before exposing the gateway or using a production model.
