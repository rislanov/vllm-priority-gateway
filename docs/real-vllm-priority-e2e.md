# Real-vLLM E2E Test

This runbook reproduces the black-box tests in `tests/e2e` against a deployed gateway and real vLLM servers. The suite has exactly three modes:

- `smoke` is safe for a production verification window. It checks management and inference readiness separately, Admin pool/backend counts, healthy and metrics-fresh backends, model visibility, one complete short stream, and required Prometheus families including all five circuit/pool gauges.
- `priority` intentionally saturates inference capacity. It observes non-zero gateway in-flight and upstream waiting work, proves background shedding and High/Critical priority continuity, then temporarily sets a pool-wide in-flight limit and proves that even Critical receives the bounded `429 gateway_overloaded`. It restores the limit and proves Critical streaming continuity again.
- `resilience` is destructive and isolated. It places a loopback fault proxy in front of one real backend, drains its siblings, proves inference-only `503` failures open the circuit while health/metrics remain fresh, then observes half-open streaming recovery and closure. Run it only against a gateway on the same host and with no production traffic.

The test is opt-in. Ordinary `go test ./...` compiles it and reports it as skipped unless `LLMGW_E2E_MODE` is set.

## Reference environment

The reference run used:

- Apple M4 with macOS;
- vLLM `0.28.0` and vLLM-Metal `0.3.0.dev`;
- two local `Qwen/Qwen3-0.6B` serving processes;
- `--max-num-seqs 1`, so each process had one running request while excess High work waited;
- four High load clients with four requests each;
- three independent Background clients, a separate High probe client, and a Critical probe client.

The captured saturated state had one running and seven waiting requests per backend. KV usage remained low because the model and prompts were small; `AllBackendsWaiting=true` was sufficient to drive the pool into `saturated`.

## 1. Start two real vLLM nodes

### Apple Silicon

Install vLLM-Metal using its [official installation instructions](https://github.com/vllm-project/vllm-metal/blob/main/docs/installation.md). Its default installer creates `~/.venv-vllm-metal` and requires native arm64 Python on Apple Silicon.

```bash
curl -fsSL https://raw.githubusercontent.com/vllm-project/vllm-metal/main/install.sh | bash
source "$HOME/.venv-vllm-metal/bin/activate"
```

Start two deliberately small-capacity nodes in separate terminals. The `0.18` memory fraction worked for the reference M4 run; adjust it for the available unified memory.

```bash
VLLM_HOST_IP=127.0.0.1 \
VLLM_METAL_MEMORY_FRACTION=0.18 \
VLLM_ENABLE_V1_MULTIPROCESSING=0 \
vllm serve Qwen/Qwen3-0.6B \
  --host 127.0.0.1 --port 8001 \
  --max-model-len 1024 --max-num-seqs 1 \
  --scheduling-policy priority \
  --served-model-name qwen-test \
  --generation-config vllm
```

```bash
VLLM_HOST_IP=127.0.0.1 \
VLLM_METAL_MEMORY_FRACTION=0.18 \
VLLM_ENABLE_V1_MULTIPROCESSING=0 \
vllm serve Qwen/Qwen3-0.6B \
  --host 127.0.0.1 --port 8002 \
  --max-model-len 1024 --max-num-seqs 1 \
  --scheduling-policy priority \
  --served-model-name qwen-test \
  --generation-config vllm
```

### Linux with NVIDIA GPUs

Use the vLLM installation appropriate for the CUDA host, then start one serving process per GPU or independently scheduled serving group:

```bash
CUDA_VISIBLE_DEVICES=0 vllm serve Qwen/Qwen3-0.6B \
  --host 0.0.0.0 --port 8001 \
  --max-model-len 1024 --max-num-seqs 1 \
  --scheduling-policy priority \
  --served-model-name qwen-test \
  --generation-config vllm

CUDA_VISIBLE_DEVICES=1 vllm serve Qwen/Qwen3-0.6B \
  --host 0.0.0.0 --port 8002 \
  --max-model-len 1024 --max-num-seqs 1 \
  --scheduling-policy priority \
  --served-model-name qwen-test \
  --generation-config vllm
```

Verify both nodes before configuring the gateway:

```bash
curl -fsS http://127.0.0.1:8001/health
curl -fsS http://127.0.0.1:8002/health
curl -fsS http://127.0.0.1:8001/metrics \
  | grep -E 'vllm:(num_requests_running|num_requests_waiting|kv_cache_usage_perc)'
```

## 2. Configure the gateway test identities

Create one public pool named `qwen` with upstream model `qwen-test`, then register both backends with `runningSoftLimit=1`.

Create these clients and one key for each client:

| Client | Class | vLLM priority | Maximum concurrency |
|---|---|---:|---:|
| `high-load-1` | High | `-10` | `4` |
| `high-load-2` | High | `-9` | `4` |
| `high-load-3` | High | `-8` | `4` |
| `high-load-4` | High | `-7` | `4` |
| `high-probe` | High | `-100` | `4` |
| `critical-probe` | Critical | `-200` | `4` |
| `low-1` | Background | `100` | `4` |
| `low-2` | Background | `101` | `4` |
| `low-3` | Background | `102` | `4` |

Every row must be a separate configured client with a distinct API key. In particular, the High and Critical probes must not reuse a High load key: otherwise a per-client concurrency lease may be exhausted by the load itself and the test would exercise the wrong rejection path. The test rejects duplicate key values without printing the secrets.

## 3. Run the production-safe smoke check

Run from an operator workstation that can reach the public API, Admin API, and metrics endpoint:

```bash
export LLMGW_E2E_MODE=smoke
export LLMGW_E2E_GATEWAY_URL='https://gateway.example.internal'
export LLMGW_E2E_ADMIN_USERNAME='operator'
export LLMGW_E2E_ADMIN_PASSWORD='read-from-the-secret-store'
export LLMGW_E2E_MODEL='qwen'
export LLMGW_E2E_HIGH_KEY='llmgw_high-probe-key'
export LLMGW_E2E_EXPECTED_BACKENDS=2

go test -count=1 -v -timeout 2m ./tests/e2e -run TestProductionSmoke
```

Expected result:

```text
--- PASS: TestProductionSmoke
PASS
```

This mode sends one four-token streaming completion. It does not saturate capacity or mutate Admin state. `/readyz` must remain HTTP 200 management readiness and `/inference-readyz` must be HTTP 200 with `status: "ready"`, at least one available pool, and the expected backend count.

## 4. Run priority isolation and recovery

Export the load and probe identities. Shell history and CI logs may retain exported values, so inject real credentials using the deployment secret mechanism rather than committing them to a file.

```bash
export LLMGW_E2E_MODE=priority
export LLMGW_E2E_GATEWAY_URL='http://127.0.0.1:8080'
export LLMGW_E2E_ADMIN_USERNAME='operator'
export LLMGW_E2E_ADMIN_PASSWORD='read-from-the-secret-store'
export LLMGW_E2E_MODEL='qwen'
export LLMGW_E2E_HIGH_KEY="$HIGH_PROBE_KEY"
export LLMGW_E2E_HIGH_LOAD_KEYS="$HIGH_LOAD_1_KEY,$HIGH_LOAD_2_KEY,$HIGH_LOAD_3_KEY,$HIGH_LOAD_4_KEY"
export LLMGW_E2E_CRITICAL_KEY="$CRITICAL_PROBE_KEY"
export LLMGW_E2E_LOW_KEYS="$LOW_1_KEY,$LOW_2_KEY,$LOW_3_KEY"
export LLMGW_E2E_EXPECTED_BACKENDS=2
export LLMGW_E2E_HIGH_REQUESTS_PER_KEY=4
export LLMGW_E2E_HIGH_MAX_TOKENS=768
export LLMGW_E2E_SATURATION_TIMEOUT=60s
export LLMGW_E2E_RECOVERY_TIMEOUT=45s
export LLMGW_E2E_PROBE_TIMEOUT=60s

go test -count=1 -v -timeout 10m ./tests/e2e
```

The priority test passes only when all of the following are observed:

1. High streaming requests drive the selected pool into `saturated` or `emergency`, with `GatewayInflight > 0` and `TotalWaiting > 0` in Admin runtime.
2. By default, `AllBackendsWaiting` is true.
3. Ordinary Low, Low with a spoofed body/header priority, and Low with a session-affinity header all receive `429`, a positive `Retry-After`, and `gateway_overloaded`.
4. Separate High and Critical probes receive `200` while the pool is saturated.
5. A temporary positive `MaxGatewayInflight` at or below observed in-flight work causes a separate Critical request to receive the same bounded pool-safety `429`; after the exact original limit is restored, a complete Critical stream succeeds again.
6. Immediately after load cancellation, Low remains rejected during hysteresis.
7. The pool recovers through `busy` to `normal`, and Low is accepted again.

The load uses streaming deliberately. A queued non-streaming generation does not deliver response headers until its complete response is ready and can exceed the gateway response-header timeout under deep queues.

### Optional one-backend drain case

The drain case mutates live gateway state. Enable it only in an isolated environment or approved maintenance window:

```bash
export LLMGW_E2E_DRAIN_BACKEND_ID=1
go test -count=1 -v -timeout 10m ./tests/e2e -run TestPriorityIsolationWithRealVLLM
```

Before the first mutation, the test captures the pool fields and every backend field/drain state in the selected pool. It drains the chosen backend, confirms that Low remains rejected and a complete High stream still succeeds through remaining capacity, then resumes the backend and waits until `draining=false`. Best-effort, idempotent cleanup attempts the exact captured pool limits and every backend value even after an assertion failure. It joins every failed update and continues with later resources; any resource whose update fails may remain mutated and must be restored manually.

## 5. Run isolated circuit recovery

Use the same local two-vLLM configuration, choose one enabled/non-draining backend in the `qwen` pool, and obtain its Admin ID. The gateway URL must use `localhost` or a loopback IP (`127.0.0.0/8` or `::1`); the test refuses any non-loopback gateway because its proxy listens only on `127.0.0.1` and must be reachable by that same gateway process.

```bash
export LLMGW_E2E_MODE=resilience
export LLMGW_E2E_GATEWAY_URL='http://127.0.0.1:8080'
export LLMGW_E2E_ADMIN_USERNAME='operator'
export LLMGW_E2E_ADMIN_PASSWORD='read-from-the-secret-store'
export LLMGW_E2E_MODEL='qwen'
export LLMGW_E2E_HIGH_KEY="$HIGH_PROBE_KEY"
export LLMGW_E2E_EXPECTED_BACKENDS=2
export LLMGW_E2E_CIRCUIT_BACKEND_ID=1
# Defaults to the gateway's documented failure threshold of 5.
export LLMGW_E2E_CIRCUIT_FAILURE_COUNT=5
export LLMGW_E2E_SATURATION_TIMEOUT=60s
export LLMGW_E2E_RECOVERY_TIMEOUT=45s
export LLMGW_E2E_PROBE_TIMEOUT=60s

go test -count=1 -v -timeout 10m ./tests/e2e -run TestCircuitBreakerRecoveryWithRealVLLM
```

The scenario captures originals before mutation, starts a loopback reverse proxy to the selected real-vLLM URL, sets both pool safety limits to unlimited for fault isolation, updates the target URL, and waits for a clean baseline: `CircuitState=closed`, circuit available, zero circuit failures, healthy, and metrics-fresh. It then drains every sibling. The proxy passes `/health`, `/metrics`, and all non-faulting inference headers, statuses, bytes, and streaming through to real vLLM. While faulting, only `/v1/chat/completions`, `/v1/completions`, and `/v1/responses` return deterministic OpenAI-shaped HTTP 503 with error code `e2e_injected_failure`; health and metrics continue to come from real vLLM.

Each configured failure has a distinct `X-Request-Id`, and every response must have HTTP 503 plus the exact OpenAI error code `e2e_injected_failure`. Pass criteria are: target Admin runtime remains healthy/fresh and reaches `CircuitState=open`; `/inference-readyz` becomes HTTP 503 `unavailable`; after fault disable and the configured cooldown, Admin exposes half-open probe capacity and pool availability; one complete stream closes the circuit; `/inference-readyz` returns HTTP 200 `ready`. Cleanup attempts the original target URL, every sibling drain state, all other backend fields, and both original pool limits before stopping the proxy. It reports all failed updates and continues later restores; any resource whose restore update fails may remain mutated and requires manual recovery. No API-key or Basic-auth value is included in test errors or logs.

## Configuration knobs

| Variable | Default | Purpose |
|---|---:|---|
| `LLMGW_E2E_EXPECTED_BACKENDS` | `2` | Minimum ready and healthy/fresh backend count |
| `LLMGW_E2E_HIGH_REQUESTS_PER_KEY` | `4` | Concurrent long streams per High load key |
| `LLMGW_E2E_HIGH_MAX_TOKENS` | `768` | Tokens requested by each saturation stream |
| `LLMGW_E2E_SATURATION_TIMEOUT` | `60s` | Maximum time to reach saturated/emergency |
| `LLMGW_E2E_RECOVERY_TIMEOUT` | `45s` | Maximum time for each recovery-state wait |
| `LLMGW_E2E_PROBE_TIMEOUT` | `60s` | Per-request timeout for smoke and continuity probes |
| `LLMGW_E2E_REQUIRE_ALL_WAITING` | `true` | Require queue-based saturation on every available backend |
| `LLMGW_E2E_DRAIN_BACKEND_ID` | unset | Opt into the Admin drain/resume scenario |
| `LLMGW_E2E_CIRCUIT_BACKEND_ID` | required in `resilience` | Positive Admin backend ID placed behind the fault proxy |
| `LLMGW_E2E_CIRCUIT_FAILURE_COUNT` | `5` | Positive number of distinct inference failures; keep aligned with `LLMGW_CIRCUIT_FAILURE_THRESHOLD` |

On larger production GPUs, increase the number of High keys, requests per key, token count, or prompt size until the pool reaches the configured saturation threshold. If the intended production calibration reaches saturation through KV pressure without every backend reporting a waiting queue, set `LLMGW_E2E_REQUIRE_ALL_WAITING=false`; the Low/High admission assertions remain mandatory.

## Build a portable Linux x86-64 test binary

The suite uses only the Go standard library and can be cross-compiled on macOS:

```bash
make build-e2e-linux-amd64
file dist/llmgw-e2e-linux-amd64
```

Copy the binary to an operator host and run it with the same environment variables:

```bash
./llmgw-e2e-linux-amd64 -test.v -test.timeout=10m
```

## Failure interpretation

- Readiness succeeds but Admin status has no healthy, metrics-fresh backend: monitoring or vLLM metrics compatibility is broken.
- The pool never saturates: the generated load is below physical capacity or thresholds are calibrated too high for this test shape.
- A Low probe returns `200` while saturated: priority admission isolation is broken.
- A High or Critical probe returns `429`: its configured class/concurrency is wrong or priority admission is broken.
- The intentional pool-safety Critical probe returns `200`: the Admin pool mutation did not publish, or observed load ended before the guard was exercised.
- A probe returns `502` after a long queue wait: inspect the gateway response-header timeout and prefer streaming for long queued generations.
- Recovery skips the expected state or times out: inspect current traffic, pressure thresholds, and overload recovery windows.
- Resilience mode rejects the gateway URL: run the gateway and test on the same host and use `localhost` or a loopback IP.
- The circuit never opens: align `LLMGW_E2E_CIRCUIT_FAILURE_COUNT` with the gateway threshold and confirm all siblings were drained.
- Health or metrics becomes stale while inference is faulted: the proxy is not passing management paths to real vLLM, so circuit-isolation evidence is invalid.
