# Real-GPU vLLM validation

This procedure validates behavior that the deterministic fake backend cannot prove: current OpenAI API compatibility, upstream cancellation on a real engine, queue metrics under GPU contention, vLLM priority scheduling, gateway hysteretic recovery, and circuit recovery against a live inference endpoint.

The smoke, priority/pool-safety, and circuit-recovery core is automated by [`tests/e2e`](../tests/e2e) and documented in [real-vllm-priority-e2e.md](real-vllm-priority-e2e.md). Run all three modes first in the appropriate safety window; use this broader procedure for endpoint compatibility, cancellation evidence, affinity observations, threshold calibration, and retained production sign-off artifacts.

## Recorded RTX 4070 Ti Docker evidence

On 2026-08-28 the canonical [Docker quick start](../README.md#docker-quick-start) and all three automated real-vLLM modes passed on an NVIDIA GeForce RTX 4070 Ti with 12,282 MiB VRAM and driver `610.47`. The Linux-container daemon was Docker `29.1.2` with Compose `2.40.3`; the repository base commit was `a138ab8`. Compose ran two pinned `vllm/vllm-openai:v0.28.0` services with `Qwen/Qwen3-0.6B`, `max-num-seqs=1`, priority scheduling, and no published vLLM ports.

The exact documented `docker compose config`, container GPU probe, `up --build --wait`, health/readiness, model listing, and checked-in streaming request all passed. The gateway image healthcheck used its own static binary and reached healthy before Compose returned. Smoke observed one available pool and two healthy, metrics-fresh backends; complete-stream first byte was `240.6163ms` before restart and `242.46ms` after restart. The four-client × four-request, 768-token priority profile reached `GatewayInflight=16`, `TotalWaiting=14`, and `AllBackendsWaiting=true`; Low traffic was rejected while High/Critical continued, the pool-safety and one-backend drain checks passed, and recovery reached `busy` in `14.0900556s` and `normal` in `24.9508766s`. Resilience opened the selected circuit after exactly five injected inference failures, made inference readiness HTTP 503, and returned through half-open to closed/readiness HTTP 200 with a `368.449514ms` recovery first byte. Cleanup restored limits `32/8`, both internal backend URLs, both drain flags, and two healthy/fresh closed circuits. Restarting only the gateway preserved revision `63`, Admin state, rotated client keys, and two-backend inference readiness. The complete Go suite and `go vet ./...` also passed in a Linux Go container.

The representative non-secret commands and durable summary are in [acceptance-evidence.md](acceptance-evidence.md). This is a development-sized CUDA/Docker gate, not production-model sizing or latency calibration. The broader endpoint, cancellation, affinity, and retained-artifact procedure below remains available when those claims are required.

## 1. Prerequisites

- Two GPUs or two independently reachable vLLM serving groups are preferred. One GPU is enough for compatibility, cancellation, and priority tests.
- vLLM with `--scheduling-policy priority` support.
- Gateway, `loadgen`, `curl`, and `jq` available on a host that can reach vLLM.
- Four gateway clients mapped to critical/high/normal/background, with distinct one-time keys.
- One public model pool named `gpu-test`, mapped to the exact vLLM model name.

Set reusable shell values:

```bash
export MODEL='Qwen/Qwen2.5-7B-Instruct'
export GATEWAY='http://127.0.0.1:8080'
export VLLM_A='http://127.0.0.1:8001'
export VLLM_B='http://127.0.0.1:8002'
export ADMIN_USER='operator'
export ADMIN_PASSWORD='replace-with-your-password'
```

Start vLLM A (and B on another GPU/host when available):

```bash
CUDA_VISIBLE_DEVICES=0 vllm serve "$MODEL" --host 127.0.0.1 --port 8001 \
  --scheduling-policy priority --enable-request-id-headers

CUDA_VISIBLE_DEVICES=1 vllm serve "$MODEL" --host 127.0.0.1 --port 8002 \
  --scheduling-policy priority --enable-request-id-headers
```

Register the pool/backends and clients in the Admin UI. Wait for Dashboard to show healthy, fresh metrics. Capture baseline evidence:

```bash
curl -sS "$VLLM_A/health"
curl -sS "$VLLM_A/metrics" | grep -E 'vllm:(num_requests_running|num_requests_waiting|kv_cache_usage_perc)'
curl -sS -u "$ADMIN_USER:$ADMIN_PASSWORD" "$GATEWAY/admin/api/status" | jq .
```

## 2. OpenAI compatibility

Run all supported routes through the gateway:

```bash
curl -sS "$GATEWAY/v1/models" -H "Authorization: Bearer $HIGH_KEY" | jq .

curl -sS "$GATEWAY/v1/completions" -H "Authorization: Bearer $HIGH_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpu-test","prompt":"Count from one to five.","max_tokens":32}' | jq .

curl -sS "$GATEWAY/v1/chat/completions" -H "Authorization: Bearer $HIGH_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpu-test","messages":[{"role":"user","content":"Count from one to five."}],"max_tokens":32}' | jq .

curl -sS "$GATEWAY/v1/responses" -H "Authorization: Bearer $HIGH_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpu-test","input":"Count from one to five.","max_output_tokens":32}' | jq .

curl -N "$GATEWAY/v1/completions" -H "Authorization: Bearer $HIGH_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpu-test","prompt":"Count from one to five.","max_tokens":32,"stream":true}'

curl -N "$GATEWAY/v1/chat/completions" -H "Authorization: Bearer $HIGH_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpu-test","messages":[{"role":"user","content":"Count from one to five."}],"max_tokens":32,"stream":true}'

curl -N "$GATEWAY/v1/responses" -H "Authorization: Bearer $HIGH_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpu-test","input":"Count from one to five.","max_output_tokens":32,"stream":true}'
```

Pass criteria: ordinary and streaming variants of all routes return a valid upstream response; every stream delivers its first event before generation completes and ends cleanly; gateway logs use the public model and record a backend; vLLM receives the configured upstream model and gateway-generated request ID.

## 3. Streaming cancellation

Start a deliberately long stream and terminate curl after two seconds:

```bash
before=$(curl -sS "$GATEWAY/metrics" | awk '/^llmgw_stream_disconnects_total/{sum += $2} END{print sum+0}')
curl --max-time 2 -N "$GATEWAY/v1/completions" \
  -H "Authorization: Bearer $HIGH_KEY" -H 'Content-Type: application/json' \
  -d '{"model":"gpu-test","prompt":"Write a very long numbered technical essay.","max_tokens":4096,"stream":true}' || true
sleep 2
after=$(curl -sS "$GATEWAY/metrics" | awk '/^llmgw_stream_disconnects_total/{sum += $2} END{print sum+0}')
printf 'disconnect counter: before=%s after=%s\n' "$before" "$after"
curl -sS "$VLLM_A/metrics" | grep 'vllm:num_requests_running'
```

Pass criteria: the disconnect counter increases, the request completion log has `disconnect=true`, the client/backend in-flight gauges return to baseline, and the upstream running count falls after cancellation.

## 4. Queue detection and load-aware routing

Generate enough low-priority work to exceed physical capacity:

```bash
./dist/loadgen-linux-amd64 -url "$GATEWAY" -key "$BACKGROUND_KEY" -model gpu-test \
  -parallelism 64 -requests 100000 -prompt-size 2048 -max-tokens 512 -stream -json > /tmp/background-load.json &
LOAD_PID=$!
printf '%s\n' "$LOAD_PID" > /tmp/llmgw-background-load.pid

for i in $(seq 1 30); do
  date -u +%FT%TZ
  curl -sS "$VLLM_A/metrics" | grep -E 'vllm:(num_requests_running|num_requests_waiting|kv_cache_usage_perc)'
  curl -sS -u "$ADMIN_USER:$ADMIN_PASSWORD" "$GATEWAY/admin/api/status" | jq '{revision,pools:[.pools[]|{name:.publicModelName,runtime}],backends:[.backends[]|{name,runtime}]}'
  sleep 1
done | tee /tmp/queue-pressure-evidence.txt

kill -0 "$LOAD_PID"
printf 'background load remains active as PID %s for sections 5 and 6\n' "$LOAD_PID"
```

With two backends, repeat after applying load directly to only vLLM A. Pass criteria: waiting requests and pressure rise on A; new gateway requests prefer the lower-pressure B; stale, unhealthy, or draining endpoints receive no new work.

### Soft session affinity and cache locality

With two healthy backends below the configured affinity pressure ceiling, send a sequence that reuses one session ID:

```bash
for i in $(seq 1 20); do
  curl -sS "$GATEWAY/v1/chat/completions" \
    -H "Authorization: Bearer $HIGH_KEY" \
    -H 'X-LLM-Session-Id: gpu-affinity-check-1' \
    -H 'Content-Type: application/json' \
    -d '{"model":"gpu-test","messages":[{"role":"user","content":"Continue the same technical discussion."}],"max_tokens":16}' \
    >/dev/null
done
```

Inspect the gateway completion logs and both vLLM request logs. Pass criteria: all requests select the same backend while it stays eligible and below `LLMGW_SESSION_AFFINITY_MAX_PRESSURE`. To prove that the header is stripped, place a controlled header-capturing reverse proxy between the gateway and vLLM for this run; absence from ordinary vLLM logs is not evidence because vLLM does not normally log arbitrary request headers. Then raise load on that preferred backend above the ceiling, drain it, or make its metrics stale and repeat the request. The session must move to an eligible backend without a client error; overload uses least-pressure fallback, while other eligibility changes recompute rendezvous hashing over the remaining set. Removing the header must restore ordinary least-pressure selection. Record per-backend KV utilization and TTFT as evidence of locality impact; this experiment validates routing behavior but does not claim block-level cache awareness.

## 5. Priority isolation

The recommended repeatable gate is:

```bash
LLMGW_E2E_MODE=priority make test-real-vllm
```

Configure all required identities and tuning variables as described in [real-vllm-priority-e2e.md](real-vllm-priority-e2e.md). The automated test covers three independent Low probes, body/header priority spoofing, session-affinity bypass resistance, non-zero pool gateway-inflight/waiting observation, High/Critical continuity while saturated, a temporary pool-wide limit that also rejects Critical, exact limit restoration, optional one-backend drain, and hysteretic recovery. The manual mixed-load run below remains useful for longer percentile and capacity-calibration evidence.

Run the fixed seeded mix while background traffic already fills the queue:

```bash
LOAD_PID="$(cat /tmp/llmgw-background-load.pid)"
kill -0 "$LOAD_PID"

./dist/loadgen-linux-amd64 -url "$GATEWAY" -model gpu-test \
  -parallelism 64 -requests 2000 -prompt-size 1024 -max-tokens 256 -stream -seed 42 \
  -class-keys "critical=$CRITICAL_KEY,high=$HIGH_KEY,normal=$NORMAL_KEY,background=$BACKGROUND_KEY" \
  -mix critical=10,high=20,normal=30,background=40 -json | tee /tmp/mixed-priority.json

curl -sS "$GATEWAY/metrics" | grep -E '^llmgw_(requests_total|requests_rejected_total|request_duration_seconds|ttft_seconds)'
```

Inspect the `byClass` outcome and successful-response latency summaries, then submit one request per class with unique prompts and inspect the vLLM access/request logs. Pass criteria: vLLM sees the configured values (for example critical `-100`, high `-10`, normal `0`, background `100`); client-supplied header/body escalation is overwritten; under saturation, lower classes receive admission `429` before critical/high classes; accepted high-priority requests retain materially better TTFT than queued background traffic.

## 6. Hysteresis, recovery, and health transitions

Keep the section 4 background generator active for the first phase. Record the sustained state, stop it with a timestamp, and then observe recovery continuously:

```bash
LOAD_PID="$(cat /tmp/llmgw-background-load.pid)"
kill -0 "$LOAD_PID"

poll_status() {
  date -u +%FT%TZ
  curl -sS -u "$ADMIN_USER:$ADMIN_PASSWORD" "$GATEWAY/admin/api/status" \
    | jq -c '[.pools[]|{model:.publicModelName,state:.runtime.State,pressure:.runtime.BestBackendPressure}]'
}

for i in $(seq 1 5); do
  poll_status
  sleep 1
done | tee /tmp/hysteresis-sustained.jsonl

date -u +%FT%TZ | tee /tmp/background-load-stopped-at.txt
kill "$LOAD_PID"
wait "$LOAD_PID" || true
rm -f /tmp/llmgw-background-load.pid

for i in $(seq 1 20); do
  poll_status
  sleep 1
done | tee /tmp/hysteresis-recovery.jsonl
```

After the pool has returned to normal, create a spike shorter than the configured three-second enter window and confirm that it does not promote the pool:

```bash
./dist/loadgen-linux-amd64 -url "$GATEWAY" -key "$BACKGROUND_KEY" -model gpu-test \
  -parallelism 64 -requests 100000 -prompt-size 2048 -max-tokens 512 -stream >/tmp/short-spike.log &
SPIKE_PID=$!
date -u +%FT%TZ | tee /tmp/short-spike-started-at.txt
sleep 2
kill "$SPIKE_PID"
wait "$SPIKE_PID" || true
date -u +%FT%TZ | tee /tmp/short-spike-stopped-at.txt

for i in $(seq 1 8); do
  poll_status
  sleep 1
done | tee /tmp/hysteresis-short-spike.jsonl
```

Finally, exercise the configured health failure and consecutive-success recovery counts with no load running. Set `VLLM_A_PID` to the actual server process, or substitute the equivalent controlled stop/start operation for your process manager:

```bash
: "${VLLM_A_PID:?set VLLM_A_PID to the vLLM A server process}"
date -u +%FT%TZ | tee /tmp/vllm-a-paused-at.txt
kill -STOP "$VLLM_A_PID"
for i in $(seq 1 8); do
  poll_status
  sleep 1
done | tee /tmp/health-failure.jsonl

date -u +%FT%TZ | tee /tmp/vllm-a-resumed-at.txt
kill -CONT "$VLLM_A_PID"
for i in $(seq 1 8); do
  poll_status
  sleep 1
done | tee /tmp/health-recovery.jsonl
```

Pass criteria:

1. A sub-three-second spike does not promote the pool to a higher overload state.
2. Sustained pressure promotes the pool after the configured enter window.
3. Once pressure falls below recovery thresholds, the pool does not recover before the configured ten-second recovery window.
4. Emergency/saturated recovery steps down progressively rather than jumping directly to normal.
5. An unhealthy backend is excluded after the configured failure count and returns after the configured consecutive successes.

## 7. Inference circuit and readiness recovery

Run the isolated resilience scenario from the same host as the gateway:

```bash
LLMGW_E2E_MODE=resilience make test-real-vllm
```

Set `LLMGW_E2E_CIRCUIT_BACKEND_ID` to one backend in the selected pool and keep `LLMGW_E2E_CIRCUIT_FAILURE_COUNT` aligned with the gateway threshold (default `5`). This mode requires a loopback gateway URL, captures the target URL, all sibling drain states, all backend update fields, and both pool limits, and attempts to restore every captured value. Cleanup reports all failed updates and continues trying later resources; a resource whose update fails may remain mutated and must be restored manually. The loopback fault proxy faults only supported inference routes. Do not run this mode against production traffic.

Retain Admin snapshots showing healthy/fresh metrics alongside `closed → open → half_open → closed`, `/readyz` remaining HTTP 200, `/inference-readyz` changing `200 → 503 → 200`, and the five Prometheus families `llmgw_backend_circuit_state`, `llmgw_backend_circuit_failures`, `llmgw_pool_gateway_inflight`, `llmgw_pool_waiting_requests`, and `llmgw_pool_available_backends`. Also retain the complete recovery stream timing and the cleanup status. Do not claim a target-host gate until its real-vLLM command has actually run and its required artifacts have been retained.

## 8. Evidence to retain

Keep the vLLM version/command line, GPU model and count, gateway commit, Admin status captures, gateway metrics before/after, structured request logs, vLLM metrics/logs, loadgen JSON, and timestamps. Record deviations caused by model length, GPU memory, or vLLM-version metric names. Do not use real API keys or prompts containing sensitive data in retained artifacts.
