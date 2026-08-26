# Real-GPU vLLM validation

This procedure validates behavior that the deterministic fake backend cannot prove: current OpenAI API compatibility, upstream cancellation on a real engine, queue metrics under GPU contention, vLLM priority scheduling, and gateway hysteretic recovery.

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
  -parallelism 64 -requests 1000 -prompt-size 2048 -max-tokens 512 -stream -json > /tmp/background-load.json &
LOAD_PID=$!

for i in $(seq 1 30); do
  date -u +%FT%TZ
  curl -sS "$VLLM_A/metrics" | grep -E 'vllm:(num_requests_running|num_requests_waiting|kv_cache_usage_perc)'
  curl -sS -u "$ADMIN_USER:$ADMIN_PASSWORD" "$GATEWAY/admin/api/status" | jq '{revision,pools:[.pools[]|{name:.publicModelName,runtime}],backends:[.backends[]|{name,runtime}]}'
  sleep 1
done | tee /tmp/queue-pressure-evidence.txt

wait "$LOAD_PID"
```

With two backends, repeat after applying load directly to only vLLM A. Pass criteria: waiting requests and pressure rise on A; new gateway requests prefer the lower-pressure B; stale, unhealthy, or draining endpoints receive no new work.

## 5. Priority isolation

Run the fixed seeded mix while background traffic already fills the queue:

```bash
./dist/loadgen-linux-amd64 -url "$GATEWAY" -model gpu-test \
  -parallelism 64 -requests 2000 -prompt-size 1024 -max-tokens 256 -stream -seed 42 \
  -class-keys "critical=$CRITICAL_KEY,high=$HIGH_KEY,normal=$NORMAL_KEY,background=$BACKGROUND_KEY" \
  -mix critical=10,high=20,normal=30,background=40 -json | tee /tmp/mixed-priority.json

curl -sS "$GATEWAY/metrics" | grep -E '^llmgw_(requests_total|requests_rejected_total|request_duration_seconds|ttft_seconds)'
```

Inspect the `byClass` outcome and successful-response latency summaries, then submit one request per class with unique prompts and inspect the vLLM access/request logs. Pass criteria: vLLM sees the configured values (for example critical `-100`, high `-10`, normal `0`, background `100`); client-supplied header/body escalation is overwritten; under saturation, lower classes receive admission `429` before critical/high classes; accepted high-priority requests retain materially better TTFT than queued background traffic.

## 6. Hysteresis and recovery

During the load from section 4, poll status once per second. Stop the generator, then continue polling for at least 15 seconds:

```bash
for i in $(seq 1 20); do
  curl -sS -u "$ADMIN_USER:$ADMIN_PASSWORD" "$GATEWAY/admin/api/status" \
    | jq -c '[.pools[]|{model:.publicModelName,state:.runtime.State,pressure:.runtime.BestBackendPressure}]'
  sleep 1
done | tee /tmp/hysteresis-recovery.jsonl
```

Pass criteria:

1. A sub-three-second spike does not promote the pool to a higher overload state.
2. Sustained pressure promotes the pool after the configured enter window.
3. Once pressure falls below recovery thresholds, the pool does not recover before the configured ten-second recovery window.
4. Emergency/saturated recovery steps down progressively rather than jumping directly to normal.
5. An unhealthy backend is excluded after the configured failure count and returns after the configured consecutive successes.

## 7. Evidence to retain

Keep the vLLM version/command line, GPU model and count, gateway commit, Admin status captures, gateway metrics before/after, structured request logs, vLLM metrics/logs, loadgen JSON, and timestamps. Record deviations caused by model length, GPU memory, or vLLM-version metric names. Do not use real API keys or prompts containing sensitive data in retained artifacts.
