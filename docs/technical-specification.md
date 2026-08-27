# Technical Specification
## Lightweight vLLM Priority Gateway

> **Translation and implementation note:** This document is a faithful English translation of the original Russian specification. The approved implementation decision—Go modular monolith with an embedded server-rendered Admin UI—supersedes the alternative .NET/React technology-stack examples in section 4. The [README](../README.md) describes the software that was actually delivered.

**Document version:** 0.1<br>
**MVP goal:** development and full functional debugging on a single NVIDIA RTX 4070 Ti.<br>
**Production goal:** operation in front of a cluster of several independent vLLM serving endpoints / serving groups, with client prioritization and degradation of low-priority workloads when inference capacity is scarce.

---

# 1. System Purpose

Develop a lightweight HTTP gateway in front of one or more vLLM instances.

The Gateway must provide clients with an OpenAI-compatible API and perform the following tasks:

- authenticate clients using API keys;
- manage clients through a simple Web UI;
- assign priorities to clients;
- enforce concurrency limits and, subsequently, rate/token limits;
- monitor the status of vLLM backends;
- select the least-loaded backend;
- automatically restrict low-priority clients during overload;
- pass client priority into vLLM;
- transparently handle streaming responses;
- provide fault-tolerant exclusion of unhealthy backends;
- collect its own Prometheus telemetry.

The system **does not perform inference itself** and does not interfere with the internal implementation of vLLM.

---

# 2. Core Architectural Principle

The Gateway does not work directly with physical GPUs, but with the following entity:

**vLLM Backend**

A Backend represents the HTTP endpoint of one running vLLM API server.

For example:

```text
Model Pool: qwen-coder

├── backend-a
│   └── http://gpu-node-01:8000
│       └── 1× RTX 5090
│
├── backend-b
│   └── http://gpu-node-02:8000
│       └── 1× RTX 5090
│
└── backend-c
    └── http://ray-head:8000
        └── vLLM TP/PP
            ├── GPU node 03
            └── GPU node 04
```

For the Gateway, `backend-c` is **one backend**, even though vLLM itself uses several physical nodes.

This keeps the load balancer independent of any specific GPU topology.

vLLM officially supports TP/PP and multi-node execution, including through Ray; the Gateway must not attempt to replicate this functionality ([vLLM parallelism and scaling](https://docs.vllm.ai/en/stable/serving/parallelism_scaling/)).

---

# 3. Overall Architecture

```text
                           ┌──────────────────┐
                           │     Admin UI     │
                           │                  │
                           │ Clients          │
                           │ API Keys         │
                           │ Priorities       │
                           │ Model access     │
                           │ Backends         │
                           │ Live load        │
                           └────────┬─────────┘
                                    │
                                    ▼
                           ┌─────────────────┐
                           │ Configuration DB │
                           └────────┬────────┘
                                    │
                                    ▼
Clients ──OpenAI API──► ┌───────────────────────────┐
                        │       LLM Gateway         │
                        │                           │
                        │ Authentication            │
                        │ Client policies           │
                        │ Admission control         │
                        │ Priority assignment       │
                        │ Backend monitoring        │
                        │ Load balancing            │
                        │ Streaming proxy           │
                        │ Metrics                   │
                        └────────────┬──────────────┘
                                     │
                  ┌──────────────────┼──────────────────┐
                  ▼                  ▼                  ▼
             vLLM backend       vLLM backend       vLLM backend
                  A                  B                  C
```

---

# 4. Proposed Technology Stack

## MVP

Backend:

```text
ASP.NET Core
SQLite
EF Core or Dapper
HttpClient / IHttpClientFactory
Prometheus metrics
```

Web UI:

```text
React
```

or, if the smallest possible project is desired:

```text
ASP.NET Core + Razor/Blazor
```

For the MVP, **a single deployable service** is preferred.

```text
gateway
├── HTTP API
├── Admin API
├── Admin UI static files
├── backend monitor
└── SQLite
```

Redis, Kafka, RabbitMQ, and Kubernetes are not required for the MVP.

---

# 5. vLLM Compatibility

vLLM must be started with:

```text
--scheduling-policy priority
```

The Gateway must set:

```http
X-Vllm-Priority: <integer>
```

In current versions of vLLM, a lower priority value means a higher priority. `X-Vllm-Priority` is supported for Chat Completions, Completions, and the Responses API, and takes precedence over the `priority` value in the JSON body ([vLLM OpenAI-compatible server](https://docs.vllm.ai/en/latest/serving/online_serving/openai_compatible_server/)).

For example:

```text
Critical     → -100
High         → -10
Normal       → 0
Background   → 100
```

The Gateway must **ignore the client's `X-Vllm-Priority`**.

Clients must not be able to increase their own priority.

Algorithm:

```text
incoming X-Vllm-Priority
        ↓
      REMOVE
        ↓
lookup client policy
        ↓
X-Vllm-Priority = configuredPriority
```

---

# 6. Supported OpenAI API

## MVP

Required support:

```text
GET  /v1/models

POST /v1/chat/completions
POST /v1/completions
POST /v1/responses
```

The Gateway must not transform the vLLM response format.

Core principle:

> Validate → authorize → select backend → proxy.

Additional `/v1/*` endpoints must return a controlled error in the MVP or be explicitly enabled through an allowlist.

## Production

The allowlist of supported upstream endpoints must be configurable.

---

# 7. Model Pools

The following entity is introduced:

```text
ModelPool
```

It represents a logical model available to clients.

Example:

```text
Public name:
qwen-coder

Upstream model:
Qwen/Qwen3-Coder-Next

Backends:
gpu-01
gpu-02
gpu-03
```

The client sends:

```json
{
  "model": "qwen-coder"
}
```

The Gateway selects a backend from the `qwen-coder` pool and, if necessary, replaces the model name with the upstream name.

This makes it possible to:

- change the actual models without changing client configurations;
- perform rolling upgrades;
- maintain multiple replicas;
- prohibit specific clients from using expensive models.

---

# 8. MVP Data Model

## Client

```text
Id
Name
Enabled

PriorityClass
VllmPriority

MaxConcurrency

CreatedAt
UpdatedAt
```

Example:

```text
Name: codex-developers
PriorityClass: High
VllmPriority: -10
MaxConcurrency: 20
```

---

## ApiKey

```text
Id
ClientId

Prefix
SecretHash

CreatedAt
ExpiresAt?
RevokedAt?
LastUsedAt?
```

The API key should have approximately the following format:

```text
llmgw_xxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

The full value is shown to the user **only once**, when it is created.

Storing the plaintext key in the database is prohibited.

Store:

```text
key prefix
+
HMAC/SHA-256 hash
```

Because the key is generated using cryptographically secure randomness and has high entropy, password hashing is not required here.

---

## ClientModelAccess

```text
ClientId
ModelPoolId
Enabled
```

---

## ModelPool

```text
Id

PublicModelName
UpstreamModelName

Enabled
MaxGatewayInflight
MaxWaiting
```

The persisted safety fields are non-negative integers. `0` means disabled/unlimited. SQLite schema migration version 2 adds both columns with `NOT NULL DEFAULT 0` checks while preserving version-1 rows.

---

## Backend

```text
Id
ModelPoolId

Name
BaseUrl

Enabled
Draining

CapacityHint

UpstreamApiKey
```

For example:

```text
Name = gpu01-qwen
BaseUrl = http://10.0.0.101:8000
CapacityHint = 1.0
```

---

# 9. vLLM Monitoring

The Gateway periodically polls:

```text
GET /metrics
```

Primary metrics:

```text
vllm:num_requests_running
vllm:num_requests_waiting
vllm:kv_cache_usage_perc
```

Additional metrics:

```text
vllm:time_to_first_token_seconds
vllm:inter_token_latency_seconds
vllm:num_preemptions
```

vLLM exposes these measurements through the Prometheus-compatible `/metrics` endpoint ([vLLM Production Metrics](https://docs.vllm.ai/en/latest/usage/metrics/)).

## Important Requirement

**GPU utilization from `nvidia-smi` is NOT the primary control signal.**

100% GPU utilization may indicate a perfectly functioning inference server.

The primary signs of overload are:

```text
waiting requests are increasing
KV cache is approaching its limit
request latency is increasing
preemptions occur frequently
```

GPU utilization and GPU memory may be used in Production as additional telemetry metrics, for example through the DCGM exporter, but they must not directly determine the admission policy.

---

# 10. Backend Monitor

Each backend is polled independently.

MVP defaults:

```text
metrics interval: 1 sec
health interval: 2 sec

metrics stale after: 5 sec

unhealthy:
3 consecutive health failures

recovery:
2 consecutive successes
```

A Backend has the following state:

```text
Healthy
Busy
Saturated
Draining
Unhealthy
```

`Draining` is set manually by an operator.

When `Draining` is set:

```text
existing requests → continue
new requests      → are not assigned
```

---

# 11. Backend Pressure Calculation

For the MVP, use a simple normalized algorithm.

```text
queuePressure =
    clamp(waiting / queueSoftLimit, 0, 2)

kvPressure =
    clamp(
        (kvUsage - kvSoftLimit)
        /
        (kvHardLimit - kvSoftLimit),
        0,
        2
    )

runningPressure =
    clamp(running / runningSoftLimit, 0, 2)
```

Result:

```text
pressure =
      0.55 × queuePressure
    + 0.30 × kvPressure
    + 0.15 × runningPressure
```

Initial values:

```text
queueSoftLimit   = 2

kvSoftLimit      = 0.80
kvHardLimit      = 0.95

runningSoftLimit = backend-specific
```

All values must be configurable.

These are **initial parameters**, not universal constants.

The 4070 Ti is used specifically to calibrate the algorithm's behavior.

---

# 12. Metric Smoothing

Decisions must not be made based on a single snapshot.

Use EWMA / a rolling average over approximately:

```text
3–5 seconds
```

Hysteresis is also required.

For example:

```text
NORMAL → BUSY

pressure > 0.70
for at least 3 seconds
```

In the other direction:

```text
BUSY → NORMAL

pressure < 0.55
for at least 10 seconds
```

That is, use different enter/exit thresholds.

The goal is to prevent the following:

```text
NORMAL
BUSY
NORMAL
BUSY
NORMAL
BUSY
```

every second.

---

# 13. Pool Pressure

A Pool is considered overloaded when **even the best available backend is under load**.

```text
bestBackendPressure =
    min(
        pressure of all Healthy backends
    )
```

Initial states:

```text
NORMAL
best < 0.70

BUSY
best >= 0.70

SATURATED
best >= 1.00

EMERGENCY
best >= 1.40
```

An additional transition to SATURATED is allowed if:

```text
waiting > 0
```

on all healthy backends for a specified time window.

If there is no healthy backend at all:

```text
pool state = UNAVAILABLE
```

---

# 14. Priority Classes

Define four system classes.

| Class | vLLM priority | Purpose |
|---|---:|---|
| Critical | -100 | production interactive |
| High | -10 | developers / interactive agents |
| Normal | 0 | standard users |
| Background | 100 | CI, batch, experiments |

The numbers must be configurable.

---

# 15. Admission Control

During overload, the Gateway restricts **new** requests.

The Gateway does not cancel requests that are already running on its own initiative.

Before per-client priority admission, the single-replica implementation applies two pool-wide safety guards. A positive `MaxWaiting` rejects when the aggregate healthy/fresh, enabled, non-draining upstream waiting count is greater than or equal to the limit. A positive `MaxGatewayInflight` atomically bounds requests held across the full pool request lifecycle. Either guard returns the same bounded `429 gateway_overloaded` response below, and no priority class can bypass it. Zero disables the limit while runtime telemetry still counts gateway in-flight work.

Initial policy:

| Pool state | Critical | High | Normal | Background |
|---|---:|---:|---:|---:|
| NORMAL | 100% | 100% | 100% | 100% |
| BUSY | 100% | 100% | 100% | 50% |
| SATURATED | 100% | 100% | 50% | 0% |
| EMERGENCY | 100% | 50% | 0% | 0% |

The percentage is applied to:

```text
Client.MaxConcurrency
```

For example:

```text
background-client
MaxConcurrency = 20
```

When BUSY:

```text
effectiveConcurrency = 10
```

When SATURATED:

```text
effectiveConcurrency = 0
```

New requests receive:

```http
HTTP 429
Retry-After: 2
```

OpenAI-compatible body:

```json
{
  "error": {
    "message": "Inference cluster is currently overloaded",
    "type": "rate_limit_error",
    "code": "gateway_overloaded"
  }
}
```

---

# 16. Per-Client Concurrency

The counter must cover the entire request lifecycle.

For streaming:

```text
client request
      ↓
slot acquired
      ↓
upstream request
      ↓
SSE stream
      ↓
last chunk / disconnect / error
      ↓
slot released
```

The slot **must not be released after receiving HTTP 200**.

A streaming generation that lasts 2 minutes is considered an active request for that entire duration.

---

# 17. Routing

After successful admission, only a backend that meets all of the following criteria is selected:

```text
Enabled
AND
!Draining
AND
Healthy
AND
belongs to requested pool
```

## MVP routing algorithm

Select the backend with the minimum:

```text
pressure
```

When pressure is approximately equal, use:

```text
running requests
```

then use a random tie-break.

For example:

```text
candidate A pressure 0.27
candidate B pressure 0.31
candidate C pressure 0.91

→ A
```

---

# 18. Routing and Priority Are Independent Mechanisms

It is important to distinguish between:

### Gateway Routing

Answers the question:

> Which vLLM should receive the request?

### Gateway Admission

Answers:

> Should this client's request be accepted at all right now?

### vLLM Priority Scheduler

Answers:

> Which request already received by this vLLM should be given GPU resources first?

Flow:

```text
Client priority
      ↓
Gateway admission
      ↓
Gateway routing
      ↓
X-Vllm-Priority
      ↓
vLLM priority scheduler
```

The Gateway must remain operational even if the internal implementation of the vLLM priority scheduler changes.

---

# 19. Streaming Proxy

This is one of the critical parts of the project.

Mandatory requirements:

- SSE must be transmitted without buffering;
- chunks must be sent to the client immediately;
- client disconnect must cancel the upstream request;
- the Gateway must not accumulate the entire completion in RAM;
- a standard short `HttpClient.Timeout` must not be used;
- long-running generations must be handled correctly.

Flow:

```text
vLLM
 │
 │ SSE chunk
 ▼
Gateway
 │
 │ immediately
 ▼
Client
```

Not:

```text
vLLM
 │
 ▼
Gateway buffer
Gateway buffer
Gateway buffer
 │
 ▼
Client
```

---

# 20. Request Cancellation

When a client disconnects:

```text
HttpContext.RequestAborted
```

must be propagated to the upstream HTTP request.

After cancellation:

```text
upstream connection closed
client concurrency slot released
request marked cancelled
metrics updated
```

---

# 21. Retry Policy

Automatic HTTP retries for LLM generation must be extremely conservative.

## Allowed

One retry on another backend in the event of a transport failure, if:

```text
not a single response byte
has yet been sent to the client
```

## Prohibited

Retrying after streaming has started:

```text
data: token1
data: token2
<backend dies>
```

The Gateway must terminate the stream with an error.

It must not start a new generation on another backend and continue the stream, because the output would no longer be identical.

---

# 22. X-Request-Id

Every request must have a unique request ID.

If the client supplies:

```text
X-Request-Id
```

use it or retain it as the parent ID.

The Gateway adds its own correlation ID and passes it upstream.

vLLM can work with request ID headers when configured accordingly ([vLLM OpenAI-compatible server](https://docs.vllm.ai/en/latest/serving/online_serving/openai_compatible_server/)).

---

# 23. Admin UI — MVP

The UI must contain four main screens.

## Dashboard

```text
Pool       Backends    Running    Waiting    KV     State

qwen       3/3         22         0          64%    NORMAL
llama      2/2         31         8          91%    SATURATED
```

The implemented dashboard also shows pool `GatewayInflight`, aggregate `TotalWaiting`, `AvailableBackends`, `MaxGatewayInflight`, and `MaxWaiting`.

Backend details:

```text
gpu01

state       HEALTHY
running     14
waiting      0
kv cache    72%
pressure    0.38
circuit     closed
```

---

## Clients

```text
Name
Priority
Max concurrency
Models
Enabled
```

Actions:

```text
Create
Edit
Disable
```

---

## API Keys

```text
Client
Prefix
Created
Last used
Expires
Status
```

Actions:

```text
Generate
Revoke
```

The secret is shown only once after generation.

---

## Backends

```text
Name
Model Pool
URL
State
Pressure
Enabled
Draining
Circuit state/failures/retry/probes/availability
```

Actions:

```text
Create
Edit
Enable
Disable
Drain
```

---

# 24. Admin API — MVP

At minimum:

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

---

# 25. MVP Security

For the MVP, the Admin UI may:

- listen only on localhost/a private interface;
- or be protected by a single admin credential.

Production authentication is not required yet.

However, client API keys must be implemented correctly from the outset.

In addition, vLLM endpoints must not be directly accessible to clients.

The current vLLM documentation separately warns that the built-in `--api-key` is not a complete security boundary for all HTTP endpoints and recommends placing vLLM behind a reverse proxy/firewall with an allowlist of permitted routes ([vLLM security guidance](https://docs.vllm.ai/en/stable/usage/security/)).

Production topology:

```text
User network
      │
      ▼
LLM Gateway
      │
──── firewall ────
      │
      ▼
vLLM network
```

---

# 26. Gateway Prometheus Metrics

The Gateway must expose:

```text
/metrics
```

Minimum set:

```text
llmgw_requests_total
llmgw_requests_inflight

llmgw_requests_rejected_total

llmgw_client_inflight

llmgw_backend_requests_inflight
llmgw_backend_pressure
llmgw_backend_running_requests
llmgw_backend_waiting_requests
llmgw_backend_kv_cache_usage

llmgw_request_duration_seconds
llmgw_ttft_seconds

llmgw_stream_disconnects_total

llmgw_backend_failures_total
llmgw_retries_total

llmgw_backend_circuit_state
llmgw_backend_circuit_failures
llmgw_pool_gateway_inflight
llmgw_pool_waiting_requests
llmgw_pool_available_backends
```

Labels:

```text
client
model
backend
priority_class
status
reason
```

Labels containing request IDs and other high-cardinality values should be avoided.

---

# 27. Structured Logging

Record the following for every request:

```text
timestamp
requestId

clientId
model
priority

selectedBackend

poolState
backendPressure

httpStatus

duration
ttft

promptTokens?
completionTokens?

disconnect
retry
```

Prompts and generated content:

```text
DO NOT log by default.
```

---

# 28. MVP: What We Deliberately Do NOT Implement

The following are out of scope:

- Kubernetes integration;
- automatic backend discovery;
- Redis;
- PostgreSQL;
- distributed gateway instances;
- billing;
- monthly budgets;
- automatic GPU scaling;
- prefix-aware routing;
- vLLM autoscaling;
- a model tokenizer within the Gateway;
- NVML-based scheduling;
- cancellation of an already running background generation;
- fancy RBAC;
- OIDC.

The primary objective of the MVP:

> Prove that priority-aware admission + load-aware routing actually protect high-priority traffic.

---

# 29. Home RTX 4070 Ti Test Environment

The RTX 4070 Ti is suitable for vLLM development: the card has CUDA compute capability 8.9, while the current vLLM requirements for NVIDIA begin with compute capability 7.5. The official primary vLLM runtime targets Linux ([NVIDIA compute capability table](https://developer.nvidia.com/cuda/gpus), [vLLM CUDA requirements](https://github.com/vllm-project/vllm/blob/main/docs/getting_started/installation/gpu.cuda.inc.md), [vLLM quickstart](https://docs.vllm.ai/en/latest/getting_started/quickstart/)).

Test environment:

```text
                         RTX 4070 Ti
                             │
                             ▼
                         real vLLM
                           :8001

Gateway :8080
 │
 ├────────────► real vLLM
 │
 ├────────────► fake-vLLM-1 :8002
 │
 └────────────► fake-vLLM-2 :8003
```

---

# 30. Fake vLLM Simulator

A small test backend must be developed for the project.

It must implement:

```text
GET /health
GET /metrics
GET /v1/models

POST /v1/chat/completions
POST /v1/completions
POST /v1/responses
```

It must support regular and streaming responses.

The simulator must allow the following to be configured programmatically:

```text
running
waiting
kv_cache_usage

TTFT delay
token delay

HTTP errors
connection resets
health failures
```

For example:

```json
{
  "running": 12,
  "waiting": 8,
  "kvCacheUsage": 0.94,
  "ttftMs": 5000,
  "tokenDelayMs": 100
}
```

This makes it possible to test routing deterministically.

---

# 31. Why the Fake Backend Is Mandatory

Without it, the test:

> The Gateway switches to another node when `kv_cache=95%`

would require physically loading the GPU to the required state every time.

The simulator makes it possible to write a proper integration test:

```text
backend A pressure = 0.2
backend B pressure = 1.3

send request

assert selectedBackend == A
```

---

# 32. Real-GPU Tests on the 4070 Ti

The following must be tested against a real vLLM instance:

### Test 1 — OpenAI Compatibility

```text
Gateway → vLLM → response
```

Streaming and non-streaming.

### Test 2 — Cancellation

Start a long generation.

Disconnect the client connection.

Verify:

```text
Gateway stops upstream
slot released
```

### Test 3 — Queue Detection

Create enough parallel requests for the following to appear:

```text
vllm:num_requests_waiting > 0
```

Verify the transition:

```text
NORMAL → BUSY/SATURATED
```

### Test 4 — Priority

Create many:

```text
Background requests
```

Then send a:

```text
Critical request
```

Verify that the Gateway:

```text
1. admits the Critical request;
2. limits Background requests;
3. sets the correct X-Vllm-Priority.
```

### Test 5 — Recovery

Stop the load.

Verify:

```text
SATURATED
   ↓
BUSY
   ↓
NORMAL
```

without oscillation.

---

# 33. Optional Dual-vLLM Test on a Single 4070 Ti

For a small model, two vLLM processes may be run on one GPU with limited `gpu-memory-utilization`.

This is used **only as a functional test**:

```text
4070 Ti
 ├── vLLM A
 └── vLLM B
```

Such an environment does not model two real GPUs:

```text
A and B compete for a single CUDA device.
```

Therefore, throughput/latency results must not be used for production sizing.

---

# 34. MVP Load Generator

The repository must include a simple load generator.

Parameters:

```text
URL
API Key

parallelism
request count

prompt size
max tokens

stream true/false
```

Additionally:

```text
traffic mix
```

For example:

```text
critical     5%
high        20%
normal      35%
background  40%
```

The load generator must output:

```text
requests
successful
429
5xx

TTFT p50/p95/p99
latency p50/p95/p99
```

---

# 35. MVP Acceptance Criteria

The MVP is considered complete when the following conditions are met.

### Authentication

- unknown API key → `401`;
- revoked key → `401`;
- valid key → request is processed;
- the secret API key is not stored in plaintext.

### Model Access

- the client sees only the permitted models;
- a forbidden model → error.

### Routing

Given:

```text
backend A pressure = 0.3
backend B pressure = 1.1
```

100 new requests must not be routed to B while A is available, except in explicitly defined tie/weight cases.

### Health

When a backend fails, it is excluded from routing no later than the configured health timeout.

After recovery, the backend is automatically restored.

### Admission

Background traffic is limited before Normal/High/Critical traffic.

### Priority

The Gateway always overwrites `X-Vllm-Priority`.

### Streaming

SSE is proxied without full buffering.

### Cancellation

A client disconnect terminates the upstream request.

### Hysteresis

A brief metric spike does not cause persistent overload-state switching.

### Performance

On the fake backend, gateway-added latency on the local machine must remain negligible relative to LLM inference.

Target:

```text
p50 < 5 ms
p99 < 20 ms
```

at moderate concurrency.

This is an engineering target, not a contractual SLA.

---

# 36. Production Version

The production version is an evolution of the MVP, not a rewrite of the system.

The following interfaces are retained:

```text
ClientPolicy
ModelPool
Backend
BackendMonitor
AdmissionController
Router
```

The infrastructure implementation changes.

---

# 37. Production Topology

```text
                     ┌──────────────┐
                     │ Load Balancer│
                     └───────┬──────┘
                             │
               ┌─────────────┴─────────────┐
               ▼                           ▼
        Gateway replica A           Gateway replica B
               │                           │
               └─────────────┬─────────────┘
                             │
                  ┌──────────┴──────────┐
                  ▼                     ▼
              PostgreSQL             Valkey/Redis
                  │
                  │
         ┌────────┴─────────────────────────┐
         ▼                                  ▼
 Model Pool A                         Model Pool B
 ├─vLLM 1                             ├─vLLM 4
 ├─vLLM 2                             ├─vLLM 5
 └─vLLM 3                             └─vLLM 6
```

---

# 38. Production Gateway HA

The Gateway must support:

```text
2+ replicas
```

and be stateless with respect to HTTP requests.

Configuration:

```text
PostgreSQL
```

Distributed limits:

```text
Valkey/Redis
```

---

# 39. Distributed Concurrency Limiting

In the MVP:

```text
ConcurrentDictionary<ClientId, count>
```

is sufficient.

In production, this is incorrect with two Gateway replicas:

```text
Gateway A thinks client = 10
Gateway B thinks client = 10

actual = 20
```

Therefore, concurrency leases must be stored centrally.

Recommended implementation:

```text
Redis/Valkey atomic operation
```

with a TTL.

Each active request creates a lease:

```text
clientId
requestId
expiresAt
```

A streaming connection periodically renews its lease.

If the Gateway crashes, the TTL prevents the concurrency slot from leaking indefinitely.

---

# 40. Production Rate Limits

Add:

```text
MaxConcurrency
RequestsPerMinute
TokensPerMinute
```

Optionally:

```text
DailyTokens
MonthlyTokens
```

Initially, TPM may be calculated from the actual `usage` of completed requests.

A perfectly accurate pre-request token count estimate is not required.

---

# 41. Production Client Policy

Production Client:

```text
PriorityClass

MaxConcurrency
RequestsPerMinute
TokensPerMinute

AllowedModels

Enabled
```

Additionally, a per-model override:

```text
ClientModelPolicy

ClientId
ModelId

PriorityOverride?
MaxConcurrency?
TokensPerMinute?
```

---

# 42. Production Routing — Heterogeneous GPUs

Production must account for differences among backends:

```text
RTX 4090
RTX 5090
A100
H100
2×H100
```

The Backend receives:

```text
CapacityWeight
```

For example:

```text
backend-a = 1.0
backend-b = 1.0
backend-c = 2.0
```

When pressure is nearly equal, routing must distribute traffic according to capacity weight.

However, when there is clear overload:

```text
live pressure
```

takes precedence over static weight.

---

# 43. Production Soft Session Affinity

> **Current implementation status:** this capability is already delivered. `X-LLM-Session-Id` is limited to 256 bytes and is combined with the authenticated client ID and model-pool ID for deterministic rendezvous hashing. The winner is computed after the ordinary eligibility filters, matching the routing order below; an ineligible backend is therefore removed before rendezvous hashing. When the eligible winner's live pressure is at or above `LLMGW_SESSION_AFFINITY_MAX_PRESSURE` (default `1.0`), routing falls back to the least-pressured eligible backend. The header is not forwarded upstream, logged, or used as a metric label. This does not implement a KV block index or inspect cached prefixes.

Cache locality is useful for LLMs.

If consecutive requests from the same agent session are routed to different vLLM replicas each time, some of the benefit of the prefix cache may be lost.

In larger systems, this is already significant enough that llm-d separately implements prefix-aware routing on top of vLLM ([llm-d routing documentation](https://github.com/llm-d/llm-d/blob/main/docs/well-lit-paths/workloads/multimodal-serving.md)).

The lightweight Gateway should implement a simpler approach.

Optional header:

```text
X-LLM-Session-Id
```

When it is present:

```text
clientId
+
model
+
sessionId
```

are used for rendezvous hashing.

The selected backend becomes the preferred backend.

However, affinity is **soft**:

```text
preferred backend healthy + not overloaded
        ↓
use preferred

preferred overloaded
        ↓
select less loaded backend
```

This will allow coding agents to be routed to the same inference replica more often.

---

# 44. Production Routing Order

Final algorithm:

```text
1. filter by model pool
2. remove disabled
3. remove draining
4. remove unhealthy
5. check soft session affinity
6. evaluate live pressure
7. account for capacity weight
8. select backend
```

---

# 45. Production Circuit Breaker

A backend's inference circuit counts these qualifying failures:

```text
connection, DNS, TLS, or response-header failures
upstream 5xx responses
upstream response-body read failures
```

Health-check failure and stale metrics independently remove a backend from routing eligibility and inference readiness. They do not add inference-circuit failures, open or reopen the circuit, or heal it when health/metrics recover.

States:

```text
Closed
Open
HalfOpen
```

In HalfOpen, a limited number of probe requests are routed to the backend.

The single-replica implementation now provides this inference circuit with exact defaults:

```text
LLMGW_CIRCUIT_FAILURE_THRESHOLD=5
LLMGW_CIRCUIT_FAILURE_WINDOW=30s
LLMGW_CIRCUIT_OPEN_COOLDOWN=15s
LLMGW_CIRCUIT_HALF_OPEN_MAX_PROBES=1
```

In `closed`, qualifying failure timestamps are retained only inside the rolling window. A successful closed-state request does not clear that retained history; timestamps age out with the window. Threshold failures open the circuit. Cooldown expiry exposes bounded half-open probe capacity; a probe success closes and clears failures, a probe failure reopens immediately, and a neutral result only releases its probe slot.

Outcome precedence is explicit: connection/DNS/TLS/response-header failures, upstream HTTP `5xx`, and upstream response-body read failures are failures. A completed upstream response below `500`, including `4xx` and `429`, is success because the endpoint responded. Downstream cancellation or write failure is neutral and neither penalizes nor heals. An HTTP `5xx` is forwarded without retry; the existing single retry applies only to a transport failure before downstream response bytes.

---

# 46. Production Overload Protection

Add global safety limits for each ModelPool:

```text
MaxGatewayInflight (integer >= 0; 0 = unlimited)
MaxWaiting (integer >= 0; 0 = disabled)
```

They are intended to protect the Gateway itself and the upstream service from unbounded accumulation of HTTP connections.

The Gateway **must not create its own huge queue**.

Core philosophy:

```text
small/no gateway queue
+
fast 429
+
Retry-After
```

It is better to reject a background request than to keep 10,000 HTTP connections waiting for a GPU.

This protection is implemented for one gateway process. Pool fields are exposed through SQLite, Admin JSON/forms, immutable registry snapshots, and runtime dashboards. Distributed enforcement across gateway replicas remains out of scope.

Inference capacity has its own unauthenticated `GET /inference-readyz`: HTTP `200`/`status: ready` when at least one enabled pool has a healthy, metrics-fresh, secret-ready, non-draining backend whose circuit has capacity; otherwise HTTP `503`/`status: unavailable`. The body includes configuration `revision`, `poolAvailability`, and `backendAvailability`. Pool congestion does not make inference readiness flap. `GET /readyz` remains separate management-plane readiness and stays HTTP `200` during an inference outage.

---

# 47. Anti-Starvation

Production must provide a way to ensure a minimum level of service for Background traffic.

Optional parameter:

```text
BackgroundMinShare = 5%
```

However, this is not included in the initial production release.

By default, QoS follows the principle:

> Interactive traffic is more important than background throughput.

---

# 48. Production Admin Authentication

Replace the MVP admin auth with:

```text
OIDC
```

Roles:

```text
Viewer
Operator
Administrator
```

### Viewer

```text
view dashboard
view clients
view backends
```

### Operator

```text
drain/resume backend
disable client
```

### Administrator

```text
create keys
modify policies
change backends
change thresholds
```

---

# 49. Audit Log

Production must record:

```text
who
when
action
object
old value
new value
```

For example:

```text
2026-08-26T12:00

admin@example
changed client codex-ci
Priority Normal → Background
```

---

# 50. API Key production requirements

Add:

- expiration;
- rotation;
- multiple keys per client;
- immediate revoke;
- last used timestamp;
- key description.

For example:

```text
codex-team
 ├── developer-laptops
 ├── CI
 └── staging
```

---

# 51. Production secret management

Upstream vLLM API keys and database credentials must not be stored as plaintext in the shared configuration DB.

Support retrieving secrets from:

```text
environment
Kubernetes Secret
external secret manager
```

---

# 52. Network security

vLLM servers must be located in a private network.

Client:

```text
can only access the Gateway.
```

Gateway:

```text
can access vLLM.
```

Direct client → vLLM:

```text
blocked.
```

This is especially important because vLLM itself has operational and inference endpoints that are not always protected by its built-in API-key middleware ([vLLM security guidance](https://docs.vllm.ai/en/stable/usage/security/)).

---

# 53. Production Observability

Support:

```text
Prometheus
OpenTelemetry traces
structured logs
```

The Grafana dashboard must show:

### Cluster

```text
requests/sec
tokens/sec

p50/p95/p99 TTFT
p50/p95/p99 E2E

429 rate
5xx rate
```

### Per pool

```text
running
waiting
KV
pressure
available backends
```

### Per client

```text
inflight
RPS
tokens
429
priority
```

### Per backend

```text
running
waiting
KV
pressure
latency
errors
```

---

# 54. Alerts

At minimum:

```text
NoHealthyBackend
BackendUnhealthy
PoolSaturated
High429Rate
HighTTFT
HighBackend5xx
MetricsStale
RedisUnavailable
DatabaseUnavailable
```

---

# 55. Behaviour when Redis fails

The Production gateway must use a fail-safe strategy.

For Critical/High:

```text
optionally fail-open
```

with a local emergency limit.

For Normal/Background:

```text
fail-closed
```

or significantly reduced local concurrency.

The specific mode is configurable.

Goal:

> losing Redis must not allow background load to bring down the GPU cluster.

---

# 56. Behaviour when PostgreSQL fails

The Gateway must cache the last known configuration locally.

If PostgreSQL is temporarily unavailable:

```text
existing client policies continue working
new configuration changes unavailable
```

The Admin UI shows a degraded state.

---

# 57. Configuration versioning

Each configuration has a revision:

```text
ConfigRevision
```

Gateway replicas periodically check the revision or receive a notification.

Goal:

```text
changing a client's priority
```

must be applied to all gateway replicas without a restart.

---

# 58. Production deployment

Recommended deployment:

```text
Docker
```

and, if necessary:

```text
Kubernetes
```

However, the application must not have a hard dependency on Kubernetes.

It must work identically in:

```text
docker compose
bare VM
Kubernetes
```

---

# 59. Kubernetes integration

Production enhancement:

A backend can be registered through:

### Static

```text
URL manually configured
```

### DNS/service discovery

```text
Kubernetes Service
```

### API discovery

optionally at a later stage.

Static/DNS backends are sufficient for the MVP/first Production release.

---

# 60. Production Acceptance Criteria

## HA

Stopping one Gateway replica does not interrupt new connections to the service when a second replica is available.

## Distributed limits

A client's MaxConcurrency is enforced in aggregate across all replicas.

## Backend failure

An unhealthy backend is automatically excluded.

## Draining

A backend can be taken out of service without interrupting existing streams.

## Priority isolation

Under synthetic overload, Background traffic must not significantly degrade Critical traffic TTFT after admission control activates.

## Security

No vLLM backend is directly accessible from the client network.

## Secrets

API keys are not stored as plaintext.

## Observability

For any request, requestId must make it possible to determine:

```text
client
model
backend
priority
result
duration
```

without logging prompt content.

---

# 61. Performance targets Production

The Gateway must not become a noticeable part of inference latency.

Indicative target for the proxy layer itself:

```text
non-stream routing overhead:
p50 < 5 ms
p99 < 20 ms
```

when operating within a single datacenter/LAN and when the gateway itself is not saturated.

For streaming:

```text
the gateway must not intentionally batch/buffer SSE chunks.
```

The throughput target must be tested separately with fake backend load so that the GPU is not the test bottleneck.

---

# 62. Recommended project structure

```text
src/

  Gateway.Api/
      public OpenAI endpoints
      admin API
      middleware

  Gateway.Core/
      ClientPolicy
      AdmissionController
      Router
      BackendState
      PoolState

  Gateway.Vllm/
      VllmClient
      MetricsParser
      BackendMonitor

  Gateway.Persistence/
      SQLite/Postgres
      repositories

  Gateway.RateLimiting/
      local MVP implementation
      Redis production implementation

  Gateway.Web/
      admin UI

tests/

  UnitTests/
  IntegrationTests/
  LoadTests/

tools/

  FakeVllm/
  LoadGenerator/
```

Core must not depend on:

```text
ASP.NET
SQLite
Redis
Prometheus
```

For example:

```text
AdmissionController.Decide(
    clientPolicy,
    poolState,
    currentUsage
)
```

is pure business logic and is easy to unit-test.

---

# 63. Key interfaces

Example logical contract:

```text
IBackendRegistry

GetCandidates(model)
```

```text
IBackendMonitor

GetState(backend)
```

```text
IAdmissionController

Decide(client, pool)
```

```text
IRouter

SelectBackend(client, request, candidates)
```

```text
IConcurrencyLimiter

Acquire(client)
Release(client)
```

```text
IVllmProxy

Forward(request, backend)
```

This will allow:

```text
LocalConcurrencyLimiter
```

to be replaced with:

```text
RedisConcurrencyLimiter
```

without rewriting the proxy.

---

# 64. Development stages

## Stage 1 — Proxy foundation

Implement:

```text
OpenAI API proxy
API keys
client lookup
streaming
cancellation
```

One backend.

---

## Stage 2 — Multi-backend

Add:

```text
ModelPool
Backend
health monitoring
metrics scraping
routing
```

FakeVllm.

---

## Stage 3 — QoS

Add:

```text
PriorityClass
X-Vllm-Priority
PoolPressure
AdmissionController
MaxConcurrency
429
hysteresis
```

---

## Stage 4 — UI

Add:

```text
Clients
Keys
Models
Backends
Dashboard
```

This results in the **MVP**.

---

## Stage 5 — Real GPU validation

RTX 4070 Ti:

```text
real vLLM
load generator
queue saturation
priority test
stream test
recovery test
```

Select default thresholds.

---

## Stage 6 — Production foundations

Replace:

```text
SQLite → PostgreSQL

local concurrency
→ Redis/Valkey leases
```

Add:

```text
multiple gateway replicas
OIDC
audit
advanced metrics
distributed circuit/pool coordination (single-replica circuit breaking and pool safety are implemented)
```

---

## Stage 7 — Production routing

Add:

```text
capacity weights
soft session affinity
rolling drain
distributed configuration
```

After this, the system is considered Production V1.

---

# 65. What constitutes the MVP

Minimum practically useful product:

```text
1 Gateway instance
SQLite
Web UI

Clients
API keys
Priorities
MaxConcurrency

Model pools

N vLLM backends

/metrics monitoring

least-pressure routing

automatic background throttling

vLLM priority propagation

streaming

health checks

Prometheus metrics

FakeVllm
LoadGenerator
```

It must be possible to develop and test it entirely at home on an RTX 4070 Ti.

---

# 66. What constitutes Production V1

```text
2+ Gateway replicas

PostgreSQL
Redis/Valkey

OIDC
RBAC
Audit

distributed concurrency/rate limiting

Model pools
heterogeneous backends
capacity weights

soft session affinity

coordinated circuit breaker across replicas

draining

Prometheus
OpenTelemetry
Grafana-ready metrics

production network isolation
secret management
```

---

# 67. Core design principle

The system must not attempt to become a new vLLM scheduler.

Responsibilities are divided as follows:

```text
Gateway

WHO can use GPUs?
WHEN should request be admitted?
WHICH vLLM replica should receive it?
WHAT client priority should it have?


vLLM

HOW should GPU execute admitted requests?
HOW should batches be formed?
HOW should KV cache be managed?
HOW should requests be scheduled internally?
```

This is a fundamental architectural boundary.

---

# 68. Final target architecture

```text
                      CLIENT POLICY
                          │
            ┌─────────────┼──────────────┐
            │             │              │
            ▼             ▼              ▼
         Critical        Normal       Background
            │             │              │
            └─────────────┼──────────────┘
                          │
                          ▼
                ┌─────────────────┐
                │ Admission       │
                │ Controller      │
                └────────┬────────┘
                         │
                         ▼
                 ┌──────────────┐
                 │ Model Pool   │
                 └──────┬───────┘
                        │
                    Router
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
       vLLM A         vLLM B        vLLM C
       pressure       pressure      pressure
        0.23           0.71          1.34
          │
          ▼
    selected backend
          │
          ▼
 X-Vllm-Priority: -10
          │
          ▼
    vLLM scheduler
```

**The Gateway's primary product function is not conventional round-robin load balancing, but serving as a QoS layer for scarce GPU resources.**
