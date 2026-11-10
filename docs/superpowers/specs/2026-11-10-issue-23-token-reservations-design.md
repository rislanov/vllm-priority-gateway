# Issue #23: Request-size-aware admission — specification

Status: draft for approval; implementation has not started. Date: 2026-11-10.
Issue: https://github.com/rislanov/vllm-priority-gateway/issues/23
Branch: `codex/issue-23-token-reservations`.
Base: `c072e66466d68088c924b94421af3d7c9646d171` (`origin/main`).

The user confirmed #23 and explicitly required **preserving emergency admission**. The other decisions below are the specification author's proposals, not previously approved user requirements.

## 1. Objective and boundaries

Prevent too many large requests from being admitted concurrently before their actual consumption appears in soft TPM. Before generation, estimate input, bound output, and atomically reserve `R` tokens for the entire inference lifecycle. Apply limits by model, priority class, and client. PostgreSQL coordinates replicas; SQLite remains a single-replica deployment with in-memory coordination.

Existing client concurrency, pool inflight, High/Critical slot reserve, RPM, soft TPM, waiting limits, backend eligibility, and circuit protection remain in effect. Caller-supplied priority and token estimates are untrusted.

This does not promise GPU/KV allocation, preemption, exactly-once inference, a fixed TTFT, or global isolation during PostgreSQL outages. It adds no queue, local tokenizer library, prompt storage, new billing system, or analytics redesign. Public `/tokenize` retains its separate lifecycle and existing fail-closed behavior.

## 2. Approaches considered

| Approach | Benefit | Cost / decision |
| --- | --- | --- |
| Separate outstanding-token budget; soft TPM based on actual usage | Compatible with existing quotas; one lease per request | Recommended. Bounds unfinished work rather than reserving RPM/TPM |
| Prepay from TPM and refund the difference | Bounds per-minute consumption before dispatch | Changes the existing contract and requires debt/refund rules for refill, revision changes, and unknown usage; a separate extension |
| Input/output limits only | Simple integration | Does not bound the total work admitted concurrently; insufficient for #23 |

Emergency admission is preserved through separate per-replica token caps. The user rejected removing emergency admission to enforce a global budget.

## 3. Current architecture and change points

1. `internal/gateway/forward.go`: auth/snapshot → response-completion reservation → repeated auth → prepare payload → pool gate → admission → backend selection → proxy → deferred completion. Network estimation must not run under a registry lock or SQL transaction.
2. `internal/gateway/forward_admission.go`: builds `AdmissionRequest`, reloads stale configuration once, and uses `EmergencyAdmission` for transient coordination failures. Payload and estimate must be recomputed after reloading.
3. `internal/coordination/local/admission.go`: mutex, leases, receipts, and client/pool counters; RPM is debited at admission, TPM from valid usage in `Complete`. Nil usage currently does not debit TPM.
4. `internal/coordination/postgres/admission.go`: receipt and fingerprint, pool → client locks, fresh READ COMMITTED counts, and lease/RPM writes in one durable transaction. An ambiguous-commit retry uses the same LeaseID. Completion is serialized through the same scopes.
5. `internal/coordination/lease_manager.go`: renewal, bounded completion delivery, and partial-prefix retry. Completion-backlog overflow stops tracking; TTL provides eventual release, and actual TPM usage may never arrive.
6. `internal/proxy/tokenization.go`: `Proxy.Tokenize` already provides a shared upstream adapter. `Service.Tokenize` is the public utility lifecycle and must not be called recursively from inference.
7. Utility tokenization has separate client/backend concurrency and RPM limits with a shared PostgreSQL budget; defaults are 4 / 8 / 120 RPM with a 30s timeout. Internal preflight must account for its load.
8. `internal/coordination/postgres/replica.go`: coordination contract version is currently 3; changing persistent admission semantics requires a new contract version and upgrade evidence.

Required verification lead: local `Renew` calls `removeLease` when ClientID/PoolID does not match. Before adding token counters, a regression test must prove that a foreign identity cannot release another request's reservation. This also relates to #43; comprehensive lifecycle QA is outside #23.

## 4. Configuration contract

All token amounts are `int64`, in the range `0..10^12`; use checked arithmetic and no floating-point reservation accounting. The JSON field names below are normative. New fields are absent or zero in migrated configurations.

### Pool: `requestSizeAdmission` object

| Field | Value / rule |
| --- | --- |
| `enabled` | Defaults to false; master switch for this pool |
| `maxOutstandingTokens` | >0 when enabled; shared pool budget |
| `maxInputTokens`, `maxOutputTokens`, `defaultOutputTokens` | >0 when enabled; default ≤ maxOutput |
| `classLimits` | Fixed critical/high/normal/background fields containing maxInputTokens/maxOutputTokens/defaultOutputTokens; 0 means inherit |
| `fallbackMode` | Defaults to `reject`; optional `utf8_bytes` for self-contained text |
| `fallbackOverheadTokens` | >0 for utf8_bytes; the operator calibrates model/template overhead |
| `estimationProfile` | Nonempty identifier for a qualified tokenizer/template/model deployment when enabled; not automatically obtained proof of identity |
| `emergencyMaxOutstandingTokens` | 0 inherits maxOutstandingTokens **on each replica**; a positive value must be ≤ maxOutstandingTokens |

Client limits: `maxInputTokens`, `maxOutputTokens`, `defaultOutputTokens`, and `maxOutstandingTokens`, all defaulting to 0. The first three constrain requests in enabled pools; the last is the client's shared outstanding budget across all enabled pools and client API keys. Disabled pools do not participate in new token accounting; the UI must make this boundary explicit.

The effective input/output maximum is the minimum positive pool, class, and client value. Default precedence is client, then class, then pool; the final default is min(selected default, effective output maximum). An explicit output allowance above the maximum is an error, not a silent clamp. Pool/class/client updates validate ranges and relationships; if a cross-entity change temporarily makes a policy incompatible, admission rejects it as invalid/stale policy rather than removing limits.

Suggested configuration, **not a product default**: pool input 8192, output 2048, default 512, outstanding 32768; Background input 2048/output 256/default 128, client outstanding 8192. Calibrate these values for the target model.

With `enabled=false`, there is no preflight, new output rewriting, or token reservation; existing admission applies, including emergency and soft TPM. Changing false→true on an active pool requires draining previously admitted legacy leases: while any unexpired lease lacks token accounting, new enabled admissions receive retryable 503 `token_policy_transition`. New class/client limits do not recalculate existing token leases. Lowering a budget below outstanding usage blocks new admissions until capacity is released; existing requests are not interrupted. Changing true→false does not clear issued reservations: they complete under their stored lease contract. These old token leases remain in pool/client sums regardless of the current enabled flag; only new legacy leases issued after disabling are excluded.

For false→true, the operator must stop admission and drain **all replicas**, including legacy emergency streams, then enable the policy and resume traffic. The shared lease gate is additional protection, not distributed proof that emergency work has finished: PostgreSQL cannot see it. Each gateway tracks its own legacy emergency lifecycles by pool and returns `token_policy_transition` from the emergency path until they finish. This rejection does not fall back to legacy mode. Enabling the policy during an ongoing outage without a cluster-wide drain is unsupported.

## 5. Endpoint and payload contract

The support described below applies to **enabled** pools. Disabled pools retain existing pass-through behavior.

| Endpoint | Output limit | Input estimate |
| --- | --- | --- |
| `/v1/chat/completions` | `max_completion_tokens`; legacy `max_tokens` is accepted as an alias | `/tokenize` with messages and qualified template options |
| `/v1/completions` | `max_tokens` | `/tokenize` for string prompts; validated length for token-ID input; sum of individual items for batches |
| `/v1/responses` | `max_output_tokens` | Explicit `utf8_bytes` fallback for self-contained text; fallback=reject returns 400 `token_estimation_unsupported` |

A missing/null output limit receives the effective default. Values must be positive integers, without coercion from strings, booleans, or floating-point values. Two non-null chat aliases with different values return 400; equal values are normalized to `max_completion_tokens`, removing `max_tokens`. An output alias for the wrong endpoint or a conflicting `min_tokens` returns 400. The backend receives exactly the cap used for the reservation. Streaming follows the same rules.

Completions accepts a string, a nonempty token-ID array, or a nonempty homogeneous array of strings or token-ID arrays. IDs are nonnegative int64 values. Do not interpret a token-ID array as a batch. Allow at most 32 prompts and `n` from 1 to 32; use checked multiplication. Treat `n=null` as 1 only where upstream supports it. Validate the input limit separately for each prompt; aggregate input is their sum. Chat has one prompt. Responses does not support n.

Conservative formula: `R = n × (sum(inputEstimate[j]) + promptCount × effectiveOutputAllowance)`. It deliberately accounts for input per generation even if upstream can share it. Do not subtract input tokens for prefix-cache hits. Reject `best_of`, beam search, and unknown generation multipliers in v1 to avoid reserving less than the actual fan-out. An empty prompt may have a genuine zero count, but the accounting minimum per prompt is 1; distinguish that from an estimator failure.

In v1, an enabled policy rejects media/remote files, `previous_response_id`, server-side prompt references, Responses `background=true`, and server-side tool execution: the gateway cannot know the full input or generation lifetime. Self-contained text and inline function-call history are allowed only through a qualified adapter/fallback. The gateway does not invoke external tools. These rejections are documented opt-in compatibility boundaries, not hidden limit bypasses. Add new payload variants through explicit contract fixtures.

Reject `truncate_prompt_tokens`, Responses truncation=auto, and other input-truncation overrides in enabled v1: admission must not count one prompt and dispatch another. A client-supplied chat template is allowed only if the tokenizer adapter forwards identical options; unproven equivalence, including model-specific tool/response-format rendering, requires explicit fallback or rejection.

### Normative v1 input-field matrix

| Fields / shape | Exact adapter | Fallback canonical input |
| --- | --- | --- |
| Chat `messages` with system/developer/user/assistant roles and string content or text-only parts | Forward messages unchanged; no tool/function history | Entire messages, including roles and text parts |
| Chat `add_generation_prompt`, `continue_final_message`, `add_special_tokens`, `chat_template`, `chat_template_kwargs` | Forward all present fields; materialize true/false/false defaults for the first three when absent; if continue_final_message=true and the generation flag is absent, materialize generation=false in both requests | All these fields with the same effective defaults |
| Chat tools/tool_choice, response_format, function_call/functions; messages containing tool_calls/tool role/function role/reasoning fields | Not exact in v1: explicit fallback or rejection | Complete structures together with messages; do not extract only arguments/content |
| Completions string prompt | Forward prompt and effective add_special_tokens (default=true) | Object containing prompt and effective add_special_tokens |
| Completions token-ID prompt | Count validated IDs without calling the tokenizer; do not append special tokens | No fallback needed |
| Completions batch | Process each prompt according to the preceding two rows | One canonical input per prompt |
| Responses string input / inline message items with input_text/output_text; inline function_call/function_call_output | No direct `/tokenize` mapping | Complete input, instructions, tools (type=function only), tool_choice, text, reasoning, chat_template_kwargs |
| Responses previous_response_id, prompt reference, previous_input_messages, background=true, non-function tools | Reject | Reject |
| Media content, URL/file references, prompt_embeds, mm_processor_kwargs/media_io_kwargs, multimodal or hidden history | Reject | Reject |

Exact chat qualification remains specific to the model profile: matching fields alone do not prove renderer equivalence; fixtures must compare the profile's count with generation usage. Completions token-ID input requires a backend profile in which generation does not add hidden tokens to supplied IDs; otherwise reject it.

Top-level fields allowed as sampling/response-only in v1 and excluded from canonical input: model (recorded in the profile), stream, stream_options, output aliases, n, min_tokens, temperature, top_p, top_k, min_p, seed, stop, stop_token_ids, frequency_penalty, presence_penalty, repetition_penalty, ignore_eos, logprobs, top_logprobs, logit_bias, echo, skip_special_tokens, spaces_between_special_tokens, include_stop_str_in_output, return_token_ids, return_tokens_as_token_ids, user, metadata, store, service_tier. The output/multiplier validator checks their effects on hidden generation work; `service_tier` does not change the trusted priority class. Request-level priority is removed as today. Any other top-level field in enabled v1 is rejected with `token_estimation_unsupported` until explicitly classified. Normalize JSON null in optional input fields to upstream baseline defaults; reject null for required input. Responses accepts absent/false background and absent/disabled truncation; these are not input fields. Chat response_format always requires fallback, even without tools.

Build canonical JSON from the fixed field allowlist above using deterministic serialization, no whitespace, and UTF-8 rather than HTML escaping. Preserve nested input structures in full; validate nested media/remote references before serialization. Each batch object is independent. This table intentionally limits v1 rather than promising support for every upstream extension.

### Fallback

`utf8_bytes`: `I = max(1, UTF8ByteLength(canonicalInputJSON) + fallbackOverheadTokens)` per prompt. Canonical input includes all input-bearing fields, roles, instructions, history, tools/schema, and template options; it excludes credentials, telemetry IDs, and sampling-only fields. An unknown input-bearing extension is unsupported, not silently omitted. For Responses, canonical input includes instructions and all inline input. The byte multiplier is fixed at 1; overhead is positive and calibrated.

This is an estimate for an explicitly enabled fallback, **not a mathematical guarantee of an arbitrary model's actual token count**. The input limit applies to the estimate in this mode. Exact model-token enforcement requires a qualified upstream adapter. Do not activate fallback automatically after timeout/429/5xx: that would silently fail open and bypass utility capacity. Fallback applies only to an explicitly unsupported model/payload/endpoint (including a verified 404/501); upstream 401/403 is a configuration error returned as 503, not an unsupported case.

## 6. Estimator and routing

1. After auth, model access, payload/output validation, and the pool precheck, run bounded preflight before inference admission, without inference counters/circuit permits.
2. Normal mode uses the existing TokenizationCoordinator and `Proxy.Tokenize`. Shared client/backend utility limits cover public tokenize plus internal preflight. Debit utility RPM once per acquired preflight operation; a batch of prompts performs multiple operations sequentially. Do not debit inference RPM before successful inference admission.
3. Add `LLMGW_ADMISSION_ESTIMATE_TIMEOUT=2s`, range (0,30s]; this is the total deadline for the entire preflight, not one prompt. Limit responses to 8 MiB (`LLMGW_ADMISSION_ESTIMATE_MAX_RESPONSE_BYTES`, range 1 KiB..64 MiB); bound decoding and close the body on every outcome. Do not retain tokens/token_strs; only count and max_model_len are needed outside the adapter.
4. Validate count ≥0, max_model_len >0, integer bounds, and count/tokens consistency when tokens are present. Missing/null count or malformed/truncated responses return 503 `token_estimation_unavailable`. `I+O` for each prompt must not exceed the known model context; do not disguise this error as overload.
5. Do not treat `/tokenize` as universal JSON pass-through: the adapter builds a version-specific request from inference fields using the same chat-formatting defaults. The compatibility baseline is vLLM v0.28.0, pinned in compose; other versions require qualification fixtures.
6. A pool must be homogeneous in model/tokenizer/template/options. `estimationProfile` records an operator assertion verified by acceptance runs on every backend. An unqualified profile must not be enabled operationally; the gateway cannot prove identical hidden server settings from the model name alone.
7. Preserve normal routing/affinity and one pre-byte retry within an unchanged pool/profile. An estimate from one backend applies to another only within that profile. A pool model/profile/revision change during preflight triggers at most one re-resolution/re-estimation, then a retryable 503. Never retry Acquire with a new UUID after an ambiguous admission outcome in normal mode.
8. Before dispatch, check current auth/access, pool/profile, and backend identity. After successful admission, new size-policy limits do not apply retroactively to that lease; a model/profile change prevents dispatch of the old payload. A retry does not create another token reservation.

## 7. Atomic accounting and lifecycle

Add immutable `TokenReservation` to `AdmissionRequest` (enabled, estimate, output allowance, prompt count, n, reserved total, estimator kind/profile, client/pool limits, policy revisions). Include these fields in the fingerprint. Do not put the raw payload or even its hash in durable storage. The normal lease and reservation use one admission transaction/critical section, not two independently successful operations.

With budgets enabled:

```
poolOutstanding + R <= poolBudget
clientOutstanding + R <= clientBudget  // if clientBudget > 0
```

Budget exhaustion returns 429 `token_budget_exhausted` with Retry-After; bounded telemetry records scope=client/pool. `R` greater than the entire budget also returns 429, explaining that the request must shrink or policy must change: Retry-After does not promise sufficient future capacity. Check all limits before RPM debit/lease insertion. Compute sums in int64 with overflow guards; cast PostgreSQL SUM(bigint) only after range validation.

| Event | Outstanding | TPM / evidence |
| --- | --- | --- |
| Successful Acquire | +R exactly once | RPM once; TPM remains soft |
| Duplicate Acquire with the same fingerprint | Unchanged | Returns the same live lease / terminal rejection |
| Changed fingerprint | Unchanged | Idempotency conflict |
| Generation retry | Unchanged | Same lease, separate backend attempt |
| Renew | Extend R's lifetime without changing its amount | An invalid owner changes no fields |
| Complete with valid usage | −R once | Actual input+output debit soft TPM once; record actual−R difference |
| Complete without valid usage / cancellation / transport error | −R once after the local lifecycle finishes | Usage=unknown, not actual=0; soft TPM does not invent consumption |
| Lost completion / replica crash | R remains until TTL | Excluded from sums after TTL independently of cleanup; usage unknown |
| Late completion after expiry | No second outstanding decrement | With a retained receipt, actual TPM can be applied once as today |
| Duplicate completion | Unchanged | First terminal completion wins; later alternative usage does not overwrite it |

Conservative accounting means retaining all of R without intermediate refunds until terminal completion/expiry; an estimation error does not automatically reduce the reservation. Actual>R does not create negative counters or undo a delivered response; it is a telemetry/calibration failure. Success after early EOS releases all of R, not R−actual: completed work is no longer outstanding.

Missing usage does not imply guaranteed free consumption: TPM remains soft, cancellation can hide consumption, and TTL does not prove that GPU work has stopped. These limitations remain explicit. Requiring prepaid TPM for unknown usage would select a different approach, not an implicit part of this implementation.

PostgreSQL stores the reserved amount on request_leases, with immutable metadata and reconciliation outcome on admission_operations. Counts use expires_at>database-now under the existing pool→client locks. There is no separate aggregate-counter table to repair after a crash. Local coordination stores client/pool sums and removes exactly the stored R through the common removeLease path. All Renew/Complete operations validate LeaseID, ClientID, PoolID, RequestID, and ReplicaID against the stored owner, but not ExpiresAt from an old handle (renewal changes it).

Local receipt history is already bounded by capacity/retention. For enabled reservations, eviction must not permit replay of an otherwise valid operation timestamp: maintain a bounded per-process replay cutoff and reject older/equal timestamps after eviction. First look up an existing receipt/live lease and validate the fingerprint, then apply the cutoff only to new operations. Advance the cutoff to the maximum OperationStartedAt of evicted token receipts, but do not evict a receipt with OperationStartedAt>now until its time arrives. If no eligible terminal eviction candidate exists, return coordination_unavailable without advancing the cutoff into the future. Thus, allowed future skew of up to 5m does not block all fresh admissions for 5m; capacity exhaustion remains an explicit rejection. A new operation whose timestamp equals the cutoff receives stale_operation; the gateway does not automatically retry it with a new ID. A wall-clock regression below the cutoff may temporarily reduce availability; safety takes precedence over resurrection. Legacy-only requests retain their prior replay contract but cannot evict protected token receipts outside these rules. PostgreSQL retains its 24h receipt retention. Full parity of terminal-history retention is not promised.

## 8. Emergency — user requirement

During recoverable outages or while the recovery gate is active, High and Critical retain admission within existing class/client concurrency caps. Normal/Background do not receive emergency admission. Permanent consistency faults still prohibit new admissions. The new mechanism does not restore readiness or bypass revoked/disabled identities from the latest trusted snapshot.

For enabled pools, emergency admission atomically checks existing emergency concurrency plus `R` by client and pool **in one local critical section**. The pool cap is emergencyMaxOutstandingTokens or pool maxOutstandingTokens; the client cap is min(positive client budget, pool emergency cap), or the pool emergency cap when client=0. Sums cover only this replica's emergency requests, including those surviving recovery. Release is idempotent. Returning to normal mode does not clear emergency counters or retroactively move them into PostgreSQL.

Outage preflight does not depend on unavailable PostgreSQL: use a separate internal emergency tokenizer limiter, client concurrency=1, backend concurrency=1, client RPM=30 with burst=1, total estimate timeout=2s (or a smaller configured normal timeout). Preserve backend health/auth/secret checks. Configure these defaults through `LLMGW_ADMISSION_EMERGENCY_ESTIMATE_CLIENT_MAX_INFLIGHT`, `...BACKEND_MAX_INFLIGHT`, and `...CLIENT_RPM`, using the existing TokenizationPolicy ranges; include them in the fingerprint. Public `/tokenize` remains on the normal coordinator and receives no emergency mode.

Emergency tokenization does not modify inference circuits, record inference usage, or debit durable utility RPM. It provides additional per-replica utility capacity during outages and must not be advertised as a shared utility cap. If tokenization is unavailable or the selected policy cannot produce an estimate, that request receives 503; preserving emergency admission does not bypass size validation.

If normal TokenizationCoordinator.Acquire loses database access before upstream HTTP dispatch, the gateway may transition normal→emergency exactly once for eligible High/Critical after rechecking recoverable status. Permanent faults, utility 429, upstream timeout/5xx, and token_policy_transition do not permit this transition. The total preflight deadline starts before the first attempt and is never reset. Already obtained batch estimates may be reused only with the same snapshot/model/profile; the prompt whose acquisition failed receives a new emergency **utility** operation ID, and the normal utility ID is not replayed through the local coordinator. An unresolved durable utility lease may remain until TTL; this is part of the documented overlap. If the deadline is exhausted, return 503 rather than retrying without bounds. After this transition, the entire inference request uses an emergency reservation even if readiness recovers; repeat permanent-fault/auth/profile checks before dispatch. If the outage starts after successful normal preflight, reuse the estimate for emergency without retokenizing when the profile is unchanged.

If normal Acquire ends with an ambiguous outcome, a distributed reservation may remain until TTL alongside one emergency lifecycle. The gateway starts generation at most once before its ordinary upstream retry; it does not perform another normal allocation for that request. This overlap reduces available distributed capacity but does not apply completion to an unknown lease again. Document and test ghost RPM/leases after ambiguous commits.

With N replicas, the steady shared budget and emergency budgets may coexist: the outstanding-bookkeeping reference is `B_shared + sum(B_emergency_replica)`, while policy reductions may leave old leases above the new caps. This is not a GPU bound. No global ceiling is promised during outage/recovery; the slot reserve and new token budget are different limits. A shared pool token budget also does not guarantee token headroom for High: #23 adds no separate High token reserve.

## 9. API, telemetry, and privacy

Errors use the existing OpenAI-shaped envelope. 400: `input_token_limit_exceeded`, `output_token_limit_exceeded`, `token_estimation_unsupported`, and invalid payload. 429: `token_budget_exhausted` or existing utility/concurrency/rate reasons. 503: unavailable estimation/coordination, stale policy/profile, and enable transition. Transient 429/503 responses include Retry-After; cancellation retains existing behavior before/after response commitment.

Add numeric-only reservation fields to gateway RequestEvent and a separate bounded accounting observer event (coordinator/expiry) to expose releases without HTTP completion. Metrics: estimated/reserved/actual token histograms, outstanding gauge by pool+mode, unknown-usage/underestimate/rejection counters, preflight latency, and normal/emergency mode. Obtain the distributed gauge from an authoritative snapshot; do not sum identical shared gauges across replicas. Labels: pool, class, estimator kind, scope, mode, bounded reason; no request IDs, fingerprints, prompt hashes, or arbitrary errors.

Do not populate existing actual-usage analytics with estimates. Duplicate receipt replay must not emit another accounting event. Log only numeric amounts and bounded outcomes. Do not store prompts, generated content, token strings/IDs, raw tokenizer responses, or credentials. Check sentinel strings in stdout, observer events, SQL metadata, and errors. Upstream input/output exists only in request-lifecycle memory; no prompt cache is added.

The Admin API and existing client/pool forms expose enabled state, effective limits, class overrides, fallback, and per-replica emergency semantics. Existing edits that do not know the new fields must not silently reset policy: omitted `requestSizeAdmission`/client token fields on update preserve previous values; explicit false/0 disables/resets them. Creation defaults to zero. Persisted policy is part of the snapshot/revision, not process-only environment configuration.

## 10. Acceptance matrix

| ID | Verifiable criterion | Evidence |
| --- | --- | --- |
| AC01 | Disabled/default preserves payload and existing admission/TPM/emergency behavior; no tokenizer calls | HTTP golden tests + existing suite |
| AC02 | Pool/class/client precedence, invalid values, persistence/reload, partial legacy updates | Domain + SQLite + PostgreSQL + Admin/form tests |
| AC03 | Exact chat/completion adapter, Unicode/tools/templates; explicit Responses fallback/rejection; missing/invalid count never becomes 0 | Payload fixtures + real-model comparison |
| AC04 | All three endpoints, streaming, output aliases/defaults, n/batch/overflow, and unsupported forms | Table-driven HTTP tests |
| AC05 | Client/pool token sums are atomic with concurrency/RPM; request replay allocates no second reservation | Shared local/PG contract, concurrent two-replica test |
| AC06 | Limit reductions, class/profile changes, revocation, and enable transitions do not bypass policy | Concurrent publication/DB mutation tests |
| AC07 | Complete/cancel/error/retry/renew/expiry/cleanup cause no leaks or double release; foreign owners cannot mutate leases | Shared lifecycle matrix + race tests |
| AC08 | Missing usage, under/overestimation, completion-backlog drops, late/duplicate completion follow specified semantics | Fault injection, fake clock, bounded backlog |
| AC09 | Ambiguous Acquire/Complete and partially committed batches do not double-charge; evicted receipts cannot resurrect | Extended commit-loss proxy + local replay tests |
| AC10 | High/Critical emergency admission respects local token/concurrency caps; Normal/Background reject; tokenizer works without DB; recovery preserves active emergency accounting | Service + two HTTP gateways + outage/recovery |
| AC11 | Tokenizer is bounded by time/body/concurrency/rate, without generation circuit/analytics side effects | Slow/malformed/oversized-response and saturation tests |
| AC12 | Distinct reason/scope/mode, unknown != zero, correct shared gauge, and no content in persistence/logs | Observer/Prometheus/privacy fixtures |
| AC13 | Zero migration defaults, contract-version incompatibility, drain upgrades/downgrades, and rollback without losing active reservations | Migration + startup tests |
| AC14 | Repeatable comparison of High admission and TTFT under short-High/long-Background traffic on real vLLM | Real-vLLM report: model/version/hardware/config/seed, enabled vs disabled |

Deterministic accounting example: Bpool=1000, Bclient=800, R=600 → admit the first request, reject a second from that client; admit another client's R=400 to reach exactly 1000; reject R=1; completing the first releases 600, replaying completion releases 0. High does not bypass total token/client/RPM/TPM limits.

## 11. Real-vLLM qualification and rollout

For AC14, pin model/tokenizer/template revisions, vLLM image digest, hardware, scheduling/cache configuration, gateway commit, topology, and all budgets. Run paired disabled/enabled series with identical seeded prompts and load schedules, 3 repetitions per mode after warm-up, and at least 100 High probe attempts per repetition. Record attempted/admitted/rejected counts by reason, TTFT p50/p95/p99 **for completed High requests**, end-to-end latency including preflight, preflight latency, Background throughput, estimation error, missing usage, and peak outstanding. Do not compare TTFT only for surviving successful requests without reporting the admission fraction.

The scenario must actually hit the token budget before utility RPM or concurrency limits; tune the workload and record calibration before comparing modes. Hard pass conditions: expected token denials, eligible High admission when all budgets permit it, enforced output caps, and recovered accounting; numerical TTFT improvement is not guaranteed. The report must show the admission/TTFT change and explain any regression. A synthetic backend does not replace GPU evidence. Without a real-vLLM run, AC14 remains pending and the issue must not be declared fully complete.

Migrations: SQLite `006_request_size_admission.sql`; PostgreSQL `000007_request_size_admission.up/down.sql` (recheck numbering before implementation). Persist validated policy JSON/text while keeping domain structs comparable; do not add maps/slices to ModelPool, which is compared with `!=`. Use a fixed four-field struct for class overrides. PostgreSQL coordination contract changes 3→4. Drain/stop old processes before starting the new group; existing startup wait/unregister applies. Down migration must not remove nonzero active token leases or an unfinished contract. SQLite runtime counters are not durable; restart requires the old process to stop and does not provide HA deployment.

## 12. Sources and analysis limitations

Local source and tests at the stated base; Tier 2 graph navigation with exact source verification. Coverage was checked; no_recorded_issue means only that no gaps were recorded. The graph can conflate Go methods with the same name (for example Complete on different coordinators); material conclusions were confirmed by reading files.

Upstream baseline: [tokenize protocol v0.28.0](https://github.com/vllm-project/vllm/blob/v0.28.0/vllm/entrypoints/serve/tokenize/protocol.py), [chat protocol](https://github.com/vllm-project/vllm/blob/v0.28.0/vllm/entrypoints/openai/chat_completion/protocol.py), [completion protocol](https://github.com/vllm-project/vllm/blob/v0.28.0/vllm/entrypoints/openai/completion/protocol.py), [Responses protocol](https://github.com/vllm-project/vllm/blob/v0.28.0/vllm/entrypoints/openai/responses/protocol.py). These confirm the different request schemas/output aliases; behavior on the user's deployment has not been verified. New renderer APIs from latest documentation are not assumed available in the baseline and are not introduced as hidden dependencies.

Related plan: [implementation plan](../plans/2026-11-10-issue-23-token-reservations.md). Analysis and baseline evidence: [evidence](../../issue-23-analysis-and-evidence.md).
