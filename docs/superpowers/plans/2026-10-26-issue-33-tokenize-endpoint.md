# Authenticated Tokenization Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Expose authenticated POST /tokenize with model routing and an independent tokenization budget enforced across gateway replicas.

**Architecture:** Add a utility request path alongside inference. Reuse authentication, model registry, HTTP transport, and safe header handling, but give tokenization its own coordinator, leases, rate state, and telemetry. PostgreSQL enforces shared budgets atomically; the supported single-replica SQLite profile uses the same contract in memory.

**Tech Stack:** Go 1.27, net/http, chi, pgx/PostgreSQL, existing migration/registry infrastructure, Prometheus, Go contract/integration tests.

**Spec:** [GitHub issue #33](https://github.com/rislanov/vllm-priority-gateway/issues/33).
**Confirmed user decisions (2026-10-26):** Tokenization has a separate budget from generation; its limits are shared across all gateway replicas.
**Base:** `origin/main` at `d496a04615a8ea9e0f5f059ba319d3a58161ed54`.
**Branch:** `codex/issue-33-tokenize-endpoint`.
**Status:** Implemented and independently reviewed. Main `5ae4667` was merged first. Validation is recorded in [acceptance-evidence.md](../../acceptance-evidence.md#authenticated-tokenization--issue-33-2026-10-27).

## Global Constraints

- POST /tokenize only. No /v1/tokenize alias, /detokenize, automatic preflight token estimation, or local tokenizer. Issue #23 remains separate.
- Require the existing gateway bearer key and an accessible, enabled public model. Map it to the configured upstream model.
- Forward to the selected backend's /tokenize and preserve tokenization options/response fields. Gateway validates the JSON envelope and model; upstream validates model-specific tokenization/chat options.
- Do not call the inference Forward lifecycle, AcquirePool, AcquireBackend, inference admission/RPM/TPM, generation-priority reservations, or inference completion/analytics recorder for this endpoint.
- Do not infer token consumption from returned count/tokens/token_strs. No tokenization rows in inference analytics; no inference coverage or usage-counter changes.
- Tokenization has its own client concurrency, client request-rate, and backend concurrency limits. Multiple keys belonging to one client share that client's tokenization budget across all models and replicas.
- Saturated GPU pressure alone does not reject tokenization. Backend must still be enabled, non-draining, recently healthy, and have its configured upstream secret available.
- Use separate tokenization selection/telemetry; do not acquire, complete, heal, or probe an inference circuit. A failed tokenization request must not alter the generation circuit.
- No queue and no automatic upstream retry in the first version. One admitted operation selects one backend and makes at most one upstream HTTP request.
- Reuse request-body limits and transport settings. Add a bounded upstream/admission/delivery lifetime and release capacity on completion, cancellation, panic, or expiry.
- Fail closed with 503 on unavailable/ambiguous coordination; never replace a shared PostgreSQL budget with per-replica emergency capacity.
- Never log/persist prompts, token strings/IDs, request/response bodies, authorization values, or affinity identifiers.
- Only synthetic examples and public repository paths; no private domain, deployment prefix, identity, or screenshot.

## Policy and API decisions

The following are initial configurable defaults, not measured capacity claims:

| Environment setting | Default | Meaning |
| --- | --- | --- |
| LLMGW_TOKENIZE_CLIENT_MAX_INFLIGHT | 4 | Active tokenization requests per authenticated client |
| LLMGW_TOKENIZE_BACKEND_MAX_INFLIGHT | 8 | Active tokenization requests per configured backend |
| LLMGW_TOKENIZE_CLIENT_RPM | 120 | Sustained requests/minute per client |
| LLMGW_TOKENIZE_TIMEOUT | 30s | Admission plus upstream/delivery deadline |

Require positive values; cap concurrency at 10,000, RPM at 10,000,000, and timeout at 5m. RPM uses a token bucket with burst capacity min(RPM, 10), refill RPM/60 per second, and charges once on successful admission. Failed upstream calls still consume RPM; denied admission does not. Token counts never replenish or debit this bucket.

Each lease lasts timeout + 15s. Start the monotonic deadline before Acquire; the HTTP attempt and downstream write deadline must end before the lease can expire. No renewal is needed for this bounded utility operation. Capacity bounds active gateway HTTP operations; it cannot guarantee that a remote backend immediately stops computation after disconnect.

- 400: invalid JSON/object/model or body exceeding the existing public-handler limit.
- 401 / 403: existing invalid-key / model-not-allowed behavior.
- 429 with Retry-After: tokenization client concurrency, client rate, or backend capacity exhausted.
- 503: no healthy eligible backend, disabled utility dependency, configuration/coordination failure.
- 502: transport failure before response commit. 504: tokenization deadline expires before response commit.
- After response commit, abort a failed/truncated transfer; never append a second JSON error.
- Preserve upstream response status/body for upstream errors, including a backend that does not support /tokenize.

Configuration values become part of the existing replica policy fingerprint. Replicas with conflicting tokenization settings must fail the existing compatibility checks rather than admit with different cluster budgets. This extends the compatibility fingerprint and requires a coordinated rollout; do not promise mixed-old/new readiness.

## Review Focus

1. Simultaneous acquisitions on different replicas must not exceed client/backend budgets or spend RPM twice (Tasks 1–2).
2. A revoked key, disabled access, draining backend, or changed upstream model during admission must not reach the upstream (Tasks 2 and 4).
3. Cancelled, failed, or abandoned operations must release exactly once or expire; ambiguous commits must not create a second operation (Tasks 1–2 and 4).
4. A tokenization burst or malicious usage-shaped response must leave inference admission, circuits, metrics, and analytics unchanged (Tasks 3–5).
5. Slow/missing/unsupported upstreams and mismatched replica settings must have bounded, documented failure behavior (Tasks 2–5).

## Evidence and file map

Application routing in `cmd/gateway/application.go` forwards only /v1 paths to `internal/httpapi/public.go`; neither layer registers /tokenize. Existing `gateway.Forward` and `targetSelector.selectTarget` combine model checks with inference admission, circuit permits, and completion events, so registering the new path against `forward` is insufficient.

`compose.yaml` pins the development vLLM image to v0.28.0. Its [tokenization protocol](https://github.com/vllm-project/vllm/blob/v0.28.0/vllm/entrypoints/serve/tokenize/protocol.py) supports prompt and messages payloads and returns count, max_model_len, tokens, and optional token_strs. Its [router](https://github.com/vllm-project/vllm/blob/v0.28.0/vllm/entrypoints/serve/tokenize/api_router.py) registers POST /tokenize. Preserve optional upstream fields instead of duplicating that entire schema in Go.

New production files:
- `internal/coordination/tokenization.go`: utility coordinator contracts and policy.
- `internal/coordination/local/tokenization.go`: local contract implementation.
- `internal/coordination/postgres/tokenization.go`: transactional shared budget.
- `internal/coordination/postgres/tokenization_cleanup.go`: bounded expiry/receipt cleanup.
- `internal/store/postgres/migrations/000006_tokenization.up.sql` and matching down migration (renumber if another migration lands first).
- `internal/proxy/tokenization.go`: bounded pass-through without usage inspection.
- `internal/gateway/tokenization.go`: utility orchestration/model selection.
- `internal/httpapi/tokenization.go`: public HTTP adapter.
- `internal/observability/tokenization.go`: separate metrics and safe completion log.

Modify `internal/config/config.go`, `internal/coordination/postgres/replica.go`, `internal/gateway/service.go`, `internal/proxy/headers.go`, `internal/httpapi/public.go`, `cmd/gateway/application.go`, and corresponding tests. Add a shared contract test file, package tests for each new component, `tests/postgres/tokenization_test.go`, and `tests/e2e/tokenization_test.go`. No new admin UI or per-client database policy fields in this iteration.

### Task 1: Define the policy and independent local admission contract

**Interfaces in internal/coordination/tokenization.go:**

- `TokenizationPolicy`: ClientMaxInflight int, BackendMaxInflight int, ClientRPM int64, Timeout time.Duration; LeaseTTL derived as Timeout + 15s.
- `TokenizationCandidate`: BackendID, BackendRevision int64.
- `TokenizationAcquireRequest`: OperationID/ReplicaID uuid.UUID; RequestID string; OperationStartedAt time.Time; ConfigurationRevision/APIKeyID/ClientID/PoolID/PoolRevision int64; ordered Candidates []TokenizationCandidate.
- `TokenizationLease`: OperationID/ReplicaID uuid.UUID; ClientID/PoolID/BackendID/BackendRevision int64; AcquiredAt/ExpiresAt time.Time.
- `TokenizationDecision`: Lease *TokenizationLease; Reason string constrained to a documented finite set; RetryAt *time.Time.
- `TokenizationCoordinator`: Acquire(context.Context, TokenizationAcquireRequest) (TokenizationDecision, error); Complete(context.Context, TokenizationLease) error; Status() Status.
- Construct each coordinator with immutable TokenizationPolicy. No caller-supplied limit overrides in Acquire.
- Constructors: `local.NewTokenizationCoordinator(policy TokenizationPolicy, now func() time.Time) *TokenizationCoordinator` and `postgres.NewTokenizationCoordinator(store *pgstore.Store, policy TokenizationPolicy, timeout time.Duration) *TokenizationCoordinator` (qualify coordination types in the implementation packages).

- [x] Add shared contract cases in `internal/coordination/contracttest/tokenization.go`: two independent clients, multiple keys per client, all-model client limits, backend limits, deterministic backend choice by lowest active utility count then backend ID, RPM refill/burst, and zero inference state changes.
- [x] Assert repeat Acquire with the same operation/fingerprint returns the same decision without extra capacity/RPM; mismatched replay fails. Complete is idempotent; completed/expired operations cannot be resurrected. Reject operations older than 24h or more than 5m in the future, matching bounded receipt retention.
- [x] Run `go test ./internal/coordination/local -run Tokenization -count=1`; expect compile/assertion failures.
- [x] Implement the local coordinator with one mutex, injected clock, separate counters/rate state, expiry-aware counts, and bounded receipt storage (65,536 receipts; discard expired terminal entries only; return unavailable when full). Retain receipts for 24h and charge RPM only after a candidate is selected.
- [x] Add config parsing/default/range tests for the four environment variables. Reuse existing config error conventions and add TokenizationPolicy to the replica fingerprint; test every field changes the fingerprint.
- [x] Run `go test ./internal/config ./internal/coordination/local ./internal/coordination/postgres -run 'Tokenization|PolicyFingerprint' -count=1`; expect PASS. Commit with `feat: define independent tokenization admission policy`.

### Task 2: Implement atomic budgets shared across PostgreSQL replicas

**Interfaces:** Implement the same TokenizationCoordinator against `Store.CoordinationPool()`; expose `CleanupBatch(ctx, limit) (more bool, err error)` with batches capped at 1,000. Keep all utility storage separate from existing inference tables.

- [x] Add the migration with utility tables: `tokenization_client_scopes`, `tokenization_backend_scopes`, `tokenization_operations` (fingerprint, decision, backend identity, terminal state, retention), `tokenization_leases` (expiry-indexed and indexed by client/backend), and `tokenization_rate_state`. Only metadata is persisted. Down migration must reject active leases before dropping utility tables.
- [x] Add migration and shared contract tests against two coordinator instances connected to the same database. Use small test limits (client=1, backend=2, RPM=2) and concurrent goroutines to assert exactly the permitted number of admissions.
- [x] Run `make test-postgres` with a disposable `LLMGW_POSTGRES_TEST_DSN`; expect utility contract failures.
- [x] Implement Acquire in one durable transaction using `synchronous_commit=on`: insert/lock the operation receipt, then lock the client scope and candidate backend scopes in sorted ID order. Read counts after locks using fresh READ COMMITTED statements and DB time; select the least-active eligible utility backend, update the independent rate bucket, and write the lease/decision atomically.
- [x] Validate key/client/pool/access/backend identities and revisions against authoritative configuration inside the transaction. Check revoked/expired keys, disabled entities, backend drain/pool membership, and stale configuration. Do not trust only a cached candidate list or caller policy. Follow existing mutation/row-lock ordering and add concurrent mutation tests to rule out deadlocks; no inference scope lock is needed for utility counts.
- [x] Resolve acquisition retries through the same operation ID/fingerprint. On ambiguous commit, reconcile that receipt within the bounded coordination deadline; if still unknown, return unavailable without contacting upstream. Never mint a replacement operation to bypass uncertainty.
- [x] Complete only the matching operation/lease identity and retain its receipt. All counts ignore expired leases even before cleanup. Failed release may temporarily retain capacity until expiry; it cannot decrement another request or fail open.
- [x] Add tests for committed-but-response-lost acquisition, uncertain release/replay, expiry after replica loss, revoked key during lock contention, policy mismatch, concurrent cleanup, stale backend revision, and no changes in request_leases/coordination_rate_state.
- [x] Run `make test-postgres` and `go test -race ./internal/coordination/...`; require actual PostgreSQL tests to run, not only skip. Commit with `feat: coordinate tokenization budgets across replicas`.

### Task 3: Add pass-through transport with no inference side effects

**Interfaces:** Add `TokenizationRequest` in `internal/proxy/tokenization.go` with Method, Path, RequestID string; Headers http.Header; Body []byte; Backend domain.Backend; UpstreamAPIKey string. Add `(*Proxy).ForwardTokenization(context.Context, http.ResponseWriter, TokenizationRequest) Result`. Result reuses transport outcome fields; Usage and usage-parser fields remain unset.

- [x] Add `internal/proxy/tokenization_test.go` tests for byte-identical JSON success/error responses, optional response fields, no redirects, gateway-key removal, upstream credentials, request-ID propagation, hop-by-hop/Connection header filtering, and removal of affinity/priority headers.
- [x] Assert a response containing a fake inference usage object never produces usage or inference callbacks. Assert there is no automatic second backend attempt after transport failure.
- [x] Add deadline/cancellation/truncation tests before and after headers; client disconnect must close the upstream body. Bound buffering with streaming copy rather than reading the entire response into memory.
- [x] Run `go test ./internal/proxy -run Tokenization -count=1`; expect failures before implementation.
- [x] Implement using the existing no-redirect transport and shared end-to-end header filtering. Add `PrepareTokenizationHeaders(source, requestID, upstreamAPIKey) http.Header`; do not call the priority-injecting header function unchanged. Forward exactly /tokenize and no gateway deployment prefix.
- [x] Apply the existing request context deadline to upstream I/O and a downstream write deadline through ResponseController. A failed copy after commit must terminate the response using the existing HTTP abort convention. Restore any per-request write deadline as appropriate for keep-alive.
- [x] Run `go test ./internal/proxy -count=1`; expect PASS, including unchanged generation proxy tests. Commit with `feat: proxy tokenization without inference accounting`.

### Task 4: Connect authentication, backend selection, and public routing

**Interfaces:** Add `(*Service).Tokenize(ctx context.Context, writer http.ResponseWriter, request ForwardRequest) (proxy.Result, *APIError)`. Add dependencies for TokenizationCoordinator, TokenizationPolicy, the utility forwarder interface, and an optional TokenizationObserver. Define `TokenizationEvent` here with RequestID, Client, Model, Backend, Reason string; Status int; Duration time.Duration; Cancelled bool, and `TokenizationObserver` with `TokenizationComplete(TokenizationEvent)`. Service.Tokenize emits exactly one event once invoked; the handler emits only for failures before invoking the service. Neither path emits CompletePublic. Unresolved dimensions are empty; never use raw caller model names as labels.

- [x] Add `internal/gateway/tokenization_test.go` tests: valid/expired/revoked keys, missing/unknown/forbidden model, disabled pool, generic prompt and messages payloads, public-to-upstream model translation, and preservation of optional fields. Validate only JSON-object/model at the gateway; tokenization-specific schema errors come from upstream.
- [x] Add selection cases for GPU saturation, stale GPU metrics with fresh health, disabled/draining/unhealthy/stale-health backends, missing upstream secret, and all utility backend slots full. Observe health age using existing HealthInterval/HealthTimeout/UnhealthyAfter settings; zero or future LastHealthAt is not fresh.
- [x] Assert all inference acquire/circuit/observer hooks are untouched using spies. During Acquire, publish a revoked key or changed model/backend revision; ensure no upstream call follows. Exhausted utility capacity returns 429 while exhausted generation capacity alone does not.
- [x] Run `go test ./internal/gateway -run Tokenization -count=1`; expect failures.
- [x] Implement authenticate/model resolution using current registry helpers, remove caller priority from the JSON, preserve all other options, and translate model. Build eligible utility candidates from the latest registry and health snapshots without inference pressure/circuit eligibility. Start the timeout before Acquire.
- [x] Acquire once, defer idempotent release immediately after success, and revalidate current credentials/config/backend/secret and remaining deadline before dispatch. On stale configuration release and return 503; the client can retry as a new HTTP request. Use one chosen backend, with no session affinity or automatic retry.
- [x] Release using an independent context bounded by the existing CoordinationTimeout when request cancellation has already fired. Attempt cleanup on all exits/panics; expiry provides the crash/failure fallback. Release does not wait for analytics capacity.
- [x] Add `(*PublicHandler).tokenize` in `internal/httpapi/tokenization.go`: follow existing request-ID, bearer-token, body-limit, error-envelope, and post-commit-abort conventions, but use the utility completion path for early errors too.
- [x] Register POST /tokenize in NewPublicHandler and route /tokenize to that handler in application.go. Add full application-handler tests in `cmd/gateway/tokenization_test.go` so tests cannot pass while the outer route is missing; wrong methods/paths must not proxy upstream.
- [x] Wire local/PostgreSQL coordinator, policy, and utility forwarder through application.go. Add a bounded periodic utility cleanup worker using the existing worker lifecycle. Check registry/replica compatibility before utility dispatch. The telemetry observer is optional until Task 5; admission dependencies are mandatory and fail closed if absent.
- [x] Run `go test ./internal/gateway ./internal/httpapi ./cmd/gateway -count=1`; expect PASS. Commit with `feat: expose authenticated tokenization routing`.

### Task 5: Wire lifecycle, separate telemetry, and operational verification

**Interfaces:** Implement the TokenizationObserver contract from Task 4 with a dedicated metrics/logger adapter; do not attach it to the inference observer fan-out.

- [x] Add `internal/observability/tokenization_test.go`: exactly one terminal utility event per HTTP request; separate requests/duration/rejections counters; no inference requests/token/coverage/TTFT/circuit changes. Log safe metadata only; token strings must not appear even when upstream returns them.
- [x] Implement `llmgw_tokenize_requests_total{model,backend,status}`, `llmgw_tokenize_duration_seconds{model,backend}`, and `llmgw_tokenize_rejections_total{reason}`. Use configured names and bounded reasons; no request IDs in labels. Follow existing topology reconciliation to remove deleted model/backend series.
- [x] Wire the separate observer through application.go. Expose `llmgw_tokenize_coordination_available` from the utility coordinator's Status; a utility failure must not report the generation coordinator as healthy on its behalf or bypass existing compatibility gates.
- [x] Add integration cases where two gateway instances share PostgreSQL and a blocked synthetic upstream: verify global client/backend limits and RPM, capacity after cancellation, crash/expiry, and separate inference analytics before/after utility calls.
- [x] Add a real-vLLM tokenization check to the existing opt-in smoke suite using `tests/e2e/tokenization_test.go`: prompt and chat requests, public model translation, special-token options, and optional token strings. Compare direct-backend and gateway responses without retaining bodies; document when this check was not run.
- [x] Document environment defaults, independent budgets, 429/503 handling, timeout/lease behavior, SQLite single-replica scope, generic POST examples, and root-level reverse-proxy routing in `README.md`, `README.ru.md`, `.env.example`, and `docs/operations.md`.
- [x] Document rollout: apply additive migrations, drain incompatible old replicas, deploy replicas with identical policy values, wait for compatibility/readiness, then expose the endpoint. Fingerprint mismatches must not be described as harmless. Rollback drains utility requests before downgrading; no dropping active leases.
- [x] Run `go test ./...`, `go test -race ./...`, `go vet ./...`, `make test-postgres`, and `git diff --check`; expect PASS. Run the opt-in real-vLLM smoke test only against an authorized test deployment and record pass/skip explicitly.
- [x] Commit with `feat: observe and validate distributed tokenization`.

## Completion and integration

- [x] Confirm the settings chosen by the user are implemented: a separate utility budget, shared across replicas.
- [x] Confirm #31/#32 have no functional dependency on this issue; migration numbering is rechecked against main before merging.
- [x] Confirm generation admission/RPM/TPM, inference circuit state, ledger row counts, and usage coverage are unchanged by tokenization success and failure.
- [x] Record separate outcomes for unit tests, real PostgreSQL concurrency tests, and real vLLM compatibility tests. Passing the default suite alone is not evidence that the optional environments ran.
- [ ] Reference #33 in an implementation PR when one is requested. This delivery commits and pushes the feature branch, as requested; #23 remains future work.
