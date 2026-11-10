# Request-size-aware Admission — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement #23: input estimation, server-side output caps, and atomic outstanding-token reservations while preserving emergency admission.

**Architecture:** A pure policy/payload layer prepares an immutable reservation. The internal estimator uses the existing upstream tokenizer transport; local/PostgreSQL admission stores the reservation with the inference lease. Emergency mode has explicitly local token/utility caps and survives recovery without resetting accounting.

**Tech Stack:** Go 1.27.3, net/http, chi, pgx/PostgreSQL, SQLite, existing migrations/registry/Prometheus/contracts. No new tokenizer dependency is required.

**Spec:** [design](../specs/2026-11-10-issue-23-token-reservations-design.md).

**Status:** Implementation in progress. Base `c072e66466d68088c924b94421af3d7c9646d171`, branch `codex/issue-23-token-reservations`.

## Global Constraints

- Preserve High/Critical emergency admission, as confirmed by the user. No global ceiling is promised during outage/recovery.
- Disabled/default retains existing inference/soft-TPM/public-tokenize semantics.
- Token amounts are int64, range `0..10^12`, with checked addition/multiplication. No raw content in persistence/logs.
- All three inference endpoints must have an explicit enabled-policy contract; unsupported input is not counted as zero.
- Normal allocation atomically creates one receipt + one lease + one RPM debit + one reservation. Retry/expiry/replay does not create a second reservation.
- Preserve the existing pool → client → lease → receipt lock order and READ COMMITTED post-lock counts; network preflight runs outside locks.
- ModelPool remains a comparable Go struct. ClassLimits is a fixed struct, not a map.
- vLLM v0.28.0 is the qualification baseline. Neither the latest renderer nor a new model is automatically qualified.
- Local receipt history is bounded; PostgreSQL has a 24h replay window. Preserve the distinction between their guarantees.

## Review Focus

1. Concurrent enabled-policy updates, legacy leases, and emergency overlap must not reset outstanding amounts or silently admit an unestimated request (Tasks 1, 3–6).
2. A tokenizer may accept a similar but different prompt (templates, tools, Unicode, Responses history): the estimate must be demonstrably applicable or use explicit fallback/rejection (Task 2).
3. The same LeaseID with a foreign owner, changed R, or late completion must neither release another owner's resources nor repeat an RPM/TPM debit (Tasks 3–4).
4. Recovery during preflight and ambiguous commits must preserve at most one generation lifecycle and local caps; emergency must not mask a permanent fault (Tasks 5–6).
5. Unknown usage and telemetry replay must not present estimates as actual usage, sum shared gauges across replicas, or leak content (Tasks 3–4, 7).

## File and interface map

New focused modules:

- `internal/domain/request_size.go`: comparable persistent policy/limits and effective policy resolution.
- `internal/admission/request_size.go`: payload/output validation and checked reservation arithmetic.
- `internal/gateway/token_estimator.go`: bounded preflight orchestration and normal/emergency utility mode selection.
- `internal/coordination/token_reservation.go`: immutable admission metadata and identity helpers.
- `internal/coordination/contracttest/token_reservation.go`: shared accounting matrix for local and PostgreSQL coordination.
- `internal/observability/token_reservation.go`: numeric-only telemetry.
- New tests beside each module; `tests/postgres/token_reservation_test.go`, `tests/integration/token_reservation_test.go`, `tests/e2e/token_reservation_test.go`.

Existing integration points: `domain/types.go`, `store/models.go`, `store/clients.go`, `store/pools.go`, `store/snapshot.go`, `store/postgres/configuration.go`, `httpapi/admin.go`, `web/resources.go`, `web/templates/clients.html`, `web/templates/backends.html`, `gateway/forward.go`, `gateway/forward_admission.go`, `gateway/service.go`, `gateway/observer.go`, `coordination/{types.go,emergency.go,lease_manager.go}`, `coordination/local/admission.go`, `coordination/postgres/{admission.go,cleanup.go,replica.go}`, `cmd/gateway/application.go`, `config/config.go`, Makefile/docs.

Normative new types (bodies are left to the implementer):

```go
// domain/request_size.go
type RequestSizeLimits struct {
    MaxInputTokens, MaxOutputTokens, DefaultOutputTokens int64
}
type ClassRequestSizeLimits struct {
    Critical, High, Normal, Background RequestSizeLimits
}
type RequestSizePolicy struct {
    Enabled bool
    Limits RequestSizeLimits
    ClassLimits ClassRequestSizeLimits
    MaxOutstandingTokens, EmergencyMaxOutstandingTokens int64
    FallbackMode, EstimationProfile string
    FallbackOverheadTokens int64
}
type EffectiveRequestSizePolicy struct {
    Enabled bool
    Limits RequestSizeLimits
    ClientBudget, PoolBudget, EmergencyPoolBudget int64
    FallbackMode, EstimationProfile string
    FallbackOverheadTokens int64
}
func ResolveRequestSizePolicy(client Client, pool ModelPool) (EffectiveRequestSizePolicy, error)

// admission/request_size.go
type PreparedRequestSize struct {
    Payload []byte
    Inputs []json.RawMessage
    PromptCount, Samples int64
    OutputAllowance int64
}
func PrepareRequestSize(path string, body []byte, policy domain.EffectiveRequestSizePolicy) (PreparedRequestSize, error)
func ReservationTokens(inputEstimates []int64, outputAllowance, samples int64) (int64, error)

// coordination/token_reservation.go
type TokenReservation struct {
    Enabled bool
    InputEstimate, OutputAllowance, PromptCount, Samples, ReservedTokens int64
    ClientBudget, PoolBudget int64
    PoolPolicyRevision int64
    EstimatorKind, EstimationProfile string
}
// AdmissionRequest.TokenReservation stores the immutable value.
// Existing ClientPolicyRevision/ConfigurationRevision remain authoritative.

// gateway/token_estimator.go
type TokenEstimate struct {
    InputTokens []int64
    ContextLimits []int64 // 0 only for explicit fallback without known context
    Kind, Profile string
}
type EstimationMode string // normal | emergency
type InputEstimator interface {
    Estimate(context.Context, EstimationRequest) (TokenEstimate, *APIError)
}
type EstimationRequest struct {
    Path string
    Prepared admission.PreparedRequestSize
    Snapshot *registry.Snapshot
    Client domain.Client
    Pool domain.ModelPool
    APIKeyID int64
    RequestID string
    Mode EstimationMode
}
```

Persistent client fields: `RequestSizeLimits RequestSizeLimits`, `MaxOutstandingTokens int64`; pool field: `RequestSizeAdmission RequestSizePolicy`. Admin JSON uses the specification's exact names through explicit DTOs, not the default Go nesting shape. Use typed/enum internal errors; never include upstream response bodies in API errors.

## Task 1: Policy, persistence, and compatible Admin updates

**Files:** create `domain/request_size.go`, `domain/request_size_test.go`; modify domain/store/registry/Admin/forms from the map; create SQLite `006_request_size_admission.sql`, PostgreSQL `000007_request_size_admission.up/down.sql`, and migration tests. Check available migration numbers before creating them.

**Interfaces:** produces `RequestSizePolicy`, `RequestSizeLimits`, and `ResolveRequestSizePolicy`; snapshots carry policy/revisions; update DTOs distinguish absent fields from explicit zero/false. The PostgreSQL migration adds a nullable legacy marker / token metadata to leases/receipts; enabled defaults to false.

- [x] Add `TestResolveRequestSizePolicy`: pool input/output/default=8192/2048/512, Background=2048/256/128, client maxOutput=64 → effective input=2048/output=64/default=64. Client default=32 → default=32. Client=0 inherits; negative values and values >10^12 are rejected.
- [x] Add `TestRequestSizePolicyRoundTripAndLegacyUpdate`: creating through the old DTO defaults to disabled; a name-only update preserves token policy; explicit enabled=false disables it; class/pool/client changes update the revision; forms do not reset fields they do not expose.
- [x] Add migration tests: old schema→new zero defaults, equivalent policy after reload, and downgrade refusal with a live nonzero reservation. Use existing project fixtures, not a new test framework.
- [x] Run `go test ./internal/domain ./internal/store/... ./internal/httpapi ./internal/web -run RequestSize -count=1` and verify the expected failures for the new contracts.
- [x] Implement policy/validation, SQLite/PostgreSQL CRUD/snapshots, and Admin/form persistence. Add cross-entity validation/error handling. Use pointer/presence-aware DTOs for new JSON fields; do not refactor unrelated API validation.
- [x] Rerun the tests and separately execute migration tests against disposable PostgreSQL. Commit: `feat: persist opt-in request size admission policies`.

## Task 2: Payload bounds and bounded token estimation

**Files:** create `admission/request_size.go`, `admission/request_size_test.go`, `gateway/token_estimator.go`, `gateway/token_estimator_test.go`; modify `proxy/tokenization.go` only for the bounded internal decoder adapter and `config/config.go`; create `config/request_size.go` and adjacent tests.

**Interfaces:** consumes Task 1 policy; produces PreparedRequestSize, TokenEstimate, and InputEstimator. Transport remains `Proxy.Tokenize(ctx, proxy.TokenizationRequest) (*http.Response,error)`. Normal preflight obtains a utility lease through the existing TokenizationCoordinator; Task 5 supplies emergency acquisition dependencies.

- [x] `TestPrepareRequestSizeEndpoints`: missing cap →512 (or effective default); explicit 2049 with max2048 →400; chat aliases 128/128 normalize, 128/64 reject; preserve response max_output_tokens; reject min_tokens>cap; streaming uses the same cap.
- [x] `TestReservationTokensMultipliers`: inputs=[100,200], output=50,n=2 →800; inputs=[0],output=50,n=1 →51; reject n=0/>32, batch>32, invalid mixed batches, overflow, and amounts >10^12.
- [x] `TestEstimatorCompatibility`: provide separate fixtures for every row of the normative field matrix in spec §5; materialize Unicode, BOS/special tokens, generation prompt, and continue_final_message consistently; tools/schema/inline function history use explicit fallback/rejection. Reject unknown top-level fields; treat token-ID input as one prompt. Unsupported Responses with fallback=reject →400; utf8_bytes →byteLength(canonicalJSON)+overhead, never 0.
- [x] `TestEstimatorBoundsAndPrivacy`: 2s total deadline across the batch, 8 MiB maximum response, cancellation, noninteger/missing count, count/tokens mismatch, context overflow, 401/403, 404/501, 429/5xx; fallback only for unsupported cases, not transient failures. No utility retries, inference counters, or body content in errors.
- [x] Run `go test ./internal/admission ./internal/gateway ./internal/proxy ./internal/config -run 'RequestSize|ReservationTokens|Estimator' -count=1`; record expected failures.
- [x] Implement pure validation and estimation, including the input-bearing field table/allowlist. Fixtures pin the v0.28.0 mapping; ambiguous variants use fallback/rejection, not a content-only heuristic.
- [x] Verify PASS and run `go test ./internal/gateway ./internal/proxy -run Tokenization -count=1`. Commit: `feat: estimate request tokens and enforce output allowances`.

## Task 3: Local atomic reservations and safe owner lifecycle

**Files:** create `coordination/token_reservation.go`, `coordination/contracttest/token_reservation.go`, `coordination/local/token_reservation_test.go`; modify `coordination/types.go`, `coordination/local/admission.go`, `coordination/local/contract_test.go`.

**Interfaces:** `AdmissionRequest.TokenReservation`; `TokenReservationContract(t *testing.T, factory TokenReservationFactory)` with a factory that creates isolated coordination/configuration and supports Advance/SetPolicy/Outstanding. Do not couple assertions to the storage clock. LeaseCompletion uses the existing Usage; metadata comes from the receipt.

- [x] Encode the AC05 Bpool=1000/Bclient=800/R=600/400/2 example in the shared contract; concurrency/RPM rejection consumes no tokens; token rejection consumes no RPM; repeating the same Acquire 100 times allocates R once.
- [x] `TestTokenReservationLifecycle`: valid actual=250 → release600 and one TPM debit250; unknown usage → release600, actual unknown; expiry→release600; late actual→one TPM debit, outstanding unchanged; Complete replay→release0.
- [x] `TestTokenReservationOwnerMismatch`: change ClientID/PoolID/ReplicaID/RequestID individually in Renew and Complete; a foreign operation changes no lease, expiry, counters, or TPM. The valid owner continues operating afterward. Demonstrate the existing Renew defect with a failing regression before fixing it.
- [x] `TestTokenReservationPublicationAndReplayEviction`: policy reductions do not rewrite R; disabling a pool does not remove old token leases from client sums; enabling with legacy leases rejects until drain; an evicted receipt's original timestamp cannot resurrect it; existing live leases are checked before the cutoff. Do not evict future-start token receipts before now≥start; full capacity with all receipts protected returns unavailable without a future cutoff; current valid owners can still complete.
- [x] Run `go test ./internal/coordination/local -run TokenReservation -count=1` and expect failures; then add counters to the shared mutex section and remove only stored R through removeLease. Add a safe replay cutoff for enabled admissions and immutable fingerprint metadata.
- [x] `go test -race ./internal/coordination/local ./internal/coordination/contracttest` →PASS. Commit: `feat: reserve outstanding tokens in local admission`.

## Task 4: PostgreSQL shared reservations and recovery contracts

**Files:** modify `coordination/postgres/{admission.go,cleanup.go,replica.go}` and, if necessary, the Task 1 migration; create `tests/postgres/token_reservation_test.go`; extend `tests/postgres/commit_response_loss_test.go`, `cleanup_concurrency_test.go`, `configuration_delete_test.go`, `admission_parallel_test.go`, and startup compatibility tests.

**Interfaces:** the same AdmissionCoordinator/TokenReservationContract; PostgreSQL obtains authoritative budgets from persisted policy; normal shared outstanding=sum of unexpired reserved_tokens. ContractVersion becomes 4; RateAlgorithmVersion stays 1.

- [x] Run the shared Task 3 contract for PostgreSQL. Add 2 coordinator instances: Bpool=1000, 100 concurrent acquisitions of R=100, sufficient concurrency/RPM →exactly 10 admissions, peak1000. Different pools for one client share its client budget.
- [x] `TestPostgresTokenReservationLostCommit`: drop a successful Acquire commit response →same lease/RPM/R once; changed R/profile →idempotency conflict. Lost Complete commit responses and partial-batch retries do not double-release or double-debit TPM.
- [x] `TestPostgresTokenReservationExpiryAndPolicyRace`: counts exclude expiry before cleanup; race cleanup/Complete/Renew; configuration deletion/tombstones do not prevent terminal release; policy reductions, client class changes, enable transitions, and wrong-owner operations do not bypass limits.
- [x] Run `make test-postgres` against a disposable DSN and verify that new tests actually execute rather than skip.
- [x] Implement atomic checks/sums/writes with the existing pool→client lock order, authoritative policy validation, fingerprint, and receipt metadata. Owner matching compares immutable receipt identity. Handle SUM(bigint) without unchecked integer overflow. Preserve committed-prefix results and synchronous_commit.
- [x] Verify compatibility/drain upgrade 3→4 and rollback refusal. Measure admission contention latency using the existing test profile; do not hold SQL locks during tokenizer calls.
- [x] `make test-postgres` →PASS; `go test -race ./internal/coordination/...` →PASS. Commit: `feat: coordinate token reservations across gateway replicas`.

## Task 5: Emergency tokens and independent outage preflight

**Files:** modify `coordination/emergency.go`; add `coordination/emergency_tokens_test.go`; complete estimator emergency provider/configuration and `cmd/gateway/application.go`; extend `gateway/service_pool_test.go` and application recovery tests.

**Interfaces:** preserve existing Acquire for the disabled flow; add `EmergencyAdmission.AcquireTokens(class domain.PriorityClass, clientID, poolID int64, clientLimit int, reservation TokenReservation, emergencyPoolBudget int64) (func(), Reason, AdmissionScope)`. The release closure covers concurrency and tokens with one once guard. The emergency estimator uses a separate local TokenizationCoordinator with its own limits; do not replace the public utility coordinator.

- [x] `TestEmergencyTokenCaps`: High/Critical eligible, Normal/Background denied; Bpool=1000, clientBudget800, R600 →deny the second request from the same client; another client's R400→admit; balanced release/replay; existing class caps remain binding.
- [x] `TestEmergencyEstimatorWithoutDatabase`: unavailable PostgreSQL and a healthy tokenizer →a High request obtains a bounded estimate and local reservation. Public `/tokenize` still returns 503. Concurrent emergency preflights default to 1/client,1/backend,30 RPM/burst1. Fingerprint tests cover configuration overrides.
- [x] `TestEmergencyEstimatorTransitionMidBatch`: the first prompt has an estimate, then normal utility Acquire for the second loses DB access →one transition to local mode with a new utility ID and the remaining total deadline; retain the first estimate only with the same profile. Utility429/permanent fault/upstream timeout do not trigger fallback. Recovery does not switch this request back to normal.
- [x] `TestEmergencyTokenRecoveryOverlap`: normal readiness returns while an emergency request is live; its local reservation remains until completion. Another outage does not reset counters; permanent errors do not activate emergency. Invalid/missing estimation does not admit an unbounded request.
- [x] Run `go test ./internal/coordination ./internal/gateway ./internal/config ./cmd/gateway -run 'Emergency.*Token|EmergencyEstimator|TokenRecovery' -count=1` and confirm failures.
- [x] Implement the shared local critical section, provider selection, lifetime/release, and recovery overlap while preserving disabled-pool semantics. Account for normal and emergency tokenization budgets separately in telemetry.
- [x] Rerun targeted tests and `go test -race ./internal/coordination ./internal/gateway`; require PASS. Commit: `feat: preserve bounded emergency token admission`.

## Task 6: Integrate preflight into the inference lifecycle

**Files:** `gateway/forward.go`, `gateway/forward_admission.go`, `gateway/service.go`, `gateway/service_retry_test.go`, `gateway/usage_test.go`, `httpapi/public_test.go`; create `tests/integration/token_reservation_test.go`, `tests/postgres/token_reservation_http_test.go`; update application wiring.

**Interfaces:** `resolvedForwardRequest` stores PreparedRequestSize/TokenEstimate/TokenReservation and immutable admission mode. `acquireForwardAdmission` retains its release-callback signature; estimation runs before allocation. Recalculation after stale configuration uses the original request.Body, not the rewritten payload.

- [x] `TestTokenAdmissionHTTPMatrix`: all endpoints/streaming, no tokenizer when disabled, budget429+Retry-After, invalid size400 without generation/RPM, preflight503, route eligibility/affinity, and limits taking precedence over priority.
- [x] `TestTokenAdmissionPolicyChangedDuringEstimate`: revocation, disabled model, or profile change during blocked estimation →no dispatch; size-policy change →one re-estimation; another change →503. The enable gate covers legacy distributed leases and locally running legacy emergency requests; deployment documentation requires cluster-wide drain for emergency overlap.
- [x] `TestTokenAdmissionRetryAndCancellation`: pre-byte retry→one reservation; post-byte retry forbidden; cancellation after acquisition, upstream errors, and panics release once; streams longer than TTL renew R; backlog overflow releases through expiry.
- [x] `TestTokenAdmissionAmbiguousFallback`: unresolved normal Acquire + valid emergency →one generation; any ghost distributed reservation expires; normal replay retains its UUID; emergency completion does not mutate an unknown normal lease.
- [x] Run HTTP/integration tests and obtain expected failures; integrate Tasks 1–5, adding reasons to coordinationAPIError and the bounded decision enum.
- [x] Verify `go test ./internal/gateway ./internal/httpapi ./tests/integration -count=1`, `make test-postgres`, and `go test -race ./internal/gateway ./internal/coordination/...`. Commit: `feat: enforce request size admission across inference lifecycle`.

## Task 7: Observability, UI, and operating guidance

**Files:** `gateway/observer.go`; create `observability/token_reservation.go` and tests; modify `observability/metrics.go`, `observability/log.go`, coordinator observer hooks, Admin templates/tests, `docs/operations.md`, `docs/postgresql-production.md`, README.md/README.ru.md.

**Interfaces:** add optional `TokenReservationObserver` with `TokenReservationChanged(TokenReservationEvent)`; events contain pool/client IDs, class, mode, action (acquired/released/expired/reconciled), amounts, a usage-known flag, and a bounded reason. Do not promise exactly-once PostgreSQL event delivery: monotonic metric events are best effort; the authoritative outstanding snapshot is the source of truth. RequestEvent gains numeric estimate/reserve and estimator/mode fields.

- [x] `TestTokenReservationTelemetry`: reserve600/actual250/unknown are three distinct states; replay emits no second local accounting callback; expiry release is visible without HTTP completion. Uncertain PostgreSQL callback delivery does not affect correctness.
- [x] `TestTokenReservationPrivacy`: unique sentinel prompt/generated/tokenizer strings do not appear in persisted receipts, structured events/logs, metric labels, or API errors. Missing actual usage is not serialized as actual 0.
- [x] `TestRequestSizeAdminForms`: effective limits, clear disabled/inherited labels, and per-replica emergency copy; partial form updates preserve policy; CSRF/auth remain intact. Check the existing template test workflow.
- [x] Implement bounded metrics/observers and UI. Document utility preflight capacity planning, fallback calibration, the disabled-pool/client-budget boundary, the High-headroom limitation, unknown usage, enable drain, and recovery overlap.
- [x] Run `go test ./internal/observability ./internal/httpapi ./internal/web -count=1` and `make test-web-unit`; require PASS. Commit: `feat: expose token reservation decisions and operating limits`.

## Task 8: Qualification, real-vLLM experiment, and final acceptance

**Files:** `tests/e2e/token_reservation_test.go`, `tests/e2e/harness_test.go`, `tests/e2e/harness_unit_test.go`, `Makefile`, `docs/real-vllm-priority-e2e.md`, `docs/issue-23-analysis-and-evidence.md`.

**Interfaces:** add e2e mode `tokens` and accept it in `make test-real-vllm`. Use existing gateway/model/admin/key environment names; add `LLMGW_E2E_TOKEN_RUNS=3`, `LLMGW_E2E_TOKEN_HIGH_PROBES=100`, and a reproducible seed. Print a numeric experiment report without prompt content.

- [ ] Configuration/scenario unit tests: explain invalid modes/missing credentials; distinguish attempts/admissions/rejections in counters; do not replace missing TTFT with 0; fail the scenario if it does not reach token pressure or collect 100 probes per repetition.
- [ ] Implement the paired disabled/enabled 3× experiment from spec §11 with warm-up, checks against collateral policy edits, cleanup/restoration of test-owned settings, cap/unknown/error accounting, and ratios/percentiles. Use the same workload/model configuration for disabled and enabled runs and record their order.
- [ ] Run `go test -count=1 ./...`, `go test -race ./...`, `go vet ./...`, `go build ./cmd/...`, `make test-web-unit`, and `make test-postgres`; record commands, commit, environment/profile, pass/fail/skips. PostgreSQL requires actual two-replica executions.
- [ ] Run `LLMGW_E2E_MODE=tokens make test-real-vllm` against a dedicated real-vLLM target with a qualified model profile and safe secret handling. Run direct tokenizer-versus-generation qualification on every backend for supported exact payloads. Without an environment, mark AC14 pending; synthetic tests are not a substitute.
- [ ] Populate AC01–AC14 evidence with test links and results. Fix confirmed defects with regressions or obtain explicit acceptance of exceptions. Perform a whole-branch review before the PR.
- [ ] Commit: `test: qualify token reservations under mixed inference load`. Publish a PR/close the issue after implementation/evidence is ready, not at this draft-plan stage.

## Plan self-review and execution order

Dependencies: 1→2/3; 3→4; 2+3→5; 1–5→6; 6→7→8. Execute sequentially for now: policy/fingerprint/lifecycle contracts are shared, and premature parallel code editing increases integration risk. A separate builder review of the specification has been completed: it clarified the field matrix, normal→emergency preflight, cluster-wide drain, old reservations in disabled pools, and the bounded replay cutoff; corresponding tests were added.

Coverage map: AC01=6; AC02=1,7; AC03–04=2,6,8; AC05=3–4; AC06=1,3–4,6; AC07–09=3–4,6; AC10=5–6; AC11=2,5; AC12=7; AC13=1,4; AC14=8. The named tests above cover Review Focus. Each task ends with its own verifiable deliverable; final checks do not replace those checks.

Before implementation, approve the concrete specification, particularly opt-in Responses/media restrictions, shared utility preflight budgets, the absence of a token-priority reserve, and preserved soft-TPM unknown-usage semantics. Emergency is already a mandatory user requirement and must not be submitted for approval again.
