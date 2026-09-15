# MVP acceptance evidence

This document maps the source specification's MVP acceptance criteria to repeatable evidence. The default automated suite uses a real SQLite database, the production HTTP composition, monitor workers, and the deterministic fake vLLM server. The opt-in `tests/e2e` suite exercises a deployed gateway against real vLLM processes; final CUDA/model calibration is tracked separately because it depends on the target hardware.

## Automated acceptance matrix

| Criterion | Evidence |
|---|---|
| Unknown and revoked API keys return `401`; invalid keys are rejected before request bodies are read | `TestAuthenticationAndModelAccessAcceptance`, `TestPublicAuthenticationCasesUseSameUnauthorizedEnvelope`, `TestInvalidAPIKeyIsRejectedBeforeReadingRequestBody` |
| Plaintext API keys are absent from SQLite, WAL, and SHM files | `TestAdminAuthenticationCSRFCRUDAndOneTimeSecret`, `TestSQLiteNeverReceivesPlaintextKey` |
| A client sees only explicitly allowed models; forbidden models are rejected | `TestAuthenticationAndModelAccessAcceptance`, `TestModelsListsOnlyExplicitEnabledAccess` |
| Chat Completions, Completions, and Responses are forwarded in ordinary and streaming modes | `TestSupportedOpenAIRoutesAcceptance` |
| 100 requests select pressure `0.3` rather than `1.1` | `TestRoutingUsesLeastPressureAndHealthRecovery` |
| A stable session uses client-and-pool-scoped, order-independent rendezvous routing; overload falls back to least pressure; eligibility/retry exclusions rehash safely; retries use current state without mixing pool-model revisions; the header never reaches vLLM | `TestSelectWithSessionAffinityIsStableAcrossCandidateOrder`, `TestSelectWithSessionAffinityFallsBackWhenPreferredIsOverloaded`, `TestSelectWithSessionAffinityHonorsEligibilityAndRetryExclusion`, `TestSelectWithSessionAffinityRehashesAcrossEligibleBackendsBeforePressureFallback`, `TestSessionAffinityPrefersRendezvousBackendAndStripsHeader`, `TestSessionAffinityKeyIncludesClientAndPoolIdentity`, `TestRequestWithoutSessionAffinityUsesLeastPressure`, `TestSessionAffinityRejectsOversizedIdentifier`, `TestRetrySelectionUsesCurrentRegistryAndTime`, `TestRetrySelectionRejectsPoolModelReconfiguration` |
| Failed backends leave routing and recovered backends return; monitor polls do not follow redirects; a newly published backend incarnation cannot acquire an old ID-keyed worker | `TestRoutingUsesLeastPressureAndHealthRecovery`, `TestWorkerHealthFailureAndRecoveryCounts`, `TestWorkerDoesNotFollowHealthOrMetricsRedirects`, `TestManagerAcquireBackendRequiresExactIdentityAndKeepsOldCompletionBalanced`, `TestForwardRejectsPublishedBackendUntilMatchingRuntimeIsHealthy` |
| Qualifying inference failures use completion timestamps, open a rolling-window circuit, admit bounded half-open probes, resolve concurrent probe outcomes conservatively, and ignore stale-generation completions | `TestBreakerOpensWithinRollingWindowAndExpiresOldFailures`, `TestBreakerCooldownAllowsOnlyOneHalfOpenProbe`, `TestBreakerHalfOpenAllowsConfiguredProbeCapacity`, `TestBreakerUsesCompletionTimeForDelayedClosedFailures`, `TestBreakerUsesCompletionTimeForDelayedHalfOpenFailure`, `TestBreakerHalfOpenSuccessWaitsForOutstandingFailure`, `TestBreakerHalfOpenStaleSuccessCannotHealLaterGeneration`, `TestBreakerHalfOpenSuccessAndNeutralOrdering`, `TestManagerTimestampsBackendOutcomeAtCompletion`, `TestHTTP5xxOpensCircuitAndRoutesNextRequestToHealthyBackend` |
| Pool-wide gateway in-flight and latest eligible upstream-waiting limits reject every priority with the bounded `429`; zero remains unlimited while counted; all admitted exits release leases | `TestServicePoolMaxInflightRejectsSecondAndReleasesAfterCompletion`, `TestServicePoolMaxWaitingRejectsBeforePoolAndClientAcquisition`, `TestServiceCriticalPriorityCannotBypassPoolLimit`, `TestServiceZeroPoolLimitsAreUnlimitedButCounted`, `TestServiceReleasesPoolLeaseOnEveryAdmittedExit`, `TestManagerPoolSnapshotOverlaysLatestWaitingBeforeObserverTick` |
| Pool safety fields migrate transactionally, validate, round-trip through SQLite/Admin/UI, and retain zero semantics; a future recorded version is rejected before migrations, with its `user_version`, schema marker, and pool data/limits preserved by the regression test | `TestSQLiteMigratesVersionOnePoolSafetyDefaults`, `TestSQLiteMigrationRollsBackWhenSecondStatementFails`, `TestSQLiteRejectsFutureVersionWithoutChangingDatabase`, `TestModelPoolValidateAcceptsNonNegativeSafetyLimits`, `TestModelPoolValidateRejectsNegativeSafetyLimits`, `TestAdminPoolSafetyJSONRoundTripAndValidation`, `TestPoolSafetyFormsRenderAndPrefillExistingLimits` |
| Local coordination bounds retained idempotency state, evicts oldest terminal records under pressure without denying new active work, and fails closed when the bound is occupied entirely by active operations | `TestAdmissionCoordinatorEvictsOldestTerminalReceiptAtCapacity`, `TestAdmissionCoordinatorRejectStormDoesNotConsumeActiveCapacity`, `TestAdmissionCoordinatorFailsClosedWhenCapacityIsAllActive`, `TestAdmissionCoordinatorExpiredLeasesBecomeEvictable`, `TestCircuitCoordinatorEvictsOldestTerminalAttemptAtCapacity`, `TestCircuitCoordinatorFailsClosedWhenCapacityIsAllActive` |
| Management readiness stays available independently; inference readiness reports HTTP `200 ready` or `503 unavailable` from eligible circuit/health/secret capacity | `TestServiceInferenceReadinessMatrix`, `TestInferenceReadinessHandlerStatusAndJSONContract`, `TestInferenceReadinessHonorsContextDeadline` |
| Lower-priority traffic is shed before high/critical traffic | `TestAdmissionPriorityAndHysteresisAcceptance`, `TestEffectiveLimitUsesPriorityPolicy`, opt-in `TestPriorityIsolationWithRealVLLM` |
| Client priority escalation is removed and server policy is applied | `TestAdmissionPriorityAndHysteresisAcceptance`, `TestForwardRewritesModelAndClientControlledPriority`, opt-in `TestPriorityIsolationWithRealVLLM` |
| SSE bytes are flushed without whole-response buffering | `TestStreamingCancellationAndLeaseLifetime`, `TestStreamingIsByteExactAndRetryStopsAfterFirstByte` |
| A downstream disconnect cancels upstream work and releases leases | `TestStreamingCancellationAndLeaseLifetime`, `TestForwardCancellationStopsUpstream` |
| A short spike is ignored, sustained overload advances without request polling, recovery is hysteretic, and concurrent observer timestamps cannot transiently mark a fresh backend unavailable | `TestAdmissionPriorityAndHysteresisAcceptance`, `TestManagerAdvancesPoolHysteresisWithoutSnapshotReads`, `TestWorkerTreatsConcurrentNewerMetricsSampleAsFresh`, all `TestPoolMachine*` tests |
| A transport failure retries once before response bytes, never after streaming starts | `TestStreamingIsByteExactAndRetryStopsAfterFirstByte`, `TestForwardRetriesOneAlternateBeforeFirstByte`, `TestForwardDoesNotRetryAfterStreamStarts` |
| Admin auth, CSRF, CRUD publication, backend edit/enable/disable/drain/resume, live key usage, and concurrent one-time key displays work | `TestAdminAuthenticationCSRFCRUDAndOneTimeSecret`, `TestBackendEditPageAndEnableToggle`, `TestOverlappingKeyCreationsKeepSeparateOneTimeSecrets`, `TestAdminSecurityRequiresBasicAuthAndMatchingCSRF`, `TestAdminCRUDPublishesEveryRevisionAndDisclosesKeyOnce` |
| Metrics use bounded labels, including five circuit/pool runtime families and distinct unmanaged/unknown circuit encoding; completion logs contain policy/result fields without body or secret data | `TestMetricsExposeRequiredFamiliesWithoutHighCardinalityLabels`, `TestMetricsUnmanagedCircuitEncodingFollowsTopologyLifecycle`, `TestStructuredLoggerWritesSafeCompletionRecord` |
| The real-vLLM fault proxy preserves health, metrics, exact headers/status/bytes and incremental streaming while healthy, injects inference-only `503 e2e_injected_failure`, and Admin cleanup attempts every captured pool/backend restore, joins all failures, and continues to later successful restores | `TestFaultProxyPassesHealthMetricsAndCanToggleInference5xx`, `TestAdminMutationCleanupRestoresPoolAndBackends`, opt-in `TestCircuitBreakerRecoveryWithRealVLLM` |
| Shutdown lets a short stream finish, force-closes a stream after grace expiry, and releases upstream work, monitors, and SQLite | `TestRunLetsActiveStreamFinishInsideGracePeriod`, `TestRunForceClosesActiveStreamAfterGracePeriod`, `TestRunServesHealthAndShutsDownGracefully`, integration harness cleanup checks |
| Gateway-added fake-backend latency is measured against a warmed direct baseline and the optional engineering budget is enforced | `TestPerformanceSmoke` |
| Seeded mixed traffic is proportionally apportioned; successful latency and outcomes are available per class | `TestSmallTrafficMixDoesNotAlwaysFavorLeadingClasses`, `TestRunReportsSuccessfulLatencyAndOutcomesByClass` |

## Commands

Run the full deterministic validation on macOS or Linux:

```bash
go test ./...
go test -race ./...
go vet ./...
```

Run the opt-in local performance target (`p50 < 5 ms`, `p99 < 20 ms`) on an otherwise quiet host:

```bash
LLMGW_RUN_PERF=1 go test ./tests/integration -run TestPerformanceSmoke -count=1 -v
```

The default suite still runs a smaller performance smoke test, but only the opt-in run enforces timing because shared CI and developer machines can be noisy.

Build and inspect the Linux x86-64 delivery artifacts:

```bash
make build-linux-amd64
file dist/*-linux-amd64
```

With a running Docker daemon, verify the non-root scratch image against a fresh writable volume:

```bash
make container-smoke
```

This container smoke was not executed during the 2026-08-27 local evidence run because the Docker daemon was unavailable. No Docker image/runtime result is claimed by that run.

Run the safe post-deployment smoke check, intentional real-vLLM saturation/pool-safety suite, and isolated circuit-resilience suite with the environment from [real-vllm-priority-e2e.md](real-vllm-priority-e2e.md):

```bash
LLMGW_E2E_MODE=smoke make test-real-vllm
LLMGW_E2E_MODE=priority make test-real-vllm
LLMGW_E2E_MODE=resilience make test-real-vllm
```

## Real-GPU evidence

### Recorded Apple Silicon development run — 2026-08-27

All three opt-in real-vLLM modes passed in one isolated local run. The gateway commit at execution was `6b7a29b`. The host was a MacBook Air `Mac16,13` with Apple M4 and 24 GB unified memory. The inference stack was vLLM `0.28.0+cpu` with vLLM-Metal `0.3.0.dev20260827104907`; two loopback `Qwen/Qwen3-0.6B` serving processes each used `max-num-seqs=1`. The resilience profile set the gateway failure threshold and injected failure count to `3` and the open cooldown to `3s`. No credentials are recorded here.

Representative non-secret command shapes were:

```bash
LLMGW_E2E_MODE=smoke go test -count=1 -v -timeout 3m ./tests/e2e -run '^TestProductionSmoke$'
LLMGW_E2E_MODE=priority go test -count=1 -v -timeout 12m ./tests/e2e -run '^TestPriorityIsolationWithRealVLLM$'
LLMGW_E2E_MODE=resilience go test -count=1 -v -timeout 10m ./tests/e2e -run '^TestCircuitBreakerRecoveryWithRealVLLM$'
```

Observed evidence:

- Smoke passed with one available pool, two healthy and metrics-fresh backends, management and inference readiness available, all required metric families, and `360.398334ms` first byte for the complete real stream.
- The priority reference profile used four independent High-load clients with four 768-token requests each. Saturation reached `GatewayInflight=16`, `TotalWaiting=14`, and `AllBackendsWaiting=true`. A temporary `MaxGatewayInflight=1` rejected Critical with the bounded pool-safety envelope; every original pool field was restored before the continuity request. Recovery reached `busy` in `4.290394292s` and `normal` in `6.727045209s`.
- Resilience opened the selected backend circuit at exactly three injected inference failures while health and metrics stayed good, and `/inference-readyz` returned HTTP 503. After the 3-second cooldown, a half-open streaming probe completed with `156.983875ms` first byte; the circuit closed with zero retained failures and inference readiness returned HTTP 200. Cleanup restored both pool limits, pool runtime (`normal`, zero gateway in-flight/waiting), both backend URLs and drain flags, and both backends to healthy, metrics-fresh, closed circuits with zero failures.

Only this summarized result is durable. Raw gateway, vLLM, and terminal process logs from the run were ephemeral and were not retained, so this is not a raw-log artifact archive. The run proves the exercised Apple Silicon/vLLM-Metal topology; it does not sign off CUDA behavior, the selected production model, or production threshold/TTFT calibration. Docker container smoke was also not run because the daemon was unavailable.

### Pending target-hardware evidence

The following production claims remain pending until the three automated modes and the broader manual plan pass on the selected CUDA host/model:

- current vLLM wire compatibility for all three generation endpoints on the selected production vLLM version;
- calibrated priority scheduling and TTFT isolation under the target GPU queue contention;
- cancellation reaching a live model engine and releasing GPU work;
- threshold calibration and TTFT isolation on the target RTX 4070 Ti;
- one-versus-two real serving-group routing behavior.
- inference circuit open/half-open/closed timing and recovery through the loopback fault proxy on the selected CUDA vLLM build.

Use [real-vllm-priority-e2e.md](real-vllm-priority-e2e.md) for the automated gate and [real-gpu-testing.md](real-gpu-testing.md) to collect the additional versioned commands, status snapshots, Prometheus data, gateway logs, vLLM logs, and load-generator reports required for target-hardware sign-off.

## PostgreSQL coordination evidence — 2026-09-14

The default no-database suite covers domain/profile boundaries, SQLite grandfathering, local admission/rate/circuit contracts, nominal 24-hour receipt cleanup, and the explicit bounded-eviction exception, gateway stale/outage/recovery-gate behavior, the ordered recovery state machine, lease-manager renewal/loss/retry/backlog behavior, concurrent circuit-failure replay, notification loss/reconnect polling, migration manifests, readiness, bounded metrics/logging, and compile-time store/coordinator interfaces. The opt-in package `tests/postgres` adds concurrent migration startup, dirty-state and optional pre-16 refusal, committed configuration notification reload, real PostgreSQL CRUD/snapshot and analytics behavior, cross-replica admission contention, unlimited-pool and reduced-limit accounting, ambiguous-result replay and fingerprint conflict, completion/expiry replay safety, future clock skew, RPM and soft-TPM generation semantics, shared circuit probe capacity and same-attempt replay, expired and superseded probes, incompatible replicas, transaction-pooler CRUD/analytics/coordination plus forced server reassignment, outage status, and retained/regressed HA history contracts. It is compiled during verification but is run only through `make test-postgres` or `make test-postgres-pooler` with dedicated test DSNs.

Per the implementation-run constraint, the PostgreSQL integration package was compiled but not executed. Therefore no real PostgreSQL, PgBouncer, Docker, multi-process outage, performance, or failover-topology result is claimed here. Those remain deployment acceptance gates. In particular, a standalone database cannot prove synchronous promotion safety; record retained acknowledged leases/RPM debits and API-key revocations, blocked commits when synchronous durability is unavailable, former-primary fencing, and the managed-provider or PostgreSQL replication settings before production sign-off. See [PostgreSQL production profile](postgresql-production.md).

### PostgreSQL review-remediation verification — 2026-09-14

This later working-tree run preserves the implementation-run record above as historical evidence and adds the following results after the PostgreSQL review fixes:

- `go test ./...` — PASS on Go 1.27.0, Windows amd64.
- `go vet ./...` and `go build ./cmd/...` — PASS.
- `go test -race ./...` — PASS with CGO enabled in Linux amd64 `golang:1.27-bookworm`.
- The eight review overlay regressions — PASS for active lease renewal, stale configuration payload rebuilding, local TPM policy rollover, stale backend reconciliation, concurrent incompatible replica registration, circuit timestamp replay, closed-circuit lock freedom, and multi-batch analytics retention.
- `go test -count=1 -v -timeout 10m ./tests/postgres` — PASS against a disposable PostgreSQL 16 container. The run includes backend-neutral local/PostgreSQL contracts, two in-process gateways sharing PostgreSQL with a real `LeaseManager` and stream held beyond the original TTL, post-lock lease expiry across two pools, analytics filter/null/cache-ratio parity, failure replay after rolling prune, and half-open crashed/expired-peer ordering.
- The same PostgreSQL 16 suite now also covers accepted `+4m` clock skew, admission/probe retention cleanup boundaries with stale same-UUID replay, two HTTP gateways sharing one fake vLLM with SSE disconnect release, and a controlled TCP outage with emergency admission followed by ordered fingerprint/configuration/lease/failure/circuit recovery barriers.
- The default PostgreSQL performance measurement on Windows amd64, Intel Core i5-14600KF, Go 1.27.0, PostgreSQL 16.15 in Docker Desktop, two in-process gateways, one database, one fake vLLM, 50 requests at parallelism 1 reported gateway-added p50 `9.4281 ms` and p99 `12.0094 ms`. This developer-host measurement is not a production acceptance result. The reference gate is explicitly opt-in with `LLMGW_RUN_POSTGRES_PERF=1` and requires `LLMGW_POSTGRES_PERF_REFERENCE` to describe the CPU/host and database topology; it enforces strict p50 `< 5 ms` and p99 `< 20 ms`.
- `TestPostgresRejectsPre16Server` — PASS against a separate disposable PostgreSQL 15 container.

The PostgreSQL 16 run skipped the separately gated reference-performance, PgBouncer, and HA endpoint tests because their qualified environment/DSNs/topologies were not supplied. No production latency, PgBouncer configuration, GPU/vLLM latency, or synchronous promotion/fencing guarantee is claimed by this remediation run; those remain environment- or topology-specific acceptance gates described in the production profile.

### PostgreSQL final review verification — 2026-09-15

The subsequent review run used Go 1.27.0 on Windows amd64 and disposable PostgreSQL 16.15 and PostgreSQL 15 containers. `go test -count=1 ./...`, `go vet ./...`, and `go build ./cmd/...` passed. The full default suite also passed with `-race` and CGO enabled in Linux amd64 `golang:1.27-bookworm`. PostgreSQL-specific replica and gateway recovery tests ran separately against the actual database and passed, followed by the complete `tests/postgres` suite including the PostgreSQL 15 refusal gate.

New regressions cover caller cancellation without a shared outage latch; coordinator-owned deadlines and failures while reading circuit query results; stalled notification connection/LISTEN/close operations without stopping polling; concurrent completion extending a receipt's retention while cleanup runs; circuit cleanup UUID-array encoding; Admin mutation database failures after the readiness preflight, correct HTTP 503 classification and connection-error redaction; and extreme valid token usage without crediting TPM or producing a retry time in the past. Migration tests now start four replicas on a verified empty isolated schema and reject a schema version newer than the binary without changing it.

The maintenance regression seeds 513 expired leases plus one active lease. Under the production 50 ms coordinator timeout, the earlier per-row implementation repeatedly timed out without progress; ordered bulk locking/deletion now drains 256-row batches while preserving the active lease and all terminal receipts. Separate tests verify continued multi-batch draining, fairness, failure isolation and the maintenance pass deadline. Cleanup runs independently of recovery/readiness publication.

The separate PgBouncer tests passed through PgBouncer 1.24.0 in transaction mode with `max_prepared_statements=0`, including forced server-connection reassignment. The runtime URL used the pooler and migration URL connected directly to PostgreSQL.

The opt-in PostgreSQL reference-performance gate also passed: gateway-added p50 `4.776547 ms`, p99 `6.208169 ms` (thresholds `< 5 ms` and `< 20 ms`). The recorded reference was an Intel Core i5-14600KF Windows host, Docker Desktop Linux amd64, Go 1.27.0, PostgreSQL 16.15, and a Linux test process sharing the database's loopback network namespace. The workload used two in-process gateways, one fake vLLM, 500 requests and parallelism 1. This qualifies that local reference topology only; it does not establish cross-host or production GPU latency.

SQLite/local keeps the explicitly accepted bounded-eviction contract documented in the specification; the strict durable 24-hour replay guarantee applies to PostgreSQL. Actual synchronous promotion, loss of synchronous durability, former-primary fencing, and the target GPU/vLLM deployment remain separate operator acceptance gates. No HA promotion or target GPU result is claimed by this run.
