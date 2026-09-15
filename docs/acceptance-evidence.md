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
| Metrics use bounded labels, precise admission reasons, one queue sample, pool pressure/state, backend selections, and distinct unmanaged/unknown circuit encoding; pre-forward HTTP failures retain the same decision vocabulary; completion logs contain decision/queue/policy/result fields without body or secret data | `TestMetricsExposeRequiredFamiliesWithoutHighCardinalityLabels`, `TestMetricsUnmanagedCircuitEncodingFollowsTopologyLifecycle`, `TestForwardRecordsAdmissionWaitOnceAcrossRetry`, `TestPublicObserverCoversNonForwardedOutcomes`, `TestLoggerIncludesDecisionTelemetry` |
| The real HTTP admission path increases `priority_concurrency_limit` exactly for rejected lower classes and counts each selected backend lease | `TestAdmissionPriorityAndHysteresisAcceptance` |
| The optional Prometheus/Grafana overlay pins consumer images, binds loopback ports, uses read-only provisioning, and provides the four-query causal dashboard with bounded filters | `TestObservabilityOverlayAndDashboardContract`, `TestMetricsResponseLookup` |
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

Validate and start the optional decision-telemetry stack:

```bash
go test ./tests/compose -run TestObservabilityOverlayAndDashboardContract -count=1
docker run --rm --entrypoint /bin/promtool \
  --mount "type=bind,source=$PWD/deploy/observability,target=/etc/llmgw-observability,readonly" \
  prom/prometheus:v3.14.0 check config /etc/llmgw-observability/prometheus.yml
docker compose --env-file .env -f compose.yaml -f compose.observability.yaml config --quiet
docker compose --env-file .env -f compose.yaml -f compose.observability.yaml up -d --build --wait --wait-timeout 900
```

Prometheus is expected at `http://127.0.0.1:9090`; the provisioned Grafana dashboard is `http://127.0.0.1:3000/d/llmgw-gateway-decisions`.

This container smoke was not executed during the 2026-08-27 local evidence run because the Docker daemon was unavailable. No Docker image/runtime result is claimed by that run.

Run the safe post-deployment smoke check, intentional real-vLLM saturation/pool-safety suite, and isolated circuit-resilience suite with the environment from [real-vllm-priority-e2e.md](real-vllm-priority-e2e.md):

```bash
LLMGW_E2E_MODE=smoke make test-real-vllm
LLMGW_E2E_MODE=priority make test-real-vllm
LLMGW_E2E_MODE=resilience make test-real-vllm
```

## Real-GPU evidence

### Recorded gateway-decision telemetry run — 2026-08-30

The decision-telemetry stack at commit `f4cc474` was validated on an NVIDIA GeForce RTX 4070 Ti with 12,282 MiB VRAM and driver `616.56`. Two `vllm/vllm-openai:v0.28.0` containers served `Qwen/Qwen3-0.6B` with `max-num-seqs=1` and priority scheduling. Prometheus `3.14.0`, Grafana `13.2.0`, and the locally built gateway ran from the checked-in Compose overlay; the Prometheus gateway target was `up`, the provisioned datasource health was `OK`, and dashboard UID `llmgw-gateway-decisions` loaded successfully. No credential or generated API-key value is retained here.

The retained Grafana/Prometheus window was `2026-08-30T17:24:47Z` through `2026-08-30T17:25:21Z`. Four isolated saturation clients were temporarily assigned Background priority and each submitted four 128-token streams. The profile deliberately used lower-priority load so that the dashboard's High series contained only the protected probe rather than the load itself. The E2E harness temporarily disabled the pool's global waiting limit during class isolation, exercised `maxGatewayInflight=1` separately, and restored the original `32/8` limits after load removal. Temporary client classes and API keys were also restored/revoked.

Observed evidence on the aligned window:

- pool state reached `saturated`; `llmgw_pool_pressure{model="qwen"}` rose from approximately zero to `0.7293` and returned to `0.00188` by the end of the window;
- gateway logs and the Prometheus counter both recorded exactly four Background rejections with `decisionReason="priority_concurrency_limit"` (three attack-resistant Low probes plus the immediate hysteresis probe), while the unrelated `pool_waiting_limit` rate remained zero;
- `llmgw_backend_selected_total` increased by `22` selections across both backends;
- the High probe remained admitted: first byte moved from `186.9918ms` at baseline to `560.7569ms` under saturation (ratio `2.999`), while request duration moved from `218ms` to `575ms`; Grafana rendered request-duration and TTFT p95 at `975ms`, below one second for the calibrated local profile;
- High gateway queue-wait p95 was `4.75ms`, separating gateway decision time from the remaining upstream GPU wait;
- recovery reached `busy` after `11.5818242s` and `normal` after `23.610879s`; `TestPriorityIsolationWithRealVLLM` passed in `30.83s` (`go test` package time `31.845s`) including cleanup.

The rendered dashboard was checked in the in-app browser at the exact window. Its first row displayed, in order, `GPU pool pressure rises` (max `0.729`), `Low receives 429 decisions` (`priority_concurrency_limit`, total `4.00 req/s` over the short event window), `High traffic remains admitted` (request/TTFT p95 `975ms`), and `High gateway queue wait` (`4.75ms`). The same view also showed the pool state timeline, per-client inflight, request rates, and backend selection. The High probe remained admitted, but its first-byte latency increased from `186.9918ms` before load to `560.7569ms` under load. This is a development-sized observability and isolation gate, not a production-model latency SLO.

### Recorded RTX 4070 Ti Docker run — 2026-08-28

The canonical Docker Quick Start and all three opt-in real-vLLM modes passed on the repository working tree based at commit `a138ab8`. The host GPU was an NVIDIA GeForce RTX 4070 Ti with 12,282 MiB VRAM and driver `610.47`. Docker `29.1.2` and Compose `2.40.3` ran Linux containers. The inference stack used two private `vllm/vllm-openai:v0.28.0` services with `Qwen/Qwen3-0.6B`, `max-model-len=1024`, `max-num-seqs=1`, priority scheduling, prefix caching, prompt-token details, and request-ID headers. Each service used `--gpu-memory-utilization 0.32`; Compose reserved the NVIDIA device, and only the gateway was published to host loopback. No credentials are recorded here.

Representative non-secret command shapes were:

```console
docker compose config --quiet
docker compose run --rm --no-deps --entrypoint nvidia-smi vllm-a
docker compose up -d --build --wait --wait-timeout 900
go test -count=1 -v -timeout 10m ./tests/e2e -run '^TestProductionSmoke$'
go test -count=1 -v -timeout 10m ./tests/e2e -run '^TestPriorityIsolationWithRealVLLM$'
docker run --rm --network container:vllm-priority-gateway-gateway-1 ... golang:1.27-alpine go test -count=1 -v -timeout 10m ./tests/e2e -run '^TestCircuitBreakerRecoveryWithRealVLLM$'
docker run --rm -v /path/to/repository:/src:ro -w /src golang:1.27-alpine go test -count=1 ./...
docker run --rm -v /path/to/repository:/src:ro -w /src golang:1.27-alpine go vet ./...
```

Observed evidence:

- The documented Compose configuration, GPU probe, build/wait, management readiness, two-backend inference readiness, model listing, and checked-in SSE request passed. Compose waited for the gateway binary's HTTP healthcheck. The stream used upstream model `qwen-test`, reported vLLM `0.28.0`, and ended with `[DONE]`.
- Smoke saw one available pool, two healthy and metrics-fresh backends, all required metric families, and `240.6163ms` first byte. After restarting only the gateway, revision `63`, Admin state, the rotated client key, and two-backend readiness persisted; the repeated smoke first byte was `242.46ms`.
- The priority profile used four independent High-load clients with four 768-token requests each. Saturation reached `GatewayInflight=16`, `TotalWaiting=14`, pressure `0.7902`, and `AllBackendsWaiting=true`. Three Low probes—including body/header priority spoofing and session affinity—were rejected while High/Critical streams continued. The temporary pool-wide limit rejected Critical, one-backend drain preserved High admission, and cleanup restored the exact pool/backend state. Recovery reached `busy` in `14.0900556s` and `normal` in `24.9508766s`; the test passed in `47.93s`.
- Resilience opened backend `1` after exactly five injected failures while health and metrics remained good, changed inference readiness to HTTP 503, then recovered through half-open to a closed circuit and HTTP 200. The recovery stream first byte was `368.449514ms`; the test passed in `17.75s`. Final state was pool `normal` with limits `32/8`, two available backends at `http://vllm-a:8000` and `http://vllm-b:8000`, neither draining, both healthy/fresh, and both circuits closed.
- The Compose contract test passed on the host. The complete `go test -count=1 ./...` suite and `go vet ./...` passed in `golang:1.27-alpine`; the Docker Compose gateway image also rebuilt successfully from the tested working tree.

Only this summarized result is durable; raw process logs are not an artifact archive. The run proves the small-model CUDA/Docker topology and gateway scenarios above. It does not calibrate a production model, multi-GPU serving groups, production thresholds, or production TTFT targets.

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

### Remaining production-calibration evidence

The small-model CUDA/Docker gate above is complete. The following production claims remain pending until the broader manual plan passes with the selected production model and topology:

- current vLLM wire compatibility for all three generation endpoints on the selected production vLLM version;
- calibrated priority scheduling and TTFT isolation under the target GPU queue contention;
- cancellation reaching a live model engine and releasing GPU work;
- production threshold and TTFT calibration for the selected model;
- one-versus-two independently provisioned serving-group routing behavior.

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

### Integration with main — 2026-09-15

Integrated `main` revision `80b88b2` into the PostgreSQL feature based on `079821f`. The adaptation retains main's application/server and forwarding lifecycle, current-snapshot and retry-time API-key checks, session affinity headers, `/v1/load`, deletion controls, decision telemetry, and observation-based pool state. PostgreSQL initialization, readiness, recovery, admission, and circuit coordination now use these boundaries.

New regressions verify client/backend/pool deletion, rejection of referenced pools, transactional rollback, committed notifications, permit supersession, stale deleted topology, in-flight admission completion and replay after deletion, and preservation of pool versus client concurrency diagnostics on immutable rejected receipts. PostgreSQL migration 4 adds the bounded diagnostic scope without changing public rejection behavior. A real PostgreSQL application test verifies lease renewal continues between the shutdown signal and HTTP drain completion, then records durable release on application close. Lease completion draining uses the remaining shared shutdown context; coordinated backend completion also releases local inflight accounting if completion panics.

Validation on the integrated tree:

- Go 1.27.0 Windows amd64: `go test -count=1 -timeout=5m ./...`, `go vet ./...`, `go build ./...`, and `go mod verify` passed. All 165 tracked Go files are gofmt-canonical after checkout newline normalization.
- Linux amd64, CGO enabled: every package passed with the race detector, including actual PostgreSQL 16.15, PostgreSQL 15 refusal, and PgBouncer 1.24 transaction mode with `max_prepared_statements=0`. The full run first exposed a PostgreSQL HTTP fixture that waited for backend eligibility before the independently cached pool state; the corrected PostgreSQL package rerun passed 69 top-level tests. SSE passed ten repeated Linux runs; outage/recovery passed three repeated race runs. No data race was reported.
- Docker image build passed. SQLite and PostgreSQL containers returned HTTP 200 from `/healthz`, `/readyz`, and `/coordination-readyz`, and both shut down with exit code 0.
- The PostgreSQL reference-performance gate passed: gateway-added p50 `4.245502 ms`, p99 `5.635739 ms`. Reference topology: Intel Core i5-14600KF Windows host, Docker Desktop Linux amd64, Go 1.27.0, PostgreSQL 16.15; two in-process gateways and one fake vLLM, 500 sequential requests, test process sharing database loopback.
- The separate strict SQLite performance gate exceeded its 5/20 ms p50/p99 budget on this host: integrated tree `26.738629/231.022284 ms`; clean `main` at `80b88b2` also failed at `31.181340/475.643359 ms`. Both completed 500 requests at parallelism 16 without request errors. These single-run comparisons do not establish a latency improvement or production acceptance.

Actual synchronous promotion, loss of synchronous durability, former-primary fencing, and target GPU/vLLM tests were not run. Their deployment-specific acceptance requirements remain unchanged. Raw local logs are temporary; this summary is the durable evidence.
