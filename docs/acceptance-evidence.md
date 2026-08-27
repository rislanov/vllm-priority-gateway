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
| Failed backends leave routing and recovered backends return; monitor polls do not follow redirects | `TestRoutingUsesLeastPressureAndHealthRecovery`, `TestWorkerHealthFailureAndRecoveryCounts`, `TestWorkerDoesNotFollowHealthOrMetricsRedirects` |
| Lower-priority traffic is shed before high/critical traffic | `TestAdmissionPriorityAndHysteresisAcceptance`, `TestEffectiveLimitUsesPriorityPolicy`, opt-in `TestPriorityIsolationWithRealVLLM` |
| Client priority escalation is removed and server policy is applied | `TestAdmissionPriorityAndHysteresisAcceptance`, `TestForwardRewritesModelAndClientControlledPriority`, opt-in `TestPriorityIsolationWithRealVLLM` |
| SSE bytes are flushed without whole-response buffering | `TestStreamingCancellationAndLeaseLifetime`, `TestStreamingIsByteExactAndRetryStopsAfterFirstByte` |
| A downstream disconnect cancels upstream work and releases leases | `TestStreamingCancellationAndLeaseLifetime`, `TestForwardCancellationStopsUpstream` |
| A short spike is ignored, sustained overload advances without request polling, and recovery is hysteretic | `TestAdmissionPriorityAndHysteresisAcceptance`, `TestManagerAdvancesPoolHysteresisWithoutSnapshotReads`, all `TestPoolMachine*` tests |
| A transport failure retries once before response bytes, never after streaming starts | `TestStreamingIsByteExactAndRetryStopsAfterFirstByte`, `TestForwardRetriesOneAlternateBeforeFirstByte`, `TestForwardDoesNotRetryAfterStreamStarts` |
| Admin auth, CSRF, CRUD publication, backend edit/enable/disable/drain/resume, live key usage, and concurrent one-time key displays work | `TestAdminAuthenticationCSRFCRUDAndOneTimeSecret`, `TestBackendEditPageAndEnableToggle`, `TestOverlappingKeyCreationsKeepSeparateOneTimeSecrets`, `TestAdminSecurityRequiresBasicAuthAndMatchingCSRF`, `TestAdminCRUDPublishesEveryRevisionAndDisclosesKeyOnce` |
| Metrics use bounded labels; completion logs contain policy/result fields without body or secret data | `TestMetricsExposeRequiredFamiliesWithoutHighCardinalityLabels`, `TestStructuredLoggerWritesSafeCompletionRecord` |
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

Run the safe post-deployment smoke check or the intentional real-vLLM saturation suite with the environment from [real-vllm-priority-e2e.md](real-vllm-priority-e2e.md):

```bash
LLMGW_E2E_MODE=smoke make test-real-vllm
LLMGW_E2E_MODE=priority make test-real-vllm
```

## Real-GPU evidence

The real-vLLM E2E suite has been exercised with two vLLM-Metal processes on Apple Silicon and provides reproducible evidence for Completions streaming compatibility, queue-driven saturation, Low shedding, High/Critical continuity, optional drain, and recovery. The following production claims remain pending until the same suite and the broader manual plan pass on the target CUDA host:

- current vLLM wire compatibility for all three generation endpoints on the selected production vLLM version;
- calibrated priority scheduling and TTFT isolation under the target GPU queue contention;
- cancellation reaching a live model engine and releasing GPU work;
- threshold calibration and TTFT isolation on the target RTX 4070 Ti;
- one-versus-two real serving-group routing behavior.

Use [real-vllm-priority-e2e.md](real-vllm-priority-e2e.md) for the automated gate and [real-gpu-testing.md](real-gpu-testing.md) to collect the additional versioned commands, status snapshots, Prometheus data, gateway logs, vLLM logs, and load-generator reports required for target-hardware sign-off.
