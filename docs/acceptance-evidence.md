# MVP acceptance evidence

This document maps the source specification's MVP acceptance criteria to repeatable evidence. The automated suite uses a real SQLite database, the production HTTP composition, monitor workers, and the deterministic fake vLLM server. Tests that require CUDA and a real vLLM process are intentionally tracked separately.

## Automated acceptance matrix

| Criterion | Evidence |
|---|---|
| Unknown and revoked API keys return `401`; a valid key is accepted | `TestAuthenticationAndModelAccessAcceptance`, `TestPublicAuthenticationCasesUseSameUnauthorizedEnvelope` |
| Plaintext API keys are absent from SQLite, WAL, and SHM files | `TestAdminAuthenticationCSRFCRUDAndOneTimeSecret`, `TestSQLiteNeverReceivesPlaintextKey` |
| A client sees only explicitly allowed models; forbidden models are rejected | `TestAuthenticationAndModelAccessAcceptance`, `TestModelsListsOnlyExplicitEnabledAccess` |
| Chat Completions, Completions, and Responses are forwarded | `TestSupportedOpenAIRoutesAcceptance` |
| 100 requests select pressure `0.3` rather than `1.1` | `TestRoutingUsesLeastPressureAndHealthRecovery` |
| Failed backends leave routing and recovered backends return | `TestRoutingUsesLeastPressureAndHealthRecovery`, `TestWorkerHealthFailureAndRecoveryCounts` |
| Lower-priority traffic is shed before high/critical traffic | `TestAdmissionPriorityAndHysteresisAcceptance`, `TestEffectiveLimitUsesPriorityPolicy` |
| Client priority escalation is removed and server policy is applied | `TestAdmissionPriorityAndHysteresisAcceptance`, `TestForwardRewritesModelAndClientControlledPriority` |
| SSE bytes are flushed without whole-response buffering | `TestStreamingCancellationAndLeaseLifetime`, `TestStreamingIsByteExactAndRetryStopsAfterFirstByte` |
| A downstream disconnect cancels upstream work and releases leases | `TestStreamingCancellationAndLeaseLifetime`, `TestForwardCancellationStopsUpstream` |
| A short spike is ignored and overload recovery is hysteretic | `TestAdmissionPriorityAndHysteresisAcceptance`, all `TestPoolMachine*` tests |
| A transport failure retries once before response bytes, never after streaming starts | `TestStreamingIsByteExactAndRetryStopsAfterFirstByte`, `TestForwardRetriesOneAlternateBeforeFirstByte`, `TestForwardDoesNotRetryAfterStreamStarts` |
| Admin auth, CSRF, CRUD publication, drain/resume, and one-time key display work | `TestAdminAuthenticationCSRFCRUDAndOneTimeSecret`, `TestAdminSecurityRequiresBasicAuthAndMatchingCSRF`, `TestAdminCRUDPublishesEveryRevisionAndDisclosesKeyOnce` |
| Metrics use bounded labels; completion logs contain policy/result fields without body or secret data | `TestMetricsExposeRequiredFamiliesWithoutHighCardinalityLabels`, `TestStructuredLoggerWritesSafeCompletionRecord` |
| Shutdown is graceful and backend monitor workers do not leak | `TestRunServesHealthAndShutsDownGracefully`, integration harness cleanup checks |
| Fake-backend latency is measured and the optional engineering budget is enforced | `TestPerformanceSmoke` |

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

## Real-GPU evidence

The following claims cannot be established by a macOS fake-backend run and remain pending until a CUDA host with vLLM is available:

- current vLLM wire compatibility for all three generation endpoints;
- actual vLLM priority scheduling under GPU queue contention;
- cancellation reaching a live model engine and releasing GPU work;
- threshold calibration and TTFT isolation on the target RTX 4070 Ti;
- one-versus-two real serving-group routing behavior.

Use [real-gpu-testing.md](real-gpu-testing.md) to collect the versioned commands, status snapshots, Prometheus data, gateway logs, vLLM logs, and load-generator reports required for that sign-off.
