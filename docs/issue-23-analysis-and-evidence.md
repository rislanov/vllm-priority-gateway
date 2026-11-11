# Issue #23 — Analysis and evidence log

Planning date: 2026-11-10 (Europe/Budapest).
Issue: https://github.com/rislanov/vllm-priority-gateway/issues/23
Base commit: `c072e66466d68088c924b94421af3d7c9646d171`.
Branch: `codex/issue-23-token-reservations`, created from the updated origin/main.

## Preliminary analysis

This changes the admission protocol across policy storage, API/UI, preflight, two coordinator backends, streaming lifecycle, emergency mode, and vLLM qualification. A middleware in front of the HTTP proxy alone is insufficient.

Existing functionality to reuse:

- #22: slot reserve and total/lower-priority inflight checks. This does not protect token capacity.
- #33 (closed): authenticated `/tokenize`, a separate utility coordinator, and shared `Proxy.Tokenize`. The public utility must not be used as a nested inference lifecycle.
- Admission receipts/fingerprints, lease renewal/expiry, partial-completion retry, and PostgreSQL lost-commit tests.
- Inference usage inspection and soft-TPM debits based on actual usage; a bounded completion queue.
- PostgreSQL startup compatibility/recovery barriers and graceful unregister.

Critical risks and proposed responses:

| Risk | Proposed mitigation |
| --- | --- |
| Checking token budget before the SQL transaction, then allocating separately | Check sums and write lease/RPM/reservation in one existing admission transaction |
| Duplicates after timeout/lost commit response | Immutable reservation in the same fingerprint/receipt; normal retry uses the same operation ID |
| Double release from expiry+Complete | Stored R and one terminal/removal path; late actual usage gets a separate once-only TPM debit |
| A foreign Renew removes a local lease | Source-confirmed lead; mandatory regression/fix before adding counters; related to #43 |
| Policy changes during tokenization | Revalidate/re-estimate with bounded retry; do not dispatch an old payload after model/profile changes |
| Enabling a budget with legacy active requests | Enable drain gate; cluster-wide drain for legacy emergency overlap |
| `/tokenize` and inference format input differently | Pinned model/template profile, explicit compatibility fixtures/fallback; do not guess a Responses-to-chat conversion |
| Estimation overloads the tokenizer | Shared normal utility quotas, bounded deadline/body, separate local emergency quotas |
| Accidentally removing emergency admission | User explicitly requires preserving it; local token caps and independent emergency preflight |
| Normal/emergency overlap after recovery | Retain local lifecycle accounting until completion; explicitly document the absence of a global bound |
| Unknown usage appears as zero | Separate unknown state; do not insert estimates into actual analytics/TPM |
| Lower priority fills the pool token budget | Do not promise priority token isolation; bound request sizes/client budgets and measure real-vLLM effects; a separate token reserve is out of scope |

## Checks actually performed during planning

Environment: macOS/darwin arm64, `go version go1.27.3 darwin/arm64`.

| Check | Result |
| --- | --- |
| `git fetch origin`, issues #23 and #33 through the GitHub connector | #23 open, assigned to rislanov; #33 closed/completed |
| `go test ./internal/admission ./internal/coordination/... ./internal/gateway ./internal/proxy ./internal/httpapi ./tests/integration` | PASS (exit0), existing implementation |
| `go test -count=1 ./...` | PASS (exit0), existing implementation; not evidence for the new feature |
| Live PostgreSQL integration | Not run: `LLMGW_POSTGRES_TEST_DSN` unset; conditional tests skip |
| Real vLLM / GPU / TTFT | Not run: e2e mode/backend not configured; conditional tests skip |
| Race/vet/browser qualification of the new feature | Not run: implementation does not exist yet |

Complete planning baseline output: `/tmp/issue-23-baseline-tests.log` (a temporary local file, not a durable artifact). After implementation, evidence must reference the new tested commit and retained sanitized results rather than this baseline.

Codebase graph use followed Tier 2: index_status before discovery, search_graph, Forward/Complete traces, and exact snippets. Evidence-path coverage was checked through check_index_coverage (last check while writing the specification: generation `2026-11-10T22:38:41Z`, no_recorded_issue/metadata_match). The graph updates automatically. This is a best-effort signal, not proof of defect absence. Broad symbol searches with incomplete pagination were used only for positive discovery; no exhaustive conclusions were drawn. Source reads confirmed material findings and resolved ambiguous traces of Go methods with the same name.

Upstream schemas were checked against pinned v0.28.0 source, linked in the [specification](superpowers/specs/2026-11-10-issue-23-token-reservations-design.md). This verifies API shape, not the live model/chat behavior or every template option.

## Implementation acceptance

A separate agent performed the specification's builder review under the brainstorming skill. Its five material questions were resolved in the specification/plan: exact input fields for exact/fallback estimation; emergency transition after normal preflight failure; mandatory cluster-wide drain of legacy emergency work before enabling; retaining old reservations in sums after disabling; and a replay cutoff that does not advance into the future during receipt eviction. This was a document review, not a code/security review of the implementation.

| Criterion | Implementation evidence |
| --- | --- |
| AC01 | The disabled case of [HTTP matrix](../internal/gateway/token_admission_http_test.go) forwards without rewriting the output allowance, estimating, or reserving. The ordinary suite also exercises legacy soft TPM and emergency behavior. |
| AC02 | [Policy resolution](../internal/domain/request_size_test.go), [SQLite settings](../internal/store/sqlite_test.go), [PostgreSQL round trip](../tests/postgres/request_size_configuration_test.go), [Admin API](../internal/httpapi/request_size_test.go), and [form tests](../internal/web/request_size_test.go) exercise precedence, validation, round trip, partial updates, and effective limits. |
| AC03 | [Adapter](../internal/admission/request_size_test.go) and [estimator](../internal/gateway/token_estimator_test.go) fixtures exercise Unicode, template options, tools and Responses fallback/rejection, and missing/malformed counts. The direct vLLM qualification compared ten exact input shapes on each serving backend. |
| AC04 | [Payload matrix and arithmetic](../internal/admission/request_size_test.go), [HTTP matrix](../internal/gateway/token_admission_http_test.go), and [SQLite HTTP integration](../tests/integration/token_reservation_test.go) exercise the three endpoints, caps/defaults and aliases, streaming, invalid payloads, batches, and fan-out. |
| AC05 | [Shared coordinator contract](../internal/coordination/contracttest/token_reservation.go), [local contract](../internal/coordination/local/token_reservation_test.go), and [live PostgreSQL contention](../tests/postgres/token_reservation_test.go). In the two-replica run 100 concurrent acquisitions admitted exactly ten requests, with 1,000 tokens reserved. |
| AC06 | [Publication/replay tests](../internal/coordination/local/token_reservation_eviction_test.go), [policy changes during estimation](../internal/gateway/token_admission_http_test.go), and [PostgreSQL policy race](../tests/postgres/token_reservation_test.go) cover lowered budgets, stale policy, profile/key changes, and the legacy-lease transition. |
| AC07 | [Local lease lifecycle](../internal/coordination/local/token_reservation_test.go), [retry/cancel/panic](../internal/gateway/token_admission_http_test.go), [live PostgreSQL expiry paths](../tests/postgres/token_reservation_test.go), and [two HTTP replicas](../tests/postgres/token_reservation_http_test.go). A renewed stream held capacity beyond its original TTL, the peer received 429, and cancellation released capacity. |
| AC08 | [Fake-clock accounting](../internal/coordination/local/token_reservation_test.go), [bounded completion backlog](../internal/coordination/lease_manager_test.go), and [usage telemetry](../internal/observability/token_reservation_test.go) separate known, missing, late and duplicate usage and record actual usage above the estimate. |
| AC09 | [Live PostgreSQL lost COMMIT response](../tests/postgres/token_reservation_test.go), [partial committed prefix](../tests/postgres/completion_progress_test.go), [local eviction](../internal/coordination/local/token_reservation_eviction_test.go), and [ambiguous HTTP fallback](../internal/gateway/token_admission_http_test.go). Replay preserves the operation ID and never allocates or debits twice. |
| AC10 | [Emergency class and token caps](../internal/coordination/emergency_tokens_test.go), [independent estimation](../internal/gateway/emergency_estimator_test.go), and [two HTTP replicas with PostgreSQL disconnected](../tests/postgres/token_reservation_http_test.go). High and Critical each held 600 local tokens on separate replicas. Shared reservations stayed at zero during the outage. Recovered normal admission created a 600-token shared reservation while both local reservations remained. A second outage still enforced the original replica-local budget. |
| AC11 | [Estimator deadline and release](../internal/gateway/token_estimator_test.go), [bounded decode](../internal/proxy/token_estimate_test.go), [shared utility limits](../tests/postgres/tokenization_test.go), and [utility isolation](../internal/gateway/tokenization_test.go) cover the shared utility boundary, malformed/error responses, deadline cleanup, and cancellation. |
| AC12 | [Observability](../internal/observability/token_reservation_test.go), [replay/expiry observer](../internal/coordination/local/token_reservation_test.go), and [real PostgreSQL privacy](../tests/postgres/token_reservation_http_test.go). The latter verifies both replicas see the same shared 600-token gauge source and checks sentinel prompt, generated and tokenizer strings against receipts, logs, metrics, and API errors. |
| AC13 | [Migration test](../internal/store/postgres/request_size_migration_test.go), [contract-version upgrade](../tests/postgres/token_reservation_test.go), and [PostgreSQL production rollout](postgresql-production.md#request-size-admission) cover defaults, compatibility, drain, and downgrade guards. |
| AC14 | The [real-vLLM harness](../tests/e2e/token_reservation_test.go) runs the paired mixed workload described in the [runbook](real-vllm-priority-e2e.md#request-size-admission-with-real-vllm). Direct backend qualification and all three post-review pairs passed (479.24s). |

### Verification environment

Code verified at `2d81330`. Darwin arm64, Go 1.27.3, PostgreSQL 16.15 in Docker. PostgreSQL suites use a dedicated database. The general Go test and race suites run without PostgreSQL or GPU configuration. `make test-postgres` runs the conditional PostgreSQL tests on a live database, including two gateway instances. The earlier Task 4 suite hit a transient serialization error in `TestGatewayCoordinationResetConcurrentStarts`. Eight focused repetitions and the full rerun passed.

| Command | Result |
| --- | --- |
| `go test -count=1 ./...` | Pass. External PostgreSQL and real-vLLM tests gated by their environment variables. |
| `go test -race ./...` | Pass with the same external gates. |
| `go vet ./...` | Pass. |
| `go build ./cmd/...` | Pass. |
| `make test-web-unit` | 19 passed. |
| `make test-postgres` | Pass against PostgreSQL 16.15. Separately gated performance, PostgreSQL <16, transaction pooler, and HA scenarios skipped. |
| `go test -v ./internal/store/postgres -run TestRequestSizeMigration -count=1` with the PostgreSQL DSN | Pass. |
| `go test -v ./tests/postgres -run TestPostgresTokenEmergencyRecoveryOverlap -count=1` with the PostgreSQL DSN | Pass on two HTTP gateways through a PostgreSQL fault proxy. |

### Whole-branch review

An independent reviewer read the feature through `1b9376a` and found two preflight lifetime defects. Cleanup used the expired inference deadline, so utility leases could remain active until TTL. A normal preflight configured longer than the utility timeout could also outlive its lease. Commit `2d81330` gives cleanup a bounded fresh context and bounds every normal tokenizer call by the utility timeout within the overall preflight deadline. The regression first reproduced both findings and then passed with real local normal and emergency coordinators. The complete Go and PostgreSQL suites passed again after the fix.

### Real-vLLM qualification

The first full run used gateway commit `faea1f5`. The post-review run used `2d81330`. It ran in SQLite mode with two native vLLM-Metal processes on an Apple M4 10-core GPU with 24 GiB unified memory. The runtime reported MLX on `Device(gpu, 0)` and PyTorch on MPS. This machine ran macOS 27.2.1.

- Qwen/Qwen3-0.6B model and tokenizer revision `c1899de289a04d12100db370d81485cdf75e47ca`. Template SHA-256 `a55ee1b1660128b7098723e0abcd92caa0788061051c62d51cbe87d9cf1974d8`. Estimation profile `issue23-qwen3-metal-v1`.
- Native vLLM `0.28.0+cpu` with vLLM-Metal `0.3.0.dev20260827104907` and MLX `0.32.0`. Installed wheel RECORD fingerprints, respectively `b6f21a7e5b9a05653badc87a495c7015eaaeacf9856ad3da95ca43c32630efe1` and `9d1f8c66acf3339054372977fddc978bd3d962d4b562d7c18e70159e68872c7b`.
- Priority scheduler, four sequences per worker, context 2,048, paged KV, prefix caching disabled, Metal memory fraction 0.18 per worker. Both workers ran on the same GPU with Gloo bound to the loopback interface.
- Pool input maximum 1,024, output maximum 256, output default 32, outstanding budget 1,100, 64 gateway slots, no slot reserve, no wait queue. No class overrides or client token budgets. The clients' concurrency limits were 40 High and 32 Background, with RPM/TPM disabled. Shared utility capacity was 12/client and 12/backend with 2,400 RPM/client and burst 10. Health and metrics sampled every 250 ms. Pressure entry/recovery windows were 15 s and 500 ms.
- Direct generation reported exactly the estimated input count for all 20 checks, ten per worker. The checks covered text and batch input, token IDs, Unicode, chat history, continuation and template options/override. Fallback was set to reject on this target.

The initial calibration used 200 ms High cadence, four Background requests per burst, and a 256-token Background output allowance. It overloaded the baseline before a full pair could complete. Eight Background requests reached the existing concurrency limit and ten admitted streams exceeded the 60 s client deadline. The measured experiment used a 500 ms cadence and two Background requests per burst with a 128-token output allowance. High used 8 output tokens and 100 probes per run. The paired runs used the same seeded prompts and schedule in both modes.

`LLMGW_E2E_MODE=tokens make test-real-vllm` passed in 479.24s. The smoke, priority, reserve, and resilience scenarios skipped as designed in tokens mode.

| Pair / seed | Order | Mode | High admitted / attempts | High TTFT p50/p95/p99 (ms) | High end-to-end p50/p95/p99 (ms) | Background completed / attempts | Background tokens/s | Mean preflight ms (samples) | Pool/client token denials | Peak outstanding |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 / 23 | Off first | Off | 100/100 | 200.9 / 1001.4 / 1462.1 | 668.1 / 1473.7 / 1930.2 | 20/20 | 49.0 | — (0) | 0/0 | 0 |
| 1 / 23 | Off first | On | 100/100 | 86.1 / 166.4 / 177.8 | 270.8 / 358.8 / 371.1 | 10/20 | 25.6 | 2.37 (120) | 10/0 | 756 |
| 2 / 24 | On first | Off | 100/100 | 203.7 / 1166.7 / 2213.7 | 681.3 / 1994.9 / 2952.4 | 20/20 | 49.1 | — (0) | 0/0 | 0 |
| 2 / 24 | On first | On | 100/100 | 86.0 / 164.5 / 180.3 | 269.9 / 358.2 / 375.7 | 10/20 | 25.6 | 2.38 (120) | 10/0 | 756 |
| 3 / 25 | Off first | Off | 100/100 | 125.4 / 748.4 / 811.5 | 380.4 / 1187.2 / 1478.4 | 20/20 | 49.6 | — (0) | 0/0 | 0 |
| 3 / 25 | Off first | On | 100/100 | 77.3 / 124.8 / 159.4 | 230.1 / 298.2 / 348.4 | 10/20 | 25.6 | 2.60 (120) | 10/0 | 756 |

All 300 of 300 High requests were admitted and completed with admission off, and all 300 of 300 with it on. High p95 TTFT fell by 83–86% across the three pairs. The enabled runs rejected 30 Background requests for the pool budget and completed fewer Background tokens per second. No utility-limit rejections, unknown usage, missing TTFT, stream errors, output-cap violations, or observed underestimates occurred. The harness confirmed recovery to zero outstanding, an admitted capped High stream when capacity was available, deterministic oversized-output rejection, and restoration of the original pool policy. These measurements apply to this workload and hardware.

Documents: [specification and acceptance criteria](superpowers/specs/2026-11-10-issue-23-token-reservations-design.md), [step-by-step plan](superpowers/plans/2026-11-10-issue-23-token-reservations.md).
