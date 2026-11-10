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

All AC01–AC14 are currently **pending**: baseline tests do not prove a feature that has not been implemented. Populate this table after each corresponding task.

| Criteria | Future evidence artifact | Status |
| --- | --- | --- |
| AC01–02 | Policy/persistence/Admin/disabled contract results | pending |
| AC03–04, AC11 | Payload/estimator endpoint and boundedness fixtures | pending |
| AC05–09 | Local/PG shared contracts, concurrency, lost-commit/expiry tests | pending |
| AC10 | Emergency outage/recovery two-gateway evidence | pending |
| AC12 | Metrics/privacy and numeric unknown/actual assertions | pending |
| AC13 | Migrations, compatibility version/drain/rollback evidence | pending |
| AC14 | Real-vLLM paired experiment with hardware/model/revisions/config | pending |

Documents: [specification and acceptance criteria](superpowers/specs/2026-11-10-issue-23-token-reservations-design.md), [step-by-step plan](superpowers/plans/2026-11-10-issue-23-token-reservations.md).
