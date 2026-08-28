# Built-in Token Usage Analytics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collect per-request vLLM input, output, and cache-read token usage without storing prompt or response text, then expose it through Prometheus, an authenticated built-in Analytics UI, JSON APIs, and CSV export.

**Architecture:** A bounded passive inspector observes upstream response bytes while the existing proxy writes and flushes those same bytes unchanged. Normalized usage is attached to the gateway completion event, consumed synchronously by low-cardinality Prometheus counters and asynchronously by a batched SQLite ledger recorder. The admin service queries the ledger for server-rendered summaries/tables and a dependency-free SVG chart UI.

**Tech Stack:** Go 1.24, `net/http`, `encoding/json`, SQLite via `modernc.org/sqlite`, Prometheus client, `html/template`, embedded CSS/JavaScript, Go tests.

**Spec:** `docs/superpowers/specs/2026-08-27-usage-analytics-design.md`

## Global Constraints

- Never persist or log prompts, generated text, request/response bodies, authorization headers, API keys, or session-affinity values.
- Downstream response bytes, status, flush timing, retry semantics, and client-visible errors must remain unchanged when inspection succeeds or fails.
- Treat vLLM usage metadata as authoritative; do not retokenize.
- Store one ledger row only after both authenticated client and public model are resolved.
- Preserve nullable cache-read usage; missing cache detail is not zero.
- Use UTC ranges with inclusive `from` and exclusive `to`.
- Every production change begins with a focused failing test, followed by the smallest implementation, focused passing tests, and a commit.
- Run `go test ./...` and `git diff --check` before completion.

---

### Task 1: Normalize usage and inspect JSON/SSE passively

**Files:**

- Create: `internal/domain/usage.go`
- Create: `internal/proxy/usage.go`
- Create: `internal/proxy/usage_test.go`
- Modify: `internal/proxy/proxy.go`
- Modify: `internal/proxy/proxy_test.go`

- [ ] Add failing tests for normalized ordinary JSON usage:
  - Chat/Completions: `prompt_tokens`, `completion_tokens`, optional `prompt_tokens_details.cached_tokens`.
  - Responses API: `input_tokens`, `output_tokens`, optional `input_tokens_details.cached_tokens`.
  - Usage fields may be reordered and nested under the Responses API completed-response event.
  - Counts must be integral, non-negative `int64`; cache greater than input invalidates cache only.
  - Missing input/output makes usage unavailable; missing cache keeps a `nil` cache value.

- [ ] Add failing SSE tests that feed the same stream at every possible byte split. Cover LF/CRLF, multiple `data:` lines, comments, keep-alives, `[DONE]`, malformed events, and an event larger than the inspector limit.

- [ ] Add a failing proxy integration test that verifies the downstream body is byte-identical, each response chunk is still flushed immediately, and `proxy.Result` reports usage only after response completion.

- [ ] Run:

  ```bash
  go test ./internal/proxy -run 'TestUsage|TestForwardInspects' -count=1
  ```

  Expected: compile failures for the not-yet-defined inspector/result fields or assertion failures because usage is absent.

- [ ] Define the shared value in `internal/domain/usage.go`:

  ```go
  type TokenUsage struct {
      InputTokens      int64
      OutputTokens     int64
      CacheReadTokens  *int64
  }
  ```

- [ ] Implement a bounded `usageInspector` in `internal/proxy/usage.go` with:
  - `newUsageInspector(contentType string) *usageInspector`
  - `Write([]byte)` called after each successful downstream write
  - `Result() (*domain.TokenUsage, string, bool)` returning usage, parse-failure format (`json` or `sse`), and whether a syntactic/validation failure occurred
  - a fixed maximum JSON/SSE event capture size and permanent inspection disablement after overflow
  - JSON string/escape/depth tracking for ordinary responses, and current-event-only buffering for SSE
  - validation through `json.Decoder.UseNumber` and explicit `strconv.ParseInt` checks

- [ ] Extend `proxy.Result` with `Usage *domain.TokenUsage` and `UsageParseFailure string`. Construct the inspector from upstream `Content-Type`, feed only the bytes already written successfully downstream, and finalize it on EOF. Do not make parser errors affect `Result.Err`, retryability, status, bytes, or flushing.

- [ ] Run the focused tests and all proxy tests:

  ```bash
  go test ./internal/proxy -count=1
  ```

  Expected: PASS.

- [ ] Commit:

  ```bash
  git add internal/domain/usage.go internal/proxy/usage.go internal/proxy/usage_test.go internal/proxy/proxy.go internal/proxy/proxy_test.go
  git commit -m "feat: inspect upstream token usage"
  ```

---

### Task 2: Force streaming usage and enrich request events

**Files:**

- Create: `internal/gateway/usage_test.go`
- Modify: `internal/gateway/service.go`
- Modify: `internal/gateway/observer.go`
- Modify: `internal/gateway/service_retry_test.go`

- [ ] Add failing table tests around gateway forwarding for `/v1/chat/completions` and `/v1/completions`: when `stream:true`, the upstream JSON must contain `stream_options.include_usage=true` whether omitted, false, malformed, or containing unrelated fields. Non-streaming requests and `/v1/responses` must not gain the field.

- [ ] Add failing observer tests proving the completed event contains `OccurredAt`, stable `ClientID`, stable `ModelPoolID`, public snapshot names, final backend, status, duration/TTFT/retries/disconnect, optional token usage, and parse-failure format. Prove pre-model authentication/validation failures do not have both ledger identities.

- [ ] Run:

  ```bash
  go test ./internal/gateway -run 'TestStreamingUsage|TestRequestEventUsage' -count=1
  ```

  Expected: failures because stream options and enriched fields do not exist.

- [ ] Add `forceStreamingUsage(body, path)` after public-model parsing and before upstream-model replacement. Preserve all unrelated JSON values and override only `stream_options.include_usage` for the two supported streaming endpoints.

- [ ] Extend `gateway.RequestEvent` with:

  ```go
  OccurredAt       time.Time
  ClientID         int64
  ModelPoolID      int64
  Usage            *domain.TokenUsage
  UsageParseFailure string
  ```

  Populate `OccurredAt` from the injectable gateway clock at completion, set stable IDs immediately after each identity is resolved, and copy usage/parser outcome from `proxy.Result` in the deferred completion path.

- [ ] Re-run focused and full gateway tests:

  ```bash
  go test ./internal/gateway -count=1
  ```

  Expected: PASS.

- [ ] Commit:

  ```bash
  git add internal/gateway/usage_test.go internal/gateway/service.go internal/gateway/observer.go internal/gateway/service_retry_test.go
  git commit -m "feat: propagate request token usage"
  ```

---

### Task 3: Add Prometheus token and failure counters

**Files:**

- Modify: `internal/observability/metrics.go`
- Modify: `internal/observability/metrics_test.go`

- [ ] Extend the existing metrics test first. Assert exact samples for:
  - `llmgw_input_tokens_total{client,model}`
  - `llmgw_output_tokens_total{client,model}`
  - `llmgw_cache_read_tokens_total{client,model}`
  - `llmgw_usage_parse_failures_total{format}`
  - `llmgw_usage_persistence_failures_total`
  Also assert absent usage and `nil` cache detail do not increment token/cache counters, and request IDs never appear as labels.

- [ ] Run:

  ```bash
  go test ./internal/observability -run TestMetricsExposeRequiredFamiliesWithoutHighCardinalityLabels -count=1
  ```

  Expected: required metric families are missing.

- [ ] Register the five counters. Increment token counters only for valid `RequestEvent.Usage`, parse failure only for a non-empty normalized `json|sse` format, and expose a concurrency-safe `UsagePersistenceFailure()` callback for the recorder.

- [ ] Run:

  ```bash
  go test ./internal/observability -count=1
  ```

  Expected: PASS.

- [ ] Commit:

  ```bash
  git add internal/observability/metrics.go internal/observability/metrics_test.go
  git commit -m "feat: expose token usage metrics"
  ```

---

### Task 4: Add ordered migrations and the per-request ledger

**Files:**

- Create: `internal/analytics/types.go`
- Create: `internal/store/migrations/002_usage_analytics.sql`
- Create: `internal/store/usage.go`
- Create: `internal/store/usage_test.go`
- Modify: `internal/store/sqlite.go`
- Modify: `internal/store/sqlite_test.go`

- [ ] Add a failing upgrade test that builds a database from `001_initial.sql`, opens it through `store.Open`, verifies both migrations recorded exactly once, and reopens idempotently.

- [ ] Add failing ledger tests for a batch containing one fully metered request and one request with unavailable usage. Verify nullable columns, stable ID/name snapshots, unique request IDs, no request/response content columns, and time-ordered indexes.

- [ ] Run:

  ```bash
  go test ./internal/store -run 'TestSQLiteMigrationUpgrade|TestUsageBatch' -count=1
  ```

  Expected: missing `schema_migrations`, `usage_requests`, and batch API.

- [ ] Define analytics domain structs in `internal/analytics/types.go`: `RequestRecord`, `Filter`, `Summary`, `SeriesPoint`, `BreakdownRow`, `RequestPage`, `Dimension`, and `Dataset`. Keep JSON field names stable and token/cache fields nullable where absence matters.

- [ ] Replace the single-file migration runner with an ordered embedded runner:
  - create `schema_migrations(filename TEXT PRIMARY KEY, applied_at TEXT NOT NULL)` safely
  - lexically sort `migrations/*.sql`
  - apply each unseen file and insert its filename in the same transaction
  - leave existing `001_initial.sql` data intact on upgrade

- [ ] Add `002_usage_analytics.sql` with the exact ledger fields and indexes from the design. Use `CHECK` constraints for booleans, non-negative times/token counts, valid nullable usage combinations, and `cache_read_tokens <= input_tokens`.

- [ ] Implement `InsertUsageBatch(ctx, []analytics.RequestRecord) error`, prepared within one transaction and using bound values. Duplicate request IDs must be idempotent with `ON CONFLICT(request_id) DO NOTHING` so recorder retry cannot double count.

- [ ] Run:

  ```bash
  go test ./internal/store -count=1
  ```

  Expected: PASS.

- [ ] Commit:

  ```bash
  git add internal/analytics/types.go internal/store/migrations/002_usage_analytics.sql internal/store/usage.go internal/store/usage_test.go internal/store/sqlite.go internal/store/sqlite_test.go
  git commit -m "feat: persist per-request usage ledger"
  ```

---

### Task 5: Build the asynchronous recorder and retention lifecycle

**Files:**

- Create: `internal/analytics/recorder.go`
- Create: `internal/analytics/recorder_test.go`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `cmd/gateway/main.go`

- [ ] Add failing recorder tests using a controllable fake store for batch threshold, timed flush, queue backpressure, shutdown drain, continued processing after a failed batch, persistence-failure callback, startup cleanup, at-most-hourly cleanup, and retention disabled at zero.

- [ ] Add failing config tests for default `2160h`, a positive custom duration, `0`, malformed input, and negative rejection.

- [ ] Run:

  ```bash
  go test ./internal/analytics ./internal/config -count=1
  ```

  Expected: package/API/config fields missing.

- [ ] Implement `analytics.Recorder` as a `gateway.Observer` wrapper or observer peer with an internal bounded channel and single worker. Constants remain internal: queue capacity, batch size, short flush interval, and hourly cleanup cadence. `Complete` must ignore events lacking either stable identity, convert other events without usage to ledger rows, and block only after response completion if the queue is full.

- [ ] Define a narrow recorder store interface:

  ```go
  type RecordStore interface {
      InsertUsageBatch(context.Context, []RequestRecord) error
      DeleteUsageBefore(context.Context, time.Time) (int64, error)
  }
  ```

  `Close(ctx)` stops intake, drains, flushes, and returns the last drain error. Log only safe identifiers and counts.

- [ ] Add `AnalyticsRetention time.Duration` to config, default it to `2160h`, accept `0`, and reject negatives/malformed values.

- [ ] Wire the recorder into `observability.Multi(metrics, logger, recorder)` in `cmd/gateway/main.go`, pass the SQLite store and persistence callback, and close the HTTP server before draining the recorder within the configured shutdown grace period. Keep the existing API-key last-used recorder separately named.

- [ ] Run:

  ```bash
  go test ./internal/analytics ./internal/config ./cmd/gateway -count=1
  ```

  Expected: PASS.

- [ ] Commit:

  ```bash
  git add internal/analytics/recorder.go internal/analytics/recorder_test.go internal/config/config.go internal/config/config_test.go cmd/gateway/main.go
  git commit -m "feat: record usage asynchronously"
  ```

---

### Task 6: Implement analytics queries and pagination

**Files:**

- Modify: `internal/analytics/types.go`
- Modify: `internal/store/usage.go`
- Modify: `internal/store/usage_test.go`

- [ ] Add failing store tests for inclusive `from`, exclusive `to`, client/model/availability filters, empty ranges, auto bucket sizes (5 minutes/1 hour/1 day), coverage denominator, nullable cache aggregation, cache-hit ratio, uncached input, client/model breakdown, newest-first bounded pagination, historical dimensions, and safe aggregate overflow errors.

- [ ] Run:

  ```bash
  go test ./internal/store -run 'TestAnalytics' -count=1
  ```

  Expected: query methods do not exist.

- [ ] Define the query interface consumed by admin code:

  ```go
  type QueryStore interface {
      Analytics(context.Context, Filter) (Dataset, error)
      UsageRequests(context.Context, Filter, int, int) (RequestPage, error)
      StreamUsageRequests(context.Context, Filter, func(RequestRecord) error) error
  }
  ```

- [ ] Implement SQL with bound filters and a whitelisted bucket width. Compute summary/breakdown with checked `int64` additions, retain `nil` cache totals when no cache detail exists, and scan rows while honoring context cancellation. Return historical distinct dimensions from retained ledger snapshots.

- [ ] Run:

  ```bash
  go test ./internal/store -count=1
  ```

  Expected: PASS.

- [ ] Commit:

  ```bash
  git add internal/analytics/types.go internal/store/usage.go internal/store/usage_test.go
  git commit -m "feat: query usage analytics"
  ```

---

### Task 7: Add authenticated analytics JSON and CSV endpoints

**Files:**

- Create: `internal/httpapi/analytics.go`
- Create: `internal/httpapi/analytics_test.go`
- Modify: `internal/httpapi/admin.go`
- Modify: `internal/httpapi/admin_test.go`
- Modify: `cmd/gateway/main.go`

- [ ] Add failing handler tests for:
  - default last-24-hour range using injected `Now`
  - RFC 3339 `from`/`to`, positive client/model IDs, and `usage_available=true|false`
  - invalid/empty/reversed ranges, negative IDs, invalid booleans, pagination default 100 and maximum 500
  - JSON envelopes for `/admin/api/analytics` and `/admin/api/analytics/requests`
  - RFC 4180 CSV headers/rows, chronological stable order, active filter parity, UTF-8 download headers, formula neutralization for `= + - @`, and request cancellation
  - inherited Basic Auth and `Cache-Control: no-store` through the real admin router/security wrapper

- [ ] Run:

  ```bash
  go test ./internal/httpapi -run 'TestAnalytics|TestAdminAnalyticsSecurity' -count=1
  ```

  Expected: routes and analytics dependency are missing.

- [ ] Add `Analytics analytics.QueryStore` to `AdminDependencies` and `AdminService`, validate it in the constructor, and expose service methods used by both API and web handlers.

- [ ] Implement strict common query parsing in `internal/httpapi/analytics.go`. Register:
  - `GET /admin/api/analytics`
  - `GET /admin/api/analytics/requests`
  - `GET /admin/api/analytics/export.csv`

  CSV must contain ledger metadata only, prefix formula-leading string cells with an apostrophe before `encoding/csv`, flush incrementally, and stop on context cancellation.

- [ ] Wire `database` as the analytics query dependency in `cmd/gateway/main.go` and update existing admin test fixtures.

- [ ] Run:

  ```bash
  go test ./internal/httpapi ./cmd/gateway -count=1
  ```

  Expected: PASS.

- [ ] Commit:

  ```bash
  git add internal/httpapi/analytics.go internal/httpapi/analytics_test.go internal/httpapi/admin.go internal/httpapi/admin_test.go cmd/gateway/main.go
  git commit -m "feat: expose usage analytics API and CSV"
  ```

---

### Task 8: Build the server-rendered Analytics UI and SVG charts

**Files:**

- Create: `internal/web/templates/analytics.html`
- Modify: `internal/web/templates/layout.html`
- Modify: `internal/web/static/app.js`
- Modify: `internal/web/static/app.css`
- Modify: `internal/web/web.go`
- Modify: `internal/web/web_test.go`

- [ ] Add failing web tests for `/admin/analytics`: active navigation, UTC preset/custom controls, selected client/model filters, summary cards, breakdown table, per-request table, pagination, filter-preserving CSV link, empty state, em dashes for missing usage, semantic labels, and no prompt/response text fields.

- [ ] Add a focused JavaScript/static assertion test or Go fixture check for three chart containers with accessible text fallbacks and no external script/CDN references.

- [ ] Run:

  ```bash
  go test ./internal/web -run TestAnalytics -count=1
  ```

  Expected: page route/template missing.

- [ ] Add `Analytics` to the layout navigation and template set. Handle `GET /admin/analytics` by parsing the same filters as the admin API and querying both dataset and first request page through `AdminService`.

- [ ] Server-render:
  - range/client/model controls and presets
  - six summary cards
  - request, token, and cache chart containers with accessible tabular/text fallbacks
  - client/model breakdown and paginated request table
  - filter-preserving CSV download URL

- [ ] Extend self-hosted `app.js` to read JSON embedded with `html/template` escaping, create SVG elements through DOM APIs, draw responsive axes/lines/legends, and use `textContent` for every label. Add CSS for cards, filters, charts, responsive tables, and empty states.

- [ ] Run:

  ```bash
  go test ./internal/web -count=1
  ```

  Expected: PASS.

- [ ] Commit:

  ```bash
  git add internal/web/templates/analytics.html internal/web/templates/layout.html internal/web/static/app.js internal/web/static/app.css internal/web/web.go internal/web/web_test.go
  git commit -m "feat: add built-in analytics dashboard"
  ```

---

### Task 9: Verify the full gateway and document operations

**Files:**

- Modify: `internal/fakevllm/server.go`
- Modify: `internal/fakevllm/server_test.go`
- Modify: `cmd/gateway/main_test.go`
- Modify: `README.md`

- [ ] First add a failing end-to-end test. Drive one ordinary and one streaming Chat/Completions request through a real gateway backed by fake vLLM, then verify:
  - client-visible response bytes remain correct
  - streaming request reached fake vLLM with `include_usage=true`
  - token counters have exact client/model totals
  - two metadata-only ledger rows exist with nullable cache behavior
  - filtered JSON summary, HTML Analytics page, request table, and CSV agree
  - no prompt or generated content is present in SQLite, API, HTML, CSV, logs, or metric labels

- [ ] Run the new integration test and observe the expected failure before extending fake vLLM usage responses/wiring.

- [ ] Extend fake vLLM fixtures to emit representative ordinary JSON and final SSE usage chunks for Qwen-compatible responses. Keep test response content deterministic.

- [ ] Document:
  - `LLMGW_ANALYTICS_RETENTION=2160h` and `0` behavior
  - recommended vLLM flags `--enable-prefix-caching --enable-prompt-tokens-details`
  - cache-read tokens are a subset of input tokens
  - absent cache detail is unknown, not zero
  - metadata-only storage, 90-day default retention, expected SQLite/WAL growth, backup and cleanup implications
  - Analytics filters, per-request view, Prometheus families, and CSV export

- [ ] Run focused integration tests, then the complete suite:

  ```bash
  go test ./... -count=1
  git diff --check
  ```

  Expected: all packages PASS and no whitespace errors.

- [ ] Inspect the final diff for secret/body logging and verify migration/index names, metric names/labels, API paths, and UI route exactly match the approved design.

- [ ] Commit:

  ```bash
  git add internal/fakevllm/server.go internal/fakevllm/server_test.go cmd/gateway/main_test.go README.md
  git commit -m "test: verify token analytics end to end"
  ```

---

### Task 10: Bound extreme custom-range chart density

**Files:**

- Modify: `internal/store/usage.go`
- Modify: `internal/store/usage_test.go`
- Modify: `docs/superpowers/specs/2026-08-27-usage-analytics-design.md`

- [ ] Add a failing direct-store regression using the full RFC 3339 year range with one matching row. Prove the series is non-empty, contains at most 366 points, preserves the complete filter for aggregates, and does not rely on overflowing `time.Duration` arithmetic.

- [ ] Add a failing multi-year regression that derives the expected whole-day multiple independently and proves standard 24-hour, 7-day, and 90-day resolutions remain unchanged.

- [ ] Run:

  ```bash
  go test ./internal/store -run 'TestAnalytics(BoundsExtremeCustomRangeSeries|UsesAdaptiveWholeDayBucketsForLongRanges|DenseSeriesKeepsStandardPresetPointBounds)' -count=1
  ```

  Expected: the extreme range attempts an unbounded fine-grained/daily series or the adaptive-width expectations fail.

- [ ] Select bucket width from the exact ceiling-to-millisecond `from`/`to` bounds. Keep the existing 5-minute, 1-hour, and 1-day thresholds, then increase widths in whole-day multiples only when needed to keep the dense series at or below 366 points.

- [ ] Keep summary, breakdown, request rows, CSV filters, cache-known semantics, and truly-empty-range behavior unchanged. Do not impose a hard maximum custom period.

- [ ] Run the focused store tests, the full store/web/httpapi suites, the changed-package race suite, and `git diff --check`.

- [ ] Commit:

  ```bash
  git add internal/store/usage.go internal/store/usage_test.go docs/superpowers/specs/2026-08-27-usage-analytics-design.md docs/superpowers/plans/2026-08-27-usage-analytics.md
  git commit -m "fix: bound custom analytics series"
  ```

---

### Task 11: Reserve recorder capacity before response generation

**Files:**

- Modify: `internal/gateway/observer.go`
- Modify: `internal/gateway/service.go`
- Modify: `internal/gateway/usage_test.go`
- Modify: `internal/observability/log.go`
- Modify: `internal/observability/log_test.go`
- Modify: `internal/analytics/recorder.go`
- Modify: `internal/analytics/recorder_test.go`
- Modify: `internal/httpapi/public_test.go`
- Modify: `docs/superpowers/specs/2026-08-27-usage-analytics-design.md`

- [ ] Add failing real-`httptest.Server` regressions proving that, after a request has reserved recorder capacity, both a generated 429 JSON response and a chunked/SSE response become fully readable through EOF before a blocked analytics store is released. The tests must fail against the existing deferred blocking `ResponseComplete` behavior; an in-memory `ResponseWriter.Write` signal is insufficient.

- [ ] Add failing recorder and `observability.Multi` tests for cancellable reservation backpressure, successful-reservation rollback, duplicate IDs, shutdown races, and non-blocking terminal handoff. Name the concrete production mutation each test catches before writing it.

- [ ] Add an optional gateway observer capability that reserves a completion slot by request ID with request-context cancellation. `observability.Multi` must combine capable peers in order and roll back earlier reservations if a later peer cannot reserve.

- [ ] After authenticated client and public-model resolution, reserve recorder capacity before testing model policy, pool availability, admission, routing, or forwarding. Unknown public models remain unreserved and outside the ledger. Context cancellation while waiting must stop the request without staging a row.

- [ ] Move recorder capacity backpressure from terminal `ResponseComplete` to the reservation. A successful reservation must guarantee that `ResponseComplete` cannot block on queue capacity. Keep one writer, bounded memory, exactly-once metadata rows, no silent drops, post-response event enrichment, retention, failure accounting, and hard-bounded shutdown/store ownership.

- [ ] Update the outdated in-memory backpressure regression and comments/docs to describe pre-response reservation backpressure and actual HTTP finalization.

- [ ] Run focused analytics/gateway/observability/httpapi tests, their race suites, the real-network regressions, and `git diff --check`.

- [ ] Commit:

  ```bash
  git add internal/gateway/observer.go internal/gateway/service.go internal/gateway/usage_test.go internal/observability/log.go internal/observability/log_test.go internal/analytics/recorder.go internal/analytics/recorder_test.go internal/httpapi/public_test.go docs/superpowers/specs/2026-08-27-usage-analytics-design.md docs/superpowers/plans/2026-08-27-usage-analytics.md
  git commit -m "fix: reserve analytics recorder capacity"
  ```

---

### Task 12: Bound CSV temporary files through delivery

**Files:**

- Modify: `internal/httpapi/analytics.go`
- Modify: `internal/httpapi/analytics_test.go`

- [ ] Add a failing concurrency regression with two blocked CSV deliveries. Prove a third authenticated export immediately receives the controlled busy response and creates no additional temporary file. Release the deliveries and prove every file and slot is cleaned up.

- [ ] Keep the existing non-blocking maximum of two exports, but hold each slot from before `CreateTemp` through client delivery, file close, and removal. Rename spool-only identifiers if necessary so ownership is unambiguous.

- [ ] Preserve the invariant that SQLite streaming/cursor work finishes before any client write, and preserve `0600` mode, metadata-only contents, filter parity, chronology, RFC 4180, formula neutralization, cancellation, and pre-commit error handling. Do not add a global server write timeout.

- [ ] Run focused CSV tests, HTTP API race tests, and `git diff --check`.

- [ ] Commit:

  ```bash
  git add internal/httpapi/analytics.go internal/httpapi/analytics_test.go
  git commit -m "fix: bound analytics export lifecycle"
  ```

---

### Task 13: Accept interim null usage in vLLM SSE streams

**Files:**

- Modify: `internal/proxy/usage.go`
- Modify: `internal/proxy/usage_test.go`
- Modify: `internal/fakevllm/server.go`
- Modify: `internal/fakevllm/server_test.go`
- Modify: `cmd/gateway/main_test.go`
- Modify: `tests/integration/streaming_retry_test.go`
- Modify: `docs/superpowers/specs/2026-08-27-usage-analytics-design.md`

- [ ] Add a failing every-byte-split parser regression for ordinary Chat/Completions SSE streams containing one or more content chunks with top-level `usage: null`, followed by a final `choices: []` chunk with valid usage and `[DONE]`.

- [ ] Treat only top-level SSE `usage: null` as an absent interim candidate and continue scanning. A non-null top-level usage object remains authoritative, including fail-closed behavior when malformed. On `response.completed`, inspect nested `response.usage`; null or malformed nested terminal usage remains an authoritative parse failure. If the same completed event has top-level null and valid nested usage, use the nested usage.

- [ ] Preserve byte-exact downstream delivery, flush behavior, bounded inspection, JSON-response behavior, and existing Responses API completion semantics.

- [ ] Make the fake vLLM streaming fixture match the real lifecycle by emitting `usage: null` on content chunks before final usage. Extend the gateway end-to-end regression to prove metrics, the SQLite ledger, filtered JSON analytics, and CSV still receive the final exact token counts without a parse-failure increment. Keep the integration byte-exact expectation aligned with the fixture's include-usage response.

- [ ] Run focused proxy/fake-vLLM/gateway tests, changed-package race tests, the full suite, and `git diff --check`.

- [ ] Commit:

  ```bash
  git add internal/proxy/usage.go internal/proxy/usage_test.go internal/fakevllm/server.go internal/fakevllm/server_test.go cmd/gateway/main_test.go tests/integration/streaming_retry_test.go docs/superpowers/specs/2026-08-27-usage-analytics-design.md docs/superpowers/plans/2026-08-27-usage-analytics.md
  git commit -m "fix: accept interim null SSE usage"
  ```
