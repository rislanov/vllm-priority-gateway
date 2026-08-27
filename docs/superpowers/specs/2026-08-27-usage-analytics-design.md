# Built-in Token Usage Analytics Design

## Goal

Add first-party token-usage accounting to the gateway without changing the bytes, flush behavior, or time-to-first-byte of proxied OpenAI-compatible responses. Expose the same usage through low-cardinality Prometheus counters and an authenticated, built-in Analytics UI that supports client/model/date filtering, per-request drill-down, and CSV export.

## Scope and Decisions

- Use the `usage` metadata returned by vLLM as the authoritative token count. The gateway will not retokenize prompts or generated text.
- Persist one metadata row per authenticated inference request whose client and public model were resolved.
- Never persist request bodies, response bodies, prompts, generated text, authorization headers, API-key material, or session-affinity values.
- Retain rows for 90 days by default. Make retention configurable so operators can shorten, extend, or disable automatic expiry.
- Keep Prometheus as an observability output, not as a dependency of the built-in UI.
- Include CSV export in the first release.
- Use the existing Go, SQLite, `html/template`, and dependency-free JavaScript stack. Do not add an external charting library or CDN dependency.

## High-Level Architecture

The proxy feeds every upstream response byte to two destinations: the existing downstream writer and a bounded, passive usage inspector. The downstream copy remains authoritative. Inspector failures only make usage unavailable; they never alter response status, bytes, flushing, retry decisions, or client-visible errors.

After forwarding completes, `proxy.Result` carries optional normalized token usage. The gateway adds stable client/model identifiers and request timing to `gateway.RequestEvent`. Existing observers receive the enriched event. The Prometheus observer increments token counters, while a dedicated usage recorder writes request metadata to SQLite in background batches.

The admin service queries SQLite directly for summaries, time-series buckets, grouped breakdowns, and paginated request rows. The server-rendered Analytics page uses those results for accessible cards and tables. Self-hosted JavaScript renders SVG charts from the same authenticated JSON API. CSV export streams the filtered per-request rows.

## Normalized Token Usage

Add a shared normalized value with these fields:

```text
input_tokens          int64
output_tokens         int64
cache_read_tokens     nullable int64
```

The inspector accepts both OpenAI-compatible schemas served by vLLM:

- Chat/Completions: `usage.prompt_tokens`, `usage.completion_tokens`, and optional `usage.prompt_tokens_details.cached_tokens`.
- Responses API: `usage.input_tokens`, `usage.output_tokens`, and optional `usage.input_tokens_details.cached_tokens`.

Input and output counts must both be present, integral, and non-negative before the usage record is considered valid. Cache-read tokens are independently nullable because vLLM omits prompt-token details unless the relevant server option is enabled. A cache value larger than input tokens is invalid and must be discarded rather than silently clamped.

Thinking/reasoning tokens remain part of output tokens. Separating reasoning tokens is outside this change because current vLLM versions do not expose `completion_tokens_details.reasoning_tokens` consistently across response modes.

## Passive Response Inspection

### Ordinary JSON responses

The inspector consumes arbitrary byte chunks and incrementally locates the relevant usage object without delaying downstream writes or buffering the generated response body. Its memory is bounded independently of completion size. It must tolerate JSON strings, escaped braces, field reordering, and usage being absent.

### Server-Sent Events

The SSE inspector buffers only the current event, recognizes both LF and CRLF framing, joins multiple `data:` lines according to SSE rules, and parses JSON data events. It supports:

- Chat/Completions final usage chunks, including `choices: []`.
- Responses API completion events where usage is nested in the completed response.
- Network reads that split an SSE line or event at any byte boundary.
- `[DONE]`, comments, keep-alives, malformed events, and events without usage.

The event buffer has a fixed upper bound. If one event exceeds that limit, inspection is disabled for that response while proxying continues unchanged.

### Ensuring streaming usage

For `/v1/chat/completions` and `/v1/completions`, the gateway rewrites streaming request payloads so upstream `stream_options.include_usage` is `true`. This is a gateway-owned metering requirement even if the caller omitted the field or set it to false. Non-streaming requests already include usage in normal vLLM responses.

Cache-read detail still requires vLLM to run with prefix caching and prompt-token details enabled. Documentation will recommend:

```text
--enable-prefix-caching
--enable-prompt-tokens-details
```

Missing cache detail is represented as unavailable, not zero.

## Request Ledger

Add a `usage_requests` table through a new ordered migration. Existing installations must upgrade in place; the migration runner will apply all embedded migrations exactly once and record applied filenames.

Each row contains:

```text
id                      integer primary key
occurred_at_ms          UTC Unix milliseconds
request_id              gateway request ID
parent_request_id       optional safe parent request ID
client_id               stable client ID
client_name             client name snapshot
model_pool_id           stable model-pool ID
model_name              public model name snapshot
backend_name            selected backend name or empty
http_status             final status
duration_ms             end-to-end duration
ttft_ms                 nullable time to first byte
retry_count             transparent retry count
disconnected            boolean
usage_available         boolean
input_tokens            nullable integer
output_tokens           nullable integer
cache_read_tokens       nullable integer
```

The table intentionally has no cascading foreign keys: analytics history must survive later configuration renames or deletion support. Stable IDs provide filtering continuity, while snapshot names preserve what operators saw when the request happened.

Indexes cover time-ordered scans and the common filters:

- `(occurred_at_ms DESC)`
- `(client_id, occurred_at_ms DESC)`
- `(model_pool_id, occurred_at_ms DESC)`
- unique `request_id`

Store one row for every authenticated request after client and public-model resolution, including admission rejection, upstream error, retry, and downstream disconnect. Requests rejected before a client/model can be safely resolved remain in existing operational metrics and logs but are not inserted into the per-client ledger.

If final usage is absent or malformed, set `usage_available=false` and leave token columns `NULL`. A legitimate zero-token field remains distinguishable from missing usage.

## Recorder, Durability, and Retention

The recorder owns a bounded channel and one SQLite writer goroutine. It groups pending events into transactions, flushing on a short interval or batch-size threshold. Once an authenticated request has a resolved public-model identity, it reserves one bounded recorder-completion slot before policy rejection or upstream forwarding. Queue saturation therefore applies cancellable backpressure before new inference work or an error response begins, rather than blocking handler return after response bytes have been written or silently dropping analytics rows. A successful reservation guarantees that terminal response completion can hand the staged row to the recorder without blocking HTTP response finalization.

Database write failures do not rewrite an already delivered inference response. They are logged with safe metadata and counted by a dedicated persistence-failure Prometheus counter. The recorder continues accepting later batches. Graceful shutdown stops new ingestion, drains queued rows, commits the final batch, and then closes.

Retention cleanup runs at startup and no more than once per hour thereafter. The default is 90 days. A positive configured duration deletes rows older than `now-retention`; zero disables cleanup. Cleanup uses the time index and runs outside request handling.

## Prometheus Metrics

Add the required counters:

```text
llmgw_input_tokens_total{client,model}
llmgw_output_tokens_total{client,model}
llmgw_cache_read_tokens_total{client,model}
```

Input/output counters increment only when normalized usage is valid. The cache counter increments only when cache detail is present. Client and model names come from trusted registry state, preserving the project's low-cardinality policy.

Add operational counters for usage inspection and storage failures without client/model labels:

```text
llmgw_usage_parse_failures_total{format}
llmgw_usage_persistence_failures_total
```

Absence of usage is not automatically a parse failure: upstream errors, disconnects, and servers not configured to include usage are expected missing-data cases.

## Analytics Query Model

All ranges use UTC. `from` is inclusive and `to` is exclusive. The default range is the last 24 hours, ending at the current time. Requests outside configured retention return an empty portion rather than an error.

Supported filters:

- Custom `from` and `to` date/time.
- Presets for 1 hour, 24 hours, 7 days, 30 days, and 90 days.
- One client or all clients.
- One model pool or all models.
- Optional `usage_available` filter for request drill-down.

The query response contains:

- Summary: request count, metered request count, usage coverage, input, output, cache-read, and uncached-input tokens.
- Time series: request count, input, output, cache-read, and cache-hit ratio.
- Breakdown grouped by stable client ID and model-pool ID.
- Paginated request rows ordered newest first.
- Available client/model dimensions, including historical names present in the retained ledger.

Time-series resolution is selected automatically to bound chart size to at most 366 points:

- Range up to 24 hours: 5-minute buckets.
- More than 24 hours and up to 7 days: 1-hour buckets.
- More than 7 days: 1-day buckets while that produces at most 366 points; longer ranges use the smallest whole-day multiple that keeps the series at or below 366 points.

Resolution selection uses the exact stored-millisecond bounds rather than `time.Duration`, so the full RFC 3339 year range cannot overflow or accidentally fall back to a fine-grained bucket width. Aggregates, breakdowns, request rows, and CSV still cover the complete selected interval; only chart grouping becomes coarser for multi-year ranges.

Cache hit ratio is `cache_read_tokens / input_tokens` when input tokens are positive and cache data is available. Cache-read tokens are a subset of input tokens and must never be stacked as additional total consumption.

## Admin API

All endpoints remain behind existing Basic Auth, CSRF policy for mutations, no-store headers, and same-origin CSP.

```text
GET /admin/api/analytics
GET /admin/api/analytics/requests
GET /admin/api/analytics/export.csv
```

`/analytics` returns summary, series, breakdown, and dimensions for the selected range. `/requests` returns bounded pagination with a default page size of 100 and maximum of 500. Invalid ranges, unsupported timestamps, negative IDs, and excessive page sizes return the existing controlled admin error envelope with HTTP 400.

CSV export applies the identical range/client/model/usage filters and streams rows in stable chronological order. It includes only ledger columns, never body or secret data. String fields beginning with spreadsheet formula markers (`=`, `+`, `-`, or `@`) are prefixed safely before RFC 4180 encoding. Client cancellation stops the database scan promptly. The complete CSV is first spooled to a secure `0600` temporary file so no database cursor is held during client I/O. At most two export temporary files may exist concurrently: the non-blocking export slot remains held through delivery and cleanup, and excess exports receive a controlled retryable response.

## Built-in Analytics UI

Add `Analytics` to the existing admin navigation and serve `/admin/analytics`.

The page contains:

1. Range controls: presets, custom UTC from/to inputs, client selector, model selector, and Apply/Reset actions.
2. Summary cards: requests, usage coverage, input tokens, output tokens, cache-read tokens, and cache-hit ratio.
3. Dependency-free SVG charts:
   - Request volume over time.
   - Input and output token volume over time.
   - Cache-read tokens and cache-hit ratio over time.
4. Client/model breakdown table with request and token totals.
5. Paginated per-request table with timestamp, IDs, client, model, backend, status, duration, TTFT, retries, disconnect, and token columns.
6. CSV export link that preserves the active filters.

Cards and tables are server-rendered so the page remains useful without JavaScript. Self-hosted JavaScript progressively enhances the page by drawing responsive SVG charts from data attributes or the authenticated JSON endpoint. No raw HTML is generated from API strings, and all labels use DOM text nodes or `html/template` escaping.

Empty ranges show zeroed summaries, an explanatory chart placeholder, and empty tables. Missing usage displays as an em dash and contributes to the coverage denominator but not token totals.

## Configuration

Add one setting:

```text
LLMGW_ANALYTICS_RETENTION=2160h
```

The default `2160h` is 90 days. `0` disables automatic deletion. Negative or malformed values fail startup through existing config validation. Batch size, queue capacity, flush interval, pagination limits, and chart bucket thresholds remain internal constants for the first release.

## Error Handling and Security

- Usage parsing is fail-open for inference delivery and fail-closed for accounting: invalid numbers are never recorded as valid usage.
- Token counts use signed 64-bit integers with overflow checks during parsing and SQL aggregation.
- The database never stores prompts, outputs, request bodies, response bodies, authorization, upstream keys, or session identifiers.
- Request IDs remain safe identifiers already covered by existing structured logging policy.
- Analytics responses inherit `Cache-Control: no-store` and the current admin CSP.
- SQL uses bound parameters and whitelisted bucket sizes/order clauses.
- CSV neutralizes spreadsheet formulas and declares UTF-8 content with an attachment filename.
- Retention deletion and CSV scans honor contexts and SQLite busy-timeout behavior.

## Testing Strategy

### Proxy and payload tests

- Ordinary JSON usage in arbitrary chunk boundaries.
- Chat/Completions SSE usage split at every byte boundary.
- Responses API nested completion usage.
- LF/CRLF, multiple `data:` lines, keep-alives, `[DONE]`, malformed JSON, oversized events, missing fields, negative values, overflow, and cache greater than input.
- Byte-exact downstream output, flush behavior, TTFT, retry behavior, and cancellation remain unchanged with inspection enabled.
- Streaming request payloads force `stream_options.include_usage=true` without changing unrelated fields.

### Store and recorder tests

- Ordered migration upgrades a database created by the initial schema and is idempotent on reopen.
- Per-request insert preserves nullable usage and stable identity snapshots.
- Batch commit, queue backpressure, shutdown drain, persistence failure recovery, and retention cleanup.
- Range boundaries, client/model filters, automatic buckets, breakdown totals, usage coverage, pagination, historical dimensions, and overflow-safe aggregation.

### Metrics tests

- Exact counter families and `client,model` labels.
- Missing cache detail does not increment cache-read.
- Missing/invalid usage does not increment token counters.
- No request IDs, keys, or other high-cardinality labels appear.

### Admin API and web tests

- Authentication/security headers cover all analytics and CSV routes.
- JSON filter validation and pagination behavior.
- CSV content, filter parity, escaping, formula neutralization, and cancellation.
- Semantic HTML labels, navigation, filter state, summary cards, tables, empty states, and accessible SVG fallbacks.
- Integration test drives a fake vLLM ordinary response and streaming response through the real gateway, then verifies metrics, persisted rows, filtered API totals, HTML output, and CSV export.

## Documentation and Operations

Update the deployment documentation with the vLLM flags required for cache-read detail, the new retention setting, storage-growth guidance, the meaning of cache-read versus input tokens, missing-usage behavior, and backup implications for the enlarged SQLite WAL database.

## Non-Goals

- Token-based billing, quotas, budgets, or enforcement.
- Storing or searching prompt/response content.
- Reconstructing usage by retokenizing text.
- Separating reasoning tokens from visible output tokens.
- External BI integrations beyond Prometheus and CSV.
- Precomputed hourly/daily rollup tables in the first release.
