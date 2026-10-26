# Analytics User Time Zone Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Display and edit analytics times in the browser's time zone while retaining UTC storage, canonical links, APIs, and CSV.

**Architecture:** Keep server-rendered UTC as the no-JavaScript fallback and attach canonical timestamps as machine-readable attributes. A small dependency-free formatter/parser progressively localizes analytics text and form inputs; form submission sends canonical UTC values. Preserve original canonical strings for unchanged inputs so JavaScript's millisecond precision never truncates server timestamps.

**Tech Stack:** Go 1.27, html/template, native Date/Intl, embedded JavaScript/CSS, Node 24, Playwright.

**Spec:** [GitHub issue #32](https://github.com/rislanov/vllm-priority-gateway/issues/32); user explicitly requires the user's time zone in UI and unchanged absolute-time storage.
**Base:** `origin/main` at `d496a04615a8ea9e0f5f059ba319d3a58161ed54`.
**Branch:** `codex/issue-32-analytics-user-timezone`.
**Scope:** Plan only in this commit; implementation remains unchecked.

## Global Constraints

- Default to `Intl.DateTimeFormat().resolvedOptions().timeZone`; no fixed offset, geographic default, account setting, or new date library.
- Localize request detail, chart axes, fallback tables, From/To inputs, explanatory copy, and the analytics footer together.
- Preserve three fractional digits in request-detail display, and include the relevant numeric UTC offset so repeated local times remain distinguishable.
- Keep storage, aggregation boundaries, durations/preset widths, canonical URL/API values, and CSV exports unchanged in UTC.
- Preserve canonical RFC3339Nano strings verbatim when a boundary is unchanged. Never round-trip those strings through Date for submission or link generation.
- No-JavaScript and failed-enhancement fallback remains fully labeled UTC, including the original server interpretation of `from_local`/`to_local`.
- Scope is the analytics page. Other admin pages keep their existing UTC formatting and truthful footer.
- New or edited nonexistent local times are invalid. New or edited repeated local times select the earlier occurrence, with the selected offset visible beside the field and explanatory copy. Existing canonical values identifying the later occurrence remain exact when unchanged.
- Issue #31 is independent: localize all timestamp markers regardless of chart count; no dependency on the combined token panel.
- No private screenshots, user identities, hostnames, or deployment prefixes in fixtures/docs.

## Review Focus

1. A seconds/nanoseconds range must survive unchanged Apply, pagination, and sharing (Tasks 1–3).
2. A spring-forward gap must not silently normalize to a different wall-clock time (Task 2).
3. A fall-back fold must display the offset and retain an unchanged later occurrence (Tasks 2–3).
4. A page without JavaScript, or with a failed formatter initialization, must never submit local values to the UTC fallback parser (Tasks 1–2).
5. Date rollover, fractional offsets, empty charts, and the global footer must remain consistent (Tasks 1 and 3).

## Evidence and file map

`web.go` contains `utcTimestamp`, UTC parsing in `parseAnalyticsLocalTime`, and precise versus minute-rounded fields in `buildAnalyticsPage`. `app.js` uses `utcLabel` for axes and returns early when no chart containers exist. `layout.html` has a global "Times are shown in UTC" footer. Storage already uses absolute instants.

- Modify `internal/web/templates/analytics.html`: canonical attributes, per-time markers, zone/copy hooks, range hint/error elements.
- Modify `internal/web/templates/layout.html`: load the helper before app.js; mark the time-zone footer for analytics only.
- Create `internal/web/static/analytics-time.js`: pure conversion/formatting helpers exposed as `window.LLMGWAnalyticsTime`.
- Modify `internal/web/static/app.js`: transactional enhancement, range submission, shared axis formatter.
- Modify `internal/web/static/app.css`: compact offset hints and validation text.
- Modify `internal/web/web_test.go`: canonical attributes and UTC fallback contracts.
- Create `tests/browser/analytics-time.unit.test.mjs`: pure helper tests using the existing Node test/VM style.
- Create `tests/browser/analytics-timezone.spec.mjs`: actual browser time-zone/form regression tests.
- Create/reuse `internal/web/analytics_browser_fixture_test.go` and `tests/browser/analytics-fixtures.mjs`: deterministic rendered HTML fixture export, as described in Task 3.
- Modify `tests/browser/package.json` and `Makefile`: discover `tests/browser/*.unit.test.mjs` rather than only the clipboard unit file. Existing browser CI runs `make test-web`.

### Task 1: Add canonical timestamp markers without changing the UTC fallback

**Interfaces:** Timestamp cells contain `<time datetime="CANONICAL" data-analytics-time>` with existing UTC text inside. The form carries `data-range-from`/`data-range-to` containing exact canonical strings; each input carries its original minute-rounded UTC value separately as `data-utc-input`. An analytics-only `data-analytics-timezone` marker identifies visible zone text.

- [ ] Extend Go analytics tests to assert canonical values on request and bucket times, exact form boundaries, and unchanged minute-rounded input values. Use a fixture such as `2026-01-15T12:00:00.123456789Z` to prove the original string is retained.
- [ ] Assert all fallback text/labels/footer remain UTC; other pages must not gain an analytics zone label. Preserve `TestAnalyticsCustomUTCFormRedirectsToStrictCanonicalQuery` and the sub-minute form tests.
- [ ] Run `go test ./internal/web -run TestAnalytics -count=1`; expect missing-marker failures.
- [ ] Add the markup and analytics-specific footer hook. Make canonical boundaries available even for an empty dataset; do not depend on a populated series tbody. Keep the existing GET action and `from_local`/`to_local` names.
- [ ] Include the new helper as a self-hosted deferred script before app.js, preserving CSP and script order. Keep helper loading harmless on non-analytics pages.
- [ ] Run the focused tests; expect PASS. Commit with `feat(web): expose canonical analytics timestamps for localization`.

### Task 2: Implement local display and precise UTC form submission

**Interfaces:** Export the following pure functions through `LLMGWAnalyticsTime`; DOM wiring stays in app.js:

- `formatTimestamp(canonical, {timeZone, precision}) -> string`, where precision is `minute` or `millisecond`.
- `formatAxisTimestamp(canonical, timeZone) -> string`: compact local date/time, with zone explained next to the chart and an offset available in accessible text.
- `toLocalInput(utcMinuteValue) -> string`: browser-local `YYYY-MM-DDTHH:mm`.
- `parseLocalInput(value) -> {canonical, offsetLabel} | {error}`: strict components, local Date construction, round-trip validation, ISO UTC result.
- `resolveBoundary(value, initialValue, originalCanonical) -> {canonical, offsetLabel} | {error}`: unchanged visible value returns the original canonical string, including a later fold occurrence.

- [ ] Add unit cases asserting UTC+05:45 date rollover, UTC-negative offsets, three displayed fractional digits, invalid values, and original nanoseconds preserved without Date serialization. Run DST-dependent tests in isolated Node child processes with an explicit TZ rather than changing shared process state.
- [ ] Pin gap/fold behavior in `America/New_York`: `2026-03-08T02:30` is rejected; newly entered `2026-11-01T01:30` resolves to `2026-11-01T05:30:00.000Z`; an unchanged original `2026-11-01T06:30:00.123456789Z` stays exactly that value and shows UTC-05:00.
- [ ] Update the two unit-test entry points to include all `*.unit.test.mjs`. Run `make test-web-unit`; expect the new helper cases to fail.
- [ ] Implement the helpers with Intl/Date. Parse local components explicitly and compare them after construction to reject normalization, invalid calendar dates, and DST gaps. Avoid Date's special 1900-based handling of years 0–99. Use each timestamp's own offset, never today's offset.
- [ ] Initialize localization before the chart-only early return. Prepare formatter, input conversions, event handlers, and zone strings before changing visible labels/values; if setup fails, retain the complete UTC path. Never leave a partially localized form attached to the UTC fallback submit behavior.
- [ ] Format marked timestamp text with safe textContent, preserve datetime/data attributes, update all UTC-specific analytics copy, mark CSV as UTC, and update only the analytics footer. Replace `utcLabel` use with the shared display helper, retaining UTC fallback on formatting failure.
- [ ] On enhanced submit, prevent the native GET, resolve each boundary separately, and navigate to the existing action using URLSearchParams containing `from`, `to`, nonempty dimension filters, and `limit`. Remove `from_local`, `to_local`, `range`, and `offset`; do not send duplicate query keys. Let the existing server validate range ordering at full precision instead of comparing nanosecond values through Date.
- [ ] Preserve the initial rounded editable values independently of exact boundaries, including a sub-minute range whose end field was expanded by one minute server-side. Changing one field must preserve the other's exact canonical string.
- [ ] Show the effective offset beside both inputs; use original canonical values for unchanged inputs. Explain the earlier-occurrence rule near the form. Reject gaps with field validity text and focus the invalid field; submit no request on failure.
- [ ] Run `make test-web-unit` and `go test ./internal/web -count=1`; expect PASS. Commit with `feat(web): localize analytics times and preserve UTC range precision`.

Native Date's fold resolution and offset behavior are documented in [MDN Date](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date#date_components_and_time_zones). The explicit gap rejection above is an application decision.

### Task 3: Verify the complete browser workflow and document semantics

**Interfaces:** Browser fixtures are server-rendered using a test-only `TestWriteAnalyticsBrowserFixtures`, activated solely by `LLMGW_BROWSER_ANALYTICS_FIXTURE_DIR`. A Playwright helper creates a temp directory, invokes that Go test once, routes analytics documents to fixture files, and cleans up; scripts/styles load from the existing test gateway. Reuse the matching fixture mechanism if #31 has landed rather than adding a second exporter.

- [ ] Add deterministic fixtures for normal, empty, nanosecond/sub-minute, and DST-fold ranges in the Go fixture test. Verify their canonical attributes before export. Do not add a production fixture endpoint.
- [ ] Add Playwright contexts with timezoneId `UTC`, `Europe/Berlin`, `America/New_York`, and `Asia/Kathmandu`. Assert actual request cells, axis/table labels, filters, offsets, and footer all agree. Test 375px and 1440px viewports and both populated and empty analytics.
- [ ] Capture Apply navigation and assert exact canonical query values for unchanged and one-boundary-edited forms. Verify dimensions/limit remain, pagination resets, and existing preset/pagination/CSV links are not rewritten.
- [ ] Open the same canonical URL in two time zones and assert identical absolute interval and different display text. Cover one-nanosecond-wide ranges without collapsing or changing the submitted strings.
- [ ] Exercise the specified gap/fold cases in the browser. Disable JavaScript and block only the time-helper script in separate cases; verify both retain usable UTC labels/inputs and server-side UTC submission. Assert no chart initialization crash.
- [ ] Run `npm --prefix tests/browser run test:e2e -- analytics-timezone.spec.mjs`, `make test-web-unit`, and `go test ./internal/web ./internal/httpapi -count=1`; expect PASS.
- [ ] Update `docs/operations.md` analytics guidance: local browser display, UTC CSV/API, shared links, unchanged precision, fallback, and repeated/nonexistent local-time behavior. Use generic synthetic examples.
- [ ] Run `make test-web`, `go test ./...`, and `git diff --check`; expect PASS. Commit with `test(web): cover analytics time zones and range round trips`.

## Completion and integration

- [ ] Confirm no database migration, server default-zone change, bucket rebasing, or duration shift exists in the diff.
- [ ] If #31 has merged, rebase, mark its two token tables, and run both analytics browser suites.
- [ ] Record actual verification output and reference #32 in the implementation PR.
