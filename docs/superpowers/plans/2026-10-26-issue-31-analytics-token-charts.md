# Analytics Token Charts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Make output activity readable by separating input/output token charts and giving every analytics chart the full content width.

**Architecture:** Keep the existing server-rendered dataset and progressive SVG enhancement. Render four panels from the same series, with independent token scales and a shared time domain. Adapt SVG geometry to the panel width rather than scaling a fixed 760-unit canvas indefinitely.

**Tech Stack:** Go 1.27, html/template, embedded vanilla JavaScript/CSS, Node 24, Playwright.

**Spec:** [GitHub issue #31](https://github.com/rislanov/vllm-priority-gateway/issues/31). Requirements are captured below so this plan is usable without the chat.

**Base:** `origin/main` at `d496a04615a8ea9e0f5f059ba319d3a58161ed54`.
**Branch:** `codex/issue-31-analytics-token-charts`.
**Scope:** Implementation and validation completed; see the execution notes below.

## Global Constraints

- Panel order: Request volume, Input tokens, Output tokens, Cache usage; each spans the available content width.
- Independent input/output Y maxima, both starting at zero; common selected time domain and horizontal plot alignment.
- Preserve input/output colors and cache-null semantics, accessible descriptions, and expandable data tables.
- Preserve empty ranges, explicit zero buckets, sparse series, summary values, filters, and pagination.
- No changes to storage, aggregation, API schemas, chart dependencies, or time-zone semantics in this issue.
- Keep exactly one machine-readable series source. Do not duplicate `data-analytics-point` attributes in the new tables.
- Issue #32 independently owns time localization. Both branches start from main; when integrating them, preserve the shared time-label helper and localize every newly added table.
- Use only synthetic fixtures and generic examples; do not commit user screenshots, identities, or deployment details.

## Review Focus

1. Output values 1,000 times smaller than input must still occupy a meaningful vertical range (Task 2).
2. Duplicate data attributes must not double the chart's points (Task 1).
3. Empty/all-zero/single-point series must not produce NaN coordinates or misleading no-data messages (Tasks 1–2).
4. Wide desktop SVGs must not become excessively tall, and mobile labels must not shrink into unreadability (Task 3).
5. Cache-known gaps and no-JavaScript tables must retain their existing meaning (Tasks 1 and 3).

## Evidence and file map

At the base revision, `internal/web/static/app.js:141` uses one maximum across both token arrays. `internal/web/templates/analytics.html:73` renders three panels, and `internal/web/static/app.css:128` defines a two-column grid. `chartFrame` uses a fixed 760-unit width.

- Modify `internal/web/templates/analytics.html`: four panels, separate tables.
- Modify `internal/web/static/app.js`: select a single token field per chart; responsive plot geometry.
- Modify `internal/web/static/app.css`: one-column layout and stable chart heights.
- Modify `internal/web/web_test.go`: actual rendered HTML and fallback assertions.
- Create `internal/web/analytics_browser_fixture_test.go`: test-only deterministic HTML fixture export.
- Create `tests/browser/analytics-fixtures.mjs`: generate fixtures once and provide them to Playwright.
- Create `tests/browser/analytics-charts.spec.mjs`: rendered chart/layout regression tests.
- Existing `tests/browser/playwright.config.mjs` discovers new `*.spec.mjs` files; no new runner is required.

### Task 1: Split the rendered panels and preserve fallback data

**Interfaces:** `data-analytics-chart` values become `requests`, `input-tokens`, `output-tokens`, and `cache`. Keep the existing point fields `inputTokens` and `outputTokens` and the canonical range attributes unchanged.

- [x] Extend `TestAnalyticsPageProvidesAccessibleChartFallbacksAndSelfHostedScript` in `internal/web/web_test.go`: assert the four panel identifiers in order, distinct captions "Input token data" and "Output token data", and one series source. For N buckets, assert exactly N `data-analytics-point` rows.
- [x] Extend empty/missing-usage/zero-bucket assertions for both token tables. Preserve the cache table's unavailable values and no-JavaScript explanatory text.
- [x] Run `go test ./internal/web -run TestAnalytics -count=1`; expect failures in the newly added panel/table assertions.
- [x] Replace the combined token panel with two panels. Keep titles, IDs, descriptions, captions, and column counts consistent; render each token table with only its relevant metric.
- [x] Change `.chart-grid` to one column. Keep the existing cache panel behavior; remove the now-redundant wide-panel distinction only where it is no longer used.
- [x] Run the focused tests; expect PASS. Commit the template/style/test changes with `fix(web): split analytics token panels`.

### Task 2: Give each token chart an independent scale

**Interfaces:** Replace the combined renderer with `renderTokenChart(container, series, metric)`, where `metric` is exactly `inputTokens` or `outputTokens`. `renderChart(container, series)` dispatches the two panel identifiers to that function. Existing `chartFrame`, `drawGrid`, `drawSeries`, and `drawTimeLabels` remain shared.

- [x] Add `TestWriteAnalyticsBrowserFixtures` in the new Go fixture file. Skip unless `LLMGW_BROWSER_ANALYTICS_FIXTURE_DIR` is set; render through the existing web handler/fixture store and write synthetic `imbalanced.html`, `zero.html`, `empty.html`, and `sparse.html` there. No production endpoint or persistent database fixture is added.
- [x] Implement `createAnalyticsFixtures()` in `tests/browser/analytics-fixtures.mjs`: create a temporary directory, run `go test ./internal/web -run '^TestWriteAnalyticsBrowserFixtures$' -count=1` at the repository root with that variable, return `{directory, dispose}`, and remove the directory in teardown. Browser tests route only the analytics document to these rendered files; static scripts/styles still load from the existing real test gateway.
- [x] In `analytics-charts.spec.mjs`, assert that points [1000000, 500000] and [1000, 500] reach equivalent relative heights in their respective panels. Assert that changing input alone does not change output coordinates, both baselines represent zero, all charts align the same timestamps, and no SVG contains NaN/Infinity. Cover a single nonzero bucket and an all-zero series.
- [x] Run `npm --prefix tests/browser run test:e2e -- analytics-charts.spec.mjs`; expect failures until the new renderer exists.
- [x] Compute the maximum from only the selected metric, with a floor of 1. Preserve the input circle/solid and output square/dashed styling and provide a single relevant legend and accurate accessible title/description for each panel.
- [x] Run `go test ./internal/web -count=1` and the focused browser tests; expect PASS. Commit with `fix(web): scale token charts independently`.

### Task 3: Keep full-width charts readable at every supported viewport

**Interfaces:** `chartFrame` returns the existing frame fields plus a measured width; `frame.right` derives from that width. All series use the same left/right margins. A resize re-renders a panel from the immutable series data, without rebuilding the data source or multiplying observers.

- [x] Add browser cases at widths 375, 768, and 1440: four stacked panels with equal outer widths, no document-level horizontal overflow, legible fixed-size SVG text, unique accessible IDs, and stable plot heights. Resize an existing page and assert paths/labels are recalculated.
- [x] Run the focused browser suite; verify the geometry assertions fail before changing the fixed-width canvas.
- [x] Measure the chart container for the SVG viewBox width, keep regular/cache viewBox heights at 280/460, and derive the right edge from the measured width. Use a single ResizeObserver with a scheduled re-render only when width changes; skip zero-width hidden containers until measurable.
- [x] Keep date/time endpoint labels within the plot; use two text lines when needed rather than shrinking the entire SVG. Keep the current UTC label content in this issue.
- [x] Add a no-JavaScript browser case that opens all four tables and verifies the exact fixture values. Assert unavailable cache buckets remain gaps after enhancement.
- [x] Run `go test ./internal/web -count=1`, `make test-web`, and `git diff --check`; expect PASS. Visually inspect the synthetic imbalanced fixture at narrow and wide widths. Commit with `fix(web): keep analytics charts readable across widths`.

## Completion and integration

- [x] Run `go test ./...`, `make test-web`, and `git diff --check`; record actual results.
- [x] Confirm the implementation diff contains no store/API changes and no private screenshots or deployment identifiers.
- [x] If #32 has merged, rebase and preserve its localized formatter, time-zone copy, and all timestamp markers; rerun both analytics browser suites.
- [x] Reference #31 in implementation and acceptance evidence; push the branch as requested. No pull request was requested.

## Execution notes — 2026-10-26

- Synced main to `e93a6a5` and merged it into this branch before implementation, as requested. This includes issue #32; every new table preserves its localized timestamp markers and shared formatter.
- Reused the fixture exporter and browser helper already added by issue #32. The implementation proceeded in the clean checkout on the explicitly requested branch.
- `npm` and `make` are absent on this host. The same web checks ran with `node --test tests/browser/*.unit.test.mjs` and `node tests/browser/node_modules/@playwright/test/cli.js test --config tests/browser/playwright.config.mjs`; bundled pnpm installed the pinned dependency without a lockfile change.
- Final checks: `go test ./...`, `go vet ./...`, 19 JS unit tests, 53 browser tests (one PostgreSQL-only test skipped), Docker image build, and real two-backend vLLM inference/browser/smoke validation passed.
- Two independent reviewers found no critical or important issues. Both minor remarks were fixed and covered: first-bucket sparse annotation overlap and no-JavaScript cache unavailable/zero assertions. No review findings remain deferred.
- See [acceptance evidence](../../acceptance-evidence.md#analytics-token-charts--issue-31-2026-10-26) for the environment, scope, actual token totals, and test limitations.