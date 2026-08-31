# Behavior-Preserving Go Project Refactor Design

## Context

The gateway is a Go 1.27 modular monolith with three commands, eighteen internal packages, an embedded Admin UI, SQLite persistence, Prometheus telemetry, and unit, integration, Compose-contract, and real-vLLM acceptance tests. The public request path crosses authentication, policy, admission, routing, proxying, analytics, monitoring, and observability boundaries. The existing package boundaries are sound, but several orchestration functions have accumulated too many responsibilities.

The refactor starts from `origin/main` commit `d4e7357af7ec141aa6f12b834b7919f9373cd5e1` on branch `codex/go-project-refactor`. It must improve maintainability without changing product behavior, contracts, or operator workflows.

## Goals

- Audit every production Go package using the code graph, semantic Go tooling, tests, and direct source inspection.
- Reduce the complexity of the highest-risk orchestration and streaming paths while retaining the current package architecture.
- Make ownership of request leases, completion reservations, response finalization, goroutines, and shutdown explicit.
- Remove avoidable nesting and duplication in configuration, runtime metrics, load generation, and Admin UI handlers.
- Restore cross-platform testability by constructing a valid SQLite file URI from Windows absolute paths.
- Preserve the existing runtime dependency set and all user-visible behavior.
- Verify the result on the host and in Linux containers, then obtain and address an independent subagent review.

## Non-goals

- No clean-architecture, hexagonal, service, repository, or package-boundary rewrite.
- No new runtime dependency and no dependency upgrade without a separately demonstrated need.
- No new endpoint, feature, environment variable, database migration, metric, dashboard, or UI interaction.
- No change to distributed-system scope, deployment topology, or the real-vLLM acceptance model.
- No cosmetic repository-wide rewrite where the existing code is already idiomatic and focused.

## Compatibility invariants

The following are executable contracts and must remain unchanged:

- Public and Admin HTTP routes, methods, status codes, response bodies, OpenAI-compatible error envelopes, `Retry-After`, authentication, and CSRF behavior.
- Request and response headers, including removal of client-controlled vLLM priority and assignment of the stored server-controlled priority.
- Model authorization, model-name rewriting, pool admission, retry boundaries, circuit-breaker accounting, routing, and readiness semantics.
- Byte-for-byte streaming delivery, flush behavior, cancellation propagation, retry stopping after the first committed byte, and time-to-first-byte characteristics.
- Analytics reservation and completion lifecycle, bounded-memory passive usage inspection, durable metadata, CSV behavior, and the rule that request bodies and generated content are never persisted.
- SQLite schema, ordered migrations, data compatibility, file permissions, transaction boundaries, and retention behavior.
- Prometheus metric names, label names and bounded cardinality, log fields, Grafana queries, and topology-label cleanup.
- Environment variable names and defaults, command-line behavior, Docker image entrypoint, healthchecks, Compose service contracts, and Admin UI markup, copy, forms, and navigation.

## Chosen approach

Use an incremental, evidence-driven refactor inside the existing packages. Each change is independently testable and keeps stable interfaces at the package boundary. Characterization tests are added before any extraction whose behavior is not already directly asserted.

### Gateway request lifecycle

`gateway.Service.Forward` remains the public orchestration method. Private request-lifecycle state and focused stage helpers separate authentication and identity resolution, policy and admission, backend acquisition, payload rewriting, proxy execution, and terminal accounting. Lease and completion-reservation release stays local to the object that acquired it, with exactly-once cleanup covered by existing and additional tests.

### Proxy and usage inspection

`proxy.Proxy.Forward` retains its signature and response semantics. The single-attempt path is decomposed into request construction, upstream execution, response commitment, body copy, and outcome classification helpers.

The JSON/SSE usage inspector remains incremental and bounded. Its parser state is divided into small transition and value-capture helpers without replacing it with full-body decoding, buffering the response, reordering downstream writes, or changing parse-failure reporting.

### Application lifecycle

`cmd/gateway.run` remains the testable entry point. Dependency construction, background runtime loops, HTTP server setup, and ordered graceful shutdown are separated into focused private helpers or files in the same command package. Context cancellation and shutdown deadlines remain the source of lifecycle control; no detached goroutine is introduced.

### Focused package cleanup

- Split configuration loading into typed environment reads, default application, and validation without changing accepted values or errors.
- Extract runtime-metric label-set construction and stale-series reconciliation while preserving lock scope and label lifecycle.
- Separate load-generator worker scheduling from result aggregation while preserving request ordering assumptions and statistics.
- Decompose complex Admin UI form dispatch and edit lookup into private helpers without changing rendered HTML or error text.
- Review analytics, monitoring, store, admission, routing, pressure, registry, API-key, domain, fake-vLLM, and HTTP API packages for concrete correctness or maintainability findings. Leave already-focused code unchanged.

### Cross-platform SQLite URI

The current `store.Open` builds a `file:` URL from `filepath.ToSlash(absolutePath)`. On Windows the path starts with a drive letter rather than `/`, and SQLite parses `C:` as a URI authority, causing all database-dependent tests to fail with `invalid uri authority: C:`. A focused helper will produce a standards-compatible file URI for both Windows drive paths and POSIX absolute paths while retaining the existing pragmas. A direct regression test will assert the DSN shape independently of SQLite, and the existing store/integration suites will prove behavior.

## Error handling and concurrency

- Errors gain context only at ownership boundaries; existing sentinel/error-to-HTTP mappings and user-visible text remain stable.
- Cleanup uses `defer` only when ownership is already established and the ordering is unambiguous.
- Goroutines have a documented termination signal and a waiting owner. Channels retain bounded capacities and cancellation-aware send/receive behavior.
- Locks protect the same state as before, with external I/O and callbacks kept outside critical sections unless the current invariant explicitly requires serialization.
- No panic recovery, retry, timeout, or fallback is added without a contract test demonstrating the intended behavior.

## Implementation and review method

Work proceeds in small TDD-backed commits. Before implementation, the relevant Go skills are applied for refactoring, architecture, code quality, concurrency, database access, observability, performance, dependencies, project delivery, tooling, security, troubleshooting, and `gopls` navigation. Skills that do not match a concrete package or change are used as audit checklists rather than justification for churn.

After implementation and verification, a fresh subagent receives the exact base/head diff, graph generation and coverage limitations, changed-symbol traces, and test evidence. It reviews correctness, compatibility, concurrency, security, test coverage, and maintainability. Every material finding is either fixed and reverified or explicitly rejected with source-backed reasoning, followed by a second review pass when changes are made.

## Verification strategy

### Baseline

- Record the fresh branch SHA and dirty state.
- Run `go test -count=1 ./...` and preserve the pre-existing Windows SQLite URI failure as baseline evidence.
- Run the Linux baseline in a Go 1.27 container to distinguish the Windows path bug from platform-independent failures.

### Per-change verification

- Add or strengthen a focused test before modifying behavior-sensitive code.
- Run the narrow package test and confirm the new test fails for the intended reason where a behavior gap exists.
- Implement the smallest coherent refactor, then run the package tests, affected integration tests, and `gopls` diagnostics.
- Use repeated, shuffled, and race-enabled runs for concurrency and lifecycle paths.

### Final host verification

- `gofmt` check over all Go files.
- `go test -count=1 ./...`.
- `go test -shuffle=on -count=1 ./...`.
- `go test -race -count=1 ./...`.
- `go vet ./...`.
- `go build ./cmd/...` and static Linux command builds.
- Relevant `gopls` and optional installed-linter diagnostics with every result triaged.

### Final Docker verification

- Start the local Docker Desktop Linux engine and verify server connectivity.
- Run the complete Go test suite in a clean `golang:1.27` Linux container with the checkout mounted read-only and caches on disposable volumes.
- Run Compose contract tests against the installed Docker Compose implementation.
- Build the production Dockerfile for `linux/amd64`.
- Run `make container-smoke` (or the equivalent script invocation) to validate the scratch image, non-root runtime, writable data volume, startup, and `/healthz`.
- Inspect container logs and clean up only the task-specific containers, images, networks, and volumes created by the verification commands.

The full two-vLLM GPU stack remains an opt-in hardware acceptance layer because it requires an NVIDIA GPU visible to Docker, about 25 GiB of artifacts, and model startup. It is not substituted for the deterministic fake-backend integration suite; if the host satisfies those prerequisites, the existing documented real-vLLM scenario may be run without changing its contract.

## Documentation

The refactor does not require README, operator guide, deployment, API, or UX changes. This design and its implementation plan document the internal work. User-facing documentation changes are made only if verification finds an existing command or behavior description that is factually wrong.
