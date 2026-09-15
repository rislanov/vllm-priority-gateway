# PostgreSQL integration with main

**Goal:** Merge main into codex/postgresql and preserve both main behavior and the PostgreSQL production coordination contract.

**Architecture:** Keep main's application/server lifecycle, forwarding lifecycle, session headers, load endpoint, deletion controls and telemetry. Move PostgreSQL configuration, coordinator initialization, recovery barriers and lease completion into these existing boundaries.

**Spec:** docs/superpowers/specs/2026-09-14-postgresql-production-coordination-design.md

**Constraints:** Go 1.27; PostgreSQL 16+; SQLite stays the default single-replica profile; direct migration connection and exec query mode; ordered recovery and conservative emergency admission.

- [x] Integrate storage operations: internal/store/{backends,clients,pools,sqlite,interfaces}.go and internal/store/postgres/configuration.go. Keep revision/rate columns, main CRUD improvements and deletion semantics. Verify storage tests and PostgreSQL CRUD.
- [x] Adapt configuration and application lifecycle: internal/config/config.go, cmd/gateway/{main,application,server}.go and coordination lifecycle helpers. Preserve the shared shutdown budget, readiness endpoints, independent database pools and ordered cleanup/recovery.
- [x] Adapt forwarding and monitoring: internal/gateway/{service,forward,observer}.go and internal/monitor/manager.go. Preserve current snapshot/key revalidation, session headers, panic cleanup, decision telemetry, distributed admission and circuit permits.
- [x] Integrate Admin/UI, tests and README: internal/httpapi/admin.go, internal/web/{web,resources}.go, conflicting tests and both README files. Keep delete controls, PostgreSQL failure redaction and rate limit fields. Preserve regression tests from both branches.
- [x] Run default tests, Linux race, vet, builds, PostgreSQL 16/15, PgBouncer and local performance checks. Investigate any failures before finalizing the merge. Review the adaptation against both parent revisions and the coordination spec.
- [x] Commit the tested merge on codex/postgresql and update the existing MR's integration status if published.

Execution notes: backup ref codex/postgresql-before-main-20260915 points at 079821f; main revision 80b88b2. Storage task has independent ownership; root adapts runtime and handlers. No source changes are shared between implementers.

Validation notes: default tests, race checks for all packages, 69 real PostgreSQL/PgBouncer scenarios, vet, builds, Docker smoke, and the PostgreSQL reference-performance gate passed. HTTP fixtures now explicitly publish healthy pool state before fault injection. The strict SQLite latency gate also fails on clean main in the same host topology; it is recorded in acceptance evidence rather than treated as a newly introduced regression. HA promotion/fencing and target GPU tests require separate deployment infrastructure.
