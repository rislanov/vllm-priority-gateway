# Coordination Reset on Start Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the requested deployment-time reset environment toggle to gateway startup.

**Architecture:** Parse a default-false PostgreSQL toggle, pass a registration option
to the replica manager, and reset only mismatching registrations atomically with
startup registration. Normal heartbeats and recovery retain compatibility checks.

**Tech Stack:** Go, pgx, PostgreSQL, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-11-09-coordination-reset-on-start-design.md`

## Global Constraints

- `LLMGW_COORDINATION_RESET_ON_START=true`, default false; PostgreSQL-only when enabled.
- Operators drain and stop all old replicas before deploying identical new settings.
- Preserve matching replicas and all state outside `coordination_replicas`.
- No new dependency or schema migration.

## Review Focus

- Concurrent matching startup must preserve all new registrations.
- Failed insertion must roll back preceding deletion.
- Recovery and heartbeat must not reset later conflicts.
- Malformed env values must fail; SQLite must accept explicit false but reject true.
- Reset must not change policy fingerprint or require every peer to enable it.

## Task 1: Implement and verify the toggle

**Files:** `internal/config/config.go`, config tests;
`internal/coordination/postgres/replica.go`; `cmd/gateway/application.go` and new
`cmd/gateway/coordination_reset_test.go`; `.env.example`, `compose.yaml`,
`docs/postgresql-production.md`, `Makefile`.

**Interfaces:** Add `Config.CoordinationResetOnStart bool` and
`ReplicaManagerOptions{ResetOnStart bool}` as an optional constructor argument.

- [x] Write config rejection tests and PostgreSQL startup tests from the spec.
- [x] Run tests and confirm expected failures with current implementation.
- [x] Parse the toggle; reset/register under one table writer lock and transaction;
  wire the option and a warning into application startup.
- [x] Run focused tests, including concurrent registration and transaction rollback.
- [x] Document deployment usage and expose the default-false toggle in Compose.
- [x] Run `make test`, `go vet ./...`, and relevant real PostgreSQL/race tests.
- [x] Review the diff and report the tested behavior and operational precondition.
