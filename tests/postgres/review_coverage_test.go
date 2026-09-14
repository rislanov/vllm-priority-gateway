package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestPostgresAdmissionAcceptsAllowedFutureSkewAndRetainsFromLatestTimestamp(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)

	var databaseNow time.Time
	if err := store.CoordinationPool().QueryRow(context.Background(), "SELECT clock_timestamp()").Scan(&databaseNow); err != nil {
		t.Fatal(err)
	}
	request := admissionRequest(fixture, uuid.New())
	request.OperationStartedAt = databaseNow.Add(4 * time.Minute)
	decision, err := coordinator.Acquire(context.Background(), request)
	if err != nil || !decision.Admitted() {
		t.Fatalf("future-skew acquire=%+v err=%v", decision, err)
	}
	if _, err := coordinator.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: decision.Lease.Identity()}}); err != nil {
		t.Fatal(err)
	}

	var startedAt, completedAt, retainUntil time.Time
	if err := store.CoordinationPool().QueryRow(context.Background(), `SELECT operation_started_at,completed_at,retain_until
		FROM admission_operations WHERE lease_id=$1::uuid`, request.LeaseID).Scan(&startedAt, &completedAt, &retainUntil); err != nil {
		t.Fatal(err)
	}
	retentionBase := completedAt
	if startedAt.After(retentionBase) {
		retentionBase = startedAt
	}
	wantRetainUntil := retentionBase.Add(24 * time.Hour)
	if !retainUntil.Equal(wantRetainUntil) {
		t.Fatalf("retain_until=%s, want max(started=%s, completed=%s)+24h=%s", retainUntil, startedAt, completedAt, wantRetainUntil)
	}
}

func TestPostgresAdmissionReceiptCleanupBoundaryPreventsReadmissionAndRPMDebit(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 2)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)

	var databaseNow time.Time
	if err := store.CoordinationPool().QueryRow(context.Background(), "SELECT clock_timestamp()").Scan(&databaseNow); err != nil {
		t.Fatal(err)
	}
	request := admissionRequest(fixture, uuid.New())
	request.OperationStartedAt = databaseNow.Add(-24*time.Hour + 2*time.Second)
	decision, err := coordinator.Acquire(context.Background(), request)
	if err != nil || !decision.Admitted() {
		t.Fatalf("near-boundary acquire=%+v err=%v", decision, err)
	}
	if _, err := coordinator.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: decision.Lease.Identity()}}); err != nil {
		t.Fatal(err)
	}

	boundary := request.OperationStartedAt.Add(24 * time.Hour)
	if _, err := store.CoordinationPool().Exec(context.Background(), `UPDATE admission_operations
		SET completed_at=operation_started_at,retain_until=$2::timestamptz WHERE lease_id=$1::uuid`, request.LeaseID, boundary); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Cleanup(context.Background(), 16); err != nil {
		t.Fatal(err)
	}
	assertPostgresRowCount(t, store.CoordinationPool(), "admission_operations", "lease_id", request.LeaseID, 1)

	waitForPostgresBoundary(t, store.CoordinationPool(), boundary)
	if err := coordinator.Cleanup(context.Background(), 16); err != nil {
		t.Fatal(err)
	}
	assertPostgresRowCount(t, store.CoordinationPool(), "admission_operations", "lease_id", request.LeaseID, 0)

	var balanceBefore float64
	if err := store.CoordinationPool().QueryRow(context.Background(), `SELECT balance FROM coordination_rate_state
		WHERE client_id=$1::bigint AND rate_kind='rpm'`, fixture.client.ID).Scan(&balanceBefore); err != nil {
		t.Fatal(err)
	}
	replayed, err := coordinator.Acquire(context.Background(), request)
	if err != nil || replayed.Reason != coordination.ReasonStaleOperation || replayed.Lease != nil {
		t.Fatalf("post-cleanup replay=%+v err=%v, want stale operation without lease", replayed, err)
	}
	var balanceAfter float64
	if err := store.CoordinationPool().QueryRow(context.Background(), `SELECT balance FROM coordination_rate_state
		WHERE client_id=$1::bigint AND rate_kind='rpm'`, fixture.client.ID).Scan(&balanceAfter); err != nil {
		t.Fatal(err)
	}
	if balanceAfter != balanceBefore {
		t.Fatalf("RPM balance changed across stale replay: before=%f after=%f", balanceBefore, balanceAfter)
	}
	assertPostgresRowCount(t, store.CoordinationPool(), "request_leases", "lease_id", request.LeaseID, 0)
}

func TestPostgresHalfOpenPermitCleanupBoundaryRejectsSameUUIDReplay(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: time.Millisecond, HalfOpenMaxProbes: 1}
	coordinator, err := coordpostgres.NewCircuitCoordinator(store, time.Second, options)
	if err != nil {
		t.Fatal(err)
	}
	identity := coordination.BackendIdentity{ID: fixture.backend.ID, Revision: fixture.backend.Revision, Enabled: true}
	if err := coordinator.Reconcile(context.Background(), []coordination.BackendIdentity{identity}); err != nil {
		t.Fatal(err)
	}
	failureRequest := coordination.CircuitAcquireRequest{
		AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: time.Minute,
	}
	closed, err := coordinator.Acquire(context.Background(), failureRequest)
	if err != nil || closed.Reason != "" {
		t.Fatalf("closed acquire=%+v err=%v", closed, err)
	}
	if _, err := coordinator.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: failureRequest.AttemptID, Backend: identity, Generation: closed.Snapshot.Generation,
		Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(options.OpenCooldown + time.Millisecond)

	var databaseNow time.Time
	if err := store.CoordinationPool().QueryRow(context.Background(), "SELECT clock_timestamp()").Scan(&databaseNow); err != nil {
		t.Fatal(err)
	}
	probeRequest := coordination.CircuitAcquireRequest{
		AttemptID: uuid.New(), AcquisitionStartedAt: databaseNow.Add(-24*time.Hour + 2*time.Second),
		ReplicaID: uuid.New(), Backend: identity, ProbeTTL: time.Minute,
	}
	probe, err := coordinator.Acquire(context.Background(), probeRequest)
	if err != nil || probe.Permit == nil {
		t.Fatalf("near-boundary probe=%+v err=%v", probe, err)
	}
	if _, err := coordinator.Complete(context.Background(), coordination.CircuitCompletion{
		AttemptID: probe.Permit.PermitID, Backend: identity, Generation: probe.Permit.Generation,
		Outcome: domain.InferenceSuccess, ReportedOutcomeAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	boundary := probeRequest.AcquisitionStartedAt.Add(24 * time.Hour)
	if _, err := store.CoordinationPool().Exec(context.Background(), `UPDATE backend_circuit_probes
		SET processed_at=acquisition_started_at,reported_outcome_at=acquisition_started_at,retain_until=$2::timestamptz
		WHERE permit_id=$1::uuid`, probeRequest.AttemptID, boundary); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Cleanup(context.Background(), 16); err != nil {
		t.Fatal(err)
	}
	assertPostgresRowCount(t, store.CoordinationPool(), "backend_circuit_probes", "permit_id", probeRequest.AttemptID, 1)

	waitForPostgresBoundary(t, store.CoordinationPool(), boundary)
	if err := coordinator.Cleanup(context.Background(), 16); err != nil {
		t.Fatal(err)
	}
	assertPostgresRowCount(t, store.CoordinationPool(), "backend_circuit_probes", "permit_id", probeRequest.AttemptID, 0)
	replayed, err := coordinator.Acquire(context.Background(), probeRequest)
	if err != nil || replayed.Reason != coordination.ReasonStaleOperation || replayed.Permit != nil {
		t.Fatalf("post-cleanup probe replay=%+v err=%v, want stale operation without permit", replayed, err)
	}
	assertPostgresRowCount(t, store.CoordinationPool(), "backend_circuit_probes", "permit_id", probeRequest.AttemptID, 0)
}

func assertPostgresRowCount(t *testing.T, pool *pgxpool.Pool, table, column string, id uuid.UUID, want int) {
	t.Helper()
	var count int
	query := "SELECT count(*) FROM " + table + " WHERE " + column + "=$1::uuid"
	if err := pool.QueryRow(context.Background(), query, id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("%s row count=%d, want %d", table, count, want)
	}
}

func waitForPostgresBoundary(t *testing.T, pool *pgxpool.Pool, boundary time.Time) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var reached bool
		if err := pool.QueryRow(context.Background(), "SELECT clock_timestamp()>=$1::timestamptz", boundary).Scan(&reached); err != nil {
			t.Fatal(err)
		}
		if reached {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("PostgreSQL clock did not reach cleanup boundary %s", boundary)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
