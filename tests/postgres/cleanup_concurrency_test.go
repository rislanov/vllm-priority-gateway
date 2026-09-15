package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestPostgresReceiptCleanupPreservesConcurrentCompletion(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createRateFixture(t, store, 0, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coordinator := coordpostgres.NewAdmissionCoordinator(store, 5*time.Second)
	decision, err := coordinator.Acquire(ctx, admissionRequest(fixture, uuid.New()))
	if err != nil || !decision.Admitted() {
		t.Fatalf("admission=%+v err=%v", decision, err)
	}
	completion := coordination.LeaseCompletion{
		Lease: *decision.Lease,
		Usage: &coordination.TokenUsage{InputTokens: 7, OutputTokens: 3},
	}
	if _, err := store.ConfigPool().Exec(ctx, "DELETE FROM request_leases WHERE lease_id=$1::uuid", completion.Lease.LeaseID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ConfigPool().Exec(ctx, `UPDATE admission_operations
		SET operation_started_at=clock_timestamp()-interval '25 hours',
		lease_expired_at=clock_timestamp()-interval '24 hours',retain_until=clock_timestamp()-interval '1 second'
		WHERE lease_id=$1::uuid`, completion.Lease.LeaseID); err != nil {
		t.Fatal(err)
	}

	// Completion acquires its receipt before waiting for this TPM row. That
	// lets cleanup observe old retention while completion still owns the row.
	rateBlocker, err := store.ConfigPool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rateBlocker.Rollback(context.Background())
	if _, err := rateBlocker.Exec(ctx, "SELECT client_id FROM coordination_rate_state WHERE client_id=$1::bigint AND rate_kind='tpm' FOR UPDATE", fixture.client.ID); err != nil {
		t.Fatal(err)
	}
	type completionResult struct {
		results []coordination.CompleteResult
		err     error
	}
	completed := make(chan completionResult, 1)
	go func() {
		results, err := coordinator.Complete(ctx, []coordination.LeaseCompletion{completion})
		completed <- completionResult{results: results, err: err}
	}()
	for {
		var blocked bool
		if err := store.ConfigPool().QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
			AND wait_event_type='Lock' AND query LIKE '%coordination_rate_state%')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case result := <-completed:
			t.Fatalf("completion returned before TPM lock: %+v", result)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}

	cleaned := make(chan error, 1)
	go func() { cleaned <- coordinator.Cleanup(ctx, 1) }()
	cleanupFinished := false
	for {
		select {
		case err := <-cleaned:
			if err != nil {
				t.Fatal(err)
			}
			cleanupFinished = true
		default:
		}
		if cleanupFinished {
			break // SKIP LOCKED implementations can finish immediately.
		}
		var blocked bool
		if err := store.ConfigPool().QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
			AND wait_event_type='Lock' AND query LIKE '%admission_operations%')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break // A locking implementation may instead recheck after waiting.
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if err := rateBlocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-completed
	if result.err != nil || len(result.results) != 1 || result.results[0].Result != coordination.CompletionLeaseLost {
		t.Fatalf("first completion=%+v err=%v", result.results, result.err)
	}
	if !cleanupFinished {
		if err := <-cleaned; err != nil {
			t.Fatal(err)
		}
	}
	var retained bool
	if err := store.ConfigPool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM admission_operations
		WHERE lease_id=$1::uuid AND completed_at IS NOT NULL AND retain_until>clock_timestamp())`, completion.Lease.LeaseID).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if !retained {
		t.Error("cleanup deleted a receipt whose concurrent completion extended retention")
	}
	var balanceBefore, balanceAfter float64
	if err := store.ConfigPool().QueryRow(ctx, "SELECT balance FROM coordination_rate_state WHERE client_id=$1::bigint AND rate_kind='tpm'", fixture.client.ID).Scan(&balanceBefore); err != nil {
		t.Fatal(err)
	}
	duplicate, err := coordinator.Complete(ctx, []coordination.LeaseCompletion{completion})
	if err != nil || len(duplicate) != 1 || duplicate[0].Result != coordination.CompletionAlreadyCompleted || duplicate[0].OriginalResult != coordination.CompletionLeaseLost {
		t.Errorf("duplicate completion=%+v err=%v, want original idempotent result", duplicate, err)
	}
	if err := store.ConfigPool().QueryRow(ctx, "SELECT balance FROM coordination_rate_state WHERE client_id=$1::bigint AND rate_kind='tpm'", fixture.client.ID).Scan(&balanceAfter); err != nil {
		t.Fatal(err)
	}
	if balanceBefore != balanceAfter {
		t.Errorf("duplicate completion changed TPM balance: %v -> %v", balanceBefore, balanceAfter)
	}
}

func TestPostgresCircuitCleanupDeletesExpiredFailureReceiptBatches(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coordinator, err := coordpostgres.NewCircuitCoordinator(store, time.Second, circuitbreaker.Options{
		FailureThreshold: 5, FailureWindow: 30 * time.Second, OpenCooldown: 15 * time.Second, HalfOpenMaxProbes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := coordination.BackendIdentity{ID: fixture.backend.ID, Revision: fixture.backend.Revision, Enabled: true}
	if err := coordinator.Reconcile(ctx, []coordination.BackendIdentity{backend}); err != nil {
		t.Fatal(err)
	}
	var freshAttempt uuid.UUID
	for i := range 3 {
		request := coordination.CircuitAcquireRequest{
			AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute,
		}
		decision, err := coordinator.Acquire(ctx, request)
		if err != nil || decision.Reason != "" {
			t.Fatalf("acquire=%+v err=%v", decision, err)
		}
		if _, err := coordinator.Complete(ctx, coordination.CircuitCompletion{
			AttemptID: request.AttemptID, Backend: backend, Generation: decision.Snapshot.Generation,
			Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			if _, err := store.ConfigPool().Exec(ctx, `UPDATE backend_circuit_failure_receipts
				SET retain_until=clock_timestamp()-interval '1 second' WHERE attempt_id=$1::uuid`, request.AttemptID); err != nil {
				t.Fatal(err)
			}
		} else {
			freshAttempt = request.AttemptID
		}
	}
	for i, wantRemaining := range []int{2, 1, 1} {
		more, err := coordinator.CleanupBatch(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if more != (i < 2) {
			t.Fatalf("cleanup pass %d more=%v, want %v", i, more, i < 2)
		}
		for _, table := range []string{"backend_circuit_failure_receipts", "backend_circuit_failures"} {
			var remaining int
			if err := store.ConfigPool().QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&remaining); err != nil {
				t.Fatal(err)
			}
			if remaining != wantRemaining {
				t.Fatalf("%s remaining=%d, want %d after bounded cleanup", table, remaining, wantRemaining)
			}
		}
		assertPostgresRowCount(t, store.CoordinationPool(), "backend_circuit_failure_receipts", "attempt_id", freshAttempt, 1)
	}
}

func TestPostgresAdmissionCleanupReportsFullBatches(t *testing.T) {
	for _, completed := range []bool{false, true} {
		name := "expired_leases"
		if completed {
			name = "terminal_receipts"
		}
		t.Run(name, func(t *testing.T) {
			store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
			fixture := createFixture(t, store, 0)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
			var freshLease uuid.UUID
			for i := range 3 {
				decision, err := coordinator.Acquire(ctx, admissionRequest(fixture, uuid.New()))
				if err != nil || !decision.Admitted() {
					t.Fatalf("admission=%+v err=%v", decision, err)
				}
				if completed {
					if _, err := coordinator.Complete(ctx, []coordination.LeaseCompletion{{Lease: decision.Lease.Identity()}}); err != nil {
						t.Fatal(err)
					}
				}
				if i < 2 {
					query := "UPDATE request_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE lease_id=$1::uuid"
					if completed {
						query = "UPDATE admission_operations SET retain_until=clock_timestamp()-interval '1 second' WHERE lease_id=$1::uuid"
					}
					if _, err := store.ConfigPool().Exec(ctx, query, decision.Lease.LeaseID); err != nil {
						t.Fatal(err)
					}
				} else {
					freshLease = decision.Lease.LeaseID
				}
			}
			for i, wantRemaining := range []int{2, 1, 1} {
				more, err := coordinator.CleanupBatch(ctx, 1)
				if err != nil {
					t.Fatal(err)
				}
				if more != (i < 2) {
					t.Fatalf("cleanup pass %d more=%v, want %v", i, more, i < 2)
				}
				table := "request_leases"
				if completed {
					table = "admission_operations"
				}
				var remaining int
				if err := store.ConfigPool().QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&remaining); err != nil {
					t.Fatal(err)
				}
				if remaining != wantRemaining {
					t.Fatalf("%s remaining=%d, want %d after bounded cleanup", table, remaining, wantRemaining)
				}
				assertPostgresRowCount(t, store.CoordinationPool(), table, "lease_id", freshLease, 1)
			}
		})
	}
}

func TestPostgresExpiredLeaseCleanupDrainsBacklogWithinMaintenanceBudget(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coordinator := coordpostgres.NewAdmissionCoordinator(store, 50*time.Millisecond)
	fresh, err := coordinator.Acquire(ctx, admissionRequest(fixture, uuid.New()))
	if err != nil || !fresh.Admitted() {
		t.Fatalf("fresh admission=%+v err=%v", fresh, err)
	}
	const batch = 256
	const backlog = 2*batch + 1
	// Clone a valid admission as expired history without spending hundreds of
	// request round trips on fixture setup. The original lease stays active.
	if _, err := store.ConfigPool().Exec(ctx, `WITH receipts AS (
		INSERT INTO admission_operations(lease_id,operation_started_at,input_fingerprint,
			request_id,replica_id,client_id,pool_id,decision,decided_at)
		SELECT gen_random_uuid(),o.operation_started_at,o.input_fingerprint,
			o.request_id||'-expired-'||n::text,o.replica_id,o.client_id,o.pool_id,o.decision,o.decided_at
		FROM admission_operations o CROSS JOIN generate_series(1,$2::integer) n WHERE o.lease_id=$1::uuid
		RETURNING lease_id,request_id,replica_id,client_id,pool_id,operation_started_at)
		INSERT INTO request_leases(lease_id,request_id,replica_id,client_id,pool_id,acquired_at,expires_at)
		SELECT lease_id,request_id,replica_id,client_id,pool_id,operation_started_at,
			clock_timestamp()-interval '1 second' FROM receipts`, fresh.Lease.LeaseID, backlog); err != nil {
		t.Fatal(err)
	}
	for pass, wantRemaining := range []int{batch + 1, 1, 0} {
		passCtx, passCancel := context.WithTimeout(ctx, 250*time.Millisecond)
		started := time.Now()
		more, cleanupErr := coordinator.CleanupBatch(passCtx, batch)
		elapsed := time.Since(started)
		passCancel()
		var remaining int
		if err := store.ConfigPool().QueryRow(ctx, "SELECT count(*) FROM request_leases WHERE expires_at<=clock_timestamp()").Scan(&remaining); err != nil {
			t.Fatal(err)
		}
		t.Logf("pass=%d elapsed=%s expired_remaining=%d more=%v err=%v", pass, elapsed, remaining, more, cleanupErr)
		if cleanupErr != nil || remaining != wantRemaining || more != (pass < 2) {
			t.Errorf("pass %d: remaining=%d more=%v err=%v, want remaining=%d more=%v",
				pass, remaining, more, cleanupErr, wantRemaining, pass < 2)
		}
	}
	assertPostgresRowCount(t, store.CoordinationPool(), "request_leases", "lease_id", fresh.Lease.LeaseID, 1)
	var retained int
	if err := store.ConfigPool().QueryRow(ctx, `SELECT count(*) FROM admission_operations
		WHERE lease_expired_at IS NOT NULL AND retain_until>clock_timestamp()`).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != backlog {
		t.Errorf("retained expired receipts=%d, want %d", retained, backlog)
	}
}
