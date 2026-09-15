package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	basestore "github.com/rislanov/vllm-priority-gateway/internal/store"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

func TestTransientCoordinationFailureLatchesRecovery(t *testing.T) {
	ctx := context.Background()
	store := openPostgresRecoveryTestStore(t)
	admission := coordpostgres.NewAdmissionCoordinator(store, 50*time.Millisecond)
	circuit, err := coordpostgres.NewCircuitCoordinator(store, 50*time.Millisecond, circuitbreaker.Options{
		FailureThreshold: 5, FailureWindow: time.Minute,
		OpenCooldown: time.Second, HalfOpenMaxProbes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := admission.RefreshInflight(ctx); err != nil {
		t.Fatal(err)
	}
	if err := circuit.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	recoveries := 0
	recovery := coordination.NewRecoveryManager(coordination.RecoverySteps{
		Ping: func(context.Context) error { recoveries++; return nil },
	}, time.Now)
	admission.SetFailureObserver(recovery)
	guard := &registry.RevisionGuard{}
	if status := coordinationReadiness(admission, circuit, nil, guard, recovery); status != "ready" {
		t.Fatal(status)
	}

	blocker, err := store.ConfigPool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Rollback(context.Background()) })
	if _, err := blocker.Exec(ctx, "LOCK TABLE request_leases IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	if err := admission.RefreshInflight(ctx); err == nil {
		t.Fatal("wanted blocked coordinator timeout")
	}
	if status := recovery.Status(); status.State != coordination.RecoveryDegraded {
		t.Fatalf("failure was not latched at occurrence: %+v", status)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := admission.RefreshInflight(ctx); err != nil {
		t.Fatal(err)
	}
	if !admission.Status().Available {
		t.Fatalf("coordinator did not recover its immediate status: %+v", admission.Status())
	}
	if status := coordinationReadiness(admission, circuit, nil, guard, recovery); status == "ready" {
		t.Fatalf("readiness returned ready without mandatory recovery: recoveries=%d recovery=%s", recoveries, recovery.Status().State)
	}
	if recoveries != 0 {
		t.Fatalf("recovery ran before the explicit recovery barrier: %d", recoveries)
	}
}

func openPostgresRecoveryTestStore(t *testing.T) *pgstore.Store {
	t.Helper()
	dsn := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := pgstore.Open(ctx, pgstore.Options{
		DatabaseURL: dsn, ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestCircuitRefreshResultTimeoutLatchesRecovery(t *testing.T) {
	ctx := context.Background()
	store := openPostgresRecoveryTestStore(t)
	pool, err := store.CreatePool(ctx, basestore.CreatePoolParams{
		PublicModelName: "refresh-timeout-" + uuid.NewString(), UpstreamModelName: "model", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := store.ConfigPool().Exec(cleanupCtx, "DELETE FROM backends WHERE model_pool_id=$1", pool.ID); err != nil {
			t.Error(err)
		}
		if _, err := store.ConfigPool().Exec(cleanupCtx, "DELETE FROM model_pools WHERE id=$1", pool.ID); err != nil {
			t.Error(err)
		}
	})
	backend, err := store.CreateBackend(ctx, basestore.CreateBackendParams{
		ModelPoolID: pool.ID, Name: "refresh-timeout-" + uuid.NewString(),
		BaseURL: "http://127.0.0.1:65530", Enabled: true, CapacityHint: 1, RunningSoftLimit: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	admission := coordpostgres.NewAdmissionCoordinator(store, 50*time.Millisecond)
	circuit, err := coordpostgres.NewCircuitCoordinator(store, 50*time.Millisecond, circuitbreaker.Options{
		FailureThreshold: 5, FailureWindow: time.Minute,
		OpenCooldown: time.Second, HalfOpenMaxProbes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := circuit.Reconcile(ctx, []coordination.BackendIdentity{{ID: backend.ID, Revision: backend.Revision, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	recovery := coordination.NewRecoveryManager(coordination.RecoverySteps{
		RefreshCircuits: circuit.Refresh,
	}, time.Now)
	circuit.SetFailureObserver(recovery)
	guard := &registry.RevisionGuard{}
	if status := coordinationReadiness(admission, circuit, nil, guard, recovery); status != "ready" {
		t.Fatal(status)
	}

	// Delay a projected value rather than the initial query of circuit IDs.
	// The real coordinator's deadline therefore expires while consuming result
	// rows, exercising the same error boundary as an interrupted result stream.
	withDelayedPostgresCircuitResults(t, store, func() {
		err := circuit.Refresh(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Refresh()=%v, want the coordinator's own deadline", err)
		}
		if ctx.Err() != nil {
			t.Fatalf("parent context ended: %v", ctx.Err())
		}
		if status := circuit.Status(); status.Available {
			t.Errorf("failed circuit refresh stayed available: %+v", status)
		}
		if status := recovery.Status(); status.State != coordination.RecoveryDegraded {
			t.Errorf("result timeout did not latch recovery: %+v", status)
		}
		if status := coordinationReadiness(admission, circuit, nil, guard, recovery); status != "degraded" {
			t.Errorf("readiness=%s immediately after failed refresh, want degraded", status)
		}
	})
	if err := circuit.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if status := coordinationReadiness(admission, circuit, nil, guard, recovery); status != "degraded" {
		t.Errorf("successful refresh cleared readiness without recovery: %s", status)
	}
	if err := recovery.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if status := coordinationReadiness(admission, circuit, nil, guard, recovery); status != "ready" {
		t.Fatalf("readiness after recovery=%s, want ready", status)
	}
}

func withDelayedPostgresCircuitResults(t *testing.T, store *pgstore.Store, check func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	install, err := store.ConfigPool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer install.Rollback(context.Background())
	for _, statement := range []string{
		`CREATE FUNCTION test_delay_circuit_revision(bigint) RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.25); RETURN $1; END $$`,
		`ALTER TABLE backend_circuit_state RENAME TO test_original_circuit_state`,
		`CREATE VIEW backend_circuit_state AS SELECT backend_id,test_delay_circuit_revision(backend_revision) AS backend_revision,state,generation,opened_at,half_open_succeeded,updated_at FROM test_original_circuit_state`,
	} {
		if _, err := install.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := install.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := pgx.BeginFunc(cleanupCtx, store.ConfigPool(), func(tx pgx.Tx) error {
			for _, statement := range []string{
				`DROP VIEW backend_circuit_state`,
				`ALTER TABLE test_original_circuit_state RENAME TO backend_circuit_state`,
				`DROP FUNCTION test_delay_circuit_revision(bigint)`,
			} {
				if _, err := tx.Exec(cleanupCtx, statement); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Errorf("restore circuit table after fault injection: %v", err)
		}
	}()
	check()
}
