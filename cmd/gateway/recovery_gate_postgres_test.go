package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

func TestTransientCoordinationFailureLatchesRecovery(t *testing.T) {
	dsn := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	store, err := pgstore.Open(ctx, pgstore.Options{
		DatabaseURL: dsn, ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
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
