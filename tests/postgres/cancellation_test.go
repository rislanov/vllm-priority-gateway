package postgres_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	basestore "github.com/rislanov/vllm-priority-gateway/internal/store"
)

func TestPostgresCallerCancellationDoesNotBlockNormalRequests(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fake := fakevllm.New()
	upstream := httptest.NewServer(fake.Handler())
	t.Cleanup(upstream.Close)
	fixture := preparePostgresHTTPFixture(t, store, upstream.URL, 4)
	_, err := store.UpdateClient(context.Background(), fixture.client.ID, basestore.UpdateClientParams{
		Name: fixture.client.Name, Enabled: true, PriorityClass: domain.PriorityNormal,
		MaxConcurrency: 4, ModelPoolIDs: []int64{fixture.pool.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	recovery := coordination.NewRecoveryManager(coordination.RecoverySteps{}, time.Now)
	canceledHandlerDone := make(chan struct{}, 1)
	const coordinationTimeout = 10 * time.Second
	gatewayValue := newPostgresHTTPGateway(t, ctx, store, circuitbreaker.Options{
		FailureThreshold: 5, FailureWindow: 30 * time.Second,
		OpenCooldown: 15 * time.Second, HalfOpenMaxProbes: 1,
	}, func() bool { return recovery.Status().State == coordination.RecoveryReady }, nil, 0, postgresHTTPGatewayOptions{
		CoordinationTimeout: coordinationTimeout, CanceledHandlerDone: canceledHandlerDone,
	})
	gatewayValue.admission.SetFailureObserver(recovery)
	gatewayValue.circuit.SetFailureObserver(recovery)
	waitPostgresGatewayBackend(t, gatewayValue, fixture.backend.ID)
	waitUntil(t, 2*time.Second, func() bool {
		return gatewayValue.manager.PoolSnapshot(fixture.pool.ID, time.Now()).AvailableBackends > 0
	})

	initial, err := gatewayValue.post(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	initialBody, _ := io.ReadAll(initial.Body)
	initial.Body.Close()
	if initial.StatusCode != http.StatusOK {
		t.Fatalf("initial request=%d body=%s", initial.StatusCode, initialBody)
	}
	waitUntil(t, time.Second, func() bool { return gatewayValue.leases.ActiveCount() == 0 })
	requestsBeforeCancellation := len(fake.Snapshot().Requests)

	blocker, err := store.ConfigPool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Rollback(context.Background()) })
	if _, err := blocker.Exec(ctx, "SELECT pool_id FROM pool_admission_scopes WHERE pool_id=$1 FOR UPDATE", fixture.pool.ID); err != nil {
		t.Fatal(err)
	}
	requestCtx, disconnect := context.WithCancel(ctx)
	t.Cleanup(disconnect)
	requestDone := make(chan error, 1)
	started := time.Now()
	go func() {
		response, err := gatewayValue.post(requestCtx, false)
		if response != nil {
			response.Body.Close()
		}
		requestDone <- err
	}()
	// PostgreSQL's lock wait proves the request reached distributed admission;
	// canceling before this barrier would only exercise the HTTP client.
	waitUntil(t, 3*time.Second, func() bool {
		var blocked bool
		err := store.CoordinationPool().QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
			AND wait_event_type='Lock'
			AND query LIKE 'SELECT pool_id FROM pool_admission_scopes WHERE pool_id=%')`).Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		return blocked
	})
	disconnect()
	select {
	case err := <-requestDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("caller result=%v, want context cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal("canceled HTTP client did not return")
	}
	select {
	case <-canceledHandlerDone:
	case <-ctx.Done():
		t.Fatal("canceled gateway handler did not return")
	}
	if elapsed := time.Since(started); elapsed >= coordinationTimeout {
		t.Fatalf("handler took %s; cancellation must precede the coordinator's own deadline", elapsed)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if requests := len(fake.Snapshot().Requests); requests != requestsBeforeCancellation {
		t.Fatalf("canceled admission reached upstream: requests=%d, want %d", requests, requestsBeforeCancellation)
	}
	if err := store.CoordinationPool().Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if status := recovery.Status(); status.State != coordination.RecoveryReady {
		t.Errorf("caller cancellation degraded healthy PostgreSQL: %+v", status)
	}
	if status := gatewayValue.admission.Status(); !status.Available {
		t.Errorf("caller cancellation made admission unavailable: %+v", status)
	}

	other, err := gatewayValue.post(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(other.Body)
	other.Body.Close()
	if other.StatusCode != http.StatusOK {
		t.Fatalf("unrelated Normal request=%d body=%s; recovery=%+v", other.StatusCode, body, recovery.Status())
	}
}
