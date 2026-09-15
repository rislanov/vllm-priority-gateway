package postgres_test

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
)

// A burst that fits both limits must not become a database timeout simply
// because all admissions serialize on the pool scope. Keep the production
// timeout here; the older contention test uses two seconds.
func TestPostgresAdmissionParallelDefaultTimeout(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	const parallelism = 16
	f := updateFixtureLimits(t, store, createFixture(t, store, 0), parallelism, parallelism)
	ctx := context.Background()
	// Exclude connection establishment from the burst under test.
	connections := make([]*pgxpool.Conn, 0, parallelism)
	for i := 0; i < parallelism; i++ {
		conn, err := store.CoordinationPool().Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
	}
	for _, conn := range connections {
		conn.Release()
	}
	coordinators := []*coordpostgres.AdmissionCoordinator{
		coordpostgres.NewAdmissionCoordinator(store, 0),
		coordpostgres.NewAdmissionCoordinator(store, 0),
	}
	start := make(chan struct{})
	type outcome struct {
		decision coordination.AdmissionDecision
		err      error
	}
	results := make(chan outcome, parallelism)
	var group sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		request := admissionRequest(f, uuid.New())
		coordinator := coordinators[i%len(coordinators)]
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			decision, err := coordinator.Acquire(ctx, request)
			results <- outcome{decision, err}
		}()
	}
	close(start)
	group.Wait()
	close(results)
	admitted := 0
	for result := range results {
		if result.err != nil || result.decision.Lease == nil {
			t.Errorf("burst admission reason=%s err=%v, want admitted", result.decision.Reason, result.err)
			continue
		}
		admitted++
	}
	var leases int
	if err := store.ConfigPool().QueryRow(ctx, "SELECT count(*) FROM request_leases WHERE expires_at>clock_timestamp()").Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if admitted != parallelism || leases != parallelism {
		t.Fatalf("admitted=%d durable leases=%d, want %d", admitted, leases, parallelism)
	}
	// Even after a successful burst the global limit must still be enforced.
	decision, err := coordinators[1].Acquire(ctx, admissionRequest(f, uuid.New()))
	if err != nil || decision.Lease != nil || decision.Reason != coordination.ReasonConcurrencyExhausted {
		t.Fatalf("overflow decision=%+v err=%v", decision, err)
	}
	if err := coordinators[0].RefreshInflight(ctx); err != nil {
		t.Fatal(err)
	}
	if got := coordinators[0].PoolInflight(f.pool.ID); got != parallelism {
		t.Fatalf("pool inflight=%d, want %d", got, parallelism)
	}
}
