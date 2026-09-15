package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
)

func TestPostgresAdmissionRetainsConcurrencyScopeOnRejectedReplay(t *testing.T) {
	for _, test := range []struct {
		name      string
		poolLimit int
		scope     coordination.AdmissionScope
	}{
		{"pool", 1, coordination.AdmissionPoolScope},
		{"client", 2, coordination.AdmissionClientScope},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
			fixture := updateFixtureLimits(t, store, createFixture(t, store, 0), 1, test.poolLimit)
			coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
			ctx := context.Background()
			first, err := coordinator.Acquire(ctx, admissionRequest(fixture, uuid.New()))
			if err != nil || !first.Admitted() {
				t.Fatalf("first=%+v err=%v", first, err)
			}
			request := admissionRequest(fixture, uuid.New())
			rejected, err := coordinator.Acquire(ctx, request)
			if err != nil || rejected.Reason != coordination.ReasonConcurrencyExhausted || rejected.ConcurrencyScope != test.scope {
				t.Fatalf("rejected=%+v err=%v", rejected, err)
			}
			if _, err := coordinator.Complete(ctx, []coordination.LeaseCompletion{{Lease: first.Lease.Identity()}}); err != nil {
				t.Fatal(err)
			}
			replay, err := coordinator.Acquire(ctx, request)
			if err != nil || replay.Reason != rejected.Reason || replay.ConcurrencyScope != rejected.ConcurrencyScope {
				t.Fatalf("replay after capacity release=%+v err=%v", replay, err)
			}
		})
	}
}
