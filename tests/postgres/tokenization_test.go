package postgres_test

import (
	"context"
	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/contracttest"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPostgresTokenizationContract(t *testing.T) {
	contracttest.Tokenization(t, func(t *testing.T, p coordination.TokenizationPolicy) (coordination.TokenizationCoordinator, coordination.TokenizationAcquireRequest) {
		s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
		f := createFixture(t, s, 0)
		r := admissionRequest(f, uuid.New())
		return coordpostgres.NewTokenizationCoordinator(s, p, 2*time.Second), coordination.TokenizationAcquireRequest{OperationID: r.LeaseID, ReplicaID: r.ReplicaID, RequestID: r.RequestID, OperationStartedAt: time.Now().UTC(), ConfigurationRevision: r.ConfigurationRevision, APIKeyID: r.APIKeyID, ClientID: r.ClientID, PoolID: r.PoolID, PoolRevision: f.pool.Revision, Candidates: []coordination.TokenizationCandidate{{BackendID: f.backend.ID, BackendRevision: f.backend.Revision}}}
	})
}
func TestPostgresTokenizationSharedBudget(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, s, 0)
	r := admissionRequest(f, uuid.New())
	p := coordination.TokenizationPolicy{ClientMaxInflight: 2, BackendMaxInflight: 1, ClientRPM: 120, Timeout: time.Second}
	cs := []coordination.TokenizationCoordinator{coordpostgres.NewTokenizationCoordinator(s, p, 5*time.Second), coordpostgres.NewTokenizationCoordinator(s, p, 5*time.Second)}
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d, e := cs[i%2].Acquire(context.Background(), coordination.TokenizationAcquireRequest{OperationID: uuid.New(), ReplicaID: r.ReplicaID, RequestID: "shared", OperationStartedAt: time.Now().UTC(), ConfigurationRevision: r.ConfigurationRevision, APIKeyID: r.APIKeyID, ClientID: r.ClientID, PoolID: r.PoolID, PoolRevision: f.pool.Revision, Candidates: []coordination.TokenizationCandidate{{BackendID: f.backend.ID, BackendRevision: f.backend.Revision}}})
			if e != nil {
				t.Error(e)
			}
			if d.Lease != nil {
				admitted.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if admitted.Load() != 1 {
		t.Fatalf("global backend limit admitted %d", admitted.Load())
	}
	var count int
	if e := s.CoordinationPool().QueryRow(context.Background(), "SELECT count(*) FROM request_leases").Scan(&count); e != nil || count != 0 {
		t.Fatalf("inference side effect: %d %v", count, e)
	}
}
