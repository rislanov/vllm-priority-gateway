package postgres_test

import (
	"context"
	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/contracttest"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	basestore "github.com/rislanov/vllm-priority-gateway/internal/store"
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

func utilityRequest(f fixture) coordination.TokenizationAcquireRequest {
	return coordination.TokenizationAcquireRequest{OperationID: uuid.New(), ReplicaID: uuid.New(), RequestID: "utility", OperationStartedAt: time.Now().UTC(), ConfigurationRevision: f.revision, APIKeyID: f.key.ID, ClientID: f.client.ID, PoolID: f.pool.ID, PoolRevision: f.pool.Revision, Candidates: []coordination.TokenizationCandidate{{BackendID: f.backend.ID, BackendRevision: f.backend.Revision}}}
}

func TestPostgresTokenizationLostCommitAndCompletionReplay(t *testing.T) {
	raw := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if raw == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	runtime, armed := commitLossProxy(t, raw)
	s := openTestStoreWithMigration(t, runtime, raw)
	f := createFixture(t, s, 0)
	c := coordpostgres.NewTokenizationCoordinator(s, coordination.DefaultTokenizationPolicy(), 2*time.Second)
	r := utilityRequest(f)
	armed.Store(true)
	d, e := c.Acquire(context.Background(), r)
	if armed.Load() || e != nil || d.Lease == nil {
		t.Fatalf("ambiguous admission: %+v %v", d, e)
	}
	var n int
	var balance float64
	if e = s.CoordinationPool().QueryRow(context.Background(), "SELECT count(*) FROM tokenization_leases").Scan(&n); e != nil {
		t.Fatal(e)
	}
	if e = s.CoordinationPool().QueryRow(context.Background(), "SELECT balance FROM tokenization_rate_state").Scan(&balance); e != nil {
		t.Fatal(e)
	}
	if n != 1 || balance != 9 {
		t.Fatalf("duplicate allocation/debit: %d %v", n, balance)
	}
	forged := *d.Lease
	forged.ClientID++
	if e = c.Complete(context.Background(), forged); e == nil {
		t.Fatal("forged completion succeeded")
	}
	armed.Store(true)
	if e = c.Complete(context.Background(), *d.Lease); e == nil || armed.Load() {
		t.Fatalf("completion response was not lost: %v", e)
	}
	if e = c.Complete(context.Background(), *d.Lease); e != nil {
		t.Fatal(e)
	}
	if replay, e := c.Acquire(context.Background(), r); e != nil || replay.Lease != nil {
		t.Fatalf("completed replay: %+v %v", replay, e)
	}
}

func TestPostgresTokenizationRevokedDuringContention(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, s, 0)
	ctx := context.Background()
	c := coordpostgres.NewTokenizationCoordinator(s, coordination.DefaultTokenizationPolicy(), 2*time.Second)
	if _, e := s.CoordinationPool().Exec(ctx, "INSERT INTO tokenization_client_scopes VALUES($1)", f.client.ID); e != nil {
		t.Fatal(e)
	}
	tx, e := s.CoordinationPool().Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT client_id FROM tokenization_client_scopes WHERE client_id=$1 FOR UPDATE", f.client.ID); e != nil {
		t.Fatal(e)
	}
	r := utilityRequest(f)
	result := make(chan coordination.TokenizationDecision, 1)
	errs := make(chan error, 1)
	go func() { d, e := c.Acquire(ctx, r); result <- d; errs <- e }()
	// The row lock orders revocation before the admission's authoritative reads.
	if e = s.RevokeAPIKey(ctx, f.key.ID); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if d := <-result; d.Lease != nil || d.Reason != coordination.TokenizationStaleConfiguration {
		t.Fatal(d)
	}
	if e = <-errs; e != nil {
		t.Fatal(e)
	}
}

func TestPostgresTokenizationClientBudgetAcrossKeysAndModels(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, s, 0)
	ctx := context.Background()
	p, e := s.CreatePool(ctx, basestore.CreatePoolParams{PublicModelName: "other", UpstreamModelName: "other", Enabled: true})
	if e != nil {
		t.Fatal(e)
	}
	b, e := s.CreateBackend(ctx, basestore.CreateBackendParams{ModelPoolID: p.ID, Name: "other", BaseURL: "http://127.0.0.1:1", Enabled: true, CapacityHint: 1, RunningSoftLimit: 1})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.UpdateClient(ctx, f.client.ID, basestore.UpdateClientParams{Name: f.client.Name, Enabled: true, PriorityClass: f.client.PriorityClass, MaxConcurrency: 1, ModelPoolIDs: []int64{f.pool.ID, p.ID}})
	if e != nil {
		t.Fatal(e)
	}
	k, e := s.CreateAPIKey(ctx, basestore.CreateAPIKeyParams{ClientID: f.client.ID, Prefix: "another", SecretHash: f.key.SecretHash})
	if e != nil {
		t.Fatal(e)
	}
	f = refreshFixture(t, s, f)
	policy := coordination.DefaultTokenizationPolicy()
	policy.ClientMaxInflight = 1
	c := coordpostgres.NewTokenizationCoordinator(s, policy, 2*time.Second)
	d, e := c.Acquire(ctx, utilityRequest(f))
	if e != nil || d.Lease == nil {
		t.Fatal(d, e)
	}
	r := utilityRequest(f)
	r.APIKeyID = k.ID
	r.PoolID = p.ID
	r.PoolRevision = p.Revision
	r.Candidates = []coordination.TokenizationCandidate{{BackendID: b.ID, BackendRevision: b.Revision}}
	if d, e = c.Acquire(ctx, r); e != nil || d.Reason != coordination.TokenizationClientLimit {
		t.Fatal(d, e)
	}
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
