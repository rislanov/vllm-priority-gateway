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

func TestPostgresTokenizationDowngradeWaitsForConcurrentAdmission(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sql, e := os.ReadFile("../../internal/store/postgres/migrations/000006_tokenization.down.sql")
	if e != nil {
		t.Fatal(e)
	}
	holder, e := s.CoordinationPool().Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer holder.Rollback(ctx)
	id := uuid.New()
	if _, e = holder.Exec(ctx, "INSERT INTO tokenization_operations VALUES($1,$2,'{}',false,clock_timestamp()+interval '24 hours')", id, make([]byte, 32)); e != nil {
		t.Fatal(e)
	}
	if _, e = holder.Exec(ctx, "INSERT INTO tokenization_leases VALUES($1,1,1,clock_timestamp()+interval '1 minute')", id); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() {
		tx, e := s.CoordinationPool().Begin(ctx)
		if e != nil {
			done <- e
			return
		}
		defer tx.Rollback(context.Background())
		_, e = tx.Exec(ctx, string(sql))
		done <- e
	}()
	for {
		var waiting bool
		e = s.CoordinationPool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE NOT granted AND relation IN ('tokenization_operations'::regclass,'tokenization_leases'::regclass))").Scan(&waiting)
		if e != nil {
			t.Fatal(e)
		}
		if waiting {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(time.Millisecond)
	}
	if e = holder.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = <-done; e == nil {
		t.Fatal("down migration dropped a concurrently committed active lease")
	}
	if _, e = s.CoordinationPool().Exec(ctx, "UPDATE tokenization_leases SET expires_at=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	downgrade, e := s.CoordinationPool().Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer downgrade.Rollback(context.Background())
	if _, e = downgrade.Exec(ctx, string(sql)); e != nil {
		t.Fatalf("drained downgrade failed: %v", e)
	}
	if e = downgrade.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
}

func TestPostgresTokenizationStaleBackendAndExpiryCleanup(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, s, 0)
	ctx := context.Background()
	p := coordination.DefaultTokenizationPolicy()
	p.ClientMaxInflight = 1
	c := coordpostgres.NewTokenizationCoordinator(s, p, 2*time.Second)
	stale := utilityRequest(f)
	stale.Candidates[0].BackendRevision++
	if d, e := c.Acquire(ctx, stale); e != nil || d.Reason != coordination.TokenizationStaleConfiguration {
		t.Fatalf("stale backend should be unavailable, not capacity rejection: %+v %v", d, e)
	}
	d, e := c.Acquire(ctx, utilityRequest(f))
	if e != nil || d.Lease == nil {
		t.Fatal(d, e)
	}
	// Deterministically simulate a lost replica whose lease has expired, before
	// running maintenance. Expired leases must not consume admission capacity.
	if _, e = s.CoordinationPool().Exec(ctx, "UPDATE tokenization_leases SET expires_at=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	if d, e = c.Acquire(ctx, utilityRequest(f)); e != nil || d.Lease == nil {
		t.Fatalf("crashed replica capacity not recovered: %+v %v", d, e)
	}
	if e = c.Complete(ctx, *d.Lease); e != nil {
		t.Fatal(e)
	}
	if _, e = s.CoordinationPool().Exec(ctx, "UPDATE tokenization_operations SET retain_until=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	more, e := c.CleanupBatch(ctx, 1)
	if e != nil || !more {
		t.Fatalf("bounded cleanup did not signal backlog: %v %v", more, e)
	}
	for more && e == nil {
		more, e = c.CleanupBatch(ctx, 1)
	}
	if e != nil {
		t.Fatal(e)
	}
	var n int
	if e = s.CoordinationPool().QueryRow(ctx, "SELECT count(*) FROM tokenization_operations").Scan(&n); e != nil || n != 0 {
		t.Fatalf("receipt backlog remains: %d %v", n, e)
	}
}

func TestPostgresTokenizationPolicyMismatch(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	p := coordpostgres.ReplicaPolicy{LeaseTTL: time.Minute, LeaseRenewInterval: 10 * time.Second, Tokenization: coordination.DefaultTokenizationPolicy()}
	first := coordpostgres.NewReplicaManager(s, uuid.New(), p, time.Second)
	if e := first.Start(context.Background()); e != nil {
		t.Fatal(e)
	}
	defer first.Close()
	p.Tokenization.ClientRPM++
	second := coordpostgres.NewReplicaManager(s, uuid.New(), p, time.Second)
	defer second.Close()
	if e := second.Start(context.Background()); e == nil {
		t.Fatal("mismatched utility budgets accepted")
	}
}

func TestPostgresTokenizationConcurrentCleanupAndAdmission(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, s, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := coordination.DefaultTokenizationPolicy()
	p.ClientRPM = 100000
	c := coordpostgres.NewTokenizationCoordinator(s, p, time.Second)
	// Retained terminal receipts and expired leases coexist with active work.
	for i := 0; i < 4; i++ {
		d, e := c.Acquire(ctx, utilityRequest(f))
		if e != nil || d.Lease == nil {
			t.Fatal(d, e)
		}
		if e = c.Complete(ctx, *d.Lease); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := s.CoordinationPool().Exec(ctx, "UPDATE tokenization_operations SET retain_until=clock_timestamp()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	cleanupDone := make(chan error, 1)
	go func() {
		for i := 0; i < 20; i++ {
			if _, e := c.CleanupBatch(ctx, 1); e != nil {
				cleanupDone <- e
				return
			}
		}
		cleanupDone <- nil
	}()
	for i := 0; i < 20; i++ {
		d, e := c.Acquire(ctx, utilityRequest(f))
		if e != nil || d.Lease == nil {
			t.Fatal(d, e)
		}
		if e = c.Complete(ctx, *d.Lease); e != nil {
			t.Fatal(e)
		}
	}
	if e := <-cleanupDone; e != nil {
		t.Fatal(e)
	}
	var leases int
	if e := s.CoordinationPool().QueryRow(ctx, "SELECT count(*) FROM tokenization_leases").Scan(&leases); e != nil || leases != 0 {
		t.Fatalf("cleanup/release leaked leases: %d %v", leases, e)
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
