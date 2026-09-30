package postgres_test

import (
	"context"
	"crypto/sha256"
	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/contracttest"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	basestore "github.com/rislanov/vllm-priority-gateway/internal/store"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func priorityReserveFixtures(t *testing.T, s *pgstore.Store, limit, reserve int) map[domain.PriorityClass]fixture {
	t.Helper()
	ctx := context.Background()
	f := updateFixtureLimits(t, s, createFixture(t, s, 0), 100, limit)
	p, err := s.UpdatePool(ctx, f.pool.ID, basestore.UpdatePoolParams{PublicModelName: f.pool.PublicModelName, UpstreamModelName: f.pool.UpstreamModelName, Enabled: true, MaxGatewayInflight: limit, HighPriorityReserve: reserve})
	if err != nil {
		t.Fatal(err)
	}
	f.pool = p
	fs := map[domain.PriorityClass]fixture{domain.PriorityHigh: f}
	for _, class := range []domain.PriorityClass{domain.PriorityCritical, domain.PriorityNormal, domain.PriorityBackground} {
		client, err := s.CreateClient(ctx, basestore.CreateClientParams{Name: string(class), Enabled: true, PriorityClass: class, MaxConcurrency: 100, ModelPoolIDs: []int64{p.ID}})
		if err != nil {
			t.Fatal(err)
		}
		key, err := s.CreateAPIKey(ctx, basestore.CreateAPIKeyParams{ClientID: client.ID, Prefix: string(class), SecretHash: sha256.Sum256([]byte(class))})
		if err != nil {
			t.Fatal(err)
		}
		cf := f
		cf.client = client
		cf.key = key
		fs[class] = cf
	}
	for class, cf := range fs {
		fs[class] = refreshFixture(t, s, cf)
	}
	return fs
}
func priorityRequest(f fixture) coordination.AdmissionRequest {
	r := admissionRequest(f, uuid.New())
	r.PriorityClass = f.client.PriorityClass
	r.PoolHighPriorityReserve = f.pool.HighPriorityReserve
	return r
}
func TestPostgresPriorityReserveContract(t *testing.T) {
	contracttest.PriorityReserve(t, func(t *testing.T, limit, reserve int) (coordination.AdmissionCoordinator, func(domain.PriorityClass) coordination.AdmissionRequest) {
		s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
		fs := priorityReserveFixtures(t, s, limit, reserve)
		return coordpostgres.NewAdmissionCoordinator(s, 2*time.Second), func(class domain.PriorityClass) coordination.AdmissionRequest { return priorityRequest(fs[class]) }
	})
}
func TestPostgresPriorityReserveConcurrentReplicas(t *testing.T) {
	dsn := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	s := openTestStore(t, dsn)
	fs := priorityReserveFixtures(t, s, 20, 6)
	ctx := context.Background()
	other, err := pgstore.Open(ctx, pgstore.Options{DatabaseURL: dsn, MigrationURL: dsn, ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	cs := []*coordpostgres.AdmissionCoordinator{coordpostgres.NewAdmissionCoordinator(s, 5*time.Second), coordpostgres.NewAdmissionCoordinator(other, 5*time.Second)}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var admitted atomic.Int32
	for i := 0; i < 40; i++ {
		r := priorityRequest(fs[domain.PriorityNormal])
		if i%2 == 1 {
			r = priorityRequest(fs[domain.PriorityBackground])
		}
		c := cs[i%2]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d, e := c.Acquire(ctx, r)
			if e != nil {
				t.Error(e)
			}
			if d.Admitted() {
				admitted.Add(1)
			} else if d.Reason != coordination.ReasonPoolPriorityReserve {
				t.Error(d)
			}
		}()
	}
	close(start)
	wg.Wait()
	if admitted.Load() != 14 {
		t.Fatalf("low admitted=%d", admitted.Load())
	}
	for i := 0; i < 6; i++ {
		class := domain.PriorityHigh
		if i%2 == 1 {
			class = domain.PriorityCritical
		}
		d, e := cs[i%2].Acquire(ctx, priorityRequest(fs[class]))
		if e != nil || !d.Admitted() {
			t.Fatal(d, e)
		}
	}
	var total, lower, unique int
	if err := s.ConfigPool().QueryRow(ctx, "SELECT count(*),count(*) FILTER (WHERE priority_class IN ('normal','background')),count(DISTINCT lease_id) FROM request_leases WHERE expires_at>clock_timestamp()").Scan(&total, &lower, &unique); err != nil {
		t.Fatal(err)
	}
	if total != 20 || lower != 14 || unique != 20 {
		t.Fatalf("T=%d B=%d unique=%d", total, lower, unique)
	}
}
func TestPostgresPriorityReserveStalePolicyReplayAndExpiry(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fs := priorityReserveFixtures(t, s, 2, 1)
	ctx := context.Background()
	c := coordpostgres.NewAdmissionCoordinator(s, time.Second)
	r := priorityRequest(fs[domain.PriorityNormal])
	d, e := c.Acquire(ctx, r)
	if e != nil || !d.Admitted() {
		t.Fatal(d, e)
	}
	replay, e := c.Acquire(ctx, r)
	if e != nil || replay.Lease == nil || replay.Lease.LeaseID != r.LeaseID {
		t.Fatal(replay, e)
	}
	spoof := priorityRequest(fs[domain.PriorityNormal])
	spoof.PriorityClass = domain.PriorityHigh
	if got, e := c.Acquire(ctx, spoof); e != nil || got.Reason != coordination.ReasonStaleConfiguration {
		t.Fatal(got, e)
	}
	stale := priorityRequest(fs[domain.PriorityNormal])
	stale.PoolHighPriorityReserve = 0
	if got, e := c.Acquire(ctx, stale); e != nil || got.Reason != coordination.ReasonStaleConfiguration {
		t.Fatal(got, e)
	}
	blocked := priorityRequest(fs[domain.PriorityBackground])
	if got, e := c.Acquire(ctx, blocked); e != nil || got.Reason != coordination.ReasonPoolPriorityReserve {
		t.Fatal(got, e)
	}
	// A policy edit cannot reclassify an active lease or its replay.
	f := fs[domain.PriorityNormal]
	if _, err := s.UpdateClient(ctx, f.client.ID, basestore.UpdateClientParams{Name: f.client.Name, Enabled: true, PriorityClass: domain.PriorityHigh, MaxConcurrency: 100, ModelPoolIDs: []int64{f.pool.ID}}); err != nil {
		t.Fatal(err)
	}
	if got, e := c.Acquire(ctx, r); e != nil || !got.Admitted() {
		t.Fatal(got, e)
	}
	var class string
	if err := s.ConfigPool().QueryRow(ctx, "SELECT priority_class FROM request_leases WHERE lease_id=$1", r.LeaseID).Scan(&class); err != nil || class != "normal" {
		t.Fatal(class, err)
	}
	changed := r
	changed.PriorityClass = domain.PriorityHigh
	if got, e := c.Acquire(ctx, changed); e != nil || got.Reason != coordination.ReasonIdempotencyConflict {
		t.Fatal(got, e)
	}
	if _, err := s.ConfigPool().Exec(ctx, "UPDATE request_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE lease_id=$1", r.LeaseID); err != nil {
		t.Fatal(err)
	}
	if got, e := c.Acquire(ctx, r); e != nil || got.Reason != coordination.ReasonLeaseLost {
		t.Fatal(got, e)
	}
	if got, e := c.Acquire(ctx, blocked); e != nil || got.Reason != coordination.ReasonPoolPriorityReserve {
		t.Fatal("rejected replay changed", got, e)
	}
	next := priorityRequest(refreshFixture(t, s, fs[domain.PriorityBackground]))
	nd, e := c.Acquire(ctx, next)
	if e != nil || !nd.Admitted() {
		t.Fatal(nd, e)
	}
	c.Complete(ctx, []coordination.LeaseCompletion{{Lease: *d.Lease}})
	result, e := c.Complete(ctx, []coordination.LeaseCompletion{{Lease: *nd.Lease}})
	if e != nil || result[0].Result != coordination.CompletionReleased {
		t.Fatal(result, e)
	}
	result, e = c.Complete(ctx, []coordination.LeaseCompletion{{Lease: *nd.Lease}})
	if e != nil || result[0].Result != coordination.CompletionAlreadyCompleted {
		t.Fatal(result, e)
	}
}
func TestPostgresPriorityReserveConfigurationRoundTrip(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fs := priorityReserveFixtures(t, s, 20, 6)
	ctx := context.Background()
	f := fs[domain.PriorityNormal]
	snap, e := s.LoadSnapshot(ctx)
	if e != nil || len(snap.Pools) != 1 || snap.Pools[0].HighPriorityReserve != 6 {
		t.Fatal(snap, e)
	}
	for _, query := range []string{"UPDATE model_pools SET high_priority_reserve=-1", "UPDATE model_pools SET high_priority_reserve=21", "UPDATE model_pools SET max_gateway_inflight=0"} {
		if _, e := s.ConfigPool().Exec(ctx, query); e == nil {
			t.Fatal("invalid SQL accepted", query)
		}
	}
	if _, e := s.UpdatePool(ctx, f.pool.ID, basestore.UpdatePoolParams{PublicModelName: f.pool.PublicModelName, UpstreamModelName: f.pool.UpstreamModelName, MaxGatewayInflight: 20, HighPriorityReserve: 20}); e != nil {
		t.Fatal(e)
	}
}

func TestPostgresPriorityReserveEnablingCountsExistingLeases(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fs := priorityReserveFixtures(t, s, 2, 0)
	ctx := context.Background()
	c := coordpostgres.NewAdmissionCoordinator(s, time.Second)
	old := priorityRequest(fs[domain.PriorityNormal])
	first, e := c.Acquire(ctx, old)
	if e != nil || !first.Admitted() {
		t.Fatal(first, e)
	}
	f := fs[domain.PriorityNormal]
	if _, e := s.UpdatePool(ctx, f.pool.ID, basestore.UpdatePoolParams{PublicModelName: f.pool.PublicModelName, UpstreamModelName: f.pool.UpstreamModelName, Enabled: true, MaxGatewayInflight: 2, HighPriorityReserve: 1}); e != nil {
		t.Fatal(e)
	}
	normal := priorityRequest(refreshFixture(t, s, f))
	if got, e := c.Acquire(ctx, normal); e != nil || got.Reason != coordination.ReasonPoolPriorityReserve {
		t.Fatal(got, e)
	}
	high := priorityRequest(refreshFixture(t, s, fs[domain.PriorityHigh]))
	if got, e := c.Acquire(ctx, high); e != nil || !got.Admitted() {
		t.Fatal(got, e)
	}
	c.Complete(ctx, []coordination.LeaseCompletion{{Lease: *first.Lease}})
	if got, e := c.Acquire(ctx, priorityRequest(refreshFixture(t, s, f))); e != nil || !got.Admitted() {
		t.Fatal(got, e)
	}
}

func TestPostgresPriorityReserveContractCompatibility(t *testing.T) {
	s := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	ctx := context.Background()
	policy := coordpostgres.ReplicaPolicy{LeaseTTL: time.Minute, LeaseRenewInterval: 10 * time.Second, ContractVersion: 2}
	old := coordpostgres.NewReplicaManager(s, uuid.New(), policy, time.Second)
	if e := old.Start(ctx); e != nil {
		t.Fatal(e)
	}
	defer old.Close()
	policy.ContractVersion = 0
	current := coordpostgres.NewReplicaManager(s, uuid.New(), policy, time.Second)
	if e := current.Start(ctx); e == nil {
		current.Close()
		t.Fatal("version 3 accepted live version 2 fingerprint")
	}
}
