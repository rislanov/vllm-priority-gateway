package coordination_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/observability"
)

type leaseObserverStub struct {
	renewFailures atomic.Int32
	lost          atomic.Int32
	dropped       atomic.Int32
}

func (o *leaseObserverStub) CoordinationRenewFailure()      { o.renewFailures.Add(1) }
func (o *leaseObserverStub) CoordinationLeaseLost()         { o.lost.Add(1) }
func (o *leaseObserverStub) CoordinationCompletionDropped() { o.dropped.Add(1) }

type failingRenewCoordinator struct{ managerCoordinator }

func (*failingRenewCoordinator) Renew(context.Context, []coordination.LeaseIdentity) ([]coordination.RenewResult, error) {
	return nil, errors.New("temporary renewal failure")
}

type lostRenewCoordinator struct{ managerCoordinator }

func (*lostRenewCoordinator) Renew(_ context.Context, leases []coordination.LeaseIdentity) ([]coordination.RenewResult, error) {
	results := make([]coordination.RenewResult, len(leases))
	for index, lease := range leases {
		results[index] = coordination.RenewResult{Lease: lease, Reason: coordination.ReasonLeaseLost}
	}
	return results, nil
}

func TestLeaseManagerObservesRenewalFailureAndLostLease(t *testing.T) {
	for _, test := range []struct {
		name        string
		coordinator coordination.AdmissionCoordinator
		wantFailure int32
		wantLost    int32
	}{
		{name: "renewal failure", coordinator: &failingRenewCoordinator{}, wantFailure: 1},
		{name: "lost lease", coordinator: &lostRenewCoordinator{}, wantLost: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ticks := make(chan time.Time, 1)
			observer := &leaseObserverStub{}
			manager := coordination.NewLeaseManager(context.Background(), test.coordinator, coordination.LeaseManagerOptions{RenewTicks: ticks, CompletionBacklog: 2, Observer: observer})
			defer manager.Close()
			manager.Track(coordination.LeaseIdentity{LeaseID: uuid.New(), TTL: time.Minute})
			ticks <- time.Now()
			deadline := time.Now().Add(time.Second)
			for observer.renewFailures.Load() != test.wantFailure || observer.lost.Load() != test.wantLost {
				if time.Now().After(deadline) {
					t.Fatalf("renewFailures=%d lost=%d", observer.renewFailures.Load(), observer.lost.Load())
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

type retryCompletionCoordinator struct {
	managerCoordinator
	attempts int
}

func (f *retryCompletionCoordinator) Complete(_ context.Context, v []coordination.LeaseCompletion) ([]coordination.CompleteResult, error) {
	f.attempts++
	if f.attempts == 1 {
		return nil, errors.New("temporary")
	}
	f.complete <- v
	return make([]coordination.CompleteResult, len(v)), nil
}

func TestLeaseManagerRetriesFailedCompletionWithoutBlockingRenewals(t *testing.T) {
	f := &retryCompletionCoordinator{managerCoordinator: managerCoordinator{renew: make(chan []coordination.LeaseIdentity, 1), complete: make(chan []coordination.LeaseCompletion, 1)}}
	ticks := make(chan time.Time, 1)
	m := coordination.NewLeaseManager(context.Background(), f, coordination.LeaseManagerOptions{RenewTicks: ticks, CompletionBacklog: 4})
	defer m.Close()
	h := m.Track(coordination.LeaseIdentity{LeaseID: uuid.New(), TTL: time.Minute})
	if !h.Complete(nil) {
		t.Fatal("completion was not queued")
	}
	ticks <- time.Now()
	select {
	case <-f.renew:
	case <-time.After(time.Second):
		t.Fatal("renewal blocked behind failed completion")
	}
	select {
	case <-f.complete:
	case <-time.After(2 * time.Second):
		t.Fatal("failed completion was not retried")
	}
}

type managerCoordinator struct {
	mu       sync.Mutex
	renew    chan []coordination.LeaseIdentity
	complete chan []coordination.LeaseCompletion
}

func (f *managerCoordinator) Acquire(context.Context, coordination.AdmissionRequest) (coordination.AdmissionDecision, error) {
	return coordination.AdmissionDecision{}, nil
}
func (f *managerCoordinator) Renew(_ context.Context, v []coordination.LeaseIdentity) ([]coordination.RenewResult, error) {
	f.renew <- v
	out := make([]coordination.RenewResult, len(v))
	for i := range v {
		out[i].Lease = v[i]
	}
	return out, nil
}
func (f *managerCoordinator) Complete(_ context.Context, v []coordination.LeaseCompletion) ([]coordination.CompleteResult, error) {
	f.complete <- v
	return make([]coordination.CompleteResult, len(v)), nil
}
func (f *managerCoordinator) Status() coordination.Status {
	return coordination.Status{Available: true}
}

func TestLeaseManagerBatchesRenewalAndCompletionWithoutPerLeaseWorkers(t *testing.T) {
	f := &managerCoordinator{renew: make(chan []coordination.LeaseIdentity, 1), complete: make(chan []coordination.LeaseCompletion, 1)}
	ticks := make(chan time.Time, 1)
	m := coordination.NewLeaseManager(context.Background(), f, coordination.LeaseManagerOptions{RenewTicks: ticks, CompletionBacklog: 4})
	defer m.Close()
	l1 := coordination.LeaseIdentity{LeaseID: uuid.New(), ExpiresAt: time.Now().Add(time.Minute)}
	l2 := coordination.LeaseIdentity{LeaseID: uuid.New(), ExpiresAt: time.Now().Add(time.Minute)}
	h1 := m.Track(l1)
	_ = m.Track(l2)
	ticks <- time.Now()
	select {
	case batch := <-f.renew:
		if len(batch) != 2 {
			t.Fatalf("renew batch=%d", len(batch))
		}
	case <-time.After(time.Second):
		t.Fatal("renew timeout")
	}
	if !h1.Complete(&coordination.TokenUsage{InputTokens: 3, OutputTokens: 2}) {
		t.Fatal("first completion rejected")
	}
	if h1.Complete(nil) {
		t.Fatal("duplicate completion accepted")
	}
	select {
	case batch := <-f.complete:
		if len(batch) != 1 || batch[0].Lease.LeaseID != l1.LeaseID {
			t.Fatalf("completion=%+v", batch)
		}
	case <-time.After(time.Second):
		t.Fatal("completion timeout")
	}
}

func TestLeaseManagerBacklogOverflowFallsBackToTTL(t *testing.T) {
	blocked := &blockingCoordinator{started: make(chan struct{})}
	m := coordination.NewLeaseManager(context.Background(), blocked, coordination.LeaseManagerOptions{CompletionBacklog: 1})
	defer m.Close()
	h1 := m.Track(coordination.LeaseIdentity{LeaseID: uuid.New()})
	h2 := m.Track(coordination.LeaseIdentity{LeaseID: uuid.New()})
	h3 := m.Track(coordination.LeaseIdentity{LeaseID: uuid.New()})
	h1.Complete(nil)
	<-blocked.started
	h2.Complete(nil)
	if h3.Complete(nil) {
		t.Fatal("overflow completion accepted")
	}
	if m.DroppedCompletions() != 1 {
		t.Fatalf("dropped=%d", m.DroppedCompletions())
	}
}

func TestLeaseManagerPublishesDroppedCompletionMetric(t *testing.T) {
	metrics := observability.NewMetrics()
	blocked := &blockingCoordinator{started: make(chan struct{})}
	manager := coordination.NewLeaseManager(context.Background(), blocked, coordination.LeaseManagerOptions{
		CompletionBacklog: 1, Observer: metrics,
	})
	defer manager.Close()
	first := manager.Track(coordination.LeaseIdentity{LeaseID: uuid.New()})
	second := manager.Track(coordination.LeaseIdentity{LeaseID: uuid.New()})
	third := manager.Track(coordination.LeaseIdentity{LeaseID: uuid.New()})
	first.Complete(nil)
	<-blocked.started
	second.Complete(nil)
	if third.Complete(nil) {
		t.Fatal("overflow completion accepted")
	}
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if sample := "llmgw_coordination_dropped_completions_total 1"; !strings.Contains(response.Body.String(), sample) {
		t.Fatalf("metrics missing %q:\n%s", sample, response.Body.String())
	}
}

type failingCompletionCoordinator struct {
	managerCoordinator
	called chan struct{}
}

func (c *failingCompletionCoordinator) Complete(context.Context, []coordination.LeaseCompletion) ([]coordination.CompleteResult, error) {
	select {
	case c.called <- struct{}{}:
	default:
	}
	return nil, errors.New("temporary")
}

func TestLeaseManagerPublishesRetryBacklogDrops(t *testing.T) {
	observer := &leaseObserverStub{}
	coordinator := &failingCompletionCoordinator{called: make(chan struct{}, 2)}
	manager := coordination.NewLeaseManager(context.Background(), coordinator, coordination.LeaseManagerOptions{
		CompletionBacklog: 1,
		Observer:          observer,
	})
	defer manager.Close()
	first := manager.Track(coordination.LeaseIdentity{LeaseID: uuid.New()})
	second := manager.Track(coordination.LeaseIdentity{LeaseID: uuid.New()})
	if !first.Complete(nil) {
		t.Fatal("first completion was not queued")
	}
	<-coordinator.called
	deadline := time.Now().Add(time.Second)
	for manager.PendingCompletions() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("first failed completion was not moved to the retry backlog")
		}
		time.Sleep(time.Millisecond)
	}
	if !second.Complete(nil) {
		t.Fatal("second completion was not accepted by the direct queue")
	}
	for observer.dropped.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("retry backlog drops observed = %d, want 1", observer.dropped.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

type shutdownRetryCoordinator struct {
	managerCoordinator
	firstStarted chan struct{}
	drained      chan []coordination.LeaseCompletion
	calls        atomic.Int32
}

func (c *shutdownRetryCoordinator) Complete(ctx context.Context, items []coordination.LeaseCompletion) ([]coordination.CompleteResult, error) {
	if c.calls.Add(1) == 1 {
		close(c.firstStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	c.drained <- append([]coordination.LeaseCompletion(nil), items...)
	return make([]coordination.CompleteResult, len(items)), nil
}

func TestLeaseManagerShutdownRetriesExtractedCompletionBatch(t *testing.T) {
	coordinator := &shutdownRetryCoordinator{
		firstStarted: make(chan struct{}),
		drained:      make(chan []coordination.LeaseCompletion, 1),
	}
	manager := coordination.NewLeaseManager(context.Background(), coordinator, coordination.LeaseManagerOptions{
		CompletionBacklog: 2,
		ShutdownTimeout:   time.Second,
	})
	lease := coordination.LeaseIdentity{LeaseID: uuid.New()}
	if !manager.Track(lease).Complete(nil) {
		t.Fatal("completion was not queued")
	}
	<-coordinator.firstStarted
	if err := manager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case batch := <-coordinator.drained:
		if len(batch) != 1 || batch[0].Lease.LeaseID != lease.LeaseID {
			t.Fatalf("shutdown batch = %+v", batch)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not retry the extracted completion batch")
	}
}

func TestLeaseManagerShutdownReportsPermanentlyUndeliveredCompletions(t *testing.T) {
	observer := &leaseObserverStub{}
	manager := coordination.NewLeaseManager(context.Background(), &failingCompletionCoordinator{called: make(chan struct{}, 2)}, coordination.LeaseManagerOptions{
		CompletionBacklog: 1,
		ShutdownTimeout:   25 * time.Millisecond,
		Observer:          observer,
	})
	if !manager.Track(coordination.LeaseIdentity{LeaseID: uuid.New()}).Complete(nil) {
		t.Fatal("completion was not queued")
	}
	if err := manager.Close(); err == nil {
		t.Fatal("Close() hid an undelivered completion")
	}
	if manager.DroppedCompletions() != 1 || observer.dropped.Load() != 1 || manager.ActiveCount() != 0 {
		t.Fatalf("dropped=%d observed=%d active=%d", manager.DroppedCompletions(), observer.dropped.Load(), manager.ActiveCount())
	}
}

type blockingCoordinator struct{ started chan struct{} }

func (b *blockingCoordinator) Acquire(context.Context, coordination.AdmissionRequest) (coordination.AdmissionDecision, error) {
	return coordination.AdmissionDecision{}, nil
}
func (b *blockingCoordinator) Renew(context.Context, []coordination.LeaseIdentity) ([]coordination.RenewResult, error) {
	return nil, nil
}
func (b *blockingCoordinator) Complete(ctx context.Context, _ []coordination.LeaseCompletion) ([]coordination.CompleteResult, error) {
	select {
	case <-b.started:
	default:
		close(b.started)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (b *blockingCoordinator) Status() coordination.Status {
	return coordination.Status{Available: true}
}
