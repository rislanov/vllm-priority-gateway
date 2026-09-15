package contracttest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

type Clock struct {
	mu  sync.Mutex
	now time.Time
}

func NewClock(at time.Time) *Clock   { return &Clock{now: at} }
func (c *Clock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *Clock) Add(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

type AdmissionFactory func(*Clock) coordination.AdmissionCoordinator

// AdmissionBaselineFactory builds a backend-specific coordinator and a valid
// request generator while the assertions remain independent of storage and
// clock implementation.
type AdmissionBaselineFactory func(*testing.T, int64) (coordination.AdmissionCoordinator, func() coordination.AdmissionRequest)

func AdmissionBaseline(t *testing.T, factory AdmissionBaselineFactory) {
	t.Helper()
	t.Run("lease replay concurrency and completion", func(t *testing.T) {
		coordinator, request := factory(t, 0)
		firstRequest := request()
		first, err := coordinator.Acquire(context.Background(), firstRequest)
		if err != nil || !first.Admitted() {
			t.Fatalf("first=%+v err=%v", first, err)
		}
		replay, err := coordinator.Acquire(context.Background(), firstRequest)
		if err != nil || replay.Lease == nil || replay.Lease.LeaseID != firstRequest.LeaseID {
			t.Fatalf("replay=%+v err=%v", replay, err)
		}
		if blocked, err := coordinator.Acquire(context.Background(), request()); err != nil || blocked.Reason != coordination.ReasonConcurrencyExhausted {
			t.Fatalf("blocked=%+v err=%v", blocked, err)
		}
		results, err := coordinator.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: first.Lease.Identity()}})
		if err != nil || len(results) != 1 || results[0].Result != coordination.CompletionReleased {
			t.Fatalf("complete=%+v err=%v", results, err)
		}
		results, err = coordinator.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: first.Lease.Identity()}})
		if err != nil || len(results) != 1 || results[0].Result != coordination.CompletionAlreadyCompleted {
			t.Fatalf("duplicate complete=%+v err=%v", results, err)
		}
	})
	t.Run("RPM debit", func(t *testing.T) {
		coordinator, request := factory(t, 1)
		first, err := coordinator.Acquire(context.Background(), request())
		if err != nil || !first.Admitted() {
			t.Fatalf("first=%+v err=%v", first, err)
		}
		if _, err := coordinator.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: first.Lease.Identity()}}); err != nil {
			t.Fatal(err)
		}
		if blocked, err := coordinator.Acquire(context.Background(), request()); err != nil || blocked.Reason != coordination.ReasonRPMExhausted {
			t.Fatalf("RPM=%+v err=%v", blocked, err)
		}
	})
}

func Admission(t *testing.T, factory AdmissionFactory) {
	t.Helper()
	t.Run("idempotency expiry and unlimited pool accounting", func(t *testing.T) {
		clock := NewClock(time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC))
		c := factory(clock)
		req := request(clock.Now(), 1, 0)
		first, err := c.Acquire(context.Background(), req)
		if err != nil || !first.Admitted() {
			t.Fatalf("first=%+v err=%v", first, err)
		}
		replay, err := c.Acquire(context.Background(), req)
		if err != nil || replay.Lease == nil || replay.Lease.LeaseID != req.LeaseID {
			t.Fatalf("replay=%+v err=%v", replay, err)
		}
		next := request(clock.Now(), 1, 0)
		if decision, _ := c.Acquire(context.Background(), next); decision.Reason != coordination.ReasonConcurrencyExhausted {
			t.Fatalf("client limit=%+v", decision)
		}
		clock.Add(31 * time.Second)
		if decision, _ := c.Acquire(context.Background(), req); decision.Reason != coordination.ReasonLeaseLost {
			t.Fatalf("expired replay=%+v", decision)
		}
		if decision, _ := c.Acquire(context.Background(), request(clock.Now(), 1, 1)); !decision.Admitted() {
			t.Fatalf("expired capacity was not reusable: %+v", decision)
		}
	})
	t.Run("RPM debit and completion idempotency", func(t *testing.T) {
		clock := NewClock(time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC))
		c := factory(clock)
		req := request(clock.Now(), 2, 0)
		req.RequestsPerMinute = 1
		first, _ := c.Acquire(context.Background(), req)
		if !first.Admitted() {
			t.Fatalf("first=%+v", first)
		}
		results, err := c.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: first.Lease.Identity()}})
		if err != nil || results[0].Result != coordination.CompletionReleased {
			t.Fatalf("complete=%+v err=%v", results, err)
		}
		results, err = c.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: first.Lease.Identity()}})
		if err != nil || results[0].Result != coordination.CompletionAlreadyCompleted {
			t.Fatalf("duplicate=%+v err=%v", results, err)
		}
		if decision, _ := c.Acquire(context.Background(), requestWithRPM(clock.Now(), 2, 1)); decision.Reason != coordination.ReasonRPMExhausted {
			t.Fatalf("RPM=%+v", decision)
		}
	})
}

func request(at time.Time, client int64, pool int64) coordination.AdmissionRequest {
	return coordination.AdmissionRequest{LeaseID: uuid.New(), OperationStartedAt: at, RequestID: uuid.NewString(), ReplicaID: uuid.New(), ConfigurationRevision: 1, APIKeyID: 1, ClientID: client, PoolID: pool, ClientPolicyRevision: 1, EffectiveClientLimit: 1, ConfiguredClientLimit: 1, LeaseTTL: 30 * time.Second}
}
func requestWithRPM(at time.Time, client, pool int64) coordination.AdmissionRequest {
	r := request(at, client, pool)
	r.RequestsPerMinute = 1
	return r
}

type CircuitFactory func(*Clock) coordination.CircuitCoordinator

type CircuitBaselineFactory func(*testing.T) (coordination.CircuitCoordinator, coordination.BackendIdentity)

func CircuitBaseline(t *testing.T, factory CircuitBaselineFactory) {
	t.Helper()
	coordinator, backend := factory(t)
	if err := coordinator.Reconcile(context.Background(), []coordination.BackendIdentity{backend}); err != nil {
		t.Fatal(err)
	}
	request := coordination.CircuitAcquireRequest{
		AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute,
	}
	decision, err := coordinator.Acquire(context.Background(), request)
	if err != nil || decision.Reason != "" {
		t.Fatalf("closed acquire=%+v err=%v", decision, err)
	}
	completion := coordination.CircuitCompletion{
		AttemptID: request.AttemptID, Backend: backend, Generation: decision.Snapshot.Generation,
		Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC(),
	}
	first, err := coordinator.Complete(context.Background(), completion)
	if err != nil || first.State != domain.CircuitOpen {
		t.Fatalf("first completion=%+v err=%v", first, err)
	}
	replay, err := coordinator.Complete(context.Background(), completion)
	if err != nil || replay.State != domain.CircuitOpen {
		t.Fatalf("completion replay=%+v err=%v", replay, err)
	}
	request.AttemptID = uuid.New()
	request.AcquisitionStartedAt = time.Now().UTC()
	blocked, err := coordinator.Acquire(context.Background(), request)
	if err != nil || blocked.Reason != coordination.ReasonCircuitOpen {
		t.Fatalf("open acquire=%+v err=%v", blocked, err)
	}
}

func Circuit(t *testing.T, factory CircuitFactory) {
	t.Helper()
	clock := NewClock(time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC))
	c := factory(clock)
	backend := coordination.BackendIdentity{ID: 1, Revision: 1, Enabled: true}
	if err := c.Reconcile(context.Background(), []coordination.BackendIdentity{backend}); err != nil {
		t.Fatal(err)
	}
	closed := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: clock.Now(), ReplicaID: uuid.New(), Backend: backend, ProbeTTL: 10 * time.Second}
	decision, err := c.Acquire(context.Background(), closed)
	if err != nil || decision.Reason != "" {
		t.Fatalf("closed=%+v err=%v", decision, err)
	}
	completion := coordination.CircuitCompletion{AttemptID: closed.AttemptID, Backend: backend, Generation: decision.Snapshot.Generation, Outcome: domain.InferenceFailure, ReportedOutcomeAt: clock.Now()}
	if _, err := c.Complete(context.Background(), completion); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Complete(context.Background(), completion); err != nil {
		t.Fatalf("duplicate completion: %v", err)
	}
	clock.Add(time.Minute)
	probe := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: clock.Now(), ReplicaID: uuid.New(), Backend: backend, ProbeTTL: 10 * time.Second}
	probeDecision, err := c.Acquire(context.Background(), probe)
	if err != nil || probeDecision.Permit == nil {
		t.Fatalf("probe=%+v err=%v", probeDecision, err)
	}
	replay, err := c.Acquire(context.Background(), probe)
	if err != nil || replay.Permit == nil || replay.Permit.PermitID != probe.AttemptID {
		t.Fatalf("probe replay=%+v err=%v", replay, err)
	}
	clock.Add(11 * time.Second)
	if snapshot := c.Snapshot(backend.ID, clock.Now()); snapshot.State != domain.CircuitOpen {
		t.Fatalf("expired probe=%+v", snapshot)
	}
	late := coordination.CircuitCompletion{AttemptID: probe.AttemptID, Backend: backend, Generation: probeDecision.Permit.Generation, Outcome: domain.InferenceSuccess, ReportedOutcomeAt: clock.Now()}
	if snapshot, err := c.Complete(context.Background(), late); err != nil || snapshot.State != domain.CircuitOpen {
		t.Fatalf("late completion=%+v err=%v", snapshot, err)
	}
}
