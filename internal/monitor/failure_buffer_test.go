package monitor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/observability"
)

type blockingReplayCoordinator struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
}

func (*blockingReplayCoordinator) Reconcile(context.Context, []coordination.BackendIdentity) error {
	return nil
}
func (*blockingReplayCoordinator) Snapshot(int64, time.Time) coordination.CircuitSnapshot {
	return coordination.CircuitSnapshot{}
}
func (*blockingReplayCoordinator) Acquire(context.Context, coordination.CircuitAcquireRequest) (coordination.CircuitDecision, error) {
	return coordination.CircuitDecision{}, nil
}
func (c *blockingReplayCoordinator) Complete(context.Context, coordination.CircuitCompletion) (coordination.CircuitSnapshot, error) {
	c.startedOnce.Do(func() { close(c.started) })
	<-c.release
	return coordination.CircuitSnapshot{}, errors.New("still unavailable")
}
func (*blockingReplayCoordinator) RenewProbes(context.Context, []coordination.ProbeIdentity) ([]coordination.RenewResult, error) {
	return nil, nil
}
func (*blockingReplayCoordinator) Refresh(context.Context) error { return nil }
func (*blockingReplayCoordinator) Status() coordination.Status   { return coordination.Status{} }

func TestCircuitFailureBufferIsBoundedToNewestEvents(t *testing.T) {
	manager := &Manager{}
	var last uuid.UUID
	for index := 0; index < 4100; index++ {
		last = uuid.New()
		manager.bufferCircuitFailure(coordination.CircuitCompletion{AttemptID: last})
	}
	if len(manager.circuitBacklog) != 4096 || manager.circuitBacklog[len(manager.circuitBacklog)-1].AttemptID != last {
		t.Fatalf("backlog length=%d last=%v", len(manager.circuitBacklog), manager.circuitBacklog[len(manager.circuitBacklog)-1].AttemptID)
	}
}

func TestCircuitReplayPreservesFailureBufferedConcurrently(t *testing.T) {
	coordinator := &blockingReplayCoordinator{started: make(chan struct{}), release: make(chan struct{})}
	old := coordination.CircuitCompletion{AttemptID: uuid.New(), ReportedOutcomeAt: time.Now().UTC()}
	newer := coordination.CircuitCompletion{AttemptID: uuid.New(), ReportedOutcomeAt: time.Now().UTC()}
	manager := &Manager{options: Options{CircuitCoordinator: coordinator}, circuitBacklog: []coordination.CircuitCompletion{old}}
	done := make(chan error, 1)
	go func() { done <- manager.ReplayCircuitFailures(context.Background()) }()
	<-coordinator.started
	manager.bufferCircuitFailure(newer)
	close(coordinator.release)
	if err := <-done; err == nil {
		t.Fatal("replay unexpectedly succeeded")
	}
	if len(manager.circuitBacklog) != 2 || manager.circuitBacklog[0].AttemptID != old.AttemptID || manager.circuitBacklog[1].AttemptID != newer.AttemptID {
		t.Fatalf("backlog=%+v", manager.circuitBacklog)
	}
}

func TestRecoveryCannotBypassActiveCircuitFailureReplay(t *testing.T) {
	coordinator := &blockingReplayCoordinator{started: make(chan struct{}), release: make(chan struct{})}
	manager := NewManager(context.Background(), Options{
		CircuitCoordinator: coordinator, MetricsInterval: time.Hour, ProbeRenewInterval: time.Hour,
	})
	t.Cleanup(manager.Shutdown)
	manager.bufferCircuitFailure(coordination.CircuitCompletion{
		AttemptID: uuid.New(), ReportedOutcomeAt: time.Now().UTC(), Outcome: domain.InferenceFailure,
	})
	observerDone := make(chan struct{})
	go func() {
		manager.observePools(time.Now())
		close(observerDone)
	}()
	<-coordinator.started

	noop := func(context.Context) error { return nil }
	recovery := coordination.NewRecoveryManager(coordination.RecoverySteps{
		Ping: noop, CheckFingerprint: noop, ReloadConfiguration: noop,
		ReconcileLeases: noop, ReplayFailures: manager.ReplayCircuitFailures,
		RefreshCircuits: coordinator.Refresh,
	}, time.Now)
	recovery.MarkDegraded(errors.New("database outage"))
	recovered := make(chan error, 1)
	go func() { recovered <- recovery.Recover(context.Background()) }()
	var premature error
	bypassed := false
	select {
	case err := <-recovered:
		premature = err
		bypassed = true
	case <-time.After(100 * time.Millisecond):
	}

	close(coordinator.release)
	<-observerDone
	if bypassed {
		t.Fatalf("recovery bypassed active replay: %v", premature)
	}
	if err := <-recovered; err == nil {
		t.Fatal("recovery unexpectedly succeeded after replay failure")
	}
	if status := recovery.Status(); status.State != coordination.RecoveryDegraded {
		t.Fatalf("recovery state=%s, want %s", status.State, coordination.RecoveryDegraded)
	}
	if backlog := manager.CircuitReplayBacklog(); backlog != 1 {
		t.Fatalf("replay backlog=%d, want 1", backlog)
	}
}

type renewalObserverCoordinator struct {
	blockingReplayCoordinator
	results []coordination.RenewResult
	err     error
	renewed chan []coordination.ProbeIdentity
}

func (c *renewalObserverCoordinator) RenewProbes(_ context.Context, probes []coordination.ProbeIdentity) ([]coordination.RenewResult, error) {
	if c.renewed != nil {
		c.renewed <- append([]coordination.ProbeIdentity(nil), probes...)
	}
	return c.results, c.err
}

func TestManagerRenewsProbesOnLeaseScheduleIndependentOfMetrics(t *testing.T) {
	ticks := make(chan time.Time, 1)
	coordinator := &renewalObserverCoordinator{renewed: make(chan []coordination.ProbeIdentity, 1)}
	manager := NewManager(context.Background(), Options{
		CircuitCoordinator: coordinator,
		MetricsInterval:    time.Hour,
		ProbeTTL:           90 * time.Second,
		ProbeRenewInterval: 30 * time.Second,
		ProbeRenewTicks:    ticks,
	})
	t.Cleanup(manager.Shutdown)
	probe := coordination.ProbeIdentity{PermitID: uuid.New(), TTL: 90 * time.Second}
	manager.mu.Lock()
	manager.probes[probe.PermitID] = probe
	manager.mu.Unlock()

	ticks <- time.Now()
	select {
	case renewed := <-coordinator.renewed:
		if len(renewed) != 1 || renewed[0].PermitID != probe.PermitID {
			t.Fatalf("renewed probes = %+v", renewed)
		}
	case <-time.After(time.Second):
		t.Fatal("probe renewal remained coupled to the metrics interval")
	}
}

func TestManagerPublishesReplayDropsAndProbeRenewalOutcomes(t *testing.T) {
	metrics := observability.NewMetrics()
	manager := &Manager{options: Options{Observer: metrics}}
	for index := 0; index < 4097; index++ {
		manager.bufferCircuitFailure(coordination.CircuitCompletion{AttemptID: uuid.New(), ReportedOutcomeAt: time.Now().UTC()})
	}
	manager.circuitBacklog = []coordination.CircuitCompletion{{AttemptID: uuid.New(), ReportedOutcomeAt: time.Now().Add(-24 * time.Hour)}}
	if err := manager.ReplayCircuitFailures(context.Background()); err != nil {
		t.Fatal(err)
	}

	probe := coordination.ProbeIdentity{PermitID: uuid.New()}
	failingRenewal := &renewalObserverCoordinator{err: errors.New("database unavailable")}
	manager = &Manager{ctx: context.Background(), options: Options{CircuitCoordinator: failingRenewal, Observer: metrics}, probes: map[uuid.UUID]coordination.ProbeIdentity{probe.PermitID: probe}}
	manager.renewProbes()
	lostRenewal := &renewalObserverCoordinator{results: []coordination.RenewResult{{Lease: coordination.LeaseIdentity{LeaseID: probe.PermitID}, Reason: coordination.ReasonLeaseLost}}}
	manager = &Manager{ctx: context.Background(), options: Options{CircuitCoordinator: lostRenewal, Observer: metrics}, probes: map[uuid.UUID]coordination.ProbeIdentity{probe.PermitID: probe}}
	manager.renewProbes()

	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	text := response.Body.String()
	for _, sample := range []string{
		`llmgw_coordination_circuit_replay_dropped_total{reason="overflow"} 1`,
		`llmgw_coordination_circuit_replay_dropped_total{reason="expired"} 1`,
		`llmgw_coordination_renew_failures_total 1`,
		`llmgw_coordination_lost_leases_total 1`,
	} {
		if !strings.Contains(text, sample) {
			t.Fatalf("coordination metrics missing %q:\n%s", sample, text)
		}
	}
}
