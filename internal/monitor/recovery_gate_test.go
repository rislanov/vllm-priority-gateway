package monitor_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/monitor"
)

func TestRecoveryGateBlocksHalfOpenAndReportsUnavailable(t *testing.T) {
	circuit := &circuitCoordinatorStub{snapshot: coordination.CircuitSnapshot{State: domain.CircuitHalfOpen, Generation: 7, Available: true}}
	var ready atomic.Bool
	manager, backend := recoveryTestManager(t, circuit, ready.Load, nil)
	if got := manager.Snapshot(backend.ID, time.Now()); got.CircuitAvailable {
		t.Error("degraded half-open backend reported available")
	}
	if complete, ok := manager.AcquireBackend(backend, time.Now()); ok {
		complete(domain.InferenceNeutral)
		t.Error("probe admitted before recovery barrier")
	}
	circuit.mu.Lock()
	defer circuit.mu.Unlock()
	if len(circuit.acquisitions) != 0 {
		t.Errorf("degraded path called coordinator Acquire %d times", len(circuit.acquisitions))
	}
}

func TestRecoveryGateClosedEmergencyBuffersFailuresWithoutCoordinatorAcquire(t *testing.T) {
	circuit := &circuitCoordinatorStub{snapshot: coordination.CircuitSnapshot{State: domain.CircuitClosed, Generation: 7, Available: true}}
	manager, backend := recoveryTestManager(t, circuit, func() bool { return false }, nil)
	complete, ok := manager.AcquireBackend(backend, time.Now())
	if !ok {
		t.Fatal("closed emergency circuit rejected")
	}
	complete(domain.InferenceFailure)
	if got := manager.CircuitReplayBacklog(); got != 1 {
		t.Errorf("failure backlog=%d, want 1", got)
	}
	circuit.mu.Lock()
	defer circuit.mu.Unlock()
	if len(circuit.acquisitions) != 0 || len(circuit.completions) != 0 {
		t.Errorf("emergency used coordinator: acquisitions=%d completions=%d", len(circuit.acquisitions), len(circuit.completions))
	}
}

func TestRecoveryGateDoesNotHealPreviouslyAdmittedProbe(t *testing.T) {
	for _, outcome := range []domain.InferenceOutcome{domain.InferenceSuccess, domain.InferenceNeutral} {
		t.Run(string(outcome), func(t *testing.T) {
			circuit := &circuitCoordinatorStub{snapshot: coordination.CircuitSnapshot{State: domain.CircuitHalfOpen, Generation: 7, Available: true}}
			var ready atomic.Bool
			ready.Store(true)
			manager, backend := recoveryTestManager(t, circuit, ready.Load, nil)
			complete, ok := manager.AcquireBackend(backend, time.Now())
			if !ok {
				t.Fatal("healthy probe rejected")
			}
			ready.Store(false)
			complete(outcome)
			circuit.mu.Lock()
			defer circuit.mu.Unlock()
			if len(circuit.completions) != 0 {
				t.Errorf("terminal probe reported healing-capable outcome during outage: %+v", circuit.completions)
			}
			if got := manager.Snapshot(backend.ID, time.Now()).GatewayInflight; got != 0 {
				t.Errorf("inflight=%d", got)
			}
		})
	}
}

func TestRecoveryGateResetsEmergencyBreakerAfterSuccessfulRecovery(t *testing.T) {
	circuit := &circuitCoordinatorStub{snapshot: coordination.CircuitSnapshot{State: domain.CircuitClosed, Generation: 7, Available: true}}
	recovery := coordination.NewRecoveryManager(coordination.RecoverySteps{}, time.Now)
	recovery.MarkDegraded(errors.New("outage"))
	manager, backend := recoveryTestManager(t, circuit, func() bool { return recovery.Status().State == coordination.RecoveryReady }, recovery.Status)
	for i := 0; i < 2; i++ {
		complete, ok := manager.AcquireBackend(backend, time.Now())
		if !ok {
			t.Fatal("early rejection")
		}
		complete(domain.InferenceFailure)
	}
	if got := manager.Snapshot(backend.ID, time.Now()); got.CircuitAvailable {
		t.Error("local failure threshold did not block backend")
	}
	if err := manager.ReplayCircuitFailures(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	// No traffic occurs while ready. The next outage must still recognize the completed recovery.
	recovery.MarkDegraded(errors.New("next outage"))
	if got := manager.Snapshot(backend.ID, time.Now()); !got.CircuitAvailable {
		t.Error("prior outage's local breaker survives successful recovery")
	}
	complete, ok := manager.AcquireBackend(backend, time.Now())
	if !ok {
		t.Fatal("recovered closed backend rejected during next outage")
	}
	complete(domain.InferenceNeutral)
}

func recoveryTestManager(t *testing.T, circuit coordination.CircuitCoordinator, ready func() bool, recoveryStatus func() coordination.RecoveryStatus, buffered ...func()) (*monitor.Manager, domain.Backend) {
	t.Helper()
	options := monitorOptions(nil)
	options.MetricsInterval = time.Hour
	options.CircuitCoordinator = circuit
	options.CoordinationReady = ready
	options.RecoveryStatus = recoveryStatus
	if len(buffered) > 0 {
		options.CircuitFailureBuffered = buffered[0]
	}
	options.ReplicaID = uuid.New()
	options.ProbeTTL = time.Minute
	manager := monitor.NewManager(context.Background(), options)
	t.Cleanup(manager.Shutdown)
	backend := testBackend("http://127.0.0.1:1", 1, 9)
	if err := manager.Reconcile([]domain.Backend{backend}); err != nil {
		t.Fatal(err)
	}
	return manager, backend
}
func TestRecoveryGateRetainsPreviouslyAdmittedFailure(t *testing.T) {
	circuit := &circuitCoordinatorStub{snapshot: coordination.CircuitSnapshot{State: domain.CircuitHalfOpen, Generation: 7, Available: true}, completeErr: errors.New("database unavailable")}
	var ready atomic.Bool
	ready.Store(true)
	manager, backend := recoveryTestManager(t, circuit, ready.Load, nil)
	complete, ok := manager.AcquireBackend(backend, time.Now())
	if !ok {
		t.Fatal("probe rejected")
	}
	ready.Store(false)
	complete(domain.InferenceFailure)
	if got := manager.CircuitReplayBacklog(); got != 1 {
		t.Errorf("failure backlog=%d", got)
	}
	circuit.mu.Lock()
	defer circuit.mu.Unlock()
	if len(circuit.completions) != 1 || circuit.completions[0].Outcome != domain.InferenceFailure || circuit.completions[0].Generation != 7 {
		t.Errorf("failure lost or mutated: %+v", circuit.completions)
	}
}

func TestRecoveryGateOldEmergencyCompletionCannotTripRecoveredBreaker(t *testing.T) {
	circuit := &circuitCoordinatorStub{snapshot: coordination.CircuitSnapshot{State: domain.CircuitClosed, Generation: 7, Available: true}}
	recovery := coordination.NewRecoveryManager(coordination.RecoverySteps{}, time.Now)
	recovery.MarkDegraded(errors.New("first outage"))
	manager, backend := recoveryTestManager(t, circuit, func() bool { return recovery.Status().State == coordination.RecoveryReady }, recovery.Status)
	first, ok := manager.AcquireBackend(backend, time.Now())
	if !ok {
		t.Fatal("first rejected")
	}
	second, ok := manager.AcquireBackend(backend, time.Now())
	if !ok {
		t.Fatal("second rejected")
	}
	if err := recovery.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovery.MarkDegraded(errors.New("second outage"))
	current, ok := manager.AcquireBackend(backend, time.Now())
	if !ok {
		t.Fatal("new epoch rejected")
	}
	first(domain.InferenceFailure)
	second(domain.InferenceFailure)
	current(domain.InferenceNeutral)
	if !manager.Snapshot(backend.ID, time.Now()).CircuitAvailable {
		t.Fatal("old completions opened recovered emergency breaker")
	}
	if got := manager.CircuitReplayBacklog(); got != 2 {
		t.Errorf("old failures were not retained: backlog=%d", got)
	}
}

func TestRecoveryGatePermanentFaultDisablesEmergency(t *testing.T) {
	circuit := &circuitCoordinatorStub{snapshot: coordination.CircuitSnapshot{State: domain.CircuitClosed, Generation: 7, Available: true}}
	recovery := coordination.NewRecoveryManager(coordination.RecoverySteps{}, time.Now)
	recovery.MarkPermanent(errors.New("revision regressed"))
	manager, backend := recoveryTestManager(t, circuit, func() bool { return false }, recovery.Status)
	if manager.Snapshot(backend.ID, time.Now()).CircuitAvailable {
		t.Error("permanent fault reported available")
	}
	if complete, ok := manager.AcquireBackend(backend, time.Now()); ok {
		complete(domain.InferenceNeutral)
		t.Error("permanent fault admitted emergency")
	}
}

func TestRecoveryGateRejectsFailureBufferedAfterReplayBeforeReady(t *testing.T) {
	circuit := &circuitCoordinatorStub{snapshot: coordination.CircuitSnapshot{State: domain.CircuitClosed, Generation: 7, Available: true}}
	var manager *monitor.Manager
	var lateFailure func(domain.InferenceOutcome)
	var failDuringRefresh atomic.Bool
	failDuringRefresh.Store(true)
	recovery := coordination.NewRecoveryManager(coordination.RecoverySteps{
		ReplayFailures: func(ctx context.Context) error { return manager.ReplayCircuitFailures(ctx) },
		RefreshCircuits: func(context.Context) error {
			if failDuringRefresh.CompareAndSwap(true, false) {
				lateFailure(domain.InferenceFailure)
			}
			return nil
		},
	}, time.Now)
	recovery.MarkDegraded(errors.New("database outage"))
	var backend domain.Backend
	manager, backend = recoveryTestManager(t, circuit,
		func() bool { return recovery.Status().State == coordination.RecoveryReady }, recovery.Status,
		func() { recovery.MarkDegraded(errors.New("circuit failure awaits replay")) })
	first, ok := manager.AcquireBackend(backend, time.Now())
	if !ok {
		t.Fatal("first emergency request rejected")
	}
	lateFailure, ok = manager.AcquireBackend(backend, time.Now())
	if !ok {
		t.Fatal("second emergency request rejected")
	}
	first(domain.InferenceFailure)
	if err := recovery.Recover(context.Background()); err == nil {
		t.Error("recovery accepted a failure buffered after its replay step")
	}
	if recovery.Status().State != coordination.RecoveryDegraded || manager.CircuitReplayBacklog() != 1 {
		t.Fatalf("recovery=%+v backlog=%d; must remain degraded with late event queued", recovery.Status(), manager.CircuitReplayBacklog())
	}
	if complete, ok := manager.AcquireBackend(backend, time.Now()); ok {
		complete(domain.InferenceNeutral)
		t.Fatal("ready transition bypassed local open breaker")
	}
	if err := recovery.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if recovery.Status().State != coordination.RecoveryReady || manager.CircuitReplayBacklog() != 0 {
		t.Fatal("next recovery did not replay late failure before readiness")
	}
	circuit.mu.Lock()
	defer circuit.mu.Unlock()
	if len(circuit.completions) != 2 || circuit.completions[0].AttemptID == circuit.completions[1].AttemptID {
		t.Fatalf("replayed events=%+v", circuit.completions)
	}
}
