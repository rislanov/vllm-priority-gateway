package local

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestAdmissionCoordinatorEvictsOldestTerminalReceiptAtCapacity(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	coordinator := newAdmissionCoordinator(func() time.Time { return now }, 2)
	for index := 0; index < 10; index++ {
		decision, err := coordinator.Acquire(context.Background(), localCapacityRequest(uuid.New(), now, index))
		if err != nil || !decision.Admitted() {
			t.Fatalf("Acquire(%d) = %+v, %v", index, decision, err)
		}
		if result, err := coordinator.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: *decision.Lease}}); err != nil || result[0].Result != coordination.CompletionReleased {
			t.Fatalf("Complete(%d) = %+v, %v", index, result, err)
		}
		if len(coordinator.receipts) > 2 || !coordinator.Status().Available {
			t.Fatalf("terminal receipt pressure at %d: receipts=%d status=%+v", index, len(coordinator.receipts), coordinator.Status())
		}
		if index == 1 && len(coordinator.receipts) != 2 {
			t.Fatalf("Status evicted a terminal receipt without admission pressure: receipts=%d", len(coordinator.receipts))
		}
	}
}

func TestAdmissionCoordinatorRejectStormDoesNotConsumeActiveCapacity(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	coordinator := newAdmissionCoordinator(func() time.Time { return now }, 2)
	for index := 0; index < 10; index++ {
		request := localCapacityRequest(uuid.New(), now, index)
		request.EffectiveClientLimit = 0
		decision, err := coordinator.Acquire(context.Background(), request)
		if err != nil || decision.Reason != coordination.ReasonConcurrencyExhausted {
			t.Fatalf("Acquire(%d) = %+v, %v", index, decision, err)
		}
	}
	if len(coordinator.receipts) > 2 || !coordinator.Status().Available {
		t.Fatalf("reject storm consumed capacity: receipts=%d status=%+v", len(coordinator.receipts), coordinator.Status())
	}
	decision, err := coordinator.Acquire(context.Background(), localCapacityRequest(uuid.New(), now, 20))
	if err != nil || !decision.Admitted() {
		t.Fatalf("legitimate Acquire after reject storm = %+v, %v", decision, err)
	}
}

func TestAdmissionCoordinatorFailsClosedWhenCapacityIsAllActive(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	coordinator := newAdmissionCoordinator(func() time.Time { return now }, 2)
	for index := 0; index < 2; index++ {
		decision, err := coordinator.Acquire(context.Background(), localCapacityRequest(uuid.New(), now, index))
		if err != nil || !decision.Admitted() {
			t.Fatalf("Acquire(%d) = %+v, %v", index, decision, err)
		}
	}
	overflow, err := coordinator.Acquire(context.Background(), localCapacityRequest(uuid.New(), now, 3))
	if err != nil || overflow.Reason != coordination.ReasonCoordinationUnavailable || coordinator.Status().Available {
		t.Fatalf("overflow = %+v err=%v status=%+v", overflow, err, coordinator.Status())
	}
}

func TestAdmissionCoordinatorExpiredLeasesBecomeEvictable(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	coordinator := newAdmissionCoordinator(func() time.Time { return now }, 2)
	for index := 0; index < 2; index++ {
		request := localCapacityRequest(uuid.New(), now, index)
		request.LeaseTTL = time.Second
		decision, err := coordinator.Acquire(context.Background(), request)
		if err != nil || !decision.Admitted() {
			t.Fatalf("Acquire(%d) = %+v, %v", index, decision, err)
		}
	}
	now = now.Add(2 * time.Second)
	accepted, err := coordinator.Acquire(context.Background(), localCapacityRequest(uuid.New(), now, 3))
	if err != nil || !accepted.Admitted() || !coordinator.Status().Available {
		t.Fatalf("Acquire after lease expiry = %+v err=%v status=%+v", accepted, err, coordinator.Status())
	}
}

func TestCircuitCoordinatorEvictsOldestTerminalAttemptAtCapacity(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	coordinator, backend := newCapacityCircuit(t, func() time.Time { return now }, 2)
	for index := 0; index < 10; index++ {
		id := uuid.New()
		decision, err := coordinator.Acquire(context.Background(), coordination.CircuitAcquireRequest{
			AttemptID: id, AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute,
		})
		if err != nil || decision.Reason != "" {
			t.Fatalf("Acquire(%d) = %+v, %v", index, decision, err)
		}
		if _, err := coordinator.Complete(context.Background(), coordination.CircuitCompletion{
			AttemptID: id, Backend: backend, Generation: decision.Snapshot.Generation, Outcome: domain.InferenceSuccess, ReportedOutcomeAt: now,
		}); err != nil {
			t.Fatal(err)
		}
		if len(coordinator.attempts) > 2 || !coordinator.Status().Available {
			t.Fatalf("terminal attempt pressure at %d: attempts=%d status=%+v", index, len(coordinator.attempts), coordinator.Status())
		}
		if index == 1 && len(coordinator.attempts) != 2 {
			t.Fatalf("Status evicted a terminal attempt without acquisition pressure: attempts=%d", len(coordinator.attempts))
		}
	}
}

func TestCircuitCoordinatorFailsClosedWhenCapacityIsAllActive(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	coordinator, backend := newCapacityCircuit(t, func() time.Time { return now }, 2)
	for index := 0; index < 2; index++ {
		decision, err := coordinator.Acquire(context.Background(), coordination.CircuitAcquireRequest{
			AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute,
		})
		if err != nil || decision.Reason != "" {
			t.Fatalf("Acquire(%d) = %+v, %v", index, decision, err)
		}
	}
	overflow, err := coordinator.Acquire(context.Background(), coordination.CircuitAcquireRequest{
		AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute,
	})
	if err != nil || overflow.Reason != coordination.ReasonCoordinationUnavailable || coordinator.Status().Available {
		t.Fatalf("overflow = %+v err=%v status=%+v", overflow, err, coordinator.Status())
	}
}

func newCapacityCircuit(t *testing.T, now func() time.Time, capacity int) (*CircuitCoordinator, coordination.BackendIdentity) {
	t.Helper()
	coordinator, err := newCircuitCoordinator(circuitbreaker.Options{
		FailureThreshold: 100, FailureWindow: time.Minute, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1,
	}, now, capacity)
	if err != nil {
		t.Fatal(err)
	}
	backend := coordination.BackendIdentity{ID: 1, Revision: 1, Enabled: true}
	if err := coordinator.Reconcile(context.Background(), []coordination.BackendIdentity{backend}); err != nil {
		t.Fatal(err)
	}
	return coordinator, backend
}

func localCapacityRequest(id uuid.UUID, at time.Time, index int) coordination.AdmissionRequest {
	return coordination.AdmissionRequest{
		LeaseID: id, RequestID: id.String(), OperationStartedAt: at, ReplicaID: uuid.New(),
		ClientID: 1, PoolID: 1, ClientPolicyRevision: 1, ConfiguredClientLimit: 10, EffectiveClientLimit: 10,
		LeaseTTL: time.Minute,
	}
}
