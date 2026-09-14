package local_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestCircuitCoordinatorDeduplicatesFailureCompletion(t *testing.T) {
	now := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	c, err := local.NewCircuitCoordinator(circuitbreaker.Options{FailureThreshold: 2, FailureWindow: time.Minute, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	b := coordination.BackendIdentity{ID: 1, Revision: 1, Enabled: true}
	if err := c.Reconcile(context.Background(), []coordination.BackendIdentity{b}); err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	a := coordination.CircuitAcquireRequest{AttemptID: id, AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: b, ProbeTTL: time.Minute}
	d, _ := c.Acquire(context.Background(), a)
	if d.Reason != "" {
		t.Fatalf("acquire=%+v", d)
	}
	completion := coordination.CircuitCompletion{AttemptID: id, Backend: b, Generation: d.Snapshot.Generation, Outcome: domain.InferenceFailure, ReportedOutcomeAt: now}
	if _, err := c.Complete(context.Background(), completion); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Complete(context.Background(), completion); err != nil {
		t.Fatal(err)
	}
	if got := c.Snapshot(1, now); got.FailureCount != 1 || got.State != domain.CircuitClosed {
		t.Fatalf("snapshot=%+v", got)
	}
}

func TestExpiredProbeReopensAndLateSuccessIsStale(t *testing.T) {
	now := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	c, _ := local.NewCircuitCoordinator(circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Hour, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1}, func() time.Time { return now })
	b := coordination.BackendIdentity{ID: 1, Revision: 1, Enabled: true}
	_ = c.Reconcile(context.Background(), []coordination.BackendIdentity{b})
	first := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: b, ProbeTTL: 10 * time.Second}
	d, _ := c.Acquire(context.Background(), first)
	_, _ = c.Complete(context.Background(), coordination.CircuitCompletion{AttemptID: first.AttemptID, Backend: b, Generation: d.Snapshot.Generation, Outcome: domain.InferenceFailure, ReportedOutcomeAt: now})
	now = now.Add(time.Minute)
	probe := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: b, ProbeTTL: 10 * time.Second}
	p, _ := c.Acquire(context.Background(), probe)
	if p.Permit == nil {
		t.Fatalf("probe=%+v", p)
	}
	now = now.Add(11 * time.Second)
	snap := c.Snapshot(1, now)
	if snap.State != domain.CircuitOpen {
		t.Fatalf("expired snapshot=%+v", snap)
	}
	late, _ := c.Complete(context.Background(), coordination.CircuitCompletion{AttemptID: probe.AttemptID, Backend: b, Generation: p.Permit.Generation, Outcome: domain.InferenceSuccess, ReportedOutcomeAt: now})
	if late.State != domain.CircuitOpen {
		t.Fatalf("late completion healed circuit: %+v", late)
	}
}

func TestHalfOpenFailureWinsAfterPeerSuccess(t *testing.T) {
	now := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	c, _ := local.NewCircuitCoordinator(circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Hour, OpenCooldown: time.Minute, HalfOpenMaxProbes: 2}, func() time.Time { return now })
	backend := coordination.BackendIdentity{ID: 1, Revision: 1, Enabled: true}
	_ = c.Reconcile(context.Background(), []coordination.BackendIdentity{backend})
	initial := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute}
	decision, _ := c.Acquire(context.Background(), initial)
	_, _ = c.Complete(context.Background(), coordination.CircuitCompletion{AttemptID: initial.AttemptID, Backend: backend, Generation: decision.Snapshot.Generation, Outcome: domain.InferenceFailure, ReportedOutcomeAt: now})
	now = now.Add(time.Minute)
	first := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute}
	second := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute}
	firstDecision, _ := c.Acquire(context.Background(), first)
	secondDecision, _ := c.Acquire(context.Background(), second)
	if firstDecision.Permit == nil || secondDecision.Permit == nil {
		t.Fatalf("probe decisions = %+v %+v", firstDecision, secondDecision)
	}
	_, _ = c.Complete(context.Background(), coordination.CircuitCompletion{AttemptID: first.AttemptID, Backend: backend, Generation: firstDecision.Permit.Generation, Outcome: domain.InferenceSuccess, ReportedOutcomeAt: now})
	result, err := c.Complete(context.Background(), coordination.CircuitCompletion{AttemptID: second.AttemptID, Backend: backend, Generation: secondDecision.Permit.Generation, Outcome: domain.InferenceFailure, ReportedOutcomeAt: now.Add(time.Millisecond)})
	if err != nil || result.State != domain.CircuitOpen {
		t.Fatalf("failure did not win: snapshot=%+v err=%v", result, err)
	}
}

func TestBackendIdentityReplacementSupersedesProbeAndCannotBeHealed(t *testing.T) {
	now := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	c, _ := local.NewCircuitCoordinator(circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Hour, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1}, func() time.Time { return now })
	oldBackend := coordination.BackendIdentity{ID: 1, Revision: 1, Enabled: true}
	_ = c.Reconcile(context.Background(), []coordination.BackendIdentity{oldBackend})
	initial := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: oldBackend, ProbeTTL: time.Minute}
	decision, _ := c.Acquire(context.Background(), initial)
	_, _ = c.Complete(context.Background(), coordination.CircuitCompletion{AttemptID: initial.AttemptID, Backend: oldBackend, Generation: decision.Snapshot.Generation, Outcome: domain.InferenceFailure, ReportedOutcomeAt: now})
	now = now.Add(time.Minute)
	probe := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: oldBackend, ProbeTTL: time.Minute}
	probeDecision, _ := c.Acquire(context.Background(), probe)
	newBackend := coordination.BackendIdentity{ID: 1, Revision: 2, Enabled: true}
	if err := c.Reconcile(context.Background(), []coordination.BackendIdentity{newBackend}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := c.Complete(context.Background(), coordination.CircuitCompletion{AttemptID: probe.AttemptID, Backend: oldBackend, Generation: probeDecision.Permit.Generation, Outcome: domain.InferenceSuccess, ReportedOutcomeAt: now})
	if err != nil || snapshot.Backend != newBackend || snapshot.State != domain.CircuitClosed {
		t.Fatalf("superseded completion=%+v err=%v", snapshot, err)
	}
}

func TestProbeRenewalUsesOriginalFixedTTLAndReturnsNewExpiry(t *testing.T) {
	now := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	c, _ := local.NewCircuitCoordinator(circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Hour, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1}, func() time.Time { return now })
	backend := coordination.BackendIdentity{ID: 1, Revision: 1, Enabled: true}
	_ = c.Reconcile(context.Background(), []coordination.BackendIdentity{backend})
	initial := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: backend, ProbeTTL: 10 * time.Second}
	decision, _ := c.Acquire(context.Background(), initial)
	_, _ = c.Complete(context.Background(), coordination.CircuitCompletion{AttemptID: initial.AttemptID, Backend: backend, Generation: decision.Snapshot.Generation, Outcome: domain.InferenceFailure, ReportedOutcomeAt: now})
	now = now.Add(time.Minute)
	probeRequest := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: backend, ProbeTTL: 10 * time.Second}
	probe, _ := c.Acquire(context.Background(), probeRequest)
	now = now.Add(5 * time.Second)
	changedCallerCopy := *probe.Permit
	changedCallerCopy.TTL = time.Hour
	results, err := c.RenewProbes(context.Background(), []coordination.ProbeIdentity{changedCallerCopy})
	wantExpiry := now.Add(10 * time.Second)
	if err != nil || len(results) != 1 || results[0].Reason != "" || !results[0].Lease.ExpiresAt.Equal(wantExpiry) || results[0].Lease.TTL != 10*time.Second {
		t.Fatalf("renew=%+v err=%v wantExpiry=%v", results, err, wantExpiry)
	}
}

func TestCircuitCompletionRejectsUnboundedTimestampWithoutMutation(t *testing.T) {
	now := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	c, _ := local.NewCircuitCoordinator(circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Hour, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1}, func() time.Time { return now })
	backend := coordination.BackendIdentity{ID: 1, Revision: 1, Enabled: true}
	_ = c.Reconcile(context.Background(), []coordination.BackendIdentity{backend})
	request := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute}
	decision, _ := c.Acquire(context.Background(), request)
	_, err := c.Complete(context.Background(), coordination.CircuitCompletion{AttemptID: request.AttemptID, Backend: backend, Generation: decision.Snapshot.Generation, Outcome: domain.InferenceFailure, ReportedOutcomeAt: now.Add(6 * time.Minute)})
	var reason coordination.ReasonError
	if !errors.As(err, &reason) || reason.Reason != coordination.ReasonStaleOperation {
		t.Fatalf("completion error=%v", err)
	}
	if snapshot := c.Snapshot(backend.ID, now); snapshot.State != domain.CircuitClosed || snapshot.FailureCount != 0 {
		t.Fatalf("invalid completion mutated circuit: %+v", snapshot)
	}
}

func TestTerminalCircuitAttemptIsRetainedUntilStrictBoundary(t *testing.T) {
	now := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	c, _ := local.NewCircuitCoordinator(circuitbreaker.Options{FailureThreshold: 2, FailureWindow: time.Hour, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1}, func() time.Time { return now })
	backend := coordination.BackendIdentity{ID: 1, Revision: 1, Enabled: true}
	_ = c.Reconcile(context.Background(), []coordination.BackendIdentity{backend})
	request := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: now, ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute}
	decision, _ := c.Acquire(context.Background(), request)
	completion := coordination.CircuitCompletion{AttemptID: request.AttemptID, Backend: backend, Generation: decision.Snapshot.Generation, Outcome: domain.InferenceFailure, ReportedOutcomeAt: now}
	if _, err := c.Complete(context.Background(), completion); err != nil {
		t.Fatal(err)
	}
	now = now.Add(24*time.Hour - time.Nanosecond)
	if replay, _ := c.Acquire(context.Background(), request); replay.Reason != coordination.ReasonLeaseLost {
		t.Fatalf("terminal attempt removed early: %+v", replay)
	}
	now = now.Add(time.Nanosecond)
	if replay, _ := c.Acquire(context.Background(), request); replay.Reason != coordination.ReasonStaleOperation {
		t.Fatalf("terminal attempt survived boundary: %+v", replay)
	}
}

func TestRetainedDelayedCompletionReplayBypassesNewOperationAgeGate(t *testing.T) {
	started := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	now := started
	c, _ := local.NewCircuitCoordinator(circuitbreaker.Options{FailureThreshold: 2, FailureWindow: time.Hour, OpenCooldown: time.Minute, HalfOpenMaxProbes: 1}, func() time.Time { return now })
	backend := coordination.BackendIdentity{ID: 1, Revision: 1, Enabled: true}
	_ = c.Reconcile(context.Background(), []coordination.BackendIdentity{backend})
	request := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: started, ReplicaID: uuid.New(), Backend: backend, ProbeTTL: time.Minute}
	decision, _ := c.Acquire(context.Background(), request)
	completion := coordination.CircuitCompletion{AttemptID: request.AttemptID, Backend: backend, Generation: decision.Snapshot.Generation, Outcome: domain.InferenceFailure, ReportedOutcomeAt: started}
	now = started.Add(23 * time.Hour)
	if _, err := c.Complete(context.Background(), completion); err != nil {
		t.Fatal(err)
	}
	now = started.Add(25 * time.Hour)
	if _, err := c.Complete(context.Background(), completion); err != nil {
		t.Fatalf("retained duplicate was incorrectly treated as a new stale operation: %v", err)
	}
}
