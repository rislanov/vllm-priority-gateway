package monitor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

type blockingReplayCoordinator struct {
	started chan struct{}
	release chan struct{}
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
	close(c.started)
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
