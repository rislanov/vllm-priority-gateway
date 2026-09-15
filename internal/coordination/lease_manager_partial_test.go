package coordination

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type partialCompletionCoordinator struct {
	calls [][]LeaseCompletion
}

type partialCompletionObserver struct {
	lost int
}

func (*partialCompletionObserver) CoordinationRenewFailure() {}

func (o *partialCompletionObserver) CoordinationLeaseLost() {
	o.lost++
}

func (*partialCompletionCoordinator) Acquire(context.Context, AdmissionRequest) (AdmissionDecision, error) {
	return AdmissionDecision{}, nil
}

func (*partialCompletionCoordinator) Renew(context.Context, []LeaseIdentity) ([]RenewResult, error) {
	return nil, nil
}

func (c *partialCompletionCoordinator) Complete(_ context.Context, items []LeaseCompletion) ([]CompleteResult, error) {
	c.calls = append(c.calls, append([]LeaseCompletion(nil), items...))
	results := make([]CompleteResult, len(items))
	for i, item := range items {
		results[i] = CompleteResult{LeaseID: item.Lease.LeaseID, Result: CompletionReleased}
	}
	if len(c.calls) == 1 {
		return results[:2], errors.New("timeout after committed prefix")
	}
	return results, nil
}

func (*partialCompletionCoordinator) Status() Status {
	return Status{Available: true}
}

func TestLeaseManagerRetriesOnlyUncommittedCompletionSuffix(t *testing.T) {
	coordinator := &partialCompletionCoordinator{}
	leases := []LeaseIdentity{
		{LeaseID: uuid.New()},
		{LeaseID: uuid.New()},
		{LeaseID: uuid.New()},
	}
	manager := &LeaseManager{
		ctx:         context.Background(),
		coordinator: coordinator,
		completions: make(chan LeaseCompletion, len(leases)-1),
		backlogCap:  len(leases),
		active:      make(map[[16]byte]LeaseIdentity, len(leases)),
	}
	for _, lease := range leases {
		manager.active[lease.LeaseID] = lease
	}
	manager.completions <- LeaseCompletion{Lease: leases[1]}
	manager.completions <- LeaseCompletion{Lease: leases[2]}

	manager.complete(LeaseCompletion{Lease: leases[0]})

	if got := manager.ActiveCount(); got != 1 {
		t.Fatalf("active leases after committed prefix = %d, want 1", got)
	}
	if len(manager.pending) != 1 || len(manager.pending[0].items) != 1 || manager.pending[0].items[0].Lease.LeaseID != leases[2].LeaseID {
		t.Fatalf("retry backlog = %+v, want only uncommitted suffix", manager.pending)
	}

	manager.retryPending(time.Now().Add(time.Second))

	if got := manager.ActiveCount(); got != 0 {
		t.Fatalf("active leases after suffix retry = %d, want 0", got)
	}
	if len(coordinator.calls) != 2 || len(coordinator.calls[1]) != 1 || coordinator.calls[1][0].Lease.LeaseID != leases[2].LeaseID {
		t.Fatalf("completion calls = %+v, want second call to contain only uncommitted suffix", coordinator.calls)
	}
}

func TestLeaseManagerObservesLeaseLostFromAlreadyCompletedReplay(t *testing.T) {
	lease := LeaseIdentity{LeaseID: uuid.New()}
	observer := &partialCompletionObserver{}
	manager := &LeaseManager{
		observer: observer,
		active:   map[[16]byte]LeaseIdentity{lease.LeaseID: lease},
	}

	remaining := manager.remainingCompletions(
		[]LeaseCompletion{{Lease: lease}},
		[]CompleteResult{{
			LeaseID:        lease.LeaseID,
			Result:         CompletionAlreadyCompleted,
			OriginalResult: CompletionLeaseLost,
		}},
		nil,
	)

	if len(remaining) != 0 {
		t.Fatalf("remaining completions = %d, want 0", len(remaining))
	}
	if observer.lost != 1 {
		t.Fatalf("observed lost leases = %d, want 1 for replayed lease_lost", observer.lost)
	}
}
