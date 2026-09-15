package coordination_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

type partialRenewCoordinator struct{ managerCoordinator }

func (*partialRenewCoordinator) Renew(_ context.Context, leases []coordination.LeaseIdentity) ([]coordination.RenewResult, error) {
	return []coordination.RenewResult{{Lease: leases[0], Reason: coordination.ReasonLeaseLost}}, errors.New("later transaction failed")
}

func TestLeaseManagerAppliesCommittedRenewalsBeforeLaterBatchError(t *testing.T) {
	observer := &leaseObserverStub{}
	manager := coordination.NewLeaseManager(context.Background(), &partialRenewCoordinator{}, coordination.LeaseManagerOptions{
		RenewTicks: make(chan time.Time), Observer: observer,
	})
	defer manager.Close()
	manager.Track(coordination.LeaseIdentity{LeaseID: uuid.New(), TTL: time.Minute})
	manager.Track(coordination.LeaseIdentity{LeaseID: uuid.New(), TTL: time.Minute})
	if err := manager.Reconcile(context.Background()); err == nil {
		t.Fatal("reconciliation ignored failed later batch")
	}
	if manager.ActiveCount() != 1 || observer.lost.Load() != 1 || observer.renewFailures.Load() != 1 {
		t.Fatalf("active=%d lost=%d failures=%d", manager.ActiveCount(), observer.lost.Load(), observer.renewFailures.Load())
	}
}
