package local_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
)

func TestTokenReservationOwnerMismatch(t *testing.T) {
	for _, field := range []struct {
		name   string
		change func(*coordination.LeaseIdentity)
	}{
		{"client", func(lease *coordination.LeaseIdentity) { lease.ClientID++ }},
		{"pool", func(lease *coordination.LeaseIdentity) { lease.PoolID++ }},
		{"replica", func(lease *coordination.LeaseIdentity) { lease.ReplicaID = uuid.New() }},
		{"request", func(lease *coordination.LeaseIdentity) { lease.RequestID += "-other" }},
	} {
		t.Run(field.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 11, 10, 21, 0, 0, 0, time.UTC)
			coordinator := local.NewAdmissionCoordinator(func() time.Time { return now })
			req := request(uuid.New(), now)
			admitted, err := coordinator.Acquire(ctx, req)
			if err != nil || !admitted.Admitted() {
				t.Fatalf("acquire = %+v, %v", admitted, err)
			}
			owner := admitted.Lease.Identity()
			other := owner
			field.change(&other)
			now = now.Add(time.Second)
			renewed, err := coordinator.Renew(ctx, []coordination.LeaseIdentity{other})
			if err != nil || renewed[0].Reason != coordination.ReasonLeaseLost {
				t.Errorf("foreign renewal = %+v, %v", renewed, err)
			}
			replay, err := coordinator.Acquire(ctx, req)
			if err != nil || !replay.Admitted() || !replay.Lease.ExpiresAt.Equal(owner.ExpiresAt) {
				t.Errorf("foreign renewal affected owner lease: %+v, %v", replay, err)
			}
			result, err := coordinator.Complete(ctx, []coordination.LeaseCompletion{{Lease: other, Usage: &coordination.TokenUsage{InputTokens: 1000}}})
			if err != nil || result[0].Reason != coordination.ReasonLeaseLost {
				t.Errorf("foreign completion = %+v, %v", result, err)
			}
			result, err = coordinator.Complete(ctx, []coordination.LeaseCompletion{{Lease: owner}})
			if err != nil || result[0].Result != coordination.CompletionReleased {
				t.Errorf("owner completion = %+v, %v", result, err)
			}
			next, err := coordinator.Acquire(ctx, request(uuid.New(), now))
			if err != nil || !next.Admitted() {
				t.Errorf("foreign completion charged usage: %+v, %v", next, err)
			}
		})
	}
}
