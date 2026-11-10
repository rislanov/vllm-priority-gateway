package local

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

func TestTokenReservationPublicationAndReplayEviction(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	request := func(client, pool, amount int64) coordination.AdmissionRequest {
		return coordination.AdmissionRequest{
			LeaseID: uuid.New(), OperationStartedAt: now, RequestID: uuid.NewString(), ReplicaID: uuid.New(),
			ClientID: client, PoolID: pool, EffectiveClientLimit: 10, ConfiguredClientLimit: 10, LeaseTTL: time.Minute,
			TokenReservation: coordination.TokenReservation{
				Enabled: true, InputEstimate: amount - 1, OutputAllowance: 1, PromptCount: 1, Samples: 1, ReservedTokens: amount,
				ClientBudget: 800, PoolBudget: 1000, PoolPolicyRevision: 1, EstimatorKind: "upstream", EstimationProfile: "test",
			},
		}
	}
	acquire := func(t *testing.T, c *AdmissionCoordinator, req coordination.AdmissionRequest, reason coordination.Reason) coordination.AdmissionDecision {
		t.Helper()
		d, err := c.Acquire(ctx, req)
		if err != nil || d.Reason != reason || d.Admitted() != (reason == "") {
			t.Fatalf("acquire = %+v, %v; want %q", d, err, reason)
		}
		return d
	}
	complete := func(t *testing.T, c *AdmissionCoordinator, lease *coordination.LeaseIdentity) {
		t.Helper()
		results, err := c.Complete(ctx, []coordination.LeaseCompletion{{Lease: *lease}})
		if err != nil || len(results) != 1 || results[0].Result != coordination.CompletionReleased {
			t.Fatalf("complete = %+v, %v", results, err)
		}
	}
	t.Run("policy updates leave existing accounting intact", func(t *testing.T) {
		c := newAdmissionCoordinator(func() time.Time { return now }, 20)
		first := acquire(t, c, request(1, 1, 600), "")
		reduced := request(2, 1, 100)
		reduced.TokenReservation.PoolBudget = 500
		reduced.TokenReservation.PoolPolicyRevision++
		acquire(t, c, reduced, coordination.ReasonTokenBudgetExhausted)
		disabled := request(2, 1, 100)
		disabled.TokenReservation = coordination.TokenReservation{}
		legacy := acquire(t, c, disabled, "")
		acquire(t, c, request(1, 2, 300), coordination.ReasonTokenBudgetExhausted)
		acquire(t, c, request(3, 1, 100), coordination.ReasonTokenPolicyTransition)
		complete(t, c, legacy.Lease)
		acquire(t, c, request(3, 1, 100), "")
		complete(t, c, first.Lease)
		acquire(t, c, request(1, 2, 300), "")
	})
	t.Run("future receipt cannot set a future cutoff", func(t *testing.T) {
		c := newAdmissionCoordinator(func() time.Time { return now }, 1)
		future := request(1, 1, 600)
		future.OperationStartedAt = now.Add(time.Minute)
		complete(t, c, acquire(t, c, future, "").Lease)
		legacy := request(1, 1, 2)
		legacy.TokenReservation = coordination.TokenReservation{}
		acquire(t, c, legacy, coordination.ReasonCoordinationUnavailable)
		acquire(t, c, request(1, 1, 2), coordination.ReasonCoordinationUnavailable)
		if c.Status().Available || !c.tokenReplayCutoff.IsZero() {
			t.Fatal("protected receipt advanced the cutoff or reported false availability")
		}
		now = future.OperationStartedAt.Add(time.Second)
		acquire(t, c, request(1, 1, 600), "")
		acquire(t, c, future, coordination.ReasonStaleOperation)
	})
	t.Run("live receipt replay precedes cutoff", func(t *testing.T) {
		c := newAdmissionCoordinator(func() time.Time { return now }, 2)
		old := request(1, 1, 400)
		acquire(t, c, old, "")
		now = now.Add(time.Second)
		terminal := request(2, 1, 200)
		complete(t, c, acquire(t, c, terminal, "").Lease)
		now = now.Add(time.Second)
		acquire(t, c, request(3, 1, 200), "")
		acquire(t, c, old, "")
		acquire(t, c, terminal, coordination.ReasonStaleOperation)
	})
	t.Run("budget denial preserves RPM and rate denials preserve tokens", func(t *testing.T) {
		c := newAdmissionCoordinator(func() time.Time { return now }, 20)
		first := acquire(t, c, request(1, 1, 600), "")
		blocked := request(2, 1, 500)
		blocked.RequestsPerMinute = 1
		acquire(t, c, blocked, coordination.ReasonTokenBudgetExhausted)
		complete(t, c, first.Lease)
		blocked.LeaseID = uuid.New()
		second := acquire(t, c, blocked, "")
		limited := request(2, 1, 50)
		limited.RequestsPerMinute = 1
		acquire(t, c, limited, coordination.ReasonRPMExhausted)
		concurrency := request(2, 1, 50)
		concurrency.EffectiveClientLimit = 1
		acquire(t, c, concurrency, coordination.ReasonConcurrencyExhausted)
		if got := c.PoolOutstandingTokens(1); got != 500 {
			t.Fatalf("rejections affected outstanding: %d", got)
		}
		complete(t, c, second.Lease)
	})
}
