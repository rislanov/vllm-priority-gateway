package local_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
)

func request(id uuid.UUID, at time.Time) coordination.AdmissionRequest {
	return coordination.AdmissionRequest{LeaseID: id, OperationStartedAt: at, RequestID: "req", ReplicaID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), ConfigurationRevision: 3, APIKeyID: 5, ClientID: 7, PoolID: 11, ClientPolicyRevision: 2, EffectiveClientLimit: 1, ConfiguredClientLimit: 2, PoolGatewayInflightLimit: 1, RequestsPerMinute: 60, TokensPerMinute: 100, LeaseTTL: time.Minute}
}

func TestAcquireIsAtomicAndIdempotent(t *testing.T) {
	now := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	c := local.NewAdmissionCoordinator(func() time.Time { return now })
	first := request(uuid.New(), now)
	decision, err := c.Acquire(context.Background(), first)
	if err != nil || !decision.Admitted() {
		t.Fatalf("first = %+v, %v", decision, err)
	}
	replay, err := c.Acquire(context.Background(), first)
	if err != nil || replay.Lease == nil || replay.Lease.LeaseID != first.LeaseID {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	second := request(uuid.New(), now)
	second.RequestID = "req-2"
	decision, err = c.Acquire(context.Background(), second)
	if err != nil || decision.Reason != coordination.ReasonConcurrencyExhausted {
		t.Fatalf("second = %+v, %v", decision, err)
	}
}

func TestAcquireDetectsFingerprintConflict(t *testing.T) {
	now := time.Now().UTC()
	c := local.NewAdmissionCoordinator(func() time.Time { return now })
	r := request(uuid.New(), now)
	r.LeaseTTL = 2 * time.Minute
	if d, _ := c.Acquire(context.Background(), r); !d.Admitted() {
		t.Fatalf("first = %+v", d)
	}
	r.PoolID++
	d, err := c.Acquire(context.Background(), r)
	if err != nil || d.Reason != coordination.ReasonIdempotencyConflict {
		t.Fatalf("conflict = %+v, %v", d, err)
	}
}

func TestCompletionDebitsSoftTPMOnceAfterRefillAndReleasesLease(t *testing.T) {
	now := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	c := local.NewAdmissionCoordinator(func() time.Time { return now })
	r := request(uuid.New(), now)
	r.LeaseTTL = 2 * time.Minute
	d, _ := c.Acquire(context.Background(), r)
	now = now.Add(time.Minute)
	results, err := c.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: d.Lease.Identity(), Usage: &coordination.TokenUsage{InputTokens: 150, OutputTokens: 50}}})
	if err != nil || len(results) != 1 || results[0].Result != coordination.CompletionReleased {
		t.Fatalf("complete = %+v, %v", results, err)
	}
	again, _ := c.Complete(context.Background(), []coordination.LeaseCompletion{{Lease: d.Lease.Identity(), Usage: &coordination.TokenUsage{InputTokens: 150, OutputTokens: 50}}})
	if again[0].Result != coordination.CompletionAlreadyCompleted || again[0].OriginalResult != coordination.CompletionReleased {
		t.Fatalf("duplicate = %+v", again)
	}
	r2 := request(uuid.New(), now)
	r2.RequestID = "next"
	r2.EffectiveClientLimit = 2
	d2, _ := c.Acquire(context.Background(), r2)
	if d2.Reason != coordination.ReasonTPMExhausted {
		t.Fatalf("post-debit acquire = %+v", d2)
	}
}

func TestRenewNeverResurrectsExpiredLease(t *testing.T) {
	now := time.Now().UTC()
	c := local.NewAdmissionCoordinator(func() time.Time { return now })
	r := request(uuid.New(), now)
	r.LeaseTTL = time.Second
	d, _ := c.Acquire(context.Background(), r)
	now = now.Add(time.Second)
	got, err := c.Renew(context.Background(), []coordination.LeaseIdentity{d.Lease.Identity()})
	if err != nil || got[0].Reason != coordination.ReasonLeaseLost {
		t.Fatalf("renew = %+v, %v", got, err)
	}
}

func TestUnlimitedPoolIsStillCountedWhenLimitBecomesPositive(t *testing.T) {
	now := time.Now().UTC()
	c := local.NewAdmissionCoordinator(func() time.Time { return now })
	r := request(uuid.New(), now)
	r.PoolGatewayInflightLimit = 0
	r.EffectiveClientLimit = 2
	if d, _ := c.Acquire(context.Background(), r); !d.Admitted() {
		t.Fatalf("unlimited = %+v", d)
	}
	r2 := request(uuid.New(), now)
	r2.RequestID = "second"
	r2.PoolGatewayInflightLimit = 1
	r2.EffectiveClientLimit = 2
	if d, _ := c.Acquire(context.Background(), r2); d.Reason != coordination.ReasonConcurrencyExhausted {
		t.Fatalf("transition = %+v", d)
	}
}

func TestRejectedReceiptRetainsFutureSkewUntilStrictCleanupBoundary(t *testing.T) {
	now := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	c := local.NewAdmissionCoordinator(func() time.Time { return now })
	request := request(uuid.New(), now.Add(4*time.Minute))
	request.EffectiveClientLimit = 0
	first, err := c.Acquire(context.Background(), request)
	if err != nil || first.Reason != coordination.ReasonConcurrencyExhausted {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	now = now.Add(24 * time.Hour)
	beforeBoundary, _ := c.Acquire(context.Background(), request)
	if beforeBoundary.Reason != coordination.ReasonConcurrencyExhausted {
		t.Fatalf("receipt removed early: %+v", beforeBoundary)
	}
	now = now.Add(4 * time.Minute)
	atBoundary, _ := c.Acquire(context.Background(), request)
	if atBoundary.Reason != coordination.ReasonStaleOperation {
		t.Fatalf("receipt survived strict cleanup boundary: %+v", atBoundary)
	}
}
