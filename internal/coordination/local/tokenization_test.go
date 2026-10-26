package local

import (
	"context"
	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/contracttest"
	"testing"
	"time"
)

func tokenizationRequest(now time.Time) coordination.TokenizationAcquireRequest {
	return coordination.TokenizationAcquireRequest{OperationID: uuid.New(), ReplicaID: uuid.New(), OperationStartedAt: now, RequestID: "test", ClientID: 1, PoolID: 1, Candidates: []coordination.TokenizationCandidate{{BackendID: 1, BackendRevision: 1}, {BackendID: 2, BackendRevision: 1}}}
}
func TestTokenizationContract(t *testing.T) {
	contracttest.Tokenization(t, func(t *testing.T, p coordination.TokenizationPolicy) (coordination.TokenizationCoordinator, coordination.TokenizationAcquireRequest) {
		return NewTokenizationCoordinator(p, time.Now), tokenizationRequest(time.Now())
	})
}
func TestTokenizationBackendExpiryAndRefill(t *testing.T) {
	now := time.Now().UTC()
	p := coordination.TokenizationPolicy{ClientMaxInflight: 2, BackendMaxInflight: 1, ClientRPM: 2, Timeout: time.Second}
	c := NewTokenizationCoordinator(p, func() time.Time { return now })
	ctx := context.Background()
	r := tokenizationRequest(now)
	a, _ := c.Acquire(ctx, r)
	if a.Lease == nil || a.Lease.BackendID != 1 {
		t.Fatal(a)
	}
	r.OperationID = uuid.New()
	r.ClientID = 2
	b, _ := c.Acquire(ctx, r)
	if b.Lease == nil || b.Lease.BackendID != 2 {
		t.Fatal(b)
	}
	r.OperationID = uuid.New()
	r.ClientID = 3
	d, _ := c.Acquire(ctx, r)
	if d.Reason != coordination.TokenizationBackendLimit {
		t.Fatal(d)
	}
	forged := *a.Lease
	forged.ClientID = 99
	if e := c.Complete(ctx, forged); e == nil {
		t.Fatal("forged completion accepted")
	}
	now = now.Add(p.LeaseTTL() + time.Second)
	r.OperationID = uuid.New()
	r.OperationStartedAt = now
	d, _ = c.Acquire(ctx, r)
	if d.Lease == nil {
		t.Fatal(d)
	}
	if d, _ = c.Acquire(ctx, tokenizationRequest(now.Add(-25*time.Hour))); d.Reason != coordination.TokenizationStaleOperation {
		t.Fatal(d)
	}
}
