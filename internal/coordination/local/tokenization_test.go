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
	serialized := *a.Lease
	serialized.AcquiredAt = serialized.AcquiredAt.In(time.FixedZone("wire", 0))
	serialized.ExpiresAt = serialized.ExpiresAt.In(time.FixedZone("wire", 0))
	if e := c.Complete(ctx, serialized); e != nil {
		t.Fatalf("wire-equivalent timestamps rejected: %v", e)
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

func TestTokenizationReceiptCapacityAndRefill(t *testing.T) {
	now := time.Now().UTC()
	p := coordination.TokenizationPolicy{ClientMaxInflight: 1, BackendMaxInflight: 1, ClientRPM: 1, Timeout: time.Second}
	c := NewTokenizationCoordinator(p, func() time.Time { return now })
	ctx := context.Background()
	r := tokenizationRequest(now)
	d, e := c.Acquire(ctx, r)
	if e != nil || d.Lease == nil {
		t.Fatal(d, e)
	}
	if e = c.Complete(ctx, *d.Lease); e != nil {
		t.Fatal(e)
	}
	r.OperationID = uuid.New()
	if d, e = c.Acquire(ctx, r); e != nil || d.Reason != coordination.TokenizationRateLimit {
		t.Fatal(d, e)
	}
	now = now.Add(time.Minute)
	r.OperationID = uuid.New()
	r.OperationStartedAt = now
	if d, e = c.Acquire(ctx, r); e != nil || d.Lease == nil {
		t.Fatalf("RPM did not refill: %+v %v", d, e)
	}
}

func TestTokenizationReceiptExhaustionDoesNotBlockAnotherClient(t *testing.T) {
	now := time.Now().UTC()
	p := coordination.DefaultTokenizationPolicy()
	c := NewTokenizationCoordinator(p, func() time.Time { return now })
	p.ClientRPM = 1
	c = NewTokenizationCoordinator(p, func() time.Time { return now })
	first, e := c.Acquire(context.Background(), tokenizationRequest(now))
	if e != nil || first.Lease == nil {
		t.Fatal(first, e)
	}
	if e = c.Complete(context.Background(), *first.Lease); e != nil {
		t.Fatal(e)
	}
	// A full denied-request flood can exhaust only this client's receipt quota.
	for i := 0; i < tokenizationReceiptsPerClient+1; i++ {
		r := tokenizationRequest(now)
		d, e := c.Acquire(context.Background(), r)
		if e != nil || d.Lease != nil {
			t.Fatal(d, e)
		}
	}
	if c.receiptCounts[1] != tokenizationReceiptsPerClient {
		t.Fatal("client receipt bound not enforced")
	}
	r := tokenizationRequest(now)
	r.ClientID = 2
	d, e := c.Acquire(context.Background(), r)
	if e != nil || d.Lease == nil {
		t.Fatalf("one client's history blocked another: %+v %v", d, e)
	}
}

func TestTokenizationLocalReplayWindowCannotResurrectEvictedOperation(t *testing.T) {
	now := time.Now().UTC()
	p := coordination.DefaultTokenizationPolicy()
	c := NewTokenizationCoordinator(p, func() time.Time { return now })
	ctx := context.Background()
	r := tokenizationRequest(now)
	d, e := c.Acquire(ctx, r)
	if e != nil || d.Lease == nil {
		t.Fatal(d, e)
	}
	if e = c.Complete(ctx, *d.Lease); e != nil {
		t.Fatal(e)
	}
	now = now.Add(p.LeaseTTL() + time.Second)
	next := tokenizationRequest(now)
	d, e = c.Acquire(ctx, next)
	if e != nil || d.Lease == nil {
		t.Fatal(d, e)
	}
	if _, exists := c.receipts[r.OperationID]; exists {
		t.Fatal("completed receipt retained beyond bounded local replay window")
	}
	if d, e = c.Acquire(ctx, r); e != nil || d.Lease != nil || d.Reason != coordination.TokenizationStaleOperation {
		t.Fatalf("evicted operation replayed: %+v %v", d, e)
	}
}

func TestTokenizationDefaultRateRemainsAvailableAfterNineHours(t *testing.T) {
	now := time.Now().UTC()
	p := coordination.DefaultTokenizationPolicy()
	c := NewTokenizationCoordinator(p, func() time.Time { return now })
	ctx := context.Background()
	for i := 0; i < 66000; i++ {
		r := tokenizationRequest(now)
		d, e := c.Acquire(ctx, r)
		if e != nil || d.Lease == nil {
			t.Fatalf("request %d at default rate: %+v %v", i, d, e)
		}
		if e = c.Complete(ctx, *d.Lease); e != nil {
			t.Fatal(e)
		}
		now = now.Add(time.Second / 2)
	}
	if len(c.receipts) > 200 || c.receiptCounts[1] > 200 {
		t.Fatalf("history grew with uptime: %d", len(c.receipts))
	}
	r := tokenizationRequest(now)
	r.ClientID = 2
	if d, e := c.Acquire(ctx, r); e != nil || d.Lease == nil {
		t.Fatal(d, e)
	}
}

func TestTokenizationRejectedReplayRemainsRejectedAfterEviction(t *testing.T) {
	now := time.Now().UTC()
	p := coordination.DefaultTokenizationPolicy()
	p.ClientMaxInflight = 1
	c := NewTokenizationCoordinator(p, func() time.Time { return now })
	ctx := context.Background()
	active, e := c.Acquire(ctx, tokenizationRequest(now))
	if e != nil || active.Lease == nil {
		t.Fatal(active, e)
	}
	denied := tokenizationRequest(now)
	d, e := c.Acquire(ctx, denied)
	if e != nil || d.Reason != coordination.TokenizationClientLimit {
		t.Fatal(d, e)
	}
	if e = c.Complete(ctx, *active.Lease); e != nil {
		t.Fatal(e)
	}
	if replay, e := c.Acquire(ctx, denied); e != nil || replay.Reason != coordination.TokenizationClientLimit {
		t.Fatalf("rejection changed within replay window: %+v %v", replay, e)
	}
	now = now.Add(p.LeaseTTL() + time.Second)
	if replay, e := c.Acquire(ctx, denied); e != nil || replay.Reason != coordination.TokenizationStaleOperation || replay.Lease != nil {
		t.Fatalf("old rejection resurrected: %+v %v", replay, e)
	}
	if d, e = c.Acquire(ctx, tokenizationRequest(now)); e != nil || d.Lease == nil {
		t.Fatalf("new operation blocked: %+v %v", d, e)
	}
}
