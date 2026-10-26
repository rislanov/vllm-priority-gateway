package contracttest

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

// Tokenization exercises a separate utility budget. The factory supplies valid
// configuration identities when running against an authoritative database.
func Tokenization(t *testing.T, factory func(*testing.T, coordination.TokenizationPolicy) (coordination.TokenizationCoordinator, coordination.TokenizationAcquireRequest)) {
	t.Helper()
	policy := coordination.TokenizationPolicy{ClientMaxInflight: 1, BackendMaxInflight: 2, ClientRPM: 2, Timeout: time.Second}
	c, req := factory(t, policy)
	ctx := context.Background()
	acquire := func(r coordination.TokenizationAcquireRequest) coordination.TokenizationDecision {
		t.Helper()
		d, e := c.Acquire(ctx, r)
		if e != nil {
			t.Fatal(e)
		}
		return d
	}
	first := acquire(req)
	if first.Lease == nil {
		t.Fatalf("first admission: %+v", first)
	}
	replay := acquire(req)
	if replay.Lease == nil || !replay.Lease.Matches(*first.Lease) {
		t.Fatalf("replay changed lease: %+v", replay)
	}
	conflict := req
	conflict.RequestID = "different"
	if d := acquire(conflict); d.Reason != coordination.TokenizationConflict {
		t.Fatalf("conflicting replay: %+v", d)
	}
	next := req
	next.OperationID = uuid.New()
	if d := acquire(next); d.Reason != coordination.TokenizationClientLimit {
		t.Fatalf("client budget: %+v", d)
	}
	if e := c.Complete(ctx, *first.Lease); e != nil {
		t.Fatal(e)
	}
	if e := c.Complete(ctx, *first.Lease); e != nil {
		t.Fatal(e)
	}
	if d := acquire(req); d.Lease != nil {
		t.Fatal("completed operation resurrected")
	}
	next.OperationID = uuid.New()
	second := acquire(next)
	if second.Lease == nil {
		t.Fatalf("capacity not released: %+v", second)
	}
	if e := c.Complete(ctx, *second.Lease); e != nil {
		t.Fatal(e)
	}
	next.OperationID = uuid.New()
	if d := acquire(next); d.Reason != coordination.TokenizationRateLimit || d.RetryAt == nil {
		t.Fatalf("RPM/replay accounting: %+v", d)
	}
}
