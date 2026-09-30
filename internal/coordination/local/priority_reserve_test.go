package local_test

import (
	"context"
	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/contracttest"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func reserveRequest(at time.Time, class domain.PriorityClass, limit, reserve int) coordination.AdmissionRequest {
	r := request(uuid.New(), at)
	r.PriorityClass = class
	r.PoolGatewayInflightLimit = limit
	r.PoolHighPriorityReserve = reserve
	r.ConfiguredClientLimit = 100
	r.EffectiveClientLimit = 100
	r.RequestsPerMinute = 0
	r.TokensPerMinute = 0
	return r
}
func TestPriorityReserveContract(t *testing.T) {
	contracttest.PriorityReserve(t, func(t *testing.T, limit, reserve int) (coordination.AdmissionCoordinator, func(domain.PriorityClass) coordination.AdmissionRequest) {
		return local.NewAdmissionCoordinator(time.Now), func(class domain.PriorityClass) coordination.AdmissionRequest {
			return reserveRequest(time.Now(), class, limit, reserve)
		}
	})
}
func TestPriorityReserveReplayPolicyAndExpiry(t *testing.T) {
	ctx := context.Background()
	clock := contracttest.NewClock(time.Now())
	c := local.NewAdmissionCoordinator(clock.Now)
	r := reserveRequest(clock.Now(), domain.PriorityNormal, 2, 0)
	d, err := c.Acquire(ctx, r)
	if err != nil || !d.Admitted() {
		t.Fatal(d, err)
	}
	replay, _ := c.Acquire(ctx, r)
	if replay.Lease == nil || replay.Lease.LeaseID != r.LeaseID || c.PoolInflight(r.PoolID) != 1 {
		t.Fatal(replay)
	}
	// The existing low lease was tracked even with reserve disabled.
	high := reserveRequest(clock.Now(), domain.PriorityHigh, 2, 1)
	hd, _ := c.Acquire(ctx, high)
	if !hd.Admitted() {
		t.Fatal(hd)
	}
	c.Complete(ctx, []coordination.LeaseCompletion{{Lease: *hd.Lease}})
	blocked := reserveRequest(clock.Now(), domain.PriorityBackground, 2, 1)
	bd, _ := c.Acquire(ctx, blocked)
	if bd.Reason != coordination.ReasonPoolPriorityReserve {
		t.Fatal(bd)
	}
	changed := r
	changed.PriorityClass = domain.PriorityHigh
	if got, _ := c.Acquire(ctx, changed); got.Reason != coordination.ReasonIdempotencyConflict {
		t.Fatal(got)
	}
	// Completion uses the original receipt rather than the caller's current class.
	c.Complete(ctx, []coordination.LeaseCompletion{{Lease: *d.Lease}})
	c.Complete(ctx, []coordination.LeaseCompletion{{Lease: *d.Lease}})
	if got, _ := c.Acquire(ctx, blocked); got.Reason != coordination.ReasonPoolPriorityReserve {
		t.Fatal("rejected replay changed", got)
	}
	next := reserveRequest(clock.Now(), domain.PriorityBackground, 2, 1)
	nd, _ := c.Acquire(ctx, next)
	if !nd.Admitted() {
		t.Fatal(nd)
	}
	clock.Add(30 * time.Second)
	renewed, e := c.Renew(ctx, []coordination.LeaseIdentity{*nd.Lease})
	if e != nil || renewed[0].Reason != "" {
		t.Fatal(renewed, e)
	}
	clock.Add(40 * time.Second)
	if c.PoolInflight(next.PoolID) != 1 {
		t.Fatal("renewal lost capacity")
	}
	clock.Add(21 * time.Second)
	if c.PoolInflight(next.PoolID) != 0 {
		t.Fatal("expiry leaked capacity")
	}
	if got, _ := c.Acquire(ctx, next); got.Reason != coordination.ReasonLeaseLost {
		t.Fatal(got)
	}
	if got, _ := c.Acquire(ctx, reserveRequest(clock.Now(), domain.PriorityNormal, 2, 1)); !got.Admitted() {
		t.Fatal(got)
	}
}
func TestPriorityReserveConcurrentAcquire(t *testing.T) {
	c := local.NewAdmissionCoordinator(time.Now)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var admitted atomic.Int32
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d, err := c.Acquire(context.Background(), reserveRequest(time.Now(), domain.PriorityNormal, 20, 6))
			if err != nil {
				t.Error(err)
			}
			if d.Admitted() {
				admitted.Add(1)
			} else if d.Reason != coordination.ReasonPoolPriorityReserve {
				t.Error(d)
			}
		}()
	}
	close(start)
	wg.Wait()
	if admitted.Load() != 14 {
		t.Fatalf("admitted=%d want 14", admitted.Load())
	}
}

func TestPriorityReservePreservesClientAndRateCaps(t *testing.T) {
	for _, class := range []domain.PriorityClass{domain.PriorityHigh, domain.PriorityCritical} {
		for _, kind := range []string{"client", "rpm", "tpm"} {
			t.Run(string(class)+"/"+kind, func(t *testing.T) {
				ctx := context.Background()
				clock := contracttest.NewClock(time.Now())
				c := local.NewAdmissionCoordinator(clock.Now)
				r := reserveRequest(clock.Now(), class, 20, 6)
				r.ConfiguredClientLimit = 1
				r.EffectiveClientLimit = 1
				if kind == "rpm" {
					r.RequestsPerMinute = 1
				}
				if kind == "tpm" {
					r.TokensPerMinute = 1
				}
				d, e := c.Acquire(ctx, r)
				if e != nil || !d.Admitted() {
					t.Fatal(d, e)
				}
				if kind != "client" {
					c.Complete(ctx, []coordination.LeaseCompletion{{Lease: *d.Lease, Usage: &coordination.TokenUsage{InputTokens: 2}}})
				}
				next := r
				next.LeaseID = uuid.New()
				want := coordination.ReasonConcurrencyExhausted
				if kind == "rpm" {
					want = coordination.ReasonRPMExhausted
				}
				if kind == "tpm" {
					want = coordination.ReasonTPMExhausted
				}
				got, e := c.Acquire(ctx, next)
				if e != nil || got.Reason != want {
					t.Fatalf("%+v %v want=%s", got, e, want)
				}
			})
		}
	}
}
