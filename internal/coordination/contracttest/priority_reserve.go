package contracttest

import (
	"context"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"testing"
)

type PriorityReserveFactory func(t *testing.T, limit, reserve int) (coordination.AdmissionCoordinator, func(domain.PriorityClass) coordination.AdmissionRequest)

// PriorityReserve exercises the same admission policy against both storage profiles.
func PriorityReserve(t *testing.T, factory PriorityReserveFactory) {
	ctx := context.Background()
	t.Run("lower saturation and total exhaustion", func(t *testing.T) {
		c, request := factory(t, 20, 6)
		acquire := func(class domain.PriorityClass, want coordination.Reason) coordination.AdmissionDecision {
			t.Helper()
			d, err := c.Acquire(ctx, request(class))
			if err != nil || d.Reason != want || d.Admitted() != (want == "") {
				t.Fatalf("%s: decision=%+v err=%v want=%s", class, d, err, want)
			}
			return d
		}
		var low, high *coordination.LeaseIdentity
		for i := 0; i < 7; i++ {
			d := acquire(domain.PriorityNormal, "")
			low = d.Lease
			acquire(domain.PriorityBackground, "")
		}
		acquire(domain.PriorityNormal, coordination.ReasonPoolPriorityReserve)
		acquire(domain.PriorityBackground, coordination.ReasonPoolPriorityReserve)
		for i := 0; i < 3; i++ {
			d := acquire(domain.PriorityHigh, "")
			high = d.Lease
			acquire(domain.PriorityCritical, "")
		}
		for _, class := range []domain.PriorityClass{domain.PriorityNormal, domain.PriorityBackground, domain.PriorityHigh, domain.PriorityCritical} {
			if d := acquire(class, coordination.ReasonConcurrencyExhausted); d.ConcurrencyScope != coordination.AdmissionPoolScope {
				t.Fatalf("scope=%s", d.ConcurrencyScope)
			}
		}
		complete := func(lease *coordination.LeaseIdentity) {
			t.Helper()
			r, err := c.Complete(ctx, []coordination.LeaseCompletion{{Lease: *lease}})
			if err != nil || len(r) != 1 || r[0].Result != coordination.CompletionReleased {
				t.Fatalf("complete=%+v err=%v", r, err)
			}
		}
		complete(high)
		acquire(domain.PriorityNormal, coordination.ReasonPoolPriorityReserve)
		acquire(domain.PriorityHigh, "")
		complete(low)
		acquire(domain.PriorityBackground, "")
	})
	for _, tt := range []struct {
		name           string
		limit, reserve int
		class          domain.PriorityClass
		accepted       int
	}{
		{"high first leaves low capacity", 20, 6, domain.PriorityHigh, 10},
		{"disabled", 20, 0, domain.PriorityBackground, 20},
		{"unlimited", 0, 0, domain.PriorityNormal, 25},
		{"all reserved", 20, 20, domain.PriorityNormal, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, request := factory(t, tt.limit, tt.reserve)
			for i := 0; i < tt.accepted; i++ {
				d, err := c.Acquire(ctx, request(tt.class))
				if err != nil || !d.Admitted() {
					t.Fatalf("%d: %+v %v", i, d, err)
				}
			}
			if tt.name == "high first leaves low capacity" {
				for i := 0; i < 4; i++ {
					d, err := c.Acquire(ctx, request(domain.PriorityNormal))
					if err != nil || !d.Admitted() {
						t.Fatalf("low after high: %+v %v", d, err)
					}
				}
			}
			if tt.name == "all reserved" {
				d, err := c.Acquire(ctx, request(domain.PriorityNormal))
				if err != nil || d.Reason != coordination.ReasonPoolPriorityReserve {
					t.Fatalf("zero low capacity: %+v %v", d, err)
				}
			}
		})
	}
}
