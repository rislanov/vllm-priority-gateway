package gateway_test

import (
	"context"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPriorityReserveGatewayOverloadAndHighAdmission(t *testing.T) {
	for _, class := range []domain.PriorityClass{domain.PriorityBackground, domain.PriorityNormal, domain.PriorityHigh, domain.PriorityCritical} {
		t.Run(string(class), func(t *testing.T) {
			c := local.NewAdmissionCoordinator(func() time.Time { return poolTestNow })
			s, r, _ := newPoolService(t, poolServiceOptions{maximum: 1, reserve: 1, priority: class, coordinator: c})
			r.Body = []byte(`{"model":"public-model","priority":-100}`)
			r.Headers.Set("X-Priority-Class", "critical")
			_, _, e := s.Forward(context.Background(), httptest.NewRecorder(), r)
			if class == domain.PriorityHigh || class == domain.PriorityCritical {
				if e != nil {
					t.Fatal(e)
				}
				return
			}
			if e == nil || e.HTTPStatus != 429 || e.Code != "gateway_overloaded" || string(e.DecisionReason) != "pool_priority_reserve" || e.RetryAfter != 2*time.Second {
				t.Fatalf("reserve rejection=%+v", e)
			}
		})
	}
}
func TestPriorityReserveFailsClosedWithoutCoordinator(t *testing.T) {
	s, r, _ := newPoolService(t, poolServiceOptions{maximum: 1, reserve: 1})
	_, _, e := s.Forward(context.Background(), httptest.NewRecorder(), r)
	if e == nil || e.HTTPStatus != 503 {
		t.Fatalf("uncoordinated reserve=%+v", e)
	}
}

func TestPriorityReservePreservesWaitingAndEmergencyCaps(t *testing.T) {
	for _, class := range []domain.PriorityClass{domain.PriorityHigh, domain.PriorityCritical} {
		t.Run(string(class)+"/waiting", func(t *testing.T) {
			c := local.NewAdmissionCoordinator(func() time.Time { return poolTestNow })
			s, r, _ := newPoolService(t, poolServiceOptions{maximum: 20, reserve: 6, maxWaiting: 1, totalWaiting: 1, priority: class, coordinator: c})
			_, _, e := s.Forward(context.Background(), httptest.NewRecorder(), r)
			if e == nil || e.HTTPStatus != 429 || string(e.DecisionReason) != "pool_waiting_limit" {
				t.Fatal(e)
			}
		})
	}
	for _, class := range []domain.PriorityClass{domain.PriorityNormal, domain.PriorityBackground, domain.PriorityHigh, domain.PriorityCritical} {
		t.Run(string(class)+"/emergency disabled", func(t *testing.T) {
			c := local.NewAdmissionCoordinator(func() time.Time { return poolTestNow })
			s, r, _ := newPoolService(t, poolServiceOptions{maximum: 20, reserve: 6, priority: class, coordinator: c, coordinationReady: func() bool { return false }})
			_, _, e := s.Forward(context.Background(), httptest.NewRecorder(), r)
			if e == nil || e.HTTPStatus != 503 {
				t.Fatal(e)
			}
		})
	}
}
