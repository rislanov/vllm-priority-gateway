package gateway_test

import (
	"context"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/local"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
)

type priorityPolicyLoader struct{ data atomic.Pointer[registry.Data] }

func (l *priorityPolicyLoader) LoadSnapshot(context.Context) (registry.Data, error) {
	return *l.data.Load(), nil
}

func TestPriorityReservePublicationDuringLocalAdmission(t *testing.T) {
	for _, demotion := range []bool{false, true} {
		name := "enable reserve"
		class, reserve := domain.PriorityNormal, 0
		if demotion {
			name, class, reserve = "demote high client", domain.PriorityHigh, 1
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			secret, rawKey := []byte("01234567890123456789012345678901"), "llmgw_abcdefghijklmnopqrstuvwxyz012345"
			pool := domain.ModelPool{ID: 10, PublicModelName: "public-model", UpstreamModelName: "upstream-model", Enabled: true, MaxGatewayInflight: 2, HighPriorityReserve: reserve}
			client := domain.Client{ID: 1, Name: "client", Enabled: true, PriorityClass: class, MaxConcurrency: 4}
			backend := retryBackend(20, "gpu-20", "http://gpu-20.invalid")
			data := registry.Data{Revision: 1, Clients: []domain.Client{client}, Pools: []domain.ModelPool{pool}, Backends: []domain.Backend{backend},
				Keys:   []domain.APIKey{{ID: 2, ClientID: client.ID, Prefix: rawKey[:12], SecretHash: apikey.Digest(secret, rawKey)}},
				Access: []domain.ClientModelAccess{{ClientID: client.ID, ModelPoolID: pool.ID, Enabled: true}}}
			loader := &priorityPolicyLoader{}
			loader.data.Store(&data)
			reg := registry.New(loader)
			if err := reg.Reload(ctx); err != nil {
				t.Fatal(err)
			}
			coordinator := local.NewAdmissionCoordinator(func() time.Time { return poolTestNow })
			held, err := coordinator.Acquire(ctx, coordination.AdmissionRequest{LeaseID: uuid.New(), OperationStartedAt: poolTestNow, ClientID: 99, PoolID: pool.ID,
				ConfiguredClientLimit: 4, EffectiveClientLimit: 4, PoolGatewayInflightLimit: 2, PoolHighPriorityReserve: reserve, PriorityClass: domain.PriorityNormal, LeaseTTL: time.Minute})
			if err != nil || !held.Admitted() {
				t.Fatalf("hold lower lease: %+v %v", held, err)
			}
			runtime := newPoolRuntime(domain.PoolRuntime{PoolID: pool.ID, State: domain.PoolNormal, AvailableBackends: 1}, []domain.BackendRuntime{{BackendID: backend.ID, Healthy: true, MetricsFresh: true, CircuitAvailable: true}})
			paused, resume := make(chan struct{}), make(chan struct{})
			runtime.SetPoolSnapshotHook(func() {
				close(paused)
				select {
				case <-resume:
				case <-ctx.Done():
				}
			})
			service := gateway.New(gateway.Dependencies{Registry: reg, HMACSecret: secret, Admission: coordinator, Runtime: runtime,
				Router: routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0)), Forwarder: &poolCompletionForwarder{}, Now: func() time.Time { return poolTestNow }})
			result := make(chan *gateway.APIError, 1)
			go func() {
				_, _, apiErr := service.Forward(ctx, httptest.NewRecorder(), gateway.ForwardRequest{APIKey: rawKey, Body: []byte(`{"model":"public-model"}`)})
				result <- apiErr
			}()
			select {
			case <-paused:
			case <-ctx.Done():
				t.Fatal("admission did not pause")
			}
			pool.HighPriorityReserve, client.PriorityClass = 1, domain.PriorityNormal
			updated := data
			updated.Revision, updated.Pools, updated.Clients = 2, []domain.ModelPool{pool}, []domain.Client{client}
			loader.data.Store(&updated)
			if err := reg.Reload(ctx); err != nil {
				t.Fatal(err)
			}
			close(resume)
			select {
			case apiErr := <-result:
				if apiErr == nil || apiErr.HTTPStatus != 429 || apiErr.DecisionReason != gateway.DecisionPoolPriorityReserve {
					t.Fatalf("stale policy bypassed reserve: %+v", apiErr)
				}
			case <-ctx.Done():
				t.Fatal("admission did not complete")
			}
			if got := coordinator.PoolInflight(pool.ID); got != 1 {
				t.Fatalf("inflight = %d, want held lease only", got)
			}
		})
	}
}

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
