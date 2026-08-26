package gateway_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/proxy"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
)

func TestRetrySelectionUsesCurrentRegistryAndTime(t *testing.T) {
	started := time.Unix(1_700_000_000, 0).UTC()
	clock := &mutableClock{current: started}
	secret := []byte(strings.Repeat("s", 32))
	rawKey := "llmgw_abcdefghijklmnopqrstuvwxyz012345"
	client := domain.Client{ID: 1, Name: "client", Enabled: true, PriorityClass: domain.PriorityHigh, MaxConcurrency: 2}
	pool := domain.ModelPool{ID: 10, PublicModelName: "public-model", UpstreamModelName: "upstream-model", Enabled: true}
	key := domain.APIKey{ID: 2, ClientID: client.ID, Prefix: rawKey[:12], SecretHash: apikey.Digest(secret, rawKey)}
	backend20 := domain.Backend{ID: 20, ModelPoolID: pool.ID, Name: "gpu-20", BaseURL: "http://gpu-20.invalid", Enabled: true, CapacityHint: 1, RunningSoftLimit: 8}
	backend21 := domain.Backend{ID: 21, ModelPoolID: pool.ID, Name: "gpu-21", BaseURL: "http://gpu-21.invalid", Enabled: true, CapacityHint: 1, RunningSoftLimit: 8}
	provider := &mutableSnapshotProvider{}
	provider.Set(testSnapshot(client, key, pool, []domain.Backend{backend20, backend21}))
	runtime := &recordingRuntime{values: map[int64]domain.BackendRuntime{
		20: {BackendID: 20, Healthy: true, MetricsFresh: true, Pressure: .1},
		21: {BackendID: 21, Healthy: true, MetricsFresh: true, Pressure: .2},
	}}
	forwarder := &retryProbeForwarder{
		beforeAlternate: func() {
			backend21.Draining = true
			provider.Set(testSnapshot(client, key, pool, []domain.Backend{backend20, backend21}))
			clock.Set(started.Add(10 * time.Second))
		},
	}
	service := gateway.New(gateway.Dependencies{
		Registry: provider, HMACSecret: secret, Limiter: admission.NewLimiter(), Runtime: runtime,
		Router: routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0)), Forwarder: forwarder,
		Now: clock.Now,
	})
	response := httptest.NewRecorder()

	_, apiErr := service.Forward(context.Background(), response, gateway.ForwardRequest{
		Method: http.MethodPost, Path: "/v1/completions", Headers: make(http.Header),
		Body: []byte(`{"model":"public-model"}`), APIKey: rawKey,
	})

	if apiErr != nil {
		t.Fatalf("Forward() API error = %+v", apiErr)
	}
	if forwarder.initialBackend != 20 {
		t.Fatalf("initial backend = %d, want 20", forwarder.initialBackend)
	}
	if !errors.Is(forwarder.alternateErr, routing.ErrNoBackend) {
		t.Fatalf("alternate error = %v, want ErrNoBackend after drain", forwarder.alternateErr)
	}
	times := runtime.SnapshotTimes()
	if len(times) < 2 || !times[len(times)-1].Equal(started.Add(10*time.Second)) {
		t.Fatalf("runtime snapshot times = %v, want retry at %s", times, started.Add(10*time.Second))
	}
}

func TestRetrySelectionRejectsPoolModelReconfiguration(t *testing.T) {
	started := time.Unix(1_700_000_000, 0).UTC()
	clock := &mutableClock{current: started}
	secret := []byte(strings.Repeat("s", 32))
	rawKey := "llmgw_abcdefghijklmnopqrstuvwxyz012345"
	client := domain.Client{ID: 1, Name: "client", Enabled: true, PriorityClass: domain.PriorityHigh, MaxConcurrency: 2}
	pool := domain.ModelPool{ID: 10, PublicModelName: "public-model", UpstreamModelName: "upstream-model", Enabled: true}
	key := domain.APIKey{ID: 2, ClientID: client.ID, Prefix: rawKey[:12], SecretHash: apikey.Digest(secret, rawKey)}
	backend20 := domain.Backend{ID: 20, ModelPoolID: pool.ID, Name: "gpu-20", BaseURL: "http://gpu-20.invalid", Enabled: true, CapacityHint: 1, RunningSoftLimit: 8}
	backend21 := domain.Backend{ID: 21, ModelPoolID: pool.ID, Name: "gpu-21", BaseURL: "http://gpu-21.invalid", Enabled: true, CapacityHint: 1, RunningSoftLimit: 8}
	provider := &mutableSnapshotProvider{}
	provider.Set(testSnapshot(client, key, pool, []domain.Backend{backend20, backend21}))
	runtime := &recordingRuntime{values: map[int64]domain.BackendRuntime{
		20: {BackendID: 20, Healthy: true, MetricsFresh: true, Pressure: .1},
		21: {BackendID: 21, Healthy: true, MetricsFresh: true, Pressure: .2},
	}}
	forwarder := &retryProbeForwarder{
		beforeAlternate: func() {
			pool.UpstreamModelName = "replacement-upstream-model"
			provider.Set(testSnapshot(client, key, pool, []domain.Backend{backend20, backend21}))
		},
	}
	service := gateway.New(gateway.Dependencies{
		Registry: provider, HMACSecret: secret, Limiter: admission.NewLimiter(), Runtime: runtime,
		Router: routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0)), Forwarder: forwarder,
		Now: clock.Now,
	})
	response := httptest.NewRecorder()

	_, apiErr := service.Forward(context.Background(), response, gateway.ForwardRequest{
		Method: http.MethodPost, Path: "/v1/completions", Headers: make(http.Header),
		Body: []byte(`{"model":"public-model"}`), APIKey: rawKey,
	})

	if apiErr != nil {
		t.Fatalf("Forward() API error = %+v", apiErr)
	}
	if !errors.Is(forwarder.alternateErr, routing.ErrNoBackend) {
		t.Fatalf("alternate error = %v, want ErrNoBackend after pool model reconfiguration", forwarder.alternateErr)
	}
}

type mutableSnapshotProvider struct {
	mu       sync.RWMutex
	snapshot *registry.Snapshot
}

func (p *mutableSnapshotProvider) Snapshot() *registry.Snapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.snapshot
}

func (p *mutableSnapshotProvider) Set(snapshot *registry.Snapshot) {
	p.mu.Lock()
	p.snapshot = snapshot
	p.mu.Unlock()
}

type mutableClock struct {
	mu      sync.Mutex
	current time.Time
}

func (c *mutableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *mutableClock) Set(value time.Time) {
	c.mu.Lock()
	c.current = value
	c.mu.Unlock()
}

type recordingRuntime struct {
	mu            sync.Mutex
	values        map[int64]domain.BackendRuntime
	snapshotTimes []time.Time
}

func (r *recordingRuntime) PoolSnapshot(poolID int64, _ time.Time) domain.PoolRuntime {
	return domain.PoolRuntime{PoolID: poolID, State: domain.PoolNormal, AvailableBackends: len(r.values)}
}

func (r *recordingRuntime) Snapshot(backendID int64, at time.Time) domain.BackendRuntime {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshotTimes = append(r.snapshotTimes, at)
	return r.values[backendID]
}

func (r *recordingRuntime) IncrementInflight(backendID int64) (func(), bool) {
	r.mu.Lock()
	_, exists := r.values[backendID]
	r.mu.Unlock()
	if !exists {
		return nil, false
	}
	return func() {}, true
}

func (r *recordingRuntime) SnapshotTimes() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.snapshotTimes...)
}

type retryProbeForwarder struct {
	beforeAlternate func()
	initialBackend  int64
	alternateErr    error
}

func (f *retryProbeForwarder) Forward(_ context.Context, writer http.ResponseWriter, request proxy.Request) proxy.Result {
	f.initialBackend = request.Target.Backend.ID
	f.beforeAlternate()
	_, f.alternateErr = request.SelectAlternate(map[int64]struct{}{request.Target.Backend.ID: {}})
	writer.WriteHeader(http.StatusOK)
	return proxy.Result{BackendID: request.Target.Backend.ID, Status: http.StatusOK}
}

func testSnapshot(client domain.Client, key domain.APIKey, pool domain.ModelPool, backends []domain.Backend) *registry.Snapshot {
	byID := make(map[int64]domain.Backend, len(backends))
	for _, backend := range backends {
		byID[backend.ID] = backend
	}
	return &registry.Snapshot{
		Revision:       1,
		Clients:        map[int64]domain.Client{client.ID: client},
		KeyCandidates:  map[string][]domain.APIKey{key.Prefix: {key}},
		PoolsByID:      map[int64]domain.ModelPool{pool.ID: pool},
		PoolsByName:    map[string]domain.ModelPool{pool.PublicModelName: pool},
		Access:         map[int64]map[int64]bool{client.ID: {pool.ID: true}},
		BackendsByID:   byID,
		BackendsByPool: map[int64][]domain.Backend{pool.ID: append([]domain.Backend(nil), backends...)},
	}
}
