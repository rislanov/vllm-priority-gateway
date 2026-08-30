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

var poolTestNow = time.Unix(1_700_000_000, 0).UTC()

func TestServicePoolMaxInflightRejectsSecondAndReleasesAfterCompletion(t *testing.T) {
	forwarder := newPoolBlockingForwarder()
	service, request, runtime := newPoolService(t, poolServiceOptions{
		maximum: 1, priority: domain.PriorityHigh, forwarder: forwarder,
	})
	firstDone := forwardPoolAsync(service, context.Background(), request)
	forwarder.waitStarted(t)

	secondDone := forwardPoolAsync(service, context.Background(), request)
	second := waitPoolResult(t, secondDone, forwarder.releaseAll)
	if second.apiErr == nil || second.apiErr.HTTPStatus != http.StatusTooManyRequests || second.apiErr.Code != "gateway_overloaded" || second.apiErr.RetryAfter <= 0 {
		t.Fatalf("second request API error = %+v, want bounded 429 with Retry-After", second.apiErr)
	}
	if second.apiErr.DecisionReason != gateway.DecisionPoolInflightLimit {
		t.Fatalf("second request reason = %q, want %q", second.apiErr.DecisionReason, gateway.DecisionPoolInflightLimit)
	}
	if got := runtime.PoolInflight(); got != 1 {
		t.Fatalf("pool inflight while first request blocks = %d, want 1", got)
	}

	forwarder.releaseAll()
	first := <-firstDone
	if first.apiErr != nil || first.result.Status != http.StatusOK {
		t.Fatalf("first request result=%+v API error=%+v", first.result, first.apiErr)
	}
	if got := runtime.PoolInflight(); got != 0 {
		t.Fatalf("pool inflight after completion = %d, want 0", got)
	}
}

func TestServicePoolMaxWaitingRejectsBeforePoolAndClientAcquisition(t *testing.T) {
	observer := &backendRecordingObserver{}
	service, request, runtime := newPoolService(t, poolServiceOptions{
		maximum: 2, maxWaiting: 5, totalWaiting: 5, priority: domain.PriorityCritical,
		forwarder: &poolCompletionForwarder{}, observer: observer,
	})

	_, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
	if apiErr == nil || apiErr.HTTPStatus != http.StatusTooManyRequests || apiErr.Code != "gateway_overloaded" || apiErr.RetryAfter <= 0 {
		t.Fatalf("waiting rejection API error = %+v, want bounded 429 with Retry-After", apiErr)
	}
	if apiErr.DecisionReason != gateway.DecisionPoolWaitingLimit {
		t.Fatalf("waiting rejection reason = %q, want %q", apiErr.DecisionReason, gateway.DecisionPoolWaitingLimit)
	}
	if got := runtime.PoolAcquisitions(); got != 0 {
		t.Fatalf("pool acquisitions after waiting rejection = %d, want 0", got)
	}
	event := observer.Event()
	if event.QueueOutcome != gateway.QueueRejected || event.QueueWait < 0 {
		t.Fatalf("waiting rejection queue telemetry = outcome %q wait %s", event.QueueOutcome, event.QueueWait)
	}
	if event.DecisionReason != gateway.DecisionPoolWaitingLimit {
		t.Fatalf("waiting rejection event reason = %q, want %q", event.DecisionReason, gateway.DecisionPoolWaitingLimit)
	}
}

func TestServiceRevalidatesMaxGatewayInflightAfterConcurrentPoolUpdate(t *testing.T) {
	updatedPool := domain.ModelPool{
		ID: 10, PublicModelName: "public-model", UpstreamModelName: "upstream-model", Enabled: true,
		MaxGatewayInflight: 1,
	}
	service, request, runtime := newPoolService(t, poolServiceOptions{
		priority: domain.PriorityHigh, forwarder: &poolCompletionForwarder{},
		publishPoolOnSnapshot: &updatedPool,
	})
	heldRelease, ok := runtime.AcquirePool(updatedPool.ID, 0)
	if !ok {
		t.Fatal("failed to establish existing pool lease")
	}

	_, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
	if apiErr == nil || apiErr.HTTPStatus != http.StatusTooManyRequests || apiErr.Code != "gateway_overloaded" {
		heldRelease()
		t.Fatalf("concurrent max-inflight update API error = %+v, want pool-safety 429", apiErr)
	}
	if got := runtime.PoolAcquisitions(); got != 2 {
		heldRelease()
		t.Fatalf("pool acquisitions = %d, want existing plus one provisional lease", got)
	}
	if got := runtime.PoolInflight(); got != 1 {
		heldRelease()
		t.Fatalf("pool inflight after stale provisional release = %d, want existing lease only", got)
	}
	heldRelease()
	if got := runtime.PoolInflight(); got != 0 {
		t.Fatalf("pool inflight after existing lease release = %d, want 0", got)
	}
}

func TestServiceRevalidatesMaxWaitingAfterConcurrentPoolUpdate(t *testing.T) {
	updatedPool := domain.ModelPool{
		ID: 10, PublicModelName: "public-model", UpstreamModelName: "upstream-model", Enabled: true,
		MaxWaiting: 1,
	}
	service, request, runtime := newPoolService(t, poolServiceOptions{
		totalWaiting: 1, priority: domain.PriorityHigh, forwarder: &poolCompletionForwarder{},
		publishPoolOnSnapshot: &updatedPool,
	})

	_, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
	if apiErr == nil || apiErr.HTTPStatus != http.StatusTooManyRequests || apiErr.Code != "gateway_overloaded" {
		t.Fatalf("concurrent max-waiting update API error = %+v, want pool-safety 429", apiErr)
	}
	if got := runtime.PoolAcquisitions(); got != 1 {
		t.Fatalf("pool acquisitions = %d, want one provisional lease before revalidation", got)
	}
	if got := runtime.PoolInflight(); got != 0 {
		t.Fatalf("pool inflight after stale provisional release = %d, want 0", got)
	}
}

func TestServiceIgnoresUnrelatedRegistryRevisionDuringPoolAdmission(t *testing.T) {
	unchangedPool := domain.ModelPool{
		ID: 10, PublicModelName: "public-model", UpstreamModelName: "upstream-model", Enabled: true,
	}
	service, request, runtime := newPoolService(t, poolServiceOptions{
		priority: domain.PriorityHigh, forwarder: &poolCompletionForwarder{},
		publishPoolOnSnapshot: &unchangedPool,
	})

	result, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
	if apiErr != nil || result.Status != http.StatusOK {
		t.Fatalf("unrelated registry revision result=%+v API error=%+v, want admitted request", result, apiErr)
	}
	if got := runtime.PoolAcquisitions(); got != 1 {
		t.Fatalf("pool acquisitions after unrelated revision = %d, want exactly 1", got)
	}
	if got := runtime.PoolInflight(); got != 0 {
		t.Fatalf("pool inflight after admitted request = %d, want 0", got)
	}
}

func TestServiceCancellationStopsRelevantPoolLimitChurnWithoutLeaseLeak(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	maximum := 0
	service, request, runtime := newPoolService(t, poolServiceOptions{
		priority: domain.PriorityHigh, forwarder: &poolCompletionForwarder{},
		mutatePoolOnEverySnapshot: func(pool domain.ModelPool) domain.ModelPool {
			maximum++
			pool.MaxGatewayInflight = maximum
			cancel()
			return pool
		},
	})
	done := forwardPoolAsync(service, ctx, request)

	select {
	case completed := <-done:
		if completed.apiErr == nil || completed.apiErr.Code != "backend_unavailable" {
			t.Fatalf("cancelled admission API error = %+v, want controlled backend_unavailable", completed.apiErr)
		}
	case <-time.After(250 * time.Millisecond):
		runtime.SetPoolSnapshotHook(nil)
		<-done
		t.Fatal("cancelled admission spun while pool limits kept changing")
	}
	if got := runtime.PoolAcquisitions(); got != 1 {
		t.Fatalf("pool acquisitions before cancellation = %d, want exactly 1 provisional lease", got)
	}
	if got := runtime.PoolInflight(); got != 0 {
		t.Fatalf("pool inflight after cancellation = %d, want 0", got)
	}
}

func TestServiceZeroPoolLimitsAreUnlimitedButCounted(t *testing.T) {
	forwarder := newPoolBlockingForwarder()
	service, request, runtime := newPoolService(t, poolServiceOptions{
		priority: domain.PriorityHigh, maxConcurrency: 4, forwarder: forwarder,
	})
	firstDone := forwardPoolAsync(service, context.Background(), request)
	secondDone := forwardPoolAsync(service, context.Background(), request)
	forwarder.waitStarted(t)
	forwarder.waitStarted(t)
	if got := runtime.PoolInflight(); got != 2 {
		forwarder.releaseAll()
		<-firstDone
		<-secondDone
		t.Fatalf("unlimited pool inflight = %d, want 2", got)
	}
	forwarder.releaseAll()
	for _, done := range []<-chan poolForwardResult{firstDone, secondDone} {
		completed := <-done
		if completed.apiErr != nil || completed.result.Status != http.StatusOK {
			t.Fatalf("unlimited request result=%+v API error=%+v", completed.result, completed.apiErr)
		}
	}
	if got := runtime.PoolInflight(); got != 0 {
		t.Fatalf("unlimited pool inflight after completion = %d, want 0", got)
	}
}

func TestServiceCriticalPriorityCannotBypassPoolLimit(t *testing.T) {
	forwarder := newPoolBlockingForwarder()
	service, request, _ := newPoolService(t, poolServiceOptions{
		maximum: 1, priority: domain.PriorityCritical, maxConcurrency: 8, forwarder: forwarder,
	})
	firstDone := forwardPoolAsync(service, context.Background(), request)
	forwarder.waitStarted(t)
	secondDone := forwardPoolAsync(service, context.Background(), request)
	second := waitPoolResult(t, secondDone, forwarder.releaseAll)
	if second.apiErr == nil || second.apiErr.HTTPStatus != http.StatusTooManyRequests || second.apiErr.Code != "gateway_overloaded" {
		t.Fatalf("critical second request API error = %+v, want pool-safety 429", second.apiErr)
	}
	forwarder.releaseAll()
	<-firstDone
}

func TestServicePoolLeaseSpansRetrySelection(t *testing.T) {
	runtime := newPoolRuntime(domain.PoolRuntime{PoolID: 10, State: domain.PoolNormal, AvailableBackends: 2}, []domain.BackendRuntime{
		{BackendID: 20, Healthy: true, MetricsFresh: true, Pressure: .1, CircuitAvailable: true},
		{BackendID: 21, Healthy: true, MetricsFresh: true, Pressure: .2, CircuitAvailable: true},
	})
	forwarder := &poolRetryForwarder{runtime: runtime}
	service, request, _ := newPoolService(t, poolServiceOptions{
		maximum: 1, priority: domain.PriorityHigh, runtime: runtime, forwarder: forwarder,
		backends: []domain.Backend{
			retryBackend(20, "gpu-20", "http://gpu-20.invalid"),
			retryBackend(21, "gpu-21", "http://gpu-21.invalid"),
		},
	})

	result, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
	if apiErr != nil || result.Status != http.StatusOK {
		t.Fatalf("retry request result=%+v API error=%+v", result, apiErr)
	}
	if !forwarder.poolHeldDuringAlternate {
		t.Fatal("pool lease was not held through alternate selection")
	}
	if got := runtime.PoolInflight(); got != 0 {
		t.Fatalf("pool inflight after retry lifecycle = %d, want 0", got)
	}
}

func TestServiceReleasesPoolLeaseOnEveryAdmittedExit(t *testing.T) {
	t.Run("selection failure", func(t *testing.T) {
		runtime := newPoolRuntime(domain.PoolRuntime{PoolID: 10, State: domain.PoolNormal, AvailableBackends: 1}, []domain.BackendRuntime{
			{BackendID: 20, Healthy: false, MetricsFresh: true, CircuitAvailable: true},
		})
		service, request, _ := newPoolService(t, poolServiceOptions{maximum: 1, runtime: runtime, forwarder: &poolCompletionForwarder{}})
		_, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
		if apiErr == nil || apiErr.HTTPStatus != http.StatusServiceUnavailable {
			t.Fatalf("selection failure API error = %+v", apiErr)
		}
		assertPoolReleased(t, runtime, 1)
	})

	t.Run("upstream error", func(t *testing.T) {
		service, request, runtime := newPoolService(t, poolServiceOptions{maximum: 1, forwarder: poolErrorForwarder{}})
		_, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
		if apiErr == nil || apiErr.HTTPStatus != http.StatusBadGateway {
			t.Fatalf("upstream error API error = %+v", apiErr)
		}
		assertPoolReleased(t, runtime, 1)
	})

	t.Run("stream cancellation", func(t *testing.T) {
		forwarder := newPoolBlockingForwarder()
		service, request, runtime := newPoolService(t, poolServiceOptions{maximum: 1, forwarder: forwarder})
		ctx, cancel := context.WithCancel(context.Background())
		done := forwardPoolAsync(service, ctx, request)
		forwarder.waitStarted(t)
		if got := runtime.PoolInflight(); got != 1 {
			cancel()
			<-done
			t.Fatalf("pool inflight during stream = %d, want 1", got)
		}
		cancel()
		completed := <-done
		if completed.apiErr != nil || !completed.result.Cancelled || !errors.Is(completed.result.Err, context.Canceled) {
			t.Fatalf("cancelled result=%+v API error=%+v", completed.result, completed.apiErr)
		}
		assertPoolReleased(t, runtime, 1)
	})
}

func TestServiceReleasesPoolLeaseWhenClientLimiterRejects(t *testing.T) {
	forwarder := newPoolBlockingForwarder()
	service, request, runtime := newPoolService(t, poolServiceOptions{
		priority: domain.PriorityHigh, maxConcurrency: 1, forwarder: forwarder,
	})
	firstDone := forwardPoolAsync(service, context.Background(), request)
	forwarder.waitStarted(t)
	_, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
	if apiErr == nil || apiErr.HTTPStatus != http.StatusTooManyRequests {
		forwarder.releaseAll()
		<-firstDone
		t.Fatalf("client limiter API error = %+v", apiErr)
	}
	if apiErr.DecisionReason != gateway.DecisionPriorityConcurrencyLimit {
		forwarder.releaseAll()
		<-firstDone
		t.Fatalf("client limiter reason = %q, want %q", apiErr.DecisionReason, gateway.DecisionPriorityConcurrencyLimit)
	}
	if got := runtime.PoolInflight(); got != 1 {
		forwarder.releaseAll()
		<-firstDone
		t.Fatalf("pool inflight after client rejection = %d, want only first request", got)
	}
	forwarder.releaseAll()
	<-firstDone
	assertPoolReleased(t, runtime, 2)
}

func TestServiceInferenceReadinessMatrix(t *testing.T) {
	tests := []struct {
		name                   string
		poolEnabled            bool
		backendEnabled         bool
		backendDraining        bool
		healthy                bool
		metricsFresh           bool
		circuitState           domain.CircuitState
		circuitAvailable       bool
		upstreamAPIKeyEnv      string
		resolvedSecret         string
		maxGatewayInflight     int
		maxWaiting             int
		gatewayInflight        int
		totalWaiting           float64
		secondAvailableBackend bool
		wantStatus             string
		wantPools              int
		wantBackends           int
	}{
		{
			name: "healthy closed backends count pool once", poolEnabled: true, backendEnabled: true,
			healthy: true, metricsFresh: true, circuitState: domain.CircuitClosed, circuitAvailable: true,
			secondAvailableBackend: true, wantStatus: "ready", wantPools: 1, wantBackends: 2,
		},
		{
			name: "all circuits open", poolEnabled: true, backendEnabled: true,
			healthy: true, metricsFresh: true, circuitState: domain.CircuitOpen, circuitAvailable: false,
			wantStatus: "unavailable",
		},
		{
			name: "metrics stale", poolEnabled: true, backendEnabled: true,
			healthy: true, metricsFresh: false, circuitState: domain.CircuitClosed, circuitAvailable: true,
			wantStatus: "unavailable",
		},
		{
			name: "unhealthy", poolEnabled: true, backendEnabled: true,
			healthy: false, metricsFresh: true, circuitState: domain.CircuitClosed, circuitAvailable: true,
			wantStatus: "unavailable",
		},
		{
			name: "backend disabled", poolEnabled: true, backendEnabled: false,
			healthy: true, metricsFresh: true, circuitState: domain.CircuitClosed, circuitAvailable: true,
			wantStatus: "unavailable",
		},
		{
			name: "backend draining", poolEnabled: true, backendEnabled: true, backendDraining: true,
			healthy: true, metricsFresh: true, circuitState: domain.CircuitClosed, circuitAvailable: true,
			wantStatus: "unavailable",
		},
		{
			name: "pool disabled", poolEnabled: false, backendEnabled: true,
			healthy: true, metricsFresh: true, circuitState: domain.CircuitClosed, circuitAvailable: true,
			wantStatus: "unavailable",
		},
		{
			name: "configured secret missing", poolEnabled: true, backendEnabled: true,
			healthy: true, metricsFresh: true, circuitState: domain.CircuitClosed, circuitAvailable: true,
			upstreamAPIKeyEnv: "UPSTREAM_KEY", wantStatus: "unavailable",
		},
		{
			name: "half open capacity available", poolEnabled: true, backendEnabled: true,
			healthy: true, metricsFresh: true, circuitState: domain.CircuitHalfOpen, circuitAvailable: true,
			upstreamAPIKeyEnv: "UPSTREAM_KEY", resolvedSecret: "resolved", wantStatus: "ready", wantPools: 1, wantBackends: 1,
		},
		{
			name: "congested pool remains ready", poolEnabled: true, backendEnabled: true,
			healthy: true, metricsFresh: true, circuitState: domain.CircuitClosed, circuitAvailable: true,
			maxGatewayInflight: 1, maxWaiting: 1, gatewayInflight: 1, totalWaiting: 10,
			wantStatus: "ready", wantPools: 1, wantBackends: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := domain.ModelPool{
				ID: 10, PublicModelName: "public-model", UpstreamModelName: "upstream-model", Enabled: tt.poolEnabled,
				MaxGatewayInflight: tt.maxGatewayInflight, MaxWaiting: tt.maxWaiting,
			}
			backend := domain.Backend{
				ID: 20, ModelPoolID: pool.ID, Name: "gpu-20", BaseURL: "http://gpu-20.invalid",
				Enabled: tt.backendEnabled, Draining: tt.backendDraining, CapacityHint: 1,
				RunningSoftLimit: 8, UpstreamAPIKeyEnv: tt.upstreamAPIKeyEnv,
			}
			backends := []domain.Backend{backend}
			runtimes := []domain.BackendRuntime{{
				BackendID: backend.ID, Healthy: tt.healthy, MetricsFresh: tt.metricsFresh,
				CircuitState: tt.circuitState, CircuitAvailable: tt.circuitAvailable,
			}}
			if tt.secondAvailableBackend {
				second := backend
				second.ID = 21
				second.Name = "gpu-21"
				backends = append(backends, second)
				runtimes = append(runtimes, domain.BackendRuntime{
					BackendID: second.ID, Healthy: true, MetricsFresh: true,
					CircuitState: domain.CircuitClosed, CircuitAvailable: true,
				})
			}
			byID := make(map[int64]domain.Backend, len(backends))
			for _, item := range backends {
				byID[item.ID] = item
			}
			provider := &mutableSnapshotProvider{}
			provider.Set(&registry.Snapshot{
				Revision: 47, PoolsByID: map[int64]domain.ModelPool{pool.ID: pool},
				PoolsByName:  map[string]domain.ModelPool{pool.PublicModelName: pool},
				BackendsByID: byID, BackendsByPool: map[int64][]domain.Backend{pool.ID: backends},
			})
			runtime := newPoolRuntime(domain.PoolRuntime{
				PoolID: pool.ID, State: domain.PoolNormal, AvailableBackends: len(backends),
				GatewayInflight: tt.gatewayInflight, TotalWaiting: tt.totalWaiting,
			}, runtimes)
			service := gateway.New(gateway.Dependencies{
				Registry: provider, Runtime: runtime, Now: func() time.Time { return poolTestNow },
				LookupEnv: func(name string) (string, bool) {
					return tt.resolvedSecret, name == tt.upstreamAPIKeyEnv && tt.resolvedSecret != ""
				},
			})

			readiness := service.InferenceReadiness()
			if readiness.Status != tt.wantStatus || readiness.Revision != 47 ||
				readiness.PoolAvailability != tt.wantPools || readiness.BackendAvailability != tt.wantBackends {
				t.Fatalf("InferenceReadiness() = %+v, want status=%q revision=47 pools=%d backends=%d", readiness, tt.wantStatus, tt.wantPools, tt.wantBackends)
			}
		})
	}
}

type poolServiceOptions struct {
	maximum                   int
	maxWaiting                int
	totalWaiting              float64
	priority                  domain.PriorityClass
	maxConcurrency            int
	forwarder                 gateway.Forwarder
	observer                  gateway.Observer
	runtime                   *poolRuntimeStub
	backends                  []domain.Backend
	publishPoolOnSnapshot     *domain.ModelPool
	mutatePoolOnEverySnapshot func(domain.ModelPool) domain.ModelPool
}

func newPoolService(t *testing.T, options poolServiceOptions) (*gateway.Service, gateway.ForwardRequest, *poolRuntimeStub) {
	t.Helper()
	secret := []byte(strings.Repeat("s", 32))
	rawKey := "llmgw_abcdefghijklmnopqrstuvwxyz012345"
	priority := options.priority
	if priority == "" {
		priority = domain.PriorityHigh
	}
	maximum := options.maxConcurrency
	if maximum == 0 {
		maximum = 4
	}
	client := domain.Client{ID: 1, Name: "client", Enabled: true, PriorityClass: priority, MaxConcurrency: maximum}
	pool := domain.ModelPool{
		ID: 10, PublicModelName: "public-model", UpstreamModelName: "upstream-model", Enabled: true,
		MaxGatewayInflight: options.maximum, MaxWaiting: options.maxWaiting,
	}
	key := domain.APIKey{ID: 2, ClientID: client.ID, Prefix: rawKey[:12], SecretHash: apikey.Digest(secret, rawKey)}
	backends := options.backends
	if len(backends) == 0 {
		backends = []domain.Backend{retryBackend(20, "gpu-20", "http://gpu-20.invalid")}
	}
	provider := &mutableSnapshotProvider{}
	provider.Set(testSnapshot(client, key, pool, backends))
	runtime := options.runtime
	if runtime == nil {
		backendRuntimes := make([]domain.BackendRuntime, 0, len(backends))
		for _, backend := range backends {
			backendRuntimes = append(backendRuntimes, domain.BackendRuntime{
				BackendID: backend.ID, Healthy: true, MetricsFresh: true, Pressure: .1, CircuitAvailable: true,
			})
		}
		runtime = newPoolRuntime(domain.PoolRuntime{
			PoolID: pool.ID, State: domain.PoolNormal, AvailableBackends: len(backends), TotalWaiting: options.totalWaiting,
		}, backendRuntimes)
	}
	if options.publishPoolOnSnapshot != nil {
		updatedPool := *options.publishPoolOnSnapshot
		runtime.SetPoolSnapshotHook(func() {
			updated := testSnapshot(client, key, updatedPool, backends)
			updated.Revision = 2
			provider.Set(updated)
		})
	}
	if options.mutatePoolOnEverySnapshot != nil {
		var revision int64 = 1
		runtime.SetRepeatingPoolSnapshotHook(func() {
			updatedPool := options.mutatePoolOnEverySnapshot(pool)
			updated := testSnapshot(client, key, updatedPool, backends)
			revision++
			updated.Revision = revision
			provider.Set(updated)
		})
	}
	forwarder := options.forwarder
	if forwarder == nil {
		forwarder = &poolCompletionForwarder{}
	}
	service := gateway.New(gateway.Dependencies{
		Registry: provider, HMACSecret: secret, Limiter: admission.NewLimiter(), Runtime: runtime,
		Router: routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0)), Forwarder: forwarder,
		Observer: options.observer, Now: func() time.Time { return poolTestNow }, RetryAfter: 2 * time.Second,
	})
	return service, gateway.ForwardRequest{
		Method: http.MethodPost, Path: "/v1/completions", Headers: make(http.Header),
		Body: []byte(`{"model":"public-model"}`), APIKey: rawKey,
	}, runtime
}

type poolRuntimeStub struct {
	mu                      sync.Mutex
	pool                    domain.PoolRuntime
	backends                map[int64]domain.BackendRuntime
	poolInflight            int
	poolAcquisitions        int
	poolSnapshotHook        func()
	poolSnapshotHookRepeats bool
}

func newPoolRuntime(pool domain.PoolRuntime, backends []domain.BackendRuntime) *poolRuntimeStub {
	values := make(map[int64]domain.BackendRuntime, len(backends))
	for _, backend := range backends {
		values[backend.BackendID] = backend
	}
	return &poolRuntimeStub{pool: pool, backends: values}
}

func (r *poolRuntimeStub) PoolSnapshot(int64, time.Time) domain.PoolRuntime {
	r.mu.Lock()
	value := r.pool
	value.GatewayInflight = r.poolInflight
	hook := r.poolSnapshotHook
	if !r.poolSnapshotHookRepeats {
		r.poolSnapshotHook = nil
	}
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
	return value
}

func (r *poolRuntimeStub) SetPoolSnapshotHook(hook func()) {
	r.mu.Lock()
	r.poolSnapshotHook = hook
	r.poolSnapshotHookRepeats = false
	r.mu.Unlock()
}

func (r *poolRuntimeStub) SetRepeatingPoolSnapshotHook(hook func()) {
	r.mu.Lock()
	r.poolSnapshotHook = hook
	r.poolSnapshotHookRepeats = true
	r.mu.Unlock()
}

func (r *poolRuntimeStub) AcquirePool(_ int64, maximum int) (func(), bool) {
	r.mu.Lock()
	if maximum > 0 && r.poolInflight >= maximum {
		r.mu.Unlock()
		return nil, false
	}
	r.poolInflight++
	r.poolAcquisitions++
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			if r.poolInflight > 0 {
				r.poolInflight--
			}
			r.mu.Unlock()
		})
	}, true
}

func (r *poolRuntimeStub) Snapshot(backendID int64, _ time.Time) domain.BackendRuntime {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.backends[backendID]
}

func (r *poolRuntimeStub) AcquireBackend(expected domain.Backend, _ time.Time) (func(domain.InferenceOutcome), bool) {
	r.mu.Lock()
	backendID := expected.ID
	runtime, ok := r.backends[backendID]
	if !ok {
		r.mu.Unlock()
		return nil, false
	}
	runtime.GatewayInflight++
	r.backends[backendID] = runtime
	r.mu.Unlock()
	var once sync.Once
	return func(domain.InferenceOutcome) {
		once.Do(func() {
			r.mu.Lock()
			value := r.backends[backendID]
			if value.GatewayInflight > 0 {
				value.GatewayInflight--
			}
			r.backends[backendID] = value
			r.mu.Unlock()
		})
	}, true
}

func (r *poolRuntimeStub) PoolInflight() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.poolInflight
}

func (r *poolRuntimeStub) PoolAcquisitions() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.poolAcquisitions
}

type poolForwardResult struct {
	result proxy.Result
	apiErr *gateway.APIError
}

func forwardPoolAsync(service *gateway.Service, ctx context.Context, request gateway.ForwardRequest) <-chan poolForwardResult {
	done := make(chan poolForwardResult, 1)
	go func() {
		result, _, apiErr := service.Forward(ctx, httptest.NewRecorder(), request)
		done <- poolForwardResult{result: result, apiErr: apiErr}
	}()
	return done
}

func waitPoolResult(t *testing.T, done <-chan poolForwardResult, unblock func()) poolForwardResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(200 * time.Millisecond):
		unblock()
		return <-done
	}
}

type poolBlockingForwarder struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newPoolBlockingForwarder() *poolBlockingForwarder {
	return &poolBlockingForwarder{started: make(chan struct{}, 8), release: make(chan struct{})}
}

func (f *poolBlockingForwarder) Forward(ctx context.Context, writer http.ResponseWriter, request proxy.Request) proxy.Result {
	f.started <- struct{}{}
	select {
	case <-f.release:
		request.Target.Complete(domain.InferenceSuccess)
		writer.WriteHeader(http.StatusOK)
		return proxy.Result{BackendID: request.Target.Backend.ID, Status: http.StatusOK}
	case <-ctx.Done():
		request.Target.Complete(domain.InferenceNeutral)
		return proxy.Result{BackendID: request.Target.Backend.ID, Cancelled: true, Err: ctx.Err()}
	}
}

func (f *poolBlockingForwarder) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-f.started:
	case <-time.After(time.Second):
		t.Fatal("forwarder did not start")
	}
}

func (f *poolBlockingForwarder) releaseAll() {
	f.once.Do(func() { close(f.release) })
}

type poolCompletionForwarder struct{}

func (*poolCompletionForwarder) Forward(_ context.Context, writer http.ResponseWriter, request proxy.Request) proxy.Result {
	request.Target.Complete(domain.InferenceSuccess)
	writer.WriteHeader(http.StatusOK)
	return proxy.Result{BackendID: request.Target.Backend.ID, Status: http.StatusOK}
}

type poolErrorForwarder struct{}

func (poolErrorForwarder) Forward(_ context.Context, _ http.ResponseWriter, request proxy.Request) proxy.Result {
	request.Target.Complete(domain.InferenceFailure)
	return proxy.Result{BackendID: request.Target.Backend.ID, Err: errors.New("upstream failed")}
}

type poolRetryForwarder struct {
	runtime                 *poolRuntimeStub
	poolHeldDuringAlternate bool
}

func (f *poolRetryForwarder) Forward(_ context.Context, writer http.ResponseWriter, request proxy.Request) proxy.Result {
	request.Target.Complete(domain.InferenceFailure)
	alternate, err := request.SelectAlternate(map[int64]struct{}{request.Target.Backend.ID: {}})
	if err != nil {
		return proxy.Result{Err: err}
	}
	f.poolHeldDuringAlternate = f.runtime.PoolInflight() == 1
	alternate.Complete(domain.InferenceSuccess)
	writer.WriteHeader(http.StatusOK)
	return proxy.Result{BackendID: alternate.Backend.ID, Status: http.StatusOK, RetryCount: 1}
}

func assertPoolReleased(t *testing.T, runtime *poolRuntimeStub, wantAcquisitions int) {
	t.Helper()
	if got := runtime.PoolAcquisitions(); got != wantAcquisitions {
		t.Fatalf("pool acquisitions = %d, want %d", got, wantAcquisitions)
	}
	if got := runtime.PoolInflight(); got != 0 {
		t.Fatalf("pool inflight after exit = %d, want 0", got)
	}
}
