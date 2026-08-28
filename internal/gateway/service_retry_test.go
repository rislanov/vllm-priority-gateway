package gateway_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
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
		20: {BackendID: 20, Healthy: true, MetricsFresh: true, Pressure: .1, CircuitAvailable: true},
		21: {BackendID: 21, Healthy: true, MetricsFresh: true, Pressure: .2, CircuitAvailable: true},
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

	_, _, apiErr := service.Forward(context.Background(), response, gateway.ForwardRequest{
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
		20: {BackendID: 20, Healthy: true, MetricsFresh: true, Pressure: .1, CircuitAvailable: true},
		21: {BackendID: 21, Healthy: true, MetricsFresh: true, Pressure: .2, CircuitAvailable: true},
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

	_, _, apiErr := service.Forward(context.Background(), response, gateway.ForwardRequest{
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

func TestForwardSkipsCircuitOpenBackend(t *testing.T) {
	backends := []domain.Backend{
		retryBackend(20, "gpu-20", "http://gpu-20.invalid"),
		retryBackend(21, "gpu-21", "http://gpu-21.invalid"),
	}
	runtime := &recordingRuntime{values: map[int64]domain.BackendRuntime{
		20: {BackendID: 20, Healthy: true, MetricsFresh: true, Pressure: .1, CircuitState: domain.CircuitOpen, CircuitAvailable: false},
		21: {BackendID: 21, Healthy: true, MetricsFresh: true, Pressure: .2, CircuitState: domain.CircuitClosed, CircuitAvailable: true},
	}}
	forwarder := &completionForwarder{}
	service, request := retryService(backends, runtime, forwarder, nil, time.Now)

	result, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
	if apiErr != nil || result.Err != nil {
		t.Fatalf("Forward() result=%+v API error=%+v", result, apiErr)
	}
	if forwarder.BackendID() != 21 {
		t.Fatalf("selected backend = %d, want 21", forwarder.BackendID())
	}
	if got := runtime.Events(); strings.Join(got, ",") != "acquire-21,complete-21:success" {
		t.Fatalf("runtime events = %v", got)
	}
}

func TestForwardRetriesSelectionWhenBackendAcquisitionRaces(t *testing.T) {
	backends := []domain.Backend{
		retryBackend(20, "gpu-20", "http://gpu-20.invalid"),
		retryBackend(21, "gpu-21", "http://gpu-21.invalid"),
	}
	runtime := &recordingRuntime{
		values: map[int64]domain.BackendRuntime{
			20: {BackendID: 20, Healthy: true, MetricsFresh: true, Pressure: .1, CircuitAvailable: true},
			21: {BackendID: 21, Healthy: true, MetricsFresh: true, Pressure: .2, CircuitAvailable: true},
		},
		rejectAcquisitions: map[int64]int{20: 1},
	}
	forwarder := &completionForwarder{}
	service, request := retryService(backends, runtime, forwarder, nil, time.Now)

	result, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
	if apiErr != nil || result.Err != nil {
		t.Fatalf("Forward() result=%+v API error=%+v", result, apiErr)
	}
	if forwarder.BackendID() != 21 {
		t.Fatalf("selected backend = %d, want 21 after backend 20 acquisition rejection", forwarder.BackendID())
	}
	want := "acquire-20:rejected,acquire-21,complete-21:success"
	if got := strings.Join(runtime.Events(), ","); got != want {
		t.Fatalf("runtime events = %q, want %q", got, want)
	}
}

func TestForwardRejectsPublishedBackendUntilMatchingRuntimeIsHealthy(t *testing.T) {
	oldBackend := retryBackend(20, "gpu-20", "http://old-gpu.invalid")
	newBackend := oldBackend
	newBackend.BaseURL = "http://new-gpu.invalid"
	runtime := &recordingRuntime{
		values: map[int64]domain.BackendRuntime{
			20: {BackendID: 20, Healthy: true, MetricsFresh: true, Pressure: .1, CircuitAvailable: true},
		},
		identities: map[int64]domain.Backend{20: oldBackend},
	}
	forwarder := &completionForwarder{}
	service, request := retryService([]domain.Backend{newBackend}, runtime, forwarder, nil, time.Now)

	_, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
	if apiErr == nil || apiErr.Code != "backend_unavailable" {
		t.Fatalf("identity mismatch API error = %+v, want backend_unavailable", apiErr)
	}
	if got := forwarder.Calls(); got != 0 {
		t.Fatalf("forward calls before runtime reconcile = %d, want 0", got)
	}
	if got := strings.Join(runtime.Events(), ","); got != "acquire-20:identity-mismatch" {
		t.Fatalf("runtime events before reconcile = %q", got)
	}

	runtime.SetBackend(newBackend, domain.BackendRuntime{BackendID: 20, CircuitAvailable: true})
	_, _, apiErr = service.Forward(context.Background(), httptest.NewRecorder(), request)
	if apiErr == nil || apiErr.Code != "backend_unavailable" {
		t.Fatalf("unhealthy reconciled runtime API error = %+v, want backend_unavailable", apiErr)
	}
	if got := forwarder.Calls(); got != 0 {
		t.Fatalf("forward calls before reconciled runtime became healthy = %d, want 0", got)
	}

	runtime.SetBackend(newBackend, domain.BackendRuntime{
		BackendID: 20, Healthy: true, MetricsFresh: true, Pressure: .1, CircuitAvailable: true,
	})
	result, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
	if apiErr != nil || result.Status != http.StatusOK {
		t.Fatalf("healthy reconciled result=%+v API error=%+v", result, apiErr)
	}
	if got := forwarder.Calls(); got != 1 {
		t.Fatalf("forward calls after healthy reconcile = %d, want 1", got)
	}
}

func TestForwardCompletesFailedBackendBeforeSelectingAlternateAndBalancesObserver(t *testing.T) {
	backends := []domain.Backend{
		retryBackend(20, "gpu-20", "http://gpu-20.invalid"),
		retryBackend(21, "gpu-21", "http://gpu-21.invalid"),
	}
	runtime := &recordingRuntime{values: map[int64]domain.BackendRuntime{
		20: {BackendID: 20, Healthy: true, MetricsFresh: true, Pressure: .1, CircuitAvailable: true},
		21: {BackendID: 21, Healthy: true, MetricsFresh: true, Pressure: .2, CircuitAvailable: true},
	}}
	observer := &backendRecordingObserver{}
	forwarder := &retryProbeForwarder{beforeAlternate: func() {}}
	service, request := retryService(backends, runtime, forwarder, observer, time.Now)

	_, _, apiErr := service.Forward(context.Background(), httptest.NewRecorder(), request)
	if apiErr != nil {
		t.Fatalf("Forward() API error = %+v", apiErr)
	}
	wantEvents := "acquire-20,complete-20:failure,acquire-21,complete-21:success"
	if got := strings.Join(runtime.Events(), ","); got != wantEvents {
		t.Fatalf("runtime events = %q, want %q", got, wantEvents)
	}
	if got := observer.Deltas(); strings.Join(got, ",") != "gpu-20:+1,gpu-20:-1,gpu-21:+1,gpu-21:-1" {
		t.Fatalf("backend observer deltas = %v", got)
	}
	if event := observer.Event(); event.Backend != "gpu-21" || event.Status != http.StatusOK {
		t.Fatalf("request event = %+v, want final backend gpu-21", event)
	}
}

func TestForwardRecordsHTTP5xxFailureWithoutRetry(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{HTTPStatus: http.StatusServiceUnavailable, HTTPBody: `{"error":"busy"}`})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	backend := retryBackend(20, "gpu-20", server.URL)
	runtime := &recordingRuntime{values: map[int64]domain.BackendRuntime{
		20: {BackendID: 20, Healthy: true, MetricsFresh: true, Pressure: .1, CircuitAvailable: true},
	}}
	service, request := retryService([]domain.Backend{backend}, runtime, proxy.New(server.Client()), nil, time.Now)
	response := httptest.NewRecorder()

	result, _, apiErr := service.Forward(context.Background(), response, request)
	if apiErr != nil || result.Err != nil || result.Status != http.StatusServiceUnavailable || result.RetryCount != 0 {
		t.Fatalf("Forward() result=%+v API error=%+v", result, apiErr)
	}
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != `{"error":"busy"}` {
		t.Fatalf("response = %d %q", response.Code, response.Body.String())
	}
	if got := strings.Join(runtime.Events(), ","); got != "acquire-20,complete-20:failure" {
		t.Fatalf("runtime events = %q", got)
	}
	if requests := len(fake.Snapshot().Requests); requests != 1 {
		t.Fatalf("upstream requests = %d, want 1", requests)
	}
}

func TestForwardRecordsCancellationNeutralAndBalancesObserver(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{TTFT: 10 * time.Second})
	server := httptest.NewServer(fake.Handler())
	defer server.Close()
	backend := retryBackend(20, "gpu-20", server.URL)
	runtime := &recordingRuntime{values: map[int64]domain.BackendRuntime{
		20: {BackendID: 20, Healthy: true, MetricsFresh: true, Pressure: .1, CircuitAvailable: true},
	}}
	observer := &backendRecordingObserver{}
	service, request := retryService([]domain.Backend{backend}, runtime, proxy.New(server.Client()), observer, time.Now)
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct {
		result proxy.Result
		err    *gateway.APIError
	}, 1)
	go func() {
		result, _, apiErr := service.Forward(ctx, httptest.NewRecorder(), request)
		finished <- struct {
			result proxy.Result
			err    *gateway.APIError
		}{result: result, err: apiErr}
	}()
	waitForBackendRequest(t, fake)
	cancel()
	completed := <-finished
	if completed.err != nil || !completed.result.Cancelled || !errors.Is(completed.result.Err, context.Canceled) {
		t.Fatalf("Forward() result=%+v API error=%+v", completed.result, completed.err)
	}
	if got := strings.Join(runtime.Events(), ","); got != "acquire-20,complete-20:neutral" {
		t.Fatalf("runtime events = %q", got)
	}
	if got := observer.Deltas(); strings.Join(got, ",") != "gpu-20:+1,gpu-20:-1" {
		t.Fatalf("backend observer deltas = %v", got)
	}
	if inflight := runtime.Inflight(20); inflight != 0 {
		t.Fatalf("runtime inflight = %d, want 0", inflight)
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
	mu                 sync.Mutex
	values             map[int64]domain.BackendRuntime
	identities         map[int64]domain.Backend
	snapshotTimes      []time.Time
	rejectAcquisitions map[int64]int
	events             []string
	poolInflight       map[int64]int
}

func (r *recordingRuntime) PoolSnapshot(poolID int64, _ time.Time) domain.PoolRuntime {
	return domain.PoolRuntime{PoolID: poolID, State: domain.PoolNormal, AvailableBackends: len(r.values)}
}

func (r *recordingRuntime) AcquirePool(poolID int64, maximum int) (func(), bool) {
	r.mu.Lock()
	if r.poolInflight == nil {
		r.poolInflight = make(map[int64]int)
	}
	if maximum > 0 && r.poolInflight[poolID] >= maximum {
		r.mu.Unlock()
		return nil, false
	}
	r.poolInflight[poolID]++
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			if r.poolInflight[poolID] > 0 {
				r.poolInflight[poolID]--
			}
			r.mu.Unlock()
		})
	}, true
}

func (r *recordingRuntime) Snapshot(backendID int64, at time.Time) domain.BackendRuntime {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshotTimes = append(r.snapshotTimes, at)
	return r.values[backendID]
}

func (r *recordingRuntime) AcquireBackend(expected domain.Backend, _ time.Time) (func(domain.InferenceOutcome), bool) {
	r.mu.Lock()
	backendID := expected.ID
	runtime, exists := r.values[backendID]
	if identity, managed := r.identities[backendID]; managed && identity != expected {
		r.events = append(r.events, "acquire-"+strconv.FormatInt(backendID, 10)+":identity-mismatch")
		r.mu.Unlock()
		return nil, false
	}
	if exists && r.rejectAcquisitions[backendID] > 0 {
		r.rejectAcquisitions[backendID]--
		r.events = append(r.events, "acquire-"+strconv.FormatInt(backendID, 10)+":rejected")
		r.mu.Unlock()
		return nil, false
	}
	if exists {
		runtime.GatewayInflight++
		r.values[backendID] = runtime
		r.events = append(r.events, "acquire-"+strconv.FormatInt(backendID, 10))
	}
	r.mu.Unlock()
	if !exists {
		return nil, false
	}
	var once sync.Once
	return func(outcome domain.InferenceOutcome) {
		once.Do(func() {
			r.mu.Lock()
			value := r.values[backendID]
			value.GatewayInflight--
			r.values[backendID] = value
			r.events = append(r.events, "complete-"+strconv.FormatInt(backendID, 10)+":"+string(outcome))
			r.mu.Unlock()
		})
	}, true
}

func (r *recordingRuntime) SetBackend(backend domain.Backend, runtime domain.BackendRuntime) {
	r.mu.Lock()
	if r.identities == nil {
		r.identities = make(map[int64]domain.Backend)
	}
	r.identities[backend.ID] = backend
	r.values[backend.ID] = runtime
	r.mu.Unlock()
}

func (r *recordingRuntime) SnapshotTimes() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.snapshotTimes...)
}

func (r *recordingRuntime) Events() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *recordingRuntime) Inflight(backendID int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.values[backendID].GatewayInflight
}

type retryProbeForwarder struct {
	beforeAlternate  func()
	initialBackend   int64
	alternateBackend int64
	alternateErr     error
	result           proxy.Result
}

func (f *retryProbeForwarder) Forward(_ context.Context, writer http.ResponseWriter, request proxy.Request) proxy.Result {
	f.initialBackend = request.Target.Backend.ID
	request.Target.Complete(domain.InferenceFailure)
	f.beforeAlternate()
	alternate, err := request.SelectAlternate(map[int64]struct{}{request.Target.Backend.ID: {}})
	f.alternateErr = err
	if err == nil {
		f.alternateBackend = alternate.Backend.ID
		alternate.Complete(domain.InferenceSuccess)
	}
	result := f.result
	if result.Status == 0 {
		result.Status = http.StatusOK
	}
	if result.BackendID == 0 {
		result.BackendID = request.Target.Backend.ID
		if f.alternateBackend != 0 {
			result.BackendID = f.alternateBackend
		}
	}
	writer.WriteHeader(result.Status)
	return result
}

type completionForwarder struct {
	mu        sync.Mutex
	backendID int64
	calls     int
}

func (f *completionForwarder) Forward(_ context.Context, writer http.ResponseWriter, request proxy.Request) proxy.Result {
	f.mu.Lock()
	f.backendID = request.Target.Backend.ID
	f.calls++
	f.mu.Unlock()
	request.Target.Complete(domain.InferenceSuccess)
	writer.WriteHeader(http.StatusOK)
	return proxy.Result{BackendID: request.Target.Backend.ID, Status: http.StatusOK}
}

func (f *completionForwarder) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *completionForwarder) BackendID() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.backendID
}

type backendRecordingObserver struct {
	mu     sync.Mutex
	deltas []string
	event  gateway.RequestEvent
}

func (*backendRecordingObserver) ClientInflight(gateway.InflightEvent, int) {}

func (o *backendRecordingObserver) BackendInflight(event gateway.InflightEvent, delta int) {
	o.mu.Lock()
	o.deltas = append(o.deltas, event.Backend+":"+fmt.Sprintf("%+d", delta))
	o.mu.Unlock()
}

func (o *backendRecordingObserver) Complete(event gateway.RequestEvent) {
	o.mu.Lock()
	o.event = event
	o.mu.Unlock()
}

func (o *backendRecordingObserver) Deltas() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.deltas...)
}

func (o *backendRecordingObserver) Event() gateway.RequestEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.event
}

func retryBackend(id int64, name, baseURL string) domain.Backend {
	return domain.Backend{
		ID: id, ModelPoolID: 10, Name: name, BaseURL: baseURL,
		Enabled: true, CapacityHint: 1, RunningSoftLimit: 8,
	}
}

func retryService(backends []domain.Backend, runtime *recordingRuntime, forwarder gateway.Forwarder, observer gateway.Observer, now func() time.Time) (*gateway.Service, gateway.ForwardRequest) {
	secret := []byte(strings.Repeat("s", 32))
	rawKey := "llmgw_abcdefghijklmnopqrstuvwxyz012345"
	client := domain.Client{ID: 1, Name: "client", Enabled: true, PriorityClass: domain.PriorityHigh, MaxConcurrency: 2}
	pool := domain.ModelPool{ID: 10, PublicModelName: "public-model", UpstreamModelName: "upstream-model", Enabled: true}
	key := domain.APIKey{ID: 2, ClientID: client.ID, Prefix: rawKey[:12], SecretHash: apikey.Digest(secret, rawKey)}
	provider := &mutableSnapshotProvider{}
	provider.Set(testSnapshot(client, key, pool, backends))
	service := gateway.New(gateway.Dependencies{
		Registry: provider, HMACSecret: secret, Limiter: admission.NewLimiter(), Runtime: runtime,
		Router: routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0)), Forwarder: forwarder,
		Observer: observer, Now: now,
	})
	return service, gateway.ForwardRequest{
		Method: http.MethodPost, Path: "/v1/completions", Headers: make(http.Header),
		Body: []byte(`{"model":"public-model"}`), APIKey: rawKey,
	}
}

func waitForBackendRequest(t *testing.T, fake *fakevllm.Server) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fake.Snapshot().ActiveRequests == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("backend request did not start")
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
