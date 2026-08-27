package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/fakevllm"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/observability"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
	"github.com/rislanov/vllm-priority-gateway/internal/web"
)

func TestProjectedKeyUsageUpdatesRegistryAfterDurableWrite(t *testing.T) {
	destination := &keyUsageStoreStub{}
	projection := &keyUsageRegistryStub{}
	usedAt := time.Unix(1_700_000_000, 0).UTC()
	writer := projectedKeyUsageStore{destination: destination, projection: projection}
	if err := writer.TouchKeyLastUsed(context.Background(), 7, usedAt); err != nil {
		t.Fatal(err)
	}
	if destination.keyID != 7 || !destination.usedAt.Equal(usedAt) || projection.keyID != 7 || !projection.usedAt.Equal(usedAt) {
		t.Fatalf("destination=%+v projection=%+v", destination, projection)
	}
}

func TestPublishRuntimeMetricsSetsBackendAndPoolGauges(t *testing.T) {
	metrics := observability.NewMetrics()
	snapshot := &registry.Snapshot{
		PoolsByID: map[int64]domain.ModelPool{
			10: {ID: 10, PublicModelName: "qwen", Enabled: true},
		},
		BackendsByID: map[int64]domain.Backend{
			20: {ID: 20, ModelPoolID: 10, Name: "gpu-a", Enabled: true},
		},
	}
	runtime := runtimeMetricsStub{
		pool: domain.PoolRuntime{PoolID: 10, GatewayInflight: 4, TotalWaiting: 6, AvailableBackends: 1},
		backend: domain.BackendRuntime{
			BackendID: 20, CircuitState: domain.CircuitOpen, CircuitFailures: 5,
		},
	}
	publishRuntimeMetrics(metrics, snapshot, runtime, time.Unix(1_700_000_000, 0))

	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	text := response.Body.String()
	for _, sample := range []string{
		`llmgw_backend_circuit_state{backend="gpu-a",model="qwen"} 1`,
		`llmgw_backend_circuit_failures{backend="gpu-a",model="qwen"} 5`,
		`llmgw_pool_gateway_inflight{model="qwen"} 4`,
		`llmgw_pool_waiting_requests{model="qwen"} 6`,
		`llmgw_pool_available_backends{model="qwen"} 1`,
	} {
		if !strings.Contains(text, sample) {
			t.Fatalf("published metrics missing %q:\n%s", sample, text)
		}
	}
}

func TestPublishRuntimeMetricsRemovesInflightSeriesObservedBeforeFirstEmptyPublication(t *testing.T) {
	metrics := observability.NewMetrics()
	inflight := gateway.InflightEvent{
		Client: "removed-client", Model: "removed-model", Backend: "removed-backend",
		PriorityClass: domain.PriorityHigh,
	}
	metrics.ClientInflight(inflight, 1)
	metrics.BackendInflight(inflight, 1)
	metrics.ClientInflight(inflight, -1)
	metrics.BackendInflight(inflight, -1)

	publishRuntimeMetrics(metrics, &registry.Snapshot{
		Clients: map[int64]domain.Client{}, PoolsByID: map[int64]domain.ModelPool{},
		BackendsByID: map[int64]domain.Backend{}, Access: map[int64]map[int64]bool{},
	}, runtimeMetricsMapStub{}, time.Unix(1_700_000_000, 0))

	text := scrapeMetrics(t, metrics)
	for _, stale := range []string{
		`llmgw_requests_inflight{model="removed-model",priority_class="high"}`,
		`llmgw_client_inflight{client="removed-client",model="removed-model",priority_class="high"}`,
		`llmgw_backend_requests_inflight{backend="removed-backend",model="removed-model"}`,
	} {
		if strings.Contains(text, stale) {
			t.Fatalf("pre-publication inflight sample %q survived empty topology:\n%s", stale, text)
		}
	}
}

func TestPublishRuntimeMetricsRemovesRenamedAndDeletedTopologySeries(t *testing.T) {
	metrics := observability.NewMetrics()
	first := &registry.Snapshot{
		Clients: map[int64]domain.Client{
			1: {ID: 1, Name: "client-old", Enabled: true, PriorityClass: domain.PriorityHigh},
			2: {ID: 2, Name: "removed-client", Enabled: true, PriorityClass: domain.PriorityBackground},
		},
		PoolsByID: map[int64]domain.ModelPool{
			10: {ID: 10, PublicModelName: "qwen-old", Enabled: true},
			11: {ID: 11, PublicModelName: "removed-pool", Enabled: true},
		},
		BackendsByID: map[int64]domain.Backend{
			20: {ID: 20, ModelPoolID: 10, Name: "gpu-old", Enabled: true},
			21: {ID: 21, ModelPoolID: 11, Name: "removed-gpu", Enabled: true},
		},
		Access: map[int64]map[int64]bool{
			1: {10: true},
			2: {11: true},
		},
	}
	runtime := runtimeMetricsMapStub{
		pools: map[int64]domain.PoolRuntime{
			10: {PoolID: 10, GatewayInflight: 4, TotalWaiting: 6, AvailableBackends: 1},
			11: {PoolID: 11, GatewayInflight: 2, TotalWaiting: 8},
		},
		backends: map[int64]domain.BackendRuntime{
			20: {BackendID: 20, Pressure: .4, Running: 3, Waiting: 1, KVCacheUsage: .7, CircuitState: domain.CircuitOpen, CircuitFailures: 5},
			21: {BackendID: 21, CircuitState: domain.CircuitOpen, CircuitFailures: 2},
		},
	}
	at := time.Unix(1_700_000_000, 0)
	publishRuntimeMetrics(metrics, first, runtime, at)
	oldClientInflight := gateway.InflightEvent{
		Client: "client-old", Model: "qwen-old", PriorityClass: domain.PriorityHigh,
	}
	removedClientInflight := gateway.InflightEvent{
		Client: "removed-client", Model: "removed-pool", PriorityClass: domain.PriorityBackground,
	}
	metrics.ClientInflight(oldClientInflight, 1)
	metrics.ClientInflight(removedClientInflight, 1)
	metrics.ClientInflight(removedClientInflight, -1)
	oldInflight := gateway.InflightEvent{Model: "qwen-old", Backend: "gpu-old"}
	metrics.BackendInflight(oldInflight, 1)
	metrics.Complete(gateway.RequestEvent{
		Client: "client-a", Model: "qwen-old", Backend: "gpu-old",
		PriorityClass: domain.PriorityHigh, Status: http.StatusOK, Duration: time.Second,
	})

	second := &registry.Snapshot{
		Clients: map[int64]domain.Client{
			1: {ID: 1, Name: "client-new", Enabled: true, PriorityClass: domain.PriorityNormal},
		},
		PoolsByID: map[int64]domain.ModelPool{
			10: {ID: 10, PublicModelName: "qwen-new", Enabled: true},
		},
		BackendsByID: map[int64]domain.Backend{
			20: {ID: 20, ModelPoolID: 10, Name: "gpu-new", Enabled: true},
		},
		Access: map[int64]map[int64]bool{1: {10: true}},
	}
	runtime.pools[10] = domain.PoolRuntime{PoolID: 10, GatewayInflight: 1, TotalWaiting: 2, AvailableBackends: 1}
	runtime.backends[20] = domain.BackendRuntime{
		BackendID: 20, Pressure: .2, Running: 1, KVCacheUsage: .5,
		CircuitState: domain.CircuitHalfOpen, CircuitFailures: 1,
	}
	publishRuntimeMetrics(metrics, second, runtime, at.Add(time.Second))
	metrics.ClientInflight(oldClientInflight, -1)
	metrics.BackendInflight(oldInflight, -1)
	newInflight := gateway.InflightEvent{Model: "qwen-new", Backend: "gpu-new"}
	metrics.BackendInflight(newInflight, 1)
	metrics.BackendInflight(newInflight, -1)

	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	text := response.Body.String()
	for _, stale := range []string{
		`llmgw_requests_inflight{model="qwen-old",priority_class="high"}`,
		`llmgw_client_inflight{client="client-old",model="qwen-old",priority_class="high"}`,
		`llmgw_requests_inflight{model="removed-pool",priority_class="background"}`,
		`llmgw_client_inflight{client="removed-client",model="removed-pool",priority_class="background"}`,
		`llmgw_backend_requests_inflight{backend="gpu-old",model="qwen-old"}`,
		`llmgw_backend_pressure{backend="gpu-old",model="qwen-old"}`,
		`llmgw_backend_running_requests{backend="gpu-old",model="qwen-old"}`,
		`llmgw_backend_waiting_requests{backend="gpu-old",model="qwen-old"}`,
		`llmgw_backend_kv_cache_usage{backend="gpu-old",model="qwen-old"}`,
		`llmgw_backend_circuit_state{backend="gpu-old",model="qwen-old"}`,
		`llmgw_backend_circuit_failures{backend="gpu-old",model="qwen-old"}`,
		`llmgw_pool_gateway_inflight{model="qwen-old"}`,
		`llmgw_pool_waiting_requests{model="qwen-old"}`,
		`llmgw_pool_available_backends{model="qwen-old"}`,
		`llmgw_backend_circuit_state{backend="removed-gpu",model="removed-pool"}`,
		`llmgw_pool_gateway_inflight{model="removed-pool"}`,
	} {
		if strings.Contains(text, stale) {
			t.Fatalf("stale topology sample %q survived rename/removal:\n%s", stale, text)
		}
	}
	for _, current := range []string{
		`llmgw_requests_inflight{model="qwen-new",priority_class="normal"} 0`,
		`llmgw_client_inflight{client="client-new",model="qwen-new",priority_class="normal"} 0`,
		`llmgw_backend_requests_inflight{backend="gpu-new",model="qwen-new"} 0`,
		`llmgw_backend_pressure{backend="gpu-new",model="qwen-new"} 0.2`,
		`llmgw_backend_running_requests{backend="gpu-new",model="qwen-new"} 1`,
		`llmgw_backend_waiting_requests{backend="gpu-new",model="qwen-new"} 0`,
		`llmgw_backend_kv_cache_usage{backend="gpu-new",model="qwen-new"} 0.5`,
		`llmgw_backend_circuit_state{backend="gpu-new",model="qwen-new"} 2`,
		`llmgw_backend_circuit_failures{backend="gpu-new",model="qwen-new"} 1`,
		`llmgw_pool_gateway_inflight{model="qwen-new"} 1`,
		`llmgw_pool_waiting_requests{model="qwen-new"} 2`,
		`llmgw_pool_available_backends{model="qwen-new"} 1`,
		`llmgw_requests_total{client="client-a",model="qwen-old",priority_class="high",status_class="2xx"} 1`,
	} {
		if !strings.Contains(text, current) {
			t.Fatalf("current or historical sample %q missing after reconciliation:\n%s", current, text)
		}
	}
}

func scrapeMetrics(t *testing.T, metrics *observability.Metrics) string {
	t.Helper()
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return response.Body.String()
}

func TestAdminDashboardRendersPoolSafetyAndCircuitRuntime(t *testing.T) {
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "dashboard.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	pool, err := database.CreatePool(context.Background(), store.CreatePoolParams{
		PublicModelName: "qwen", UpstreamModelName: "fake-model", Enabled: true,
		MaxGatewayInflight: 17, MaxWaiting: 9,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateBackend(context.Background(), store.CreateBackendParams{
		ModelPoolID: pool.ID, Name: "gpu-a", BaseURL: "http://gpu-a.invalid", Enabled: true,
		CapacityHint: 1, RunningSoftLimit: 16,
	}); err != nil {
		t.Fatal(err)
	}
	registryValue := registry.New(database)
	if err := registryValue.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	runtime := runtimeMetricsStub{
		pool: domain.PoolRuntime{
			PoolID: pool.ID, State: domain.PoolBusy, GatewayInflight: 37,
			TotalWaiting: 41.5, AvailableBackends: 2,
		},
		backend: domain.BackendRuntime{
			State: domain.BackendHealthy, Healthy: true, MetricsFresh: true,
			CircuitState: domain.CircuitHalfOpen, CircuitAvailable: true,
		},
	}
	adminService, err := httpapi.NewAdminService(httpapi.AdminDependencies{
		Store: database, Registry: registryValue, Runtime: runtime,
		HMACSecret: []byte(strings.Repeat("h", 32)),
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := web.New(adminService)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d body=%s", response.Code, response.Body.String())
	}
	text := response.Body.String()
	for _, fragment := range []string{
		"Gateway inflight", "Waiting", "Available", ">37<", ">41.50<", ">2<", "Circuit", "half_open",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("dashboard missing server-rendered %q:\n%s", fragment, text)
		}
	}
}

type runtimeMetricsStub struct {
	pool    domain.PoolRuntime
	backend domain.BackendRuntime
}

func (s runtimeMetricsStub) PoolSnapshot(int64, time.Time) domain.PoolRuntime { return s.pool }
func (s runtimeMetricsStub) Snapshot(int64, time.Time) domain.BackendRuntime  { return s.backend }
func (runtimeMetricsStub) Reconcile([]domain.Backend) error                   { return nil }

type runtimeMetricsMapStub struct {
	pools    map[int64]domain.PoolRuntime
	backends map[int64]domain.BackendRuntime
}

func (s runtimeMetricsMapStub) PoolSnapshot(id int64, _ time.Time) domain.PoolRuntime {
	return s.pools[id]
}

func (s runtimeMetricsMapStub) Snapshot(id int64, _ time.Time) domain.BackendRuntime {
	return s.backends[id]
}

func TestRunServesHealthAndShutsDownGracefully(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	environment := validEnvironment(filepath.Join(t.TempDir(), "gateway.db"))
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{})
	}()

	client := &http.Client{Timeout: 200 * time.Millisecond}
	url := "http://" + listener.Addr().String() + "/healthz"
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, requestErr := client.Get(url)
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("gateway did not become healthy")
		}
		time.Sleep(10 * time.Millisecond)
	}
	readyResponse, err := client.Get("http://" + listener.Addr().String() + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	var readiness struct {
		Status              string `json:"status"`
		Revision            int64  `json:"revision"`
		BackendAvailability int    `json:"backendAvailability"`
	}
	if err := json.NewDecoder(readyResponse.Body).Decode(&readiness); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, readyResponse.Body)
	readyResponse.Body.Close()
	if readyResponse.StatusCode != http.StatusOK || readiness.Status != "ready" || readiness.Revision != 0 || readiness.BackendAvailability != 0 {
		t.Fatalf("readiness = %d %+v", readyResponse.StatusCode, readiness)
	}
	inferenceResponse, err := client.Get("http://" + listener.Addr().String() + "/inference-readyz")
	if err != nil {
		t.Fatal(err)
	}
	var inferenceReadiness struct {
		Status              string `json:"status"`
		Revision            int64  `json:"revision"`
		PoolAvailability    int    `json:"poolAvailability"`
		BackendAvailability int    `json:"backendAvailability"`
	}
	if err := json.NewDecoder(inferenceResponse.Body).Decode(&inferenceReadiness); err != nil {
		t.Fatal(err)
	}
	inferenceResponse.Body.Close()
	if inferenceResponse.StatusCode != http.StatusServiceUnavailable || inferenceReadiness.Status != "unavailable" ||
		inferenceReadiness.Revision != 0 || inferenceReadiness.PoolAvailability != 0 || inferenceReadiness.BackendAvailability != 0 {
		t.Fatalf("inference readiness = %d %+v", inferenceResponse.StatusCode, inferenceReadiness)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("gateway did not shut down within deadline")
	}
}

func TestRunLetsActiveStreamFinishInsideGracePeriod(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{Tokens: []string{"one", "two"}, TokenDelay: 100 * time.Millisecond})
	upstream := httptest.NewServer(fake.Handler())
	defer upstream.Close()
	databasePath := filepath.Join(t.TempDir(), "gateway.db")
	secret := strings.Repeat("h", 32)
	clientKey := seedGatewayDatabase(t, databasePath, upstream.URL, []byte(secret))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	environment := validEnvironment(databasePath)
	environment["LLMGW_HEALTH_INTERVAL"] = "10ms"
	environment["LLMGW_METRICS_INTERVAL"] = "10ms"
	environment["LLMGW_UNHEALTHY_AFTER"] = "1"
	environment["LLMGW_RECOVERY_AFTER"] = "1"
	environment["LLMGW_SHUTDOWN_GRACE_PERIOD"] = "1s"
	done := make(chan error, 1)
	go func() { done <- run(ctx, mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{}) }()

	baseURL := "http://" + listener.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}
	var response *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for {
		request, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/completions", strings.NewReader(`{"model":"qwen","stream":true}`))
		request.Header.Set("Authorization", "Bearer "+clientKey)
		response, err = client.Do(request)
		if err == nil && response.StatusCode == http.StatusOK {
			break
		}
		if response != nil {
			response.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream did not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	reader := bufio.NewReader(response.Body)
	first, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(first, "one") {
		t.Fatalf("first stream frame = %q err=%v", first, err)
	}
	cancel()
	rest, err := io.ReadAll(reader)
	response.Body.Close()
	if err != nil || !strings.Contains(string(rest), "two") || !strings.Contains(string(rest), "[DONE]") {
		t.Fatalf("stream remainder = %q err=%v", rest, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gateway did not finish after active stream")
	}
	if snapshot := fake.Snapshot(); snapshot.ActiveRequests != 0 {
		t.Fatalf("upstream requests still active: %+v", snapshot)
	}
	database, err := store.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatalf("database remained locked after shutdown: %v", err)
	}
	_ = database.Close()
}

func TestRunForceClosesActiveStreamAfterGracePeriod(t *testing.T) {
	fake := fakevllm.New()
	fake.SetState(fakevllm.State{Tokens: []string{"one", "two", "three"}, TokenDelay: 500 * time.Millisecond})
	upstream := httptest.NewServer(fake.Handler())
	defer upstream.Close()
	databasePath := filepath.Join(t.TempDir(), "gateway.db")
	secret := strings.Repeat("h", 32)
	clientKey := seedGatewayDatabase(t, databasePath, upstream.URL, []byte(secret))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	environment := validEnvironment(databasePath)
	environment["LLMGW_HEALTH_INTERVAL"] = "10ms"
	environment["LLMGW_METRICS_INTERVAL"] = "10ms"
	environment["LLMGW_UNHEALTHY_AFTER"] = "1"
	environment["LLMGW_RECOVERY_AFTER"] = "1"
	environment["LLMGW_SHUTDOWN_GRACE_PERIOD"] = "30ms"
	done := make(chan error, 1)
	go func() { done <- run(ctx, mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{}) }()

	baseURL := "http://" + listener.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}
	var response *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for {
		request, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/completions", strings.NewReader(`{"model":"qwen","stream":true}`))
		request.Header.Set("Authorization", "Bearer "+clientKey)
		response, err = client.Do(request)
		if err == nil && response.StatusCode == http.StatusOK {
			break
		}
		if response != nil {
			response.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream did not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	reader := bufio.NewReader(response.Body)
	first, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(first, "one") {
		t.Fatalf("first stream frame = %q err=%v", first, err)
	}
	cancel()
	_, _ = io.ReadAll(reader)
	response.Body.Close()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "graceful HTTP shutdown") {
			t.Fatalf("run error = %v, want forced shutdown error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gateway did not force-close after grace period")
	}
	deadline = time.Now().Add(time.Second)
	for fake.Snapshot().ActiveRequests != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if snapshot := fake.Snapshot(); snapshot.ActiveRequests != 0 {
		t.Fatalf("forced shutdown left upstream request active: %+v", snapshot)
	}
	database, err := store.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatalf("database remained locked after forced shutdown: %v", err)
	}
	_ = database.Close()
}

func TestRunRejectsMissingHMACSecretBeforeServing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	environment := validEnvironment(filepath.Join(t.TempDir(), "gateway.db"))
	delete(environment, "LLMGW_API_KEY_HMAC_SECRET")
	err = run(context.Background(), mapLookup(environment), listener, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "LLMGW_API_KEY_HMAC_SECRET") {
		t.Fatalf("run error = %v", err)
	}
}

func validEnvironment(databasePath string) map[string]string {
	return map[string]string{
		"LLMGW_LISTEN_ADDRESS":        "127.0.0.1:0",
		"LLMGW_DATABASE_PATH":         databasePath,
		"LLMGW_ADMIN_USERNAME":        "operator",
		"LLMGW_ADMIN_PASSWORD":        "correct horse battery staple",
		"LLMGW_API_KEY_HMAC_SECRET":   strings.Repeat("h", 32),
		"LLMGW_SHUTDOWN_GRACE_PERIOD": "500ms",
	}
}

func seedGatewayDatabase(t *testing.T, path, upstreamURL string, hmacSecret []byte) string {
	t.Helper()
	database, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := database.CreatePool(context.Background(), store.CreatePoolParams{PublicModelName: "qwen", UpstreamModelName: "fake-model", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateBackend(context.Background(), store.CreateBackendParams{ModelPoolID: pool.ID, Name: "gpu-a", BaseURL: upstreamURL, Enabled: true, CapacityHint: 1, RunningSoftLimit: 16}); err != nil {
		t.Fatal(err)
	}
	client, err := database.CreateClient(context.Background(), store.CreateClientParams{Name: "stream-client", Enabled: true, PriorityClass: domain.PriorityHigh, VLLMPriority: -10, MaxConcurrency: 1, ModelPoolIDs: []int64{pool.ID}})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := apikey.Generate(bytes.NewReader(bytes.Repeat([]byte{7}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateAPIKey(context.Background(), store.CreateAPIKeyParams{ClientID: client.ID, Prefix: plain.Prefix, SecretHash: apikey.Digest(hmacSecret, plain.Value)}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	return plain.Value
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

type keyUsageStoreStub struct {
	keyID  int64
	usedAt time.Time
}

func (s *keyUsageStoreStub) TouchKeyLastUsed(_ context.Context, keyID int64, usedAt time.Time) error {
	s.keyID, s.usedAt = keyID, usedAt
	return nil
}

type keyUsageRegistryStub struct {
	keyID  int64
	usedAt time.Time
}

func (s *keyUsageRegistryStub) MarkKeyUsed(keyID int64, usedAt time.Time) bool {
	s.keyID, s.usedAt = keyID, usedAt
	return true
}
