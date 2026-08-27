package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFindDuplicateClientKeyDoesNotExposeSecret(t *testing.T) {
	secret := "llmgw_do-not-print-this-secret"
	first, second, duplicate := findDuplicateClientKey([]namedClientKey{
		{name: "High probe", value: secret},
		{name: "Critical probe", value: "llmgw_distinct"},
		{name: "Low probe 1", value: secret},
	})
	if !duplicate {
		t.Fatal("duplicate client key was not detected")
	}
	if first != "High probe" || second != "Low probe 1" {
		t.Fatalf("duplicate roles = %q and %q", first, second)
	}
	if strings.Contains(first+second, secret) {
		t.Fatal("duplicate report exposed the API key")
	}
}

func TestFetchMetricsHonorsContextDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	t.Cleanup(server.Close)
	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	h := &remoteHarness{
		t:      t,
		cfg:    e2eConfig{baseURL: baseURL},
		client: server.Client(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, err = h.fetchMetrics(ctx)
	if err == nil {
		t.Fatal("fetchMetrics succeeded against a stalled endpoint")
	}
	if time.Since(started) > time.Second {
		t.Fatalf("fetchMetrics did not honor the deadline: %s", time.Since(started))
	}
}

func TestFaultProxyPassesHealthMetricsAndCanToggleInference5xx(t *testing.T) {
	const (
		healthBody    = `{"status":"ok"}`
		metricsBody   = "vllm:num_requests_running 1\n"
		inferenceBody = "data: {\"choices\":[{\"text\":\"hello\"}]}\n\ndata: [DONE]\n\n"
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Upstream", "real-vllm")
		switch request.URL.Path {
		case "/health":
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(writer, healthBody)
		case "/metrics":
			writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
			writer.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(writer, metricsBody)
		case "/v1/completions":
			writer.Header().Set("Content-Type", "text/event-stream")
			writer.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(writer, inferenceBody)
			if flusher, ok := writer.(http.Flusher); ok {
				flusher.Flush()
			}
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(upstream.Close)
	target, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := startFaultProxy(target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := proxy.Close(ctx); err != nil {
			t.Errorf("close fault proxy: %v", err)
		}
	})

	assertForwarded := func(path string, wantStatus int, wantContentType, wantBody string) {
		t.Helper()
		response, err := http.Get(proxy.URL() + path)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != wantStatus || response.Header.Get("Content-Type") != wantContentType || response.Header.Get("X-Upstream") != "real-vllm" || string(body) != wantBody {
			t.Fatalf("GET %s = status %d content-type %q upstream %q body %q", path, response.StatusCode, response.Header.Get("Content-Type"), response.Header.Get("X-Upstream"), body)
		}
	}
	assertForwarded("/health", http.StatusOK, "application/json", healthBody)
	assertForwarded("/metrics", http.StatusOK, "text/plain; version=0.0.4", metricsBody)
	assertForwarded("/v1/completions", http.StatusAccepted, "text/event-stream", inferenceBody)

	proxy.SetFaulting(true)
	response, err := http.Get(proxy.URL() + "/v1/completions")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("faulting inference status = %d, want 503", response.StatusCode)
	}
	if response.Header.Get("Content-Type") != "application/json" || string(body) != faultResponseBody {
		t.Fatalf("fault response content-type=%q body=%q", response.Header.Get("Content-Type"), body)
	}
	var envelope struct {
		Error struct {
			Type string `json:"type"`
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Error.Type != "server_error" || envelope.Error.Code != "e2e_injected_failure" {
		t.Fatalf("fault response is not an OpenAI error envelope: err=%v envelope=%+v", err, envelope.Error)
	}
	assertForwarded("/health", http.StatusOK, "application/json", healthBody)
	assertForwarded("/metrics", http.StatusOK, "text/plain; version=0.0.4", metricsBody)
}

func TestAdminMutationCleanupRestoresPoolAndBackends(t *testing.T) {
	pool := adminPool{
		ID: 11, PublicModelName: "qwen", UpstreamModelName: "Qwen/Qwen3", Enabled: true,
		MaxGatewayInflight: 17, MaxWaiting: 9,
	}
	backends := []adminBackend{
		{ID: 21, ModelPoolID: pool.ID, Name: "gpu-a", BaseURL: "http://127.0.0.1:9001", Enabled: true, Draining: false, CapacityHint: 1.5, RunningSoftLimit: 16, UpstreamAPIKeyEnv: "VLLM_A_KEY"},
		{ID: 22, ModelPoolID: pool.ID, Name: "gpu-b", BaseURL: "http://127.0.0.1:9002", Enabled: true, Draining: true, CapacityHint: 2.5, RunningSoftLimit: 32, UpstreamAPIKeyEnv: "VLLM_B_KEY"},
	}
	updater := &recordingAdminUpdater{pool: pool, backends: map[int64]adminBackend{21: backends[0], 22: backends[1]}, failBackendID: 21}
	cleanup, err := newAdminMutationCleanup(updater, adminStatus{Pools: []adminPool{pool}, Backends: backends}, pool.ID)
	if err != nil {
		t.Fatal(err)
	}

	updater.pool.MaxGatewayInflight = 1
	updater.pool.MaxWaiting = 2
	mutatedTarget := updater.backends[21]
	mutatedTarget.BaseURL = "http://127.0.0.1:45678"
	mutatedTarget.Draining = true
	mutatedTarget.CapacityHint = 99
	updater.backends[21] = mutatedTarget
	mutatedSibling := updater.backends[22]
	mutatedSibling.Draining = false
	mutatedSibling.RunningSoftLimit = 1
	updater.backends[22] = mutatedSibling

	err = cleanup.Restore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "restore backend 21") {
		t.Fatalf("restore error = %v, want backend 21 failure", err)
	}
	if !reflect.DeepEqual(updater.pool, pool) {
		t.Fatalf("restored pool = %+v, want %+v", updater.pool, pool)
	}
	for _, backend := range backends {
		if got := updater.backends[backend.ID]; !reflect.DeepEqual(got, backend) {
			t.Fatalf("restored backend %d = %+v, want %+v", backend.ID, got, backend)
		}
	}
	if got := updater.calls; !reflect.DeepEqual(got, []string{"pool:11", "backend:21", "backend:22"}) {
		t.Fatalf("restore calls = %#v", got)
	}
	if second := cleanup.Restore(context.Background()); second == nil || second.Error() != err.Error() {
		t.Fatalf("idempotent restore error = %v, want %v", second, err)
	}
	if len(updater.calls) != 3 {
		t.Fatalf("idempotent restore repeated mutations: calls=%#v", updater.calls)
	}
}

type recordingAdminUpdater struct {
	mu            sync.Mutex
	pool          adminPool
	backends      map[int64]adminBackend
	failBackendID int64
	calls         []string
}

func (u *recordingAdminUpdater) updatePool(_ context.Context, pool adminPool) (adminPool, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls = append(u.calls, "pool:"+strconv.FormatInt(pool.ID, 10))
	u.pool = pool
	return pool, nil
}

func (u *recordingAdminUpdater) updateBackend(_ context.Context, backend adminBackend) (adminBackend, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls = append(u.calls, "backend:"+strconv.FormatInt(backend.ID, 10))
	u.backends[backend.ID] = backend
	if backend.ID == u.failBackendID {
		return adminBackend{}, errors.New("injected update failure")
	}
	return backend, nil
}

func TestLoadE2EConfigAcceptsResilienceAndRejectsMissingCircuitBackend(t *testing.T) {
	base := map[string]string{
		"LLMGW_E2E_MODE": "resilience", "LLMGW_E2E_GATEWAY_URL": "http://127.0.0.1:8080",
		"LLMGW_E2E_ADMIN_USERNAME": "admin", "LLMGW_E2E_ADMIN_PASSWORD": "secret",
		"LLMGW_E2E_MODEL": "qwen", "LLMGW_E2E_HIGH_KEY": "llmgw_high",
		"LLMGW_E2E_CIRCUIT_BACKEND_ID": "21",
	}
	lookup := func(values map[string]string) func(string) (string, bool) {
		return func(name string) (string, bool) {
			value, ok := values[name]
			return value, ok
		}
	}
	cfg, err := parseE2EConfig(lookup(base), modeResilience)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.circuitBackendID != 21 || cfg.circuitFailureCount != 5 {
		t.Fatalf("resilience config = backend %d failures %d", cfg.circuitBackendID, cfg.circuitFailureCount)
	}

	missing := cloneStringMap(base)
	delete(missing, "LLMGW_E2E_CIRCUIT_BACKEND_ID")
	if _, err := parseE2EConfig(lookup(missing), modeResilience); err == nil || !strings.Contains(err.Error(), "LLMGW_E2E_CIRCUIT_BACKEND_ID") {
		t.Fatalf("missing circuit backend error = %v", err)
	}
	for _, value := range []string{"0", "-1", "not-an-integer"} {
		invalid := cloneStringMap(base)
		invalid["LLMGW_E2E_CIRCUIT_BACKEND_ID"] = value
		if _, err := parseE2EConfig(lookup(invalid), modeResilience); err == nil || !strings.Contains(err.Error(), "LLMGW_E2E_CIRCUIT_BACKEND_ID") {
			t.Fatalf("circuit backend %q error = %v", value, err)
		}
	}
	for _, value := range []string{"0", "-1", "not-an-integer"} {
		invalid := cloneStringMap(base)
		invalid["LLMGW_E2E_CIRCUIT_FAILURE_COUNT"] = value
		if _, err := parseE2EConfig(lookup(invalid), modeResilience); err == nil || !strings.Contains(err.Error(), "LLMGW_E2E_CIRCUIT_FAILURE_COUNT") {
			t.Fatalf("circuit failure count %q error = %v", value, err)
		}
	}

	smoke := cloneStringMap(base)
	smoke["LLMGW_E2E_MODE"] = "smoke"
	delete(smoke, "LLMGW_E2E_CIRCUIT_BACKEND_ID")
	if _, err := parseE2EConfig(lookup(smoke), modeSmoke); err != nil {
		t.Fatalf("smoke config unexpectedly required resilience fields: %v", err)
	}
	nonLoopback := cloneStringMap(base)
	nonLoopback["LLMGW_E2E_GATEWAY_URL"] = "https://gateway.example.com"
	if _, err := parseE2EConfig(lookup(nonLoopback), modeResilience); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("non-loopback resilience error = %v", err)
	}
}

func cloneStringMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func TestInferenceReadinessHonorsContextDeadline(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			<-request.Context().Done()
		}))
		t.Cleanup(server.Close)
		h := unitRemoteHarness(t, server)
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		started := time.Now()
		if _, _, err := h.fetchInferenceReadiness(ctx); err == nil {
			t.Fatal("readiness fetch succeeded against a stalled endpoint")
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("readiness fetch did not honor context deadline: %s", elapsed)
		}
	})

	for _, test := range []struct {
		name       string
		statusCode int
		status     string
		pools      int
		backends   int
	}{
		{name: "ready", statusCode: http.StatusOK, status: "ready", pools: 1, backends: 2},
		{name: "unavailable", statusCode: http.StatusServiceUnavailable, status: "unavailable", pools: 0, backends: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(test.statusCode)
				_ = json.NewEncoder(writer).Encode(inferenceReadyResponse{Status: test.status, Revision: 9, PoolAvailability: test.pools, BackendAvailability: test.backends})
			}))
			t.Cleanup(server.Close)
			h := unitRemoteHarness(t, server)
			got, statusCode, err := h.fetchInferenceReadiness(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if statusCode != test.statusCode || got.Status != test.status || got.PoolAvailability != test.pools || got.BackendAvailability != test.backends {
				t.Fatalf("readiness = statusCode %d body %+v", statusCode, got)
			}
		})
	}
}

func TestAdminPUTHelpersPreserveFieldsAuthenticateAndRedactErrors(t *testing.T) {
	const secret = "do-not-expose-admin-password"
	pool := adminPool{
		ID: 11, PublicModelName: "qwen", UpstreamModelName: "Qwen/Qwen3", Enabled: true,
		MaxGatewayInflight: 17, MaxWaiting: 9,
	}
	backend := adminBackend{
		ID: 21, ModelPoolID: pool.ID, Name: "gpu-a", BaseURL: "http://127.0.0.1:9001", Enabled: true,
		Draining: true, CapacityHint: 1.5, RunningSoftLimit: 16, UpstreamAPIKeyEnv: "VLLM_A_KEY",
	}
	var seenPool poolUpdateInput
	var seenBackend backendUpdateInput
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/admin/api/status" {
			http.SetCookie(writer, &http.Cookie{Name: "llmgw_csrf", Value: "csrf-value", Path: "/"})
			_ = json.NewEncoder(writer).Encode(adminStatus{})
			return
		}
		username, password, ok := request.BasicAuth()
		if !ok || username != "admin" || password != secret || request.Header.Get("X-CSRF-Token") != "csrf-value" {
			http.Error(writer, "missing auth or csrf", http.StatusForbidden)
			return
		}
		switch request.URL.Path {
		case "/admin/api/pools/11":
			if err := json.NewDecoder(request.Body).Decode(&seenPool); err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(writer).Encode(pool)
		case "/admin/api/backends/21":
			if err := json.NewDecoder(request.Body).Decode(&seenBackend); err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(writer).Encode(backend)
		case "/admin/api/backends/22":
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(writer, "server echoed "+secret)
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	h := unitRemoteHarness(t, server)
	h.cfg.adminUsername = "admin"
	h.cfg.adminPassword = secret
	if _, err := h.updatePool(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	if _, err := h.updateBackend(context.Background(), backend); err != nil {
		t.Fatal(err)
	}
	wantPool := poolUpdateInput{
		PublicModelName: pool.PublicModelName, UpstreamModelName: pool.UpstreamModelName, Enabled: pool.Enabled,
		MaxGatewayInflight: pool.MaxGatewayInflight, MaxWaiting: pool.MaxWaiting,
	}
	if !reflect.DeepEqual(seenPool, wantPool) {
		t.Fatalf("pool PUT = %+v, want %+v", seenPool, wantPool)
	}
	wantBackend := backendUpdateInput{
		ModelPoolID: backend.ModelPoolID, Name: backend.Name, BaseURL: backend.BaseURL, Enabled: backend.Enabled,
		Draining: backend.Draining, CapacityHint: backend.CapacityHint, RunningSoftLimit: backend.RunningSoftLimit,
		UpstreamAPIKeyEnv: backend.UpstreamAPIKeyEnv,
	}
	if !reflect.DeepEqual(seenBackend, wantBackend) {
		t.Fatalf("backend PUT = %+v, want %+v", seenBackend, wantBackend)
	}
	backend.ID = 22
	_, err := h.updateBackend(context.Background(), backend)
	if err == nil || !strings.Contains(err.Error(), "PUT /admin/api/backends/22 = 500") {
		t.Fatalf("backend failure = %v", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("admin update error exposed a secret: %v", err)
	}
}

func unitRemoteHarness(t *testing.T, server *httptest.Server) *remoteHarness {
	t.Helper()
	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	jar, err := cookieJar()
	if err != nil {
		t.Fatal(err)
	}
	client := server.Client()
	client.Jar = jar
	return &remoteHarness{t: t, cfg: e2eConfig{baseURL: baseURL, probeTimeout: time.Second}, client: client}
}
