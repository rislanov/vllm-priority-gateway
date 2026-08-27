package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type e2eMode string

const (
	modeSmoke      e2eMode = "smoke"
	modePriority   e2eMode = "priority"
	modeResilience e2eMode = "resilience"
)

type e2eConfig struct {
	baseURL             *url.URL
	adminUsername       string
	adminPassword       string
	model               string
	highKey             string
	highLoadKeys        []string
	criticalKey         string
	lowKeys             []string
	expectedBackends    int
	highRequestsPerKey  int
	highMaxTokens       int
	saturationTimeout   time.Duration
	recoveryTimeout     time.Duration
	probeTimeout        time.Duration
	requireAllWaiting   bool
	drainBackendID      int64
	circuitBackendID    int64
	circuitFailureCount int
}

func loadE2EConfig(t *testing.T, required e2eMode) e2eConfig {
	t.Helper()
	mode := e2eMode(strings.TrimSpace(os.Getenv("LLMGW_E2E_MODE")))
	if mode == "" {
		t.Skip("set LLMGW_E2E_MODE=smoke, priority, or resilience to run external gateway checks")
	}
	if mode != modeSmoke && mode != modePriority && mode != modeResilience {
		t.Fatalf("LLMGW_E2E_MODE = %q, want smoke, priority, or resilience", mode)
	}
	if required != modeSmoke && mode != required {
		t.Skipf("set LLMGW_E2E_MODE=%s to run this external scenario", required)
	}
	cfg, err := parseE2EConfig(os.LookupEnv, mode)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func parseE2EConfig(lookup func(string) (string, bool), mode e2eMode) (e2eConfig, error) {
	configuredMode := e2eMode(strings.TrimSpace(lookupValue(lookup, "LLMGW_E2E_MODE")))
	if configuredMode != mode || (mode != modeSmoke && mode != modePriority && mode != modeResilience) {
		return e2eConfig{}, fmt.Errorf("LLMGW_E2E_MODE must be exactly smoke, priority, or resilience")
	}
	baseURLValue, err := requiredLookup(lookup, "LLMGW_E2E_GATEWAY_URL")
	if err != nil {
		return e2eConfig{}, err
	}
	baseURL, err := url.Parse(baseURLValue)
	if err != nil || (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		return e2eConfig{}, fmt.Errorf("LLMGW_E2E_GATEWAY_URL must be an absolute HTTP(S) URL")
	}
	if baseURL.RawQuery != "" || baseURL.Fragment != "" || baseURL.User != nil {
		return e2eConfig{}, fmt.Errorf("LLMGW_E2E_GATEWAY_URL must not contain credentials, a query, or a fragment")
	}
	baseURL.Path = strings.TrimSuffix(baseURL.Path, "/")

	adminUsername, err := requiredLookup(lookup, "LLMGW_E2E_ADMIN_USERNAME")
	if err != nil {
		return e2eConfig{}, err
	}
	adminPassword, err := requiredLookup(lookup, "LLMGW_E2E_ADMIN_PASSWORD")
	if err != nil {
		return e2eConfig{}, err
	}
	model, err := requiredLookup(lookup, "LLMGW_E2E_MODEL")
	if err != nil {
		return e2eConfig{}, err
	}
	highKey, err := requiredLookup(lookup, "LLMGW_E2E_HIGH_KEY")
	if err != nil {
		return e2eConfig{}, err
	}
	expectedBackends, err := positiveIntLookup(lookup, "LLMGW_E2E_EXPECTED_BACKENDS", 2)
	if err != nil {
		return e2eConfig{}, err
	}
	highRequestsPerKey, err := positiveIntLookup(lookup, "LLMGW_E2E_HIGH_REQUESTS_PER_KEY", 4)
	if err != nil {
		return e2eConfig{}, err
	}
	highMaxTokens, err := positiveIntLookup(lookup, "LLMGW_E2E_HIGH_MAX_TOKENS", 768)
	if err != nil {
		return e2eConfig{}, err
	}
	saturationTimeout, err := positiveDurationLookup(lookup, "LLMGW_E2E_SATURATION_TIMEOUT", time.Minute)
	if err != nil {
		return e2eConfig{}, err
	}
	recoveryTimeout, err := positiveDurationLookup(lookup, "LLMGW_E2E_RECOVERY_TIMEOUT", 45*time.Second)
	if err != nil {
		return e2eConfig{}, err
	}
	probeTimeout, err := positiveDurationLookup(lookup, "LLMGW_E2E_PROBE_TIMEOUT", time.Minute)
	if err != nil {
		return e2eConfig{}, err
	}
	requireAllWaiting, err := boolLookup(lookup, "LLMGW_E2E_REQUIRE_ALL_WAITING", true)
	if err != nil {
		return e2eConfig{}, err
	}
	drainBackendID, err := nonNegativeInt64Lookup(lookup, "LLMGW_E2E_DRAIN_BACKEND_ID", 0)
	if err != nil {
		return e2eConfig{}, err
	}
	cfg := e2eConfig{
		baseURL:            baseURL,
		adminUsername:      adminUsername,
		adminPassword:      adminPassword,
		model:              model,
		highKey:            highKey,
		expectedBackends:   expectedBackends,
		highRequestsPerKey: highRequestsPerKey,
		highMaxTokens:      highMaxTokens,
		saturationTimeout:  saturationTimeout,
		recoveryTimeout:    recoveryTimeout,
		probeTimeout:       probeTimeout,
		requireAllWaiting:  requireAllWaiting,
		drainBackendID:     drainBackendID,
	}

	if mode == modePriority {
		if cfg.highLoadKeys, err = listLookup(lookup, "LLMGW_E2E_HIGH_LOAD_KEYS", 1); err != nil {
			return e2eConfig{}, err
		}
		if cfg.criticalKey, err = requiredLookup(lookup, "LLMGW_E2E_CRITICAL_KEY"); err != nil {
			return e2eConfig{}, err
		}
		if cfg.lowKeys, err = listLookup(lookup, "LLMGW_E2E_LOW_KEYS", 3); err != nil {
			return e2eConfig{}, err
		}
		keys := []namedClientKey{
			{name: "LLMGW_E2E_HIGH_KEY", value: cfg.highKey},
			{name: "LLMGW_E2E_CRITICAL_KEY", value: cfg.criticalKey},
		}
		for index, key := range cfg.highLoadKeys {
			keys = append(keys, namedClientKey{name: fmt.Sprintf("LLMGW_E2E_HIGH_LOAD_KEYS[%d]", index), value: key})
		}
		for index, key := range cfg.lowKeys {
			keys = append(keys, namedClientKey{name: fmt.Sprintf("LLMGW_E2E_LOW_KEYS[%d]", index), value: key})
		}
		if first, second, duplicate := findDuplicateClientKey(keys); duplicate {
			return e2eConfig{}, fmt.Errorf("%s and %s must belong to separate clients and use distinct API keys", first, second)
		}
	}
	if mode == modeResilience {
		if !isLoopbackHost(baseURL.Hostname()) {
			return e2eConfig{}, fmt.Errorf("LLMGW_E2E_GATEWAY_URL must use localhost or a loopback IP in resilience mode")
		}
		if cfg.circuitBackendID, err = positiveInt64Lookup(lookup, "LLMGW_E2E_CIRCUIT_BACKEND_ID"); err != nil {
			return e2eConfig{}, err
		}
		if cfg.circuitFailureCount, err = positiveIntLookup(lookup, "LLMGW_E2E_CIRCUIT_FAILURE_COUNT", 5); err != nil {
			return e2eConfig{}, err
		}
	}
	return cfg, nil
}

func lookupValue(lookup func(string) (string, bool), name string) string {
	value, _ := lookup(name)
	return strings.TrimSpace(value)
}

func requiredLookup(lookup func(string) (string, bool), name string) (string, error) {
	value := lookupValue(lookup, name)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func listLookup(lookup func(string) (string, bool), name string, minimum int) ([]string, error) {
	raw, err := requiredLookup(lookup, name)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	if len(values) < minimum {
		return nil, fmt.Errorf("%s must contain at least %d value(s)", name, minimum)
	}
	return values, nil
}

func positiveIntLookup(lookup func(string) (string, bool), name string, fallback int) (int, error) {
	value := lookupValue(lookup, name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func positiveInt64Lookup(lookup func(string) (string, bool), name string) (int64, error) {
	value, err := requiredLookup(lookup, name)
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func nonNegativeInt64Lookup(lookup func(string) (string, bool), name string, fallback int64) (int64, error) {
	value := lookupValue(lookup, name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return parsed, nil
}

func positiveDurationLookup(lookup func(string) (string, bool), name string, fallback time.Duration) (time.Duration, error) {
	value := lookupValue(lookup, name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", name)
	}
	return parsed, nil
}

func boolLookup(lookup func(string) (string, bool), name string, fallback bool) (bool, error) {
	value := lookupValue(lookup, name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", name)
	}
	return parsed, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	parsed := net.ParseIP(host)
	return parsed != nil && parsed.IsLoopback()
}

const faultResponseBody = "{\"error\":{\"message\":\"Injected real-vLLM E2E inference failure\",\"type\":\"server_error\",\"code\":\"e2e_injected_failure\"}}\n"

type faultProxy struct {
	url       string
	faulting  atomic.Bool
	server    *http.Server
	serveDone chan error
}

func startFaultProxy(target *url.URL) (*faultProxy, error) {
	if target == nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
		return nil, fmt.Errorf("fault proxy target must be an absolute HTTP(S) URL")
	}
	reverse := httputil.NewSingleHostReverseProxy(target)
	reverse.FlushInterval = -1
	reverse.ErrorHandler = func(writer http.ResponseWriter, _ *http.Request, _ error) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(writer, "{\"error\":{\"message\":\"Fault proxy could not reach upstream\",\"type\":\"server_error\",\"code\":\"e2e_proxy_upstream_error\"}}\n")
	}
	proxy := &faultProxy{serveDone: make(chan error, 1)}
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if proxy.faulting.Load() && isInferencePath(request.URL.Path) {
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(writer, faultResponseBody)
			return
		}
		reverse.ServeHTTP(writer, request)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen for fault proxy: %w", err)
	}
	proxy.url = "http://" + listener.Addr().String()
	proxy.server = &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		proxy.serveDone <- proxy.server.Serve(listener)
	}()
	return proxy, nil
}

func isInferencePath(path string) bool {
	switch path {
	case "/v1/chat/completions", "/v1/completions", "/v1/responses":
		return true
	default:
		return false
	}
}

func (proxy *faultProxy) URL() string {
	return proxy.url
}

func (proxy *faultProxy) SetFaulting(enabled bool) {
	proxy.faulting.Store(enabled)
}

func (proxy *faultProxy) Close(ctx context.Context) error {
	shutdownErr := proxy.server.Shutdown(ctx)
	var closeErr error
	if shutdownErr != nil {
		closeErr = proxy.server.Close()
	}
	serveErr := <-proxy.serveDone
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(shutdownErr, closeErr, serveErr)
}

type namedClientKey struct {
	name  string
	value string
}

func findDuplicateClientKey(keys []namedClientKey) (string, string, bool) {
	seen := make(map[string]string, len(keys))
	for _, key := range keys {
		if first, exists := seen[key.value]; exists {
			return first, key.name, true
		}
		seen[key.value] = key.name
	}
	return "", "", false
}

type readyResponse struct {
	Status              string `json:"status"`
	Revision            int64  `json:"revision"`
	BackendAvailability int    `json:"backendAvailability"`
}

type inferenceReadyResponse struct {
	Status              string `json:"status"`
	Revision            int64  `json:"revision"`
	PoolAvailability    int    `json:"poolAvailability"`
	BackendAvailability int    `json:"backendAvailability"`
}

type adminStatus struct {
	Revision int64          `json:"revision"`
	Pools    []adminPool    `json:"pools"`
	Backends []adminBackend `json:"backends"`
}

type adminPool struct {
	ID                 int64       `json:"id"`
	PublicModelName    string      `json:"publicModelName"`
	UpstreamModelName  string      `json:"upstreamModelName"`
	Enabled            bool        `json:"enabled"`
	MaxGatewayInflight int         `json:"maxGatewayInflight"`
	MaxWaiting         int         `json:"maxWaiting"`
	Runtime            poolRuntime `json:"runtime"`
}

type poolRuntime struct {
	State               string  `json:"State"`
	BestBackendPressure float64 `json:"BestBackendPressure"`
	AvailableBackends   int     `json:"AvailableBackends"`
	AllBackendsWaiting  bool    `json:"AllBackendsWaiting"`
	GatewayInflight     int     `json:"GatewayInflight"`
	TotalWaiting        float64 `json:"TotalWaiting"`
}

type adminBackend struct {
	ID                int64          `json:"id"`
	ModelPoolID       int64          `json:"modelPoolId"`
	Name              string         `json:"name"`
	BaseURL           string         `json:"baseUrl"`
	Enabled           bool           `json:"enabled"`
	Draining          bool           `json:"draining"`
	CapacityHint      float64        `json:"capacityHint"`
	RunningSoftLimit  float64        `json:"runningSoftLimit"`
	UpstreamAPIKeyEnv string         `json:"upstreamApiKeyEnv"`
	Runtime           backendRuntime `json:"runtime"`
}

type backendRuntime struct {
	State                 string    `json:"State"`
	CircuitState          string    `json:"CircuitState"`
	CircuitFailures       int       `json:"CircuitFailures"`
	CircuitRetryAt        time.Time `json:"CircuitRetryAt"`
	CircuitProbesInFlight int       `json:"CircuitProbesInFlight"`
	CircuitAvailable      bool      `json:"CircuitAvailable"`
	Healthy               bool      `json:"Healthy"`
	MetricsFresh          bool      `json:"MetricsFresh"`
	Running               float64   `json:"Running"`
	Waiting               float64   `json:"Waiting"`
	KVCacheUsage          float64   `json:"KVCacheUsage"`
	Pressure              float64   `json:"Pressure"`
	GatewayInflight       int       `json:"GatewayInflight"`
}

func (status adminStatus) requirePool(t *testing.T, model string) adminPool {
	t.Helper()
	for _, pool := range status.Pools {
		if pool.PublicModelName == model {
			return pool
		}
	}
	t.Fatalf("admin status does not contain pool %q", model)
	return adminPool{}
}

func (status adminStatus) requireBackend(t *testing.T, id int64) adminBackend {
	t.Helper()
	for _, backend := range status.Backends {
		if backend.ID == id {
			return backend
		}
	}
	t.Fatalf("admin status does not contain backend %d", id)
	return adminBackend{}
}

func (status adminStatus) requireHealthyFreshBackends(t *testing.T, poolID int64, minimum int) {
	t.Helper()
	count := 0
	for _, backend := range status.Backends {
		if backend.ModelPoolID == poolID && backend.Enabled && !backend.Draining && backend.Runtime.Healthy && backend.Runtime.MetricsFresh {
			count++
		}
	}
	if count < minimum {
		t.Fatalf("healthy metrics-fresh backends for pool %d = %d, want at least %d; backends=%+v", poolID, count, minimum, status.Backends)
	}
}

type modelsResponse struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

func (models modelsResponse) contains(model string) bool {
	for _, item := range models.Data {
		if item.ID == model {
			return true
		}
	}
	return false
}

type metricsResponse string

func (metrics metricsResponse) containsFamily(family string) bool {
	for _, line := range strings.Split(string(metrics), "\n") {
		if strings.HasPrefix(line, "# HELP "+family+" ") || strings.HasPrefix(line, family+"{") || strings.HasPrefix(line, family+" ") {
			return true
		}
	}
	return false
}

type remoteHarness struct {
	t      *testing.T
	cfg    e2eConfig
	client *http.Client
}

func newRemoteHarness(t *testing.T, cfg e2eConfig) *remoteHarness {
	t.Helper()
	jar, err := cookieJar()
	if err != nil {
		t.Fatal(err)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	return &remoteHarness{t: t, cfg: cfg, client: &http.Client{Transport: transport, Jar: jar}}
}

func cookieJar() (http.CookieJar, error) {
	return cookiejar.New(nil)
}

func (h *remoteHarness) endpoint(path string) string {
	base := *h.cfg.baseURL
	base.Path = strings.TrimSuffix(base.Path, "/") + path
	base.RawPath = ""
	return base.String()
}

func (h *remoteHarness) ready() readyResponse {
	h.t.Helper()
	var output readyResponse
	h.getJSON("/readyz", "", false, &output)
	return output
}

func (h *remoteHarness) inferenceReadiness() (inferenceReadyResponse, int) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.probeTimeout)
	defer cancel()
	readiness, status, err := h.fetchInferenceReadiness(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	return readiness, status
}

func (h *remoteHarness) fetchInferenceReadiness(ctx context.Context) (inferenceReadyResponse, int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.endpoint("/inference-readyz"), nil)
	if err != nil {
		return inferenceReadyResponse{}, 0, fmt.Errorf("create GET /inference-readyz: %w", err)
	}
	response, err := h.client.Do(request)
	if err != nil {
		return inferenceReadyResponse{}, 0, fmt.Errorf("GET /inference-readyz: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return inferenceReadyResponse{}, response.StatusCode, fmt.Errorf("read /inference-readyz: %w", err)
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusServiceUnavailable {
		return inferenceReadyResponse{}, response.StatusCode, fmt.Errorf("GET /inference-readyz = %d", response.StatusCode)
	}
	var readiness inferenceReadyResponse
	if err := json.Unmarshal(body, &readiness); err != nil {
		return inferenceReadyResponse{}, response.StatusCode, fmt.Errorf("decode /inference-readyz: %w", err)
	}
	return readiness, response.StatusCode, nil
}

func (h *remoteHarness) adminStatus() adminStatus {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.probeTimeout)
	defer cancel()
	output, err := h.fetchAdminStatus(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	return output
}

func (h *remoteHarness) fetchAdminStatus(ctx context.Context) (adminStatus, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.endpoint("/admin/api/status"), nil)
	if err != nil {
		return adminStatus{}, fmt.Errorf("create GET /admin/api/status: %w", err)
	}
	request.SetBasicAuth(h.cfg.adminUsername, h.cfg.adminPassword)
	response, err := h.client.Do(request)
	if err != nil {
		return adminStatus{}, fmt.Errorf("GET /admin/api/status: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return adminStatus{}, fmt.Errorf("read GET /admin/api/status: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return adminStatus{}, fmt.Errorf("GET /admin/api/status = %d", response.StatusCode)
	}
	var output adminStatus
	if err := json.Unmarshal(body, &output); err != nil {
		return adminStatus{}, fmt.Errorf("decode GET /admin/api/status: %w", err)
	}
	return output, nil
}

func (h *remoteHarness) models(key string) modelsResponse {
	h.t.Helper()
	var output modelsResponse
	h.getJSON("/v1/models", key, false, &output)
	return output
}

func (h *remoteHarness) metrics() metricsResponse {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.probeTimeout)
	defer cancel()
	metrics, err := h.fetchMetrics(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	return metrics
}

func (h *remoteHarness) fetchMetrics(ctx context.Context) (metricsResponse, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.endpoint("/metrics"), nil)
	if err != nil {
		return "", fmt.Errorf("create GET /metrics: %w", err)
	}
	response, err := h.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("GET /metrics: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return "", fmt.Errorf("read /metrics: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET /metrics = %d: %s", response.StatusCode, bodyExcerpt(body))
	}
	return metricsResponse(body), nil
}

func (h *remoteHarness) getJSON(path, key string, admin bool, output any) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.probeTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.endpoint(path), nil)
	if err != nil {
		h.t.Fatal(err)
	}
	if key != "" {
		request.Header.Set("Authorization", "Bearer "+key)
	}
	if admin {
		request.SetBasicAuth(h.cfg.adminUsername, h.cfg.adminPassword)
	}
	response, err := h.client.Do(request)
	if err != nil {
		h.t.Fatalf("GET %s: %v", path, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		h.t.Fatalf("read %s: %v", path, err)
	}
	if response.StatusCode != http.StatusOK {
		h.t.Fatalf("GET %s = %d: %s", path, response.StatusCode, bodyExcerpt(body))
	}
	if err := json.Unmarshal(body, output); err != nil {
		h.t.Fatalf("decode %s: %v; body=%s", path, err, bodyExcerpt(body))
	}
}

type completionRequest struct {
	Key          string
	Prompt       string
	MaxTokens    int
	Stream       bool
	IgnoreEOS    bool
	Priority     *int
	ExtraHeaders map[string]string
}

type requestResult struct {
	Status     int
	RetryAfter string
	Body       []byte
	FirstByte  time.Duration
	Duration   time.Duration
	StreamDone bool
}

func (result requestResult) validateInjectedFailure() error {
	if result.Status != http.StatusServiceUnavailable {
		return fmt.Errorf("injected inference status = %d, want 503", result.Status)
	}
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(result.Body, &envelope); err != nil {
		return fmt.Errorf("decode injected inference error: %w", err)
	}
	if envelope.Error.Code != "e2e_injected_failure" {
		return fmt.Errorf("injected inference error code = %q, want e2e_injected_failure", envelope.Error.Code)
	}
	return nil
}

func (result requestResult) requireInjectedFailure(t *testing.T) {
	t.Helper()
	if err := result.validateInjectedFailure(); err != nil {
		t.Fatal(err)
	}
}

func (h *remoteHarness) completion(parent context.Context, input completionRequest) requestResult {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(parent, h.cfg.probeTimeout)
	defer cancel()
	payload := map[string]any{
		"model": h.cfg.model, "prompt": input.Prompt, "max_tokens": input.MaxTokens,
		"temperature": 0, "stream": input.Stream,
	}
	if input.IgnoreEOS {
		payload["ignore_eos"] = true
	}
	if input.Priority != nil {
		payload["priority"] = *input.Priority
	}
	body, err := json.Marshal(payload)
	if err != nil {
		h.t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint("/v1/completions"), bytes.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+input.Key)
	request.Header.Set("Content-Type", "application/json")
	for name, value := range input.ExtraHeaders {
		request.Header.Set(name, value)
	}
	started := time.Now()
	response, err := h.client.Do(request)
	if err != nil {
		h.t.Fatalf("POST /v1/completions: %v", err)
	}
	defer response.Body.Close()
	result := requestResult{Status: response.StatusCode, RetryAfter: response.Header.Get("Retry-After")}
	buffer := make([]byte, 32<<10)
	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			if result.FirstByte == 0 {
				result.FirstByte = time.Since(started)
			}
			result.Body = append(result.Body, buffer[:n]...)
			if len(result.Body) > 8<<20 {
				h.t.Fatal("completion response exceeded 8 MiB")
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			h.t.Fatalf("read completion response: %v", readErr)
		}
	}
	result.Duration = time.Since(started)
	result.StreamDone = !input.Stream || bytes.Contains(result.Body, []byte("data: [DONE]"))
	return result
}

func (result requestResult) requireStatus(t *testing.T, want int) {
	t.Helper()
	if result.Status != want {
		t.Fatalf("completion status = %d, want %d; body=%s", result.Status, want, bodyExcerpt(result.Body))
	}
}

func isClosedCircuitBaseline(backend adminBackend, baseURL string) bool {
	return backend.BaseURL == baseURL && backend.Runtime.Healthy && backend.Runtime.MetricsFresh &&
		backend.Runtime.CircuitState == "closed" && backend.Runtime.CircuitAvailable && backend.Runtime.CircuitFailures == 0
}

func isolatePoolGatewayInflight(pool adminPool, maximum int) adminPool {
	pool.MaxGatewayInflight = maximum
	pool.MaxWaiting = 0
	return pool
}

func samePoolConfiguration(left, right adminPool) bool {
	return left.ID == right.ID && left.PublicModelName == right.PublicModelName &&
		left.UpstreamModelName == right.UpstreamModelName && left.Enabled == right.Enabled &&
		left.MaxGatewayInflight == right.MaxGatewayInflight && left.MaxWaiting == right.MaxWaiting
}

func (result requestResult) requireCompleteStream(t *testing.T) {
	t.Helper()
	result.requireStatus(t, http.StatusOK)
	if result.FirstByte <= 0 {
		t.Fatal("streaming completion returned no response bytes")
	}
	if !result.StreamDone {
		t.Fatalf("streaming completion ended without [DONE]; body=%s", bodyExcerpt(result.Body))
	}
}

func (result requestResult) requireOverloaded(t *testing.T) {
	t.Helper()
	result.requireStatus(t, http.StatusTooManyRequests)
	if result.RetryAfter == "" {
		t.Fatal("429 response has no Retry-After header")
	}
	retryAfter, err := strconv.Atoi(result.RetryAfter)
	if err != nil || retryAfter <= 0 {
		t.Fatalf("Retry-After = %q, want a positive integer number of seconds", result.RetryAfter)
	}
	var envelope struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(result.Body, &envelope); err != nil {
		t.Fatalf("decode 429 response: %v; body=%s", err, bodyExcerpt(result.Body))
	}
	if envelope.Error.Code != "gateway_overloaded" || envelope.Error.Type != "rate_limit_error" {
		t.Fatalf("429 error = %+v, want gateway_overloaded rate_limit_error", envelope.Error)
	}
}

func (h *remoteHarness) waitForPool(predicate func(adminPool) bool, timeout time.Duration) adminPool {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	var last adminPool
	for time.Now().Before(deadline) {
		last = h.adminStatus().requirePool(h.t, h.cfg.model)
		if predicate(last) {
			return last
		}
		time.Sleep(250 * time.Millisecond)
	}
	h.t.Fatalf("pool %q did not reach expected state within %s; last=%+v", h.cfg.model, timeout, last.Runtime)
	return adminPool{}
}

func (h *remoteHarness) waitForBackend(id int64, predicate func(adminBackend) bool, timeout time.Duration) adminBackend {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	var last adminBackend
	for time.Now().Before(deadline) {
		status := h.adminStatus()
		for _, backend := range status.Backends {
			if backend.ID == id {
				last = backend
				if predicate(backend) {
					return backend
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	h.t.Fatalf("backend %d did not reach expected state within %s; last=%+v", id, timeout, last)
	return adminBackend{}
}

func (h *remoteHarness) drainBackend(id int64) {
	h.adminMutation(fmt.Sprintf("/admin/api/backends/%d/drain", id))
}

func (h *remoteHarness) resumeBackend(id int64) {
	h.adminMutation(fmt.Sprintf("/admin/api/backends/%d/resume", id))
}

func (h *remoteHarness) adminMutation(path string) {
	h.t.Helper()
	_ = h.adminStatus()
	adminURL, err := url.Parse(h.endpoint(path))
	if err != nil {
		h.t.Fatal(err)
	}
	csrf := ""
	for _, cookie := range h.client.Jar.Cookies(adminURL) {
		if cookie.Name == "llmgw_csrf" {
			csrf = cookie.Value
		}
	}
	if csrf == "" {
		h.t.Fatal("admin status did not set llmgw_csrf cookie")
	}
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.probeTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint(path), nil)
	if err != nil {
		h.t.Fatal(err)
	}
	request.SetBasicAuth(h.cfg.adminUsername, h.cfg.adminPassword)
	request.Header.Set("X-CSRF-Token", csrf)
	response, err := h.client.Do(request)
	if err != nil {
		h.t.Fatalf("POST %s: %v", path, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		h.t.Fatalf("read POST %s: %v", path, err)
	}
	if response.StatusCode != http.StatusOK {
		h.t.Fatalf("POST %s = %d: %s", path, response.StatusCode, bodyExcerpt(body))
	}
}

type poolUpdateInput struct {
	PublicModelName    string `json:"publicModelName"`
	UpstreamModelName  string `json:"upstreamModelName"`
	Enabled            bool   `json:"enabled"`
	MaxGatewayInflight int    `json:"maxGatewayInflight"`
	MaxWaiting         int    `json:"maxWaiting"`
}

type backendUpdateInput struct {
	ModelPoolID       int64   `json:"modelPoolId"`
	Name              string  `json:"name"`
	BaseURL           string  `json:"baseUrl"`
	Enabled           bool    `json:"enabled"`
	Draining          bool    `json:"draining"`
	CapacityHint      float64 `json:"capacityHint"`
	RunningSoftLimit  float64 `json:"runningSoftLimit"`
	UpstreamAPIKeyEnv string  `json:"upstreamApiKeyEnv"`
}

func (h *remoteHarness) updatePool(ctx context.Context, pool adminPool) (adminPool, error) {
	input := poolUpdateInput{
		PublicModelName: pool.PublicModelName, UpstreamModelName: pool.UpstreamModelName, Enabled: pool.Enabled,
		MaxGatewayInflight: pool.MaxGatewayInflight, MaxWaiting: pool.MaxWaiting,
	}
	var output adminPool
	if err := h.adminPUT(ctx, "/admin/api/pools/"+strconv.FormatInt(pool.ID, 10), input, &output); err != nil {
		return adminPool{}, err
	}
	return output, nil
}

func (h *remoteHarness) updateBackend(ctx context.Context, backend adminBackend) (adminBackend, error) {
	input := backendUpdateInput{
		ModelPoolID: backend.ModelPoolID, Name: backend.Name, BaseURL: backend.BaseURL,
		Enabled: backend.Enabled, Draining: backend.Draining, CapacityHint: backend.CapacityHint,
		RunningSoftLimit: backend.RunningSoftLimit, UpstreamAPIKeyEnv: backend.UpstreamAPIKeyEnv,
	}
	var output adminBackend
	if err := h.adminPUT(ctx, "/admin/api/backends/"+strconv.FormatInt(backend.ID, 10), input, &output); err != nil {
		return adminBackend{}, err
	}
	return output, nil
}

func (h *remoteHarness) adminPUT(ctx context.Context, path string, input, output any) error {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.cfg.probeTimeout)
		defer cancel()
	}
	if _, err := h.fetchAdminStatus(ctx); err != nil {
		return err
	}
	adminURL, err := url.Parse(h.endpoint(path))
	if err != nil {
		return fmt.Errorf("create PUT %s URL: %w", path, err)
	}
	csrf := ""
	for _, cookie := range h.client.Jar.Cookies(adminURL) {
		if cookie.Name == "llmgw_csrf" {
			csrf = cookie.Value
		}
	}
	if csrf == "" {
		return fmt.Errorf("PUT %s: admin status did not set llmgw_csrf cookie", path)
	}
	body, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("encode PUT %s: %w", path, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, h.endpoint(path), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create PUT %s: %w", path, err)
	}
	request.SetBasicAuth(h.cfg.adminUsername, h.cfg.adminPassword)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-CSRF-Token", csrf)
	response, err := h.client.Do(request)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", path, err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read PUT %s: %w", path, err)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s = %d", path, response.StatusCode)
	}
	if err := json.Unmarshal(responseBody, output); err != nil {
		return fmt.Errorf("decode PUT %s: %w", path, err)
	}
	return nil
}

type adminStateUpdater interface {
	updatePool(context.Context, adminPool) (adminPool, error)
	updateBackend(context.Context, adminBackend) (adminBackend, error)
}

type adminMutationCleanup struct {
	updater  adminStateUpdater
	pool     adminPool
	backends []adminBackend
	once     sync.Once
	err      error
}

func newAdminMutationCleanup(updater adminStateUpdater, status adminStatus, poolID int64) (*adminMutationCleanup, error) {
	var originalPool adminPool
	poolFound := false
	for _, pool := range status.Pools {
		if pool.ID == poolID {
			originalPool = pool
			poolFound = true
			break
		}
	}
	if !poolFound {
		return nil, fmt.Errorf("admin status does not contain pool %d", poolID)
	}
	backends := make([]adminBackend, 0)
	for _, backend := range status.Backends {
		if backend.ModelPoolID == poolID {
			backends = append(backends, backend)
		}
	}
	sort.Slice(backends, func(i, j int) bool { return backends[i].ID < backends[j].ID })
	return &adminMutationCleanup{updater: updater, pool: originalPool, backends: backends}, nil
}

func (cleanup *adminMutationCleanup) Restore(ctx context.Context) error {
	cleanup.once.Do(func() {
		var failures []error
		if _, err := cleanup.updater.updatePool(ctx, cleanup.pool); err != nil {
			failures = append(failures, fmt.Errorf("restore pool %d: %w", cleanup.pool.ID, err))
		}
		for _, backend := range cleanup.backends {
			if _, err := cleanup.updater.updateBackend(ctx, backend); err != nil {
				failures = append(failures, fmt.Errorf("restore backend %d: %w", backend.ID, err))
			}
		}
		cleanup.err = errors.Join(failures...)
	})
	return cleanup.err
}

type loadResult struct {
	Status int
	Err    error
}

type highLoad struct {
	t       *testing.T
	done    chan struct{}
	mu      sync.Mutex
	results []loadResult
}

func (h *remoteHarness) startHighLoad(ctx context.Context) *highLoad {
	h.t.Helper()
	load := &highLoad{t: h.t, done: make(chan struct{})}
	var group sync.WaitGroup
	for _, key := range h.cfg.highLoadKeys {
		for range h.cfg.highRequestsPerKey {
			group.Add(1)
			go func(key string) {
				defer group.Done()
				result := h.highLoadRequest(ctx, key)
				load.mu.Lock()
				load.results = append(load.results, result)
				load.mu.Unlock()
			}(key)
		}
	}
	go func() {
		group.Wait()
		close(load.done)
	}()
	return load
}

func (h *remoteHarness) highLoadRequest(ctx context.Context, key string) loadResult {
	payload, err := json.Marshal(map[string]any{
		"model": h.cfg.model, "prompt": "High-priority e2e saturation workload.",
		"max_tokens": h.cfg.highMaxTokens, "temperature": 0, "ignore_eos": true, "stream": true,
	})
	if err != nil {
		return loadResult{Err: err}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint("/v1/completions"), bytes.NewReader(payload))
	if err != nil {
		return loadResult{Err: err}
	}
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	response, err := h.client.Do(request)
	if err != nil {
		return loadResult{Err: err}
	}
	defer response.Body.Close()
	_, err = io.Copy(io.Discard, response.Body)
	return loadResult{Status: response.StatusCode, Err: err}
}

func (load *highLoad) wait(timeout time.Duration) {
	load.t.Helper()
	select {
	case <-load.done:
	case <-time.After(timeout):
		load.t.Errorf("high load did not stop within %s", timeout)
	}
}

func (load *highLoad) requireNoHTTPFailures(t *testing.T) {
	t.Helper()
	load.mu.Lock()
	defer load.mu.Unlock()
	for _, result := range load.results {
		if result.Status != 0 && result.Status != http.StatusOK {
			t.Errorf("high-load request returned HTTP %d", result.Status)
		}
		if result.Err != nil && !errors.Is(result.Err, context.Canceled) {
			t.Errorf("high-load request failed at transport/body layer: %v", result.Err)
		}
	}
}

func intPointer(value int) *int { return &value }

func bodyExcerpt(body []byte) string {
	const maximum = 512
	trimmed := strings.TrimSpace(string(body))
	if len(trimmed) > maximum {
		return trimmed[:maximum] + "…"
	}
	return trimmed
}
